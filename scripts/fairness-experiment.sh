#!/usr/bin/env bash
# fairness-experiment.sh: the noisy-neighbor experiment. Tenant A submits one
# big batch; a few seconds later tenants B, C and D each submit a small one.
# Runs with fair scheduling and with first come, first served, several times
# each under identical conditions, and reports how long the small tenants
# wait.
#
# Usage:   scripts/fairness-experiment.sh
# Needs:   Docker with Compose, curl, and awk.
# Options (environment variables):
#   RUNS=5            runs per mode
#   BIG=3000          requests in tenant A's batch (the full experiment: 50000)
#   SMALL=100         requests in each small tenant's batch
#   SMALL_DELAY=5     seconds after the big batch that the small ones arrive
#   SEED=42           fake model seed: same delays, errors and crashes per request
#
# Identical conditions: every run uses the same input files, the same arrival
# timing, and the same fake model seed. The fake model and the server are
# restarted before each run, so the seed's per-request counters start from
# zero. Modes alternate (fair, fifo, fair, ...) so slow drift in the machine
# does not favor one mode. The rest of the settings are compose.yaml's: 16
# workers, a window of 32, and the fake model at 50-500 ms per request.
#
# Output: a results table (median and range per mode), a bar chart, and the
# per-run numbers, printed and saved under results/.

set -euo pipefail

RUNS=${RUNS:-5}
BIG=${BIG:-3000}
SMALL=${SMALL:-100}
SMALL_DELAY=${SMALL_DELAY:-5}
SEED=${SEED:-42}
API=http://localhost:8080
SMALL_TENANTS="b c d"

cd "$(dirname "$0")/.."
WORK=$(mktemp -d)
OUT="results/fairness-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
CSV="$OUT/runs.csv"

# Prints a step heading.
step() {
	printf '\n==> %s\n' "$1"
}

# Runs one SQL query against the service's database and prints the result.
sql() {
	docker compose exec -T postgres psql -U postgres -d batch -Atc "$1"
}

# Prints a string field from compact JSON on stdin, like "id".
json_string() {
	grep -o "\"$1\":\"[^\"]*\"" | head -1 | cut -d'"' -f4
}

# Writes an input file of $2 requests for tenant $1 to stdout. The prompt
# names the tenant, so every request body is unique (the seed relies on it).
input_file() {
	for i in $(seq 0 $(($2 - 1))); do
		printf '{"custom_id": "%s-%d", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "fake", "messages": [{"role": "user", "content": "Tenant %s, ticket %d: please classify"}]}}\n' "$1" "$i" "$1" "$i"
	done
}

# Uploads tenant $1's input file of $2 requests and prints its file ID.
upload() {
	input_file "$1" "$2" >"$WORK/$1.jsonl"
	curl -sf -H "Authorization: Bearer sk-dev-tenant-$1" -F purpose=batch -F "file=@$WORK/$1.jsonl" "$API/v1/files" | json_string id
}

# Creates a batch for tenant $1 from file $2, tagged with run $3 and role $4,
# and prints its ID.
create_batch() {
	curl -sf -H "Authorization: Bearer sk-dev-tenant-$1" -H 'Content-Type: application/json' \
		-d "{\"input_file_id\": \"$2\", \"endpoint\": \"/v1/chat/completions\", \"completion_window\": \"24h\", \"metadata\": {\"experiment\": \"$3\", \"role\": \"$4\"}}" \
		"$API/v1/batches" | json_string id
}

# Restarts the server and the fake model with fair scheduling on ($1 = true)
# or off, and the fixed seed.
restart_stack() {
	cat >"$WORK/settings.yaml" <<EOF
services:
  server:
    environment:
      FAIR_SCHEDULING: "$1"
  fakemodel:
    environment:
      FAKE_MODEL_SEED: "$SEED"
EOF
	docker compose -f compose.yaml -f "$WORK/settings.yaml" up -d --force-recreate --wait server fakemodel >"$WORK/compose.log" 2>&1 ||
		{ cat "$WORK/compose.log"; exit 1; }
}

# On exit, put the server and fake model back on their normal settings.
cleanup() {
	step "Restoring the normal settings"
	docker compose up -d --force-recreate --wait server fakemodel >/dev/null 2>&1 || echo "warning: could not restore the stack"
	rm -rf "$WORK"
}
trap cleanup EXIT

step "Starting the stack (build output goes to a log, shown only on failure)"
docker compose up -d --build --wait >"$WORK/compose.log" 2>&1 || { cat "$WORK/compose.log"; exit 1; }

step "Uploading the input files (reused by every run)"
BIG_FILE=$(upload a "$BIG")
# Each small tenant's file ID goes in a file, since macOS's bash 3.2 has no
# associative arrays.
for tenant in $SMALL_TENANTS; do
	upload "$tenant" "$SMALL" >"$WORK/$tenant.file"
done
echo "tenant a: $BIG requests; tenants $SMALL_TENANTS: $SMALL each; small ones arrive ${SMALL_DELAY}s later"

echo "mode,run,small_completion_s,small_wait_p50_s,small_wait_p99_s,small_first_slot_s,big_completion_s,throughput_rps,arrival_gap_s,retried_jobs" >"$CSV"

for run in $(seq 1 "$RUNS"); do
	for mode in fair fifo; do
		TAG="$mode-$run-$(date +%s)"
		step "Run $run of $RUNS, $mode"
		restart_stack "$([ "$mode" = fair ] && echo true || echo false)"

		BATCHES=$(create_batch a "$BIG_FILE" "$TAG" big)
		sleep "$SMALL_DELAY"
		for tenant in $SMALL_TENANTS; do
			create_batch "$tenant" "$(cat "$WORK/$tenant.file")" "$TAG" small >"$WORK/$tenant.id" &
		done
		wait
		for tenant in $SMALL_TENANTS; do
			BATCHES="$BATCHES $(cat "$WORK/$tenant.id")"
		done

		# Wait until all four batches are completed.
		for _ in $(seq 1 3600); do
			LEFT=$(sql "SELECT count(*) FROM batches WHERE metadata->>'experiment' = '$TAG' AND status <> 'completed'")
			[ "$LEFT" -eq 0 ] && break
			sleep 1
		done
		[ "$LEFT" -eq 0 ] || { echo "run $TAG did not finish"; exit 1; }

		# All metrics in seconds, from the database's own timestamps.
		ROW=$(sql "
			WITH b AS (SELECT * FROM batches WHERE metadata->>'experiment' = '$TAG'),
			     small AS (SELECT * FROM b WHERE metadata->>'role' = 'small'),
			     big AS (SELECT * FROM b WHERE metadata->>'role' = 'big'),
			     small_requests AS (
				SELECT extract(epoch FROM r.started_at - s.created_at) AS wait
				FROM requests r JOIN small s ON s.id = r.batch_id)
			SELECT concat_ws(',',
				round((SELECT max(extract(epoch FROM completed_at - created_at)) FROM small)::numeric, 2),
				round((SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY wait) FROM small_requests)::numeric, 2),
				round((SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY wait) FROM small_requests)::numeric, 2),
				round((SELECT max(first_slot) FROM (
					SELECT extract(epoch FROM min(r.queued_at) - s.created_at) AS first_slot
					FROM requests r JOIN small s ON s.id = r.batch_id GROUP BY s.id, s.created_at) f)::numeric, 2),
				round((SELECT extract(epoch FROM completed_at - created_at) FROM big)::numeric, 2),
				round(((SELECT sum(request_total) FROM b) /
					extract(epoch FROM (SELECT max(completed_at) FROM b) - (SELECT min(created_at) FROM b)))::numeric, 1),
				round((SELECT extract(epoch FROM min(s.created_at) - (SELECT created_at FROM big)) FROM small s)::numeric, 2),
				(SELECT count(*) FROM river_job WHERE args->>'batch_id' IN (SELECT id FROM b) AND attempt > 1))")
		echo "$mode,$run,$ROW" >>"$CSV"
		echo "$ROW" | awk -F, '{printf "small tenants done after %ss (per-request wait p50 %ss, p99 %ss; first slot after %ss); big done after %ss; %s req/s; %s jobs retried\n", $1, $2, $3, $4, $5, $6, $8}'
	done
done

# Summarizes one CSV column ($1) for one mode ($2) as "median [min-max]".
summarize() {
	awk -F, -v col="$1" -v mode="$2" 'NR > 1 && $1 == mode { print $col }' "$CSV" | sort -g |
		awk '{ v[NR] = $1 } END {
			median = (NR % 2) ? v[(NR + 1) / 2] : (v[NR / 2] + v[NR / 2 + 1]) / 2
			printf "%g [%g–%g]", median, v[1], v[NR]
		}'
}

# Prints the median of one CSV column ($1) for one mode ($2).
median() {
	awk -F, -v col="$1" -v mode="$2" 'NR > 1 && $1 == mode { print $col }' "$CSV" | sort -g |
		awk '{ v[NR] = $1 } END { print (NR % 2) ? v[(NR + 1) / 2] : (v[NR / 2] + v[NR / 2 + 1]) / 2 }'
}

# Prints a horizontal bar of $1 out of a maximum of $2, 40 characters wide.
bar() {
	awk -v value="$1" -v max="$2" 'BEGIN { n = (max > 0) ? int(value / max * 40 + 0.5) : 0; if (n < 1 && value > 0) n = 1; s = ""; for (i = 0; i < n; i++) s = s "#"; print s }'
}

{
	echo "## Fairness experiment"
	echo
	echo "Command: \`RUNS=$RUNS BIG=$BIG SMALL=$SMALL SMALL_DELAY=$SMALL_DELAY SEED=$SEED scripts/fairness-experiment.sh\`"
	echo
	echo "Tenant A: one batch of $BIG requests. Tenants B, C, D: one batch of $SMALL requests each, arriving ${SMALL_DELAY}s later."
	echo "$RUNS runs per mode, same seed and arrival timing; values are median [min–max] over the runs, in seconds unless noted."
	echo
	echo "| Metric | Fair scheduling | First come, first served |"
	echo "|---|---|---|"
	echo "| Small tenants: all done (slowest of B, C, D) | $(summarize 3 fair) | $(summarize 3 fifo) |"
	echo "| Small tenants: per-request wait, p50 | $(summarize 4 fair) | $(summarize 4 fifo) |"
	echo "| Small tenants: per-request wait, p99 | $(summarize 5 fair) | $(summarize 5 fifo) |"
	echo "| Small tenants: wait for first slot (slowest) | $(summarize 6 fair) | $(summarize 6 fifo) |"
	echo "| Big tenant: done | $(summarize 7 fair) | $(summarize 7 fifo) |"
	echo "| Total throughput (requests/s) | $(summarize 8 fair) | $(summarize 8 fifo) |"
	echo
	echo "Per-request wait = from the small batch's creation until that request's model call started."
	echo "Jobs retried per run (same seed, so this should match across runs): $(awk -F, 'NR > 1 { printf "%s%s", sep, $10; sep = ", " }' "$CSV")."
	echo
	MAX=$(awk -F, 'NR > 1 { if ($7 > m) m = $7; if ($3 > m) m = $3 } END { print m }' "$CSV")
	echo '```'
	echo "Median time until done (seconds)"
	printf "small, fair  %7.1f  %s\n" "$(median 3 fair)" "$(bar "$(median 3 fair)" "$MAX")"
	printf "small, fifo  %7.1f  %s\n" "$(median 3 fifo)" "$(bar "$(median 3 fifo)" "$MAX")"
	printf "big,   fair  %7.1f  %s\n" "$(median 7 fair)" "$(bar "$(median 7 fair)" "$MAX")"
	printf "big,   fifo  %7.1f  %s\n" "$(median 7 fifo)" "$(bar "$(median 7 fifo)" "$MAX")"
	echo '```'
} >"$OUT/results.md"

step "Results (also saved to $OUT/results.md and $OUT/runs.csv)"
cat "$OUT/results.md"
