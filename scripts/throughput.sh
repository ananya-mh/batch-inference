#!/usr/bin/env bash
# throughput.sh: measures the most requests per second the service can push
# through, with a near-instant fake model, under different River fetch
# settings, worker counts, and window sizes.
#
# Usage:   scripts/throughput.sh
# Needs:   Docker with Compose, curl, awk.
# Options: REQUESTS=10000   requests per batch
#          RUNS=3           batches per setting
#
# The fake model answers in 1-5 ms with no errors, so the model is never
# the limit and what is measured is the service itself. Throughput is a
# batch's requests divided by the time from in_progress to completed. Each
# setting restarts the server; compose.yaml's settings are restored at the
# end. Output goes to results/throughput-<time>/.

set -euo pipefail

REQUESTS=${REQUESTS:-10000}
RUNS=${RUNS:-3}
API=http://localhost:8080
AUTH="Authorization: Bearer sk-dev-tenant-d" # a development tenant from compose.yaml

# Settings to compare: "label|RIVER_FETCH_COOLDOWN|WORKERS|SCHEDULER_WINDOW".
SETTINGS="
defaults (River's 100 ms fetch cooldown)|100ms|16|32
fetch cooldown 50 ms|50ms|16|32
fetch cooldown 10 ms|10ms|16|32
fetch cooldown 1 ms|1ms|16|32
fetch cooldown 1 ms, window 128|1ms|16|128
fetch cooldown 1 ms, 64 workers, window 128|1ms|64|128
"

cd "$(dirname "$0")/.."
WORK=$(mktemp -d)
OUT="results/throughput-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"

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

# Prints the sum of all lines of one metric from /metrics, like
# "batch_workers_busy" (0 if absent).
metric() {
	curl -sf "$API/metrics" | awk -v name="$1" '$1 == name { sum += $2 } END { print sum + 0 }'
}

# Prints "median min max" of the numbers on stdin, one per line.
median_range() {
	sort -g | awk '{ v[NR] = $1 } END {
		m = (NR % 2) ? v[(NR + 1) / 2] : (v[NR / 2] + v[NR / 2 + 1]) / 2
		printf "%g %g %g", m, v[1], v[NR] }'
}

# On exit, put the server and fake model back on compose.yaml's settings.
cleanup() {
	step "Restoring the normal settings"
	docker compose up -d --force-recreate --wait server fakemodel >/dev/null 2>&1 || echo "warning: could not restore the stack"
	rm -rf "$WORK"
}
trap cleanup EXIT

step "Starting the stack"
docker compose up -d --build --wait >"$WORK/compose.log" 2>&1 || { cat "$WORK/compose.log"; exit 1; }

awk -v n="$REQUESTS" 'BEGIN { for (i = 0; i < n; i++)
	printf "{\"custom_id\": \"t-%d\", \"method\": \"POST\", \"url\": \"/v1/chat/completions\", \"body\": {\"model\": \"fake\", \"messages\": [{\"role\": \"user\", \"content\": \"ticket %d\"}]}}\n", i, i }' >"$WORK/input.jsonl"
FILE=$(curl -sf -H "$AUTH" -F purpose=batch -F "file=@$WORK/input.jsonl" "$API/v1/files" | json_string id)

echo "setting,run,requests_per_s,busy_workers_mean" >"$OUT/runs.csv"
: >"$WORK/table.md"

echo "$SETTINGS" | while IFS='|' read -r LABEL COOLDOWN WORKERS WINDOW; do
	[ -n "$LABEL" ] || continue
	step "$LABEL"
	cat >"$WORK/settings.yaml" <<EOF
services:
  server:
    environment:
      RIVER_FETCH_COOLDOWN: "$COOLDOWN"
      WORKERS: "$WORKERS"
      SCHEDULER_WINDOW: "$WINDOW"
  fakemodel:
    environment:
      FAKE_MODEL_MIN_DELAY: 1ms
      FAKE_MODEL_MAX_DELAY: 5ms
      FAKE_MODEL_ERROR_RATE: "0"
      FAKE_MODEL_CRASH_RATE: "0"
EOF
	docker compose -f compose.yaml -f "$WORK/settings.yaml" up -d --force-recreate --wait server fakemodel >"$WORK/compose.log" 2>&1 ||
		{ cat "$WORK/compose.log"; exit 1; }
	: >"$WORK/rates.txt"
	: >"$WORK/busy.txt"
	for run in $(seq 1 "$RUNS"); do
		BATCH=$(curl -sf -H "$AUTH" -H 'Content-Type: application/json' \
			-d "{\"input_file_id\": \"$FILE\", \"endpoint\": \"/v1/chat/completions\", \"completion_window\": \"24h\"}" \
			"$API/v1/batches" | json_string id)
		# Sample busy workers every 0.2 s until the batch is done.
		: >"$WORK/busy-run.txt"
		while [ "$(sql "SELECT status FROM batches WHERE id = '$BATCH'")" != completed ]; do
			metric batch_workers_busy >>"$WORK/busy-run.txt"
			sleep 0.2
		done
		RATE=$(sql "SELECT round(request_total / extract(epoch FROM completed_at - in_progress_at)::numeric, 0) FROM batches WHERE id = '$BATCH'")
		BUSY=$(awk '{ s += $1 } END { printf "%.1f", (NR ? s / NR : 0) }' "$WORK/busy-run.txt")
		echo "run $run: $RATE requests/s, $BUSY of $WORKERS workers busy on average"
		echo "$LABEL,$run,$RATE,$BUSY" >>"$OUT/runs.csv"
		echo "$RATE" >>"$WORK/rates.txt"
		echo "$BUSY" >>"$WORK/busy.txt"
	done
	read -r R_MED R_MIN R_MAX <<<"$(median_range <"$WORK/rates.txt")"
	read -r B_MED _ _ <<<"$(median_range <"$WORK/busy.txt")"
	echo "| $LABEL | $COOLDOWN | $WORKERS | $WINDOW | $R_MED [$R_MIN–$R_MAX] | $B_MED of $WORKERS |" >>"$WORK/table.md"
done

{
	echo "## Throughput"
	echo
	echo "Command: \`REQUESTS=$REQUESTS RUNS=$RUNS scripts/throughput.sh\`, commit \`$(git rev-parse --short HEAD)\`, run on $(date -u +%Y-%m-%d)."
	echo "Near-instant fake model (1–5 ms, no errors), one batch of $REQUESTS requests at a time, $RUNS batches per setting; median [min–max]."
	echo
	echo "| Setting | RIVER_FETCH_COOLDOWN | WORKERS | SCHEDULER_WINDOW | Requests/s | Busy workers (average) |"
	echo "|---|---|---|---|---|---|"
	cat "$WORK/table.md"
} >"$OUT/results.md"

step "Results (also saved to $OUT/results.md)"
cat "$OUT/results.md"
