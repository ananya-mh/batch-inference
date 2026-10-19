#!/usr/bin/env bash
# measure.sh: measures the service end to end and writes a results table,
# with the machine and commit the numbers came from.
#
# Usage:   scripts/measure.sh [section ...]       (default: all sections)
# Sections:
#   upload     upload a ~92 MB, 440,000-line file 3 times: time and peak server memory
#   status     a 50,000-line batch: status request latency while it runs and after,
#              and the time to write its results file; 3 batches
#   cancel     cancel a 2,000-line batch with the window full: time until cancelled; 10 runs
#   expiry     batches with a 10 s window: time from deadline to expired; 5 batches
#   overdue    crash-test.sh --expiry: time from server start to the overdue batch
#              being expired; 3 runs
# Needs:   Docker with Compose, curl, awk. Refuses to run with uncommitted
#          changes to the code, so the numbers always belong to a commit.
#
# Each section restarts the server and fake model with the settings it needs
# and puts compose.yaml's settings back at the end. Output goes to
# results/measurements-<time>/ (results.md, plus raw numbers per section).
#
# Keep the machine awake while it runs: on a laptop, closing the lid pauses
# Docker and wrecks the timings. On macOS, run it as
# caffeinate -i scripts/measure.sh (and keep the lid open).

set -euo pipefail

SECTIONS=${*:-upload status cancel expiry overdue}
API=http://localhost:8080
AUTH="Authorization: Bearer sk-dev-tenant-c" # a development tenant from compose.yaml

cd "$(dirname "$0")/.."
WORK=$(mktemp -d)
OUT="results/measurements-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
ROWS="$OUT/rows.md"
: >"$ROWS"

# Prints a step heading.
step() {
	printf '\n==> %s\n' "$1"
}

# Runs one SQL query against the service's database and prints the result.
# Its input is /dev/null: "docker compose exec" would otherwise read the
# caller's input, which inside a "while read" loop swallows the loop's lines.
sql() {
	docker compose exec -T postgres psql -U postgres -d batch -Atc "$1" </dev/null
}

# Prints a string field from compact JSON on stdin, like "id".
json_string() {
	grep -o "\"$1\":\"[^\"]*\"" | head -1 | cut -d'"' -f4
}

# Prints a number field from compact JSON on stdin, like "completed".
json_number() {
	grep -o "\"$1\":[0-9]*" | head -1 | cut -d: -f2
}

# Prints the value of one metric line from the server's /metrics, like
# "process_resident_memory_bytes" (0 if absent).
metric() {
	curl -sf "$API/metrics" | awk -v name="$1" '$1 == name { print $2; found = 1 } END { if (!found) print 0 }'
}

# Prints "p50 p90 p99 max" (in ms) of the seconds on stdin, one per line.
percentiles_ms() {
	sort -g | awk '{ v[NR] = $1 * 1000 } END {
		printf "%.1f %.1f %.1f %.1f", v[int(NR * 0.5 + 0.5)], v[int(NR * 0.9 + 0.5)], v[int(NR * 0.99 + 0.5)], v[NR] }'
}

# Prints "median min max" of the numbers on stdin, one per line.
median_range() {
	sort -g | awk '{ v[NR] = $1 } END {
		m = (NR % 2) ? v[(NR + 1) / 2] : (v[NR / 2] + v[NR / 2 + 1]) / 2
		printf "%g %g %g", m, v[1], v[NR] }'
}

# Adds a row to the results table.
row() { # row <measurement> <result> <based on> <section>
	# shellcheck disable=SC2016 # the backticks are Markdown, not a command
	printf '| %s | %s | %s | `scripts/measure.sh %s` |\n' "$1" "$2" "$3" "$4" >>"$ROWS"
}

# Restarts the server and fake model with extra environment settings, given
# as "service:NAME=value" words.
restart_with() {
	{
		echo "services:"
		for service in server fakemodel; do
			echo "  $service:"
			echo "    environment:"
			for setting in "$@"; do
				case "$setting" in "$service:"*) echo "      ${setting#*:}" | sed 's/=/: "/; s/$/"/' ;; esac
			done
			echo "      _MEASURE: \"1\"" # keeps the environment block valid when empty
		done
	} >"$WORK/settings.yaml"
	docker compose -f compose.yaml -f "$WORK/settings.yaml" up -d --force-recreate --wait server fakemodel >"$WORK/compose.log" 2>&1 ||
		{ cat "$WORK/compose.log"; exit 1; }
}

# Writes an input file of $1 requests to $2.
input_file() {
	awk -v n="$1" 'BEGIN { for (i = 0; i < n; i++)
		printf "{\"custom_id\": \"m-%d\", \"method\": \"POST\", \"url\": \"/v1/chat/completions\", \"body\": {\"model\": \"fake\", \"messages\": [{\"role\": \"user\", \"content\": \"Classify ticket %d: my invoice shows the wrong amount\"}]}}\n", i, i }' >"$2"
}

# Uploads file $1 and prints its file ID.
upload() {
	curl -sf -H "$AUTH" -F purpose=batch -F "file=@$1" "$API/v1/files" | json_string id
}

# Creates a batch from file $1 with completion window $2 and prints its ID.
create_batch() {
	curl -sf -H "$AUTH" -H 'Content-Type: application/json' \
		-d "{\"input_file_id\": \"$1\", \"endpoint\": \"/v1/chat/completions\", \"completion_window\": \"$2\"}" \
		"$API/v1/batches" | json_string id
}

# Prints the status of batch $1.
batch_status() {
	curl -sf -H "$AUTH" "$API/v1/batches/$1" | json_string status
}

# Waits until batch $1 reaches status $2.
wait_for() {
	for _ in $(seq 1 6000); do
		[ "$(batch_status "$1")" = "$2" ] && return 0
		sleep 0.1
	done
	echo "batch $1 never reached $2"
	exit 1
}

# On exit, put the server and fake model back on compose.yaml's settings.
cleanup() {
	step "Restoring the normal settings"
	docker compose up -d --force-recreate --wait server fakemodel >/dev/null 2>&1 || echo "warning: could not restore the stack"
	rm -rf "$WORK"
}
trap cleanup EXIT

if ! git diff --quiet HEAD -- . ':!README.md' ':!docs'; then
	echo "There are uncommitted code changes; commit them first so the numbers belong to a commit."
	exit 1
fi

step "Starting the stack"
docker compose up -d --build --wait >"$WORK/compose.log" 2>&1 || { cat "$WORK/compose.log"; exit 1; }

# ----------------------------------------------------------------------------
if echo "$SECTIONS" | grep -qw upload; then
	step "upload: a ~92 MB, 440,000-line file, 3 times"
	restart_with
	input_file 440000 "$WORK/big.jsonl"
	SIZE_MB=$(awk -v b="$(wc -c <"$WORK/big.jsonl")" 'BEGIN { printf "%.1f", b / 1e6 }')
	for i in 1 2 3; do
		BASELINE=$(metric process_resident_memory_bytes)
		# Sample the server's memory every 0.1 s while the upload runs.
		: >"$WORK/memory.txt"
		( while [ ! -f "$WORK/stop" ]; do metric process_resident_memory_bytes >>"$WORK/memory.txt"; sleep 0.1; done ) &
		SAMPLER=$!
		TIME=$(curl -sf -o /dev/null -w '%{time_total}' -H "$AUTH" -F purpose=batch -F "file=@$WORK/big.jsonl" "$API/v1/files")
		touch "$WORK/stop"
		wait "$SAMPLER"
		rm -f "$WORK/stop"
		PEAK=$(sort -g "$WORK/memory.txt" | tail -1)
		echo "upload $i: ${TIME}s, server memory $(awk -v b="$BASELINE" 'BEGIN { printf "%.0f", b / 1048576 }') MiB before, peak $(awk -v p="$PEAK" 'BEGIN { printf "%.0f", p / 1048576 }') MiB" | tee -a "$OUT/upload.txt"
		echo "$TIME $PEAK" >>"$WORK/uploads.txt"
	done
	read -r T_MED T_MIN T_MAX <<<"$(awk '{ printf "%.2f\n", $1 }' "$WORK/uploads.txt" | median_range)"
	PEAK_MAX=$(awk '{ print $2 }' "$WORK/uploads.txt" | sort -g | tail -1)
	row "Upload a ${SIZE_MB} MB, 440,000-line file (checked line by line as it streams in)" \
		"${T_MED} s median [${T_MIN}–${T_MAX}], $(awk -v s="$SIZE_MB" -v t="$T_MED" 'BEGIN { printf "%.0f", s / t }') MB/s; server memory peaked at $(awk -v p="$PEAK_MAX" 'BEGIN { printf "%.0f", p / 1048576 }') MiB" \
		"3 uploads" upload
fi

# ----------------------------------------------------------------------------
if echo "$SECTIONS" | grep -qw status; then
	step "status: 50,000-line batches with a near-instant model, 3 times"
	# Status latency and file writing depend on the batch's size, not the
	# model's speed, so a near-instant model keeps this section short.
	restart_with fakemodel:FAKE_MODEL_MIN_DELAY=1ms fakemodel:FAKE_MODEL_MAX_DELAY=5ms \
		fakemodel:FAKE_MODEL_ERROR_RATE=0 fakemodel:FAKE_MODEL_CRASH_RATE=0 server:RIVER_FETCH_COOLDOWN=10ms
	input_file 50000 "$WORK/fifty.jsonl"
	FILE=$(upload "$WORK/fifty.jsonl")
	: >"$WORK/during.txt"
	: >"$WORK/after.txt"
	: >"$WORK/writes.txt"
	for i in 1 2 3; do
		WRITE_SUM_BEFORE=$(metric batch_results_files_write_seconds_sum)
		BATCH=$(create_batch "$FILE" 24h)
		wait_for "$BATCH" in_progress
		for _ in $(seq 1 200); do
			curl -sf -o /dev/null -w '%{time_total}\n' -H "$AUTH" "$API/v1/batches/$BATCH" >>"$WORK/during.txt"
			sleep 0.02
		done
		wait_for "$BATCH" completed
		for _ in $(seq 1 200); do
			curl -sf -o /dev/null -w '%{time_total}\n' -H "$AUTH" "$API/v1/batches/$BATCH" >>"$WORK/after.txt"
		done
		WRITE=$(awk -v a="$(metric batch_results_files_write_seconds_sum)" -v b="$WRITE_SUM_BEFORE" 'BEGIN { printf "%.3f", a - b }')
		echo "$WRITE" >>"$WORK/writes.txt"
		echo "batch $i: files written in ${WRITE}s" | tee -a "$OUT/status.txt"
	done
	cp "$WORK/during.txt" "$OUT/status-latency-during.txt"
	cp "$WORK/after.txt" "$OUT/status-latency-after.txt"
	read -r D50 D90 D99 DMAX <<<"$(percentiles_ms <"$WORK/during.txt")"
	read -r A50 A90 A99 AMAX <<<"$(percentiles_ms <"$WORK/after.txt")"
	read -r W_MED W_MIN W_MAX <<<"$(median_range <"$WORK/writes.txt")"
	row "Batch status (\`GET /v1/batches/{id}\`) on a 50,000-request batch, while it runs" \
		"p50 ${D50} ms, p90 ${D90} ms, p99 ${D99} ms, max ${DMAX} ms" "600 requests (200 per batch, 3 batches)" status
	row "Batch status on a 50,000-request batch, after it completes" \
		"p50 ${A50} ms, p90 ${A90} ms, p99 ${A99} ms, max ${AMAX} ms" "600 requests (200 per batch, 3 batches)" status
	row "Write the results file of a completed 50,000-request batch (inside Postgres)" \
		"${W_MED} s median [${W_MIN}–${W_MAX}]" "3 batches" status
fi

# ----------------------------------------------------------------------------
if echo "$SECTIONS" | grep -qw cancel; then
	step "cancel: 2,000-line batch, window full, 10 times"
	restart_with
	input_file 2000 "$WORK/two-thousand.jsonl"
	FILE=$(upload "$WORK/two-thousand.jsonl")
	: >"$WORK/cancel.txt"
	for i in $(seq 1 10); do
		BATCH=$(create_batch "$FILE" 24h)
		# Wait until 100 are done, so the window of 32 is full.
		for _ in $(seq 1 600); do
			DONE=$(curl -sf -H "$AUTH" "$API/v1/batches/$BATCH" | json_number completed)
			[ "${DONE:-0}" -ge 100 ] && break
			sleep 0.05
		done
		CALL=$(curl -sf -o /dev/null -w '%{time_total}' -X POST -H "$AUTH" "$API/v1/batches/$BATCH/cancel")
		wait_for "$BATCH" cancelled
		read -r TOOK RUNNING <<<"$(sql "SELECT round(extract(epoch FROM cancelled_at - cancelling_at)::numeric, 3),
			(SELECT count(*) FROM requests r WHERE r.batch_id = b.id AND r.started_at < b.cancelling_at
			   AND r.finished_at >= b.cancelling_at AND r.status IN ('succeeded', 'failed'))
			FROM batches b WHERE id = '$BATCH'" | tr '|' ' ')"
		echo "run $i: cancel call ${CALL}s, cancelled ${TOOK}s after cancelling, $RUNNING were mid-call" | tee -a "$OUT/cancel.txt"
		echo "$TOOK $CALL $RUNNING" >>"$WORK/cancel.txt"
	done
	read -r C_MED C_MIN C_MAX <<<"$(awk '{ printf "%.2f\n", $1 }' "$WORK/cancel.txt" | median_range)"
	read -r CALL_MED _ _ <<<"$(awk '{ print $2 }' "$WORK/cancel.txt" | median_range)"
	read -r R_MED R_MIN R_MAX <<<"$(awk '{ print $3 }' "$WORK/cancel.txt" | median_range)"
	row "Cancel → \`cancelled\`, window of 32 full, fake model 50–500 ms" \
		"${C_MED} s median [${C_MIN}–${C_MAX}]; the cancel call itself returns in $(awk -v c="$CALL_MED" 'BEGIN { printf "%.0f", c * 1000 }') ms; ${R_MED} [${R_MIN}–${R_MAX}] requests were mid-call" \
		"10 cancels" cancel
fi

# ----------------------------------------------------------------------------
if echo "$SECTIONS" | grep -qw expiry; then
	step "expiry: 2,000-line batches with a 10 s window, 5 times"
	restart_with server:COMPLETION_WINDOWS=24h,10s
	input_file 2000 "$WORK/expiring.jsonl"
	FILE=$(upload "$WORK/expiring.jsonl")
	: >"$WORK/expiry.txt"
	for i in 1 2 3 4 5; do
		BATCH=$(create_batch "$FILE" 10s)
		wait_for "$BATCH" expired
		TOOK=$(sql "SELECT round(extract(epoch FROM expired_at - expires_at)::numeric * 1000, 1) FROM batches WHERE id = '$BATCH'")
		echo "batch $i: expired ${TOOK} ms after its deadline" | tee -a "$OUT/expiry.txt"
		echo "$TOOK" >>"$WORK/expiry.txt"
	done
	read -r E_MED E_MIN E_MAX <<<"$(median_range <"$WORK/expiry.txt")"
	row "Deadline → \`expired\` (server running), 2,000-request batch with a 10 s window" \
		"${E_MED} ms median [${E_MIN}–${E_MAX}]" "5 batches" expiry
fi

# ----------------------------------------------------------------------------
if echo "$SECTIONS" | grep -qw overdue; then
	step "overdue: crash-test.sh --expiry, 3 times"
	: >"$WORK/overdue.txt"
	for i in 1 2 3; do
		scripts/crash-test.sh --expiry 500 >"$WORK/crash.out" 2>&1 || { cat "$WORK/crash.out"; exit 1; }
		cp "$WORK/crash.out" "$OUT/overdue-run-$i.txt"
		# The crash test measures from the restarted server's own start (its
		# cleanup restarts the server again, so it can't be read afterwards).
		TOOK=$(awk -F': *' '/Expired after server start:/ { sub(/s$/, "", $2); print $2 }' "$WORK/crash.out")
		echo "run $i: the overdue batch expired ${TOOK}s after the restarted server started" | tee -a "$OUT/overdue.txt"
		echo "$TOOK" >>"$WORK/overdue.txt"
	done
	read -r O_MED O_MIN O_MAX <<<"$(median_range <"$WORK/overdue.txt")"
	row "Overdue batch after a restart (server killed mid-batch, down until 10 s past the deadline): server start → \`expired\`" \
		"${O_MED} s median [${O_MIN}–${O_MAX}]" "3 crash tests, 500 requests each" overdue
fi

# ----------------------------------------------------------------------------
step "Machine"
CHIP=$( (sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2) | sed 's/^ *//')
CORES=$( (sysctl -n hw.ncpu 2>/dev/null || nproc) )
RAM_GB=$( (sysctl -n hw.memsize 2>/dev/null || awk '/MemTotal/ { print $2 * 1024 }' /proc/meminfo) | awk '{ printf "%.0f", $1 / 1073741824 }')
if command -v sw_vers >/dev/null; then
	OS="$(sw_vers -productName) $(sw_vers -productVersion)"
else
	OS=$(uname -sr)
fi
DOCKER_CPUS=$(docker info --format '{{.NCPU}}')
DOCKER_MEM_GB=$(docker info --format '{{.MemTotal}}' | awk '{ printf "%.1f", $1 / 1073741824 }')
DOCKER_VERSION=$(docker info --format '{{.ServerVersion}}')
POSTGRES=$(sql "SHOW server_version")
GO_VERSION=$(curl -sf "$API/metrics" | grep -o 'go_info{version="[^"]*"' | cut -d'"' -f2)
COMMIT=$(git rev-parse --short HEAD)

{
	echo "## Measurements"
	echo
	echo "Machine: $CHIP ($CORES cores), $RAM_GB GB RAM, $OS. Docker $DOCKER_VERSION with $DOCKER_CPUS CPUs and $DOCKER_MEM_GB GB for its VM (everything below runs inside it). Postgres $POSTGRES, server built with $GO_VERSION. Commit \`$COMMIT\`. Run on $(date -u +%Y-%m-%d)."
	echo
	echo "Settings are compose.yaml's (16 workers, window of 32, fake model at 50–500 ms with 2% errors and 1% cut-off replies) unless a row says otherwise. The \`status\` rows use a near-instant fake model (1–5 ms, no errors) and \`RIVER_FETCH_COOLDOWN=10ms\` so 50,000-request batches finish in minutes; status latency and file writing depend on batch size, not model speed."
	echo
	echo "| Measurement | Result | Based on | Command |"
	echo "|---|---|---|---|"
	cat "$ROWS"
} >"$OUT/results.md"

step "Results (also saved to $OUT/results.md)"
cat "$OUT/results.md"
