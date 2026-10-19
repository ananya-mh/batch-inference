#!/usr/bin/env bash
# crash-test.sh: kills the server with SIGKILL in the middle of a batch,
# restarts it, and checks that no work was lost and nothing was saved twice.
#
# Usage:   scripts/crash-test.sh [number of requests]     (default 2000)
# Needs:   Docker with Compose, and curl.
# Options: KILL_AT_PERCENT=10   how far through the batch to kill the server
#
# The fake model runs with its normal flakiness (2% errors, 1% cut-off
# replies), so retries happen too. For this test only, the server rescues
# jobs of a dead worker after 20 seconds instead of 5 minutes; the normal
# settings are restored at the end.

set -euo pipefail

LINES=${1:-2000}
KILL_AT_PERCENT=${KILL_AT_PERCENT:-10}
API=http://localhost:8080
AUTH="Authorization: Bearer sk-dev-tenant-a" # a development tenant from compose.yaml

cd "$(dirname "$0")/.."
WORK=$(mktemp -d)

# Runs Compose with the faster rescue settings layered on top.
compose_fast() {
	docker compose -f compose.yaml -f "$WORK/fast-rescue.yaml" "$@"
}

# Runs one SQL query against the service's database and prints the result.
sql() {
	docker compose exec -T postgres psql -U postgres -d batch -Atc "$1"
}

# Prints a string field from compact JSON on stdin, like "id".
json_string() {
	grep -o "\"$1\":\"[^\"]*\"" | head -1 | cut -d'"' -f4
}

# Prints a number field from compact JSON on stdin, like "completed".
json_number() {
	grep -o "\"$1\":[0-9]*" | head -1 | cut -d: -f2
}

# Prints a step heading.
step() {
	printf '\n==> %s\n' "$1"
}

# On exit, put the server back on its normal settings and remove temp files.
cleanup() {
	step "Restoring the normal server settings"
	docker compose up -d --wait server >/dev/null 2>&1 || echo "warning: could not restart the server"
	rm -rf "$WORK"
}
trap cleanup EXIT

cat >"$WORK/fast-rescue.yaml" <<'EOF'
services:
  server:
    environment:
      JOB_TIMEOUT: 10s
      RESCUE_STUCK_JOBS_AFTER: 20s
EOF

step "Starting the stack (build output goes to a log, shown only on failure)"
if ! compose_fast up -d --build --wait >"$WORK/compose.log" 2>&1; then
	cat "$WORK/compose.log"
	exit 1
fi

step "Uploading $LINES requests and creating a batch"
for i in $(seq 0 $((LINES - 1))); do
	printf '{"custom_id": "ticket-%d", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "fake", "messages": [{"role": "user", "content": "Classify ticket %d: my invoice is wrong"}]}}\n' "$i" "$i"
done >"$WORK/input.jsonl"
FILE_ID=$(curl -sf -H "$AUTH" -F purpose=batch -F "file=@$WORK/input.jsonl" "$API/v1/files" | json_string id)
BATCH_ID=$(curl -sf -H "$AUTH" -H 'Content-Type: application/json' \
	-d "{\"input_file_id\": \"$FILE_ID\", \"endpoint\": \"/v1/chat/completions\", \"completion_window\": \"24h\"}" \
	"$API/v1/batches" | json_string id)
echo "batch $BATCH_ID"

KILL_AT=$((LINES * KILL_AT_PERCENT / 100))
step "Waiting until $KILL_AT requests ($KILL_AT_PERCENT%) are done"
for _ in $(seq 1 600); do
	DONE=$(curl -sf -H "$AUTH" "$API/v1/batches/$BATCH_ID" | json_number completed)
	[ "${DONE:-0}" -ge "$KILL_AT" ] && break
	sleep 0.2
done
echo "$DONE done"

IN_FLIGHT=$(sql "SELECT count(*) FROM requests WHERE batch_id = '$BATCH_ID' AND status IN ('queued', 'running')")
RUNNING=$(sql "SELECT count(*) FROM requests WHERE batch_id = '$BATCH_ID' AND status = 'running'")
step "Killing the server with SIGKILL ($IN_FLIGHT requests in flight, $RUNNING mid-call to the model)"
docker compose kill -s KILL server >/dev/null 2>&1
sleep 2

step "Restarting the server"
compose_fast up -d --wait server >/dev/null 2>&1
RESTARTED_AT=$(date +%s)

step "Waiting for the batch to finish"
STATUS=""
for _ in $(seq 1 600); do
	STATUS=$(curl -sf -H "$AUTH" "$API/v1/batches/$BATCH_ID" | json_string status || true)
	[ "$STATUS" = "completed" ] && break
	sleep 1
done
RECOVERY_SECONDS=$(($(date +%s) - RESTARTED_AT))
echo "status: $STATUS"

step "Checking the database"
RESULTS=$(sql "SELECT count(*) FROM results WHERE batch_id = '$BATCH_ID'")
DISTINCT_RESULTS=$(sql "SELECT count(DISTINCT custom_id) FROM results WHERE batch_id = '$BATCH_ID'")
SUCCEEDED=$(sql "SELECT count(*) FROM results WHERE batch_id = '$BATCH_ID' AND succeeded")
FAILED=$(sql "SELECT count(*) FROM results WHERE batch_id = '$BATCH_ID' AND NOT succeeded")
RESCUED=$(sql "SELECT count(*) FROM river_job WHERE args->>'batch_id' = '$BATCH_ID' AND errors::text LIKE '%rescued%'")
RETRIED=$(sql "SELECT count(*) FROM river_job WHERE args->>'batch_id' = '$BATCH_ID' AND attempt > 1")
OUTPUT_LINES=$(sql "SELECT coalesce((SELECT line_count FROM files f JOIN batches b ON f.id = b.output_file_id WHERE b.id = '$BATCH_ID'), 0)")
ERROR_LINES=$(sql "SELECT coalesce((SELECT line_count FROM files f JOIN batches b ON f.id = b.error_file_id WHERE b.id = '$BATCH_ID'), 0)")
DISTINCT_IN_FILES=$(sql "SELECT count(DISTINCT l.custom_id) FROM file_lines l JOIN batches b ON l.file_id IN (b.output_file_id, b.error_file_id) WHERE b.id = '$BATCH_ID'")

PASS=true
check() { # check <description> <condition result>
	if [ "$2" = "true" ]; then echo "  ok    $1"; else echo "  FAIL  $1"; PASS=false; fi
}
check "batch completed" "$([ "$STATUS" = "completed" ] && echo true)"
check "every request has exactly one result ($RESULTS results, $DISTINCT_RESULTS distinct)" \
	"$([ "$RESULTS" -eq "$LINES" ] && [ "$DISTINCT_RESULTS" -eq "$LINES" ] && echo true)"
check "every custom_id is in exactly one output file ($((OUTPUT_LINES + ERROR_LINES)) lines, $DISTINCT_IN_FILES distinct)" \
	"$([ $((OUTPUT_LINES + ERROR_LINES)) -eq "$LINES" ] && [ "$DISTINCT_IN_FILES" -eq "$LINES" ] && echo true)"
check "the crash really interrupted work ($RUNNING requests were mid-call)" "$([ "$RUNNING" -gt 0 ] && echo true)"

cat <<EOF

==================== crash test summary ====================
 Result:                    $([ "$PASS" = true ] && echo PASS || echo FAIL)
 Requests in batch:         $LINES
 Killed at:                 $DONE done, $IN_FLIGHT in flight ($RUNNING mid-call)
 Jobs rescued by River:     $RESCUED
 Jobs retried (any reason): $RETRIED
 Results saved:             $RESULTS ($DISTINCT_RESULTS distinct custom_ids)
 Succeeded / failed:        $SUCCEEDED / $FAILED
 Results / errors file:     $OUTPUT_LINES / $ERROR_LINES lines
 Finished after restart:    ${RECOVERY_SECONDS}s
=============================================================
EOF

[ "$PASS" = true ]
