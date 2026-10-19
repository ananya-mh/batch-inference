#!/usr/bin/env bash
# crash-test.sh: kills the server with SIGKILL in the middle of a batch,
# restarts it, and checks that no work was lost and nothing was saved twice.
#
# Usage:   scripts/crash-test.sh [--expiry] [number of requests]   (default 2000)
# Needs:   Docker with Compose, and curl.
# Options: KILL_AT_PERCENT=10   how far through the batch to kill the server
#
# Default mode: restart straight away; the batch must complete with exactly
# one result per request.
#
# --expiry: the batch has a 30-second completion window, and the server stays
# down until 10 seconds after the deadline. After the restart, the overdue
# batch must expire promptly: results saved before the deadline are kept,
# the rest are expired, and nothing saved after the deadline is kept.
#
# The fake model runs with its normal flakiness (2% errors, 1% cut-off
# replies), so retries happen too. For this test only, the server rescues
# jobs of a dead worker after 20 seconds instead of 5 minutes (and allows the
# 30-second window); the normal settings are restored at the end.

set -euo pipefail

MODE=normal
if [ "${1:-}" = "--expiry" ]; then
	MODE=expiry
	shift
fi
LINES=${1:-2000}
KILL_AT_PERCENT=${KILL_AT_PERCENT:-10}
WINDOW_SECONDS=30          # --expiry only
DOWN_AFTER_DEADLINE=10     # --expiry only: seconds the server stays down past the deadline
API=http://localhost:8080
AUTH="Authorization: Bearer sk-dev-tenant-a" # a development tenant from compose.yaml

cd "$(dirname "$0")/.."
WORK=$(mktemp -d)

# Runs Compose with this test's settings layered on top.
compose_test() {
	docker compose -f compose.yaml -f "$WORK/test-settings.yaml" "$@"
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

# Prints a step heading.
step() {
	printf '\n==> %s\n' "$1"
}

# Prints the batch's current status.
batch_status() {
	curl -sf -H "$AUTH" "$API/v1/batches/$BATCH_ID" | json_string status || true
}

# On exit, put the server back on its normal settings and remove temp files.
cleanup() {
	step "Restoring the normal server settings"
	docker compose up -d --wait server >/dev/null 2>&1 || echo "warning: could not restart the server"
	rm -rf "$WORK"
}
trap cleanup EXIT

cat >"$WORK/test-settings.yaml" <<EOF
services:
  server:
    environment:
      JOB_TIMEOUT: 10s
      RESCUE_STUCK_JOBS_AFTER: 20s
      COMPLETION_WINDOWS: "24h,${WINDOW_SECONDS}s"
EOF

step "Starting the stack (build output goes to a log, shown only on failure)"
if ! compose_test up -d --build --wait >"$WORK/compose.log" 2>&1; then
	cat "$WORK/compose.log"
	exit 1
fi

WINDOW=24h
[ "$MODE" = expiry ] && WINDOW="${WINDOW_SECONDS}s"
step "Uploading $LINES requests and creating a batch (completion window $WINDOW)"
for i in $(seq 0 $((LINES - 1))); do
	printf '{"custom_id": "ticket-%d", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "fake", "messages": [{"role": "user", "content": "Classify ticket %d: my invoice is wrong"}]}}\n' "$i" "$i"
done >"$WORK/input.jsonl"
FILE_ID=$(curl -sf -H "$AUTH" -F purpose=batch -F "file=@$WORK/input.jsonl" "$API/v1/files" | json_string id)
BATCH_JSON=$(curl -sf -H "$AUTH" -H 'Content-Type: application/json' \
	-d "{\"input_file_id\": \"$FILE_ID\", \"endpoint\": \"/v1/chat/completions\", \"completion_window\": \"$WINDOW\"}" \
	"$API/v1/batches")
BATCH_ID=$(echo "$BATCH_JSON" | json_string id)
EXPIRES_AT=$(echo "$BATCH_JSON" | json_number expires_at)
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

if [ "$MODE" = expiry ]; then
	WAIT=$((EXPIRES_AT + DOWN_AFTER_DEADLINE - $(date +%s)))
	step "Keeping the server down until ${DOWN_AFTER_DEADLINE}s after the deadline (${WAIT}s)"
	[ "$WAIT" -gt 0 ] && sleep "$WAIT"
	TARGET=expired
else
	sleep 2
	TARGET=completed
fi

step "Restarting the server"
compose_test up -d --wait server >/dev/null 2>&1
RESTARTED_AT=$(date +%s)
# When the restarted server process started, by the Docker VM's clock, which
# the database shares. Read now: cleanup restarts the server again at the end.
SERVER_STARTED=$(curl -sf "$API/metrics" | awk '$1 == "process_start_time_seconds" { print $2 }')

step "Waiting for the batch to be $TARGET"
STATUS=""
for _ in $(seq 1 600); do
	STATUS=$(batch_status)
	[ "$STATUS" = "$TARGET" ] && break
	sleep 0.2
done
RECOVERY_SECONDS=$(($(date +%s) - RESTARTED_AT))
echo "status: $STATUS"

step "Checking the database"
RESULTS=$(sql "SELECT count(*) FROM results WHERE batch_id = '$BATCH_ID'")
DISTINCT_RESULTS=$(sql "SELECT count(DISTINCT custom_id) FROM results WHERE batch_id = '$BATCH_ID'")
SUCCEEDED=$(sql "SELECT count(*) FROM requests WHERE batch_id = '$BATCH_ID' AND status = 'succeeded'")
FAILED=$(sql "SELECT count(*) FROM requests WHERE batch_id = '$BATCH_ID' AND status = 'failed'")
EXPIRED=$(sql "SELECT count(*) FROM requests WHERE batch_id = '$BATCH_ID' AND status = 'expired'")
RESCUED=$(sql "SELECT count(*) FROM river_job WHERE args->>'batch_id' = '$BATCH_ID' AND errors::text LIKE '%rescued%'")
RETRIED=$(sql "SELECT count(*) FROM river_job WHERE args->>'batch_id' = '$BATCH_ID' AND attempt > 1")
OUTPUT_LINES=$(sql "SELECT coalesce((SELECT line_count FROM files f JOIN batches b ON f.id = b.output_file_id WHERE b.id = '$BATCH_ID'), 0)")
ERROR_LINES=$(sql "SELECT coalesce((SELECT line_count FROM files f JOIN batches b ON f.id = b.error_file_id WHERE b.id = '$BATCH_ID'), 0)")
DISTINCT_IN_FILES=$(sql "SELECT count(DISTINCT l.custom_id) FROM file_lines l JOIN batches b ON l.file_id IN (b.output_file_id, b.error_file_id) WHERE b.id = '$BATCH_ID'")

PASS=true
check() { # check <description> <condition result>
	if [ "$2" = "true" ]; then echo "  ok    $1"; else echo "  FAIL  $1"; PASS=false; fi
}
check "batch $TARGET" "$([ "$STATUS" = "$TARGET" ] && echo true)"
check "every custom_id is in exactly one output file ($((OUTPUT_LINES + ERROR_LINES)) lines, $DISTINCT_IN_FILES distinct)" \
	"$([ $((OUTPUT_LINES + ERROR_LINES)) -eq "$LINES" ] && [ "$DISTINCT_IN_FILES" -eq "$LINES" ] && echo true)"
check "no request has more than one result ($RESULTS results, $DISTINCT_RESULTS distinct)" \
	"$([ "$RESULTS" -eq "$DISTINCT_RESULTS" ] && echo true)"
check "the crash really interrupted work ($RUNNING requests were mid-call)" "$([ "$RUNNING" -gt 0 ] && echo true)"

if [ "$MODE" = expiry ]; then
	SAVED_LATE=$(sql "SELECT count(*) FROM requests r JOIN batches b ON b.id = r.batch_id WHERE b.id = '$BATCH_ID' AND r.status IN ('succeeded', 'failed') AND r.finished_at >= b.expires_at")
	EXPIRED_LINES=$(sql "SELECT count(*) FROM file_lines l JOIN batches b ON l.file_id = b.error_file_id WHERE b.id = '$BATCH_ID' AND l.line LIKE '%batch_expired%'")
	EXPIRED_AFTER_DEADLINE=$(sql "SELECT round(extract(epoch FROM expired_at - expires_at)::numeric, 1) FROM batches WHERE id = '$BATCH_ID'")
	EXPIRED_AFTER_START=$(sql "SELECT round((extract(epoch FROM expired_at) - $SERVER_STARTED)::numeric, 2) FROM batches WHERE id = '$BATCH_ID'")
	check "every request finished before the deadline or expired ($SUCCEEDED + $FAILED + $EXPIRED)" \
		"$([ $((SUCCEEDED + FAILED + EXPIRED)) -eq "$LINES" ] && [ "$EXPIRED" -gt 0 ] && echo true)"
	check "no result saved after the deadline was kept ($SAVED_LATE)" "$([ "$SAVED_LATE" -eq 0 ] && echo true)"
	check "expired requests say batch_expired in the errors file ($EXPIRED_LINES)" "$([ "$EXPIRED_LINES" -eq "$EXPIRED" ] && echo true)"
	check "expired within 5s of the restart (${RECOVERY_SECONDS}s)" "$([ "$RECOVERY_SECONDS" -le 5 ] && echo true)"
else
	check "every request has exactly one result ($RESULTS of $LINES)" "$([ "$RESULTS" -eq "$LINES" ] && echo true)"
fi

cat <<EOF

==================== crash test summary ====================
 Mode:                      $MODE
 Result:                    $([ "$PASS" = true ] && echo PASS || echo FAIL)
 Requests in batch:         $LINES
 Killed at:                 $DONE done, $IN_FLIGHT in flight ($RUNNING mid-call)
 Jobs rescued by River:     $RESCUED
 Jobs retried (any reason): $RETRIED
 Results saved:             $RESULTS ($DISTINCT_RESULTS distinct custom_ids)
 Succeeded / failed:        $SUCCEEDED / $FAILED
EOF
if [ "$MODE" = expiry ]; then
	cat <<EOF
 Expired:                   $EXPIRED
 Server down past deadline: ${DOWN_AFTER_DEADLINE}s
 Expired after restart:     ${RECOVERY_SECONDS}s (${EXPIRED_AFTER_DEADLINE}s after the deadline)
 Expired after server start: ${EXPIRED_AFTER_START}s
EOF
else
	echo " Finished after restart:    ${RECOVERY_SECONDS}s"
fi
cat <<EOF
 Results / errors file:     $OUTPUT_LINES / $ERROR_LINES lines
=============================================================
EOF

[ "$PASS" = true ]
