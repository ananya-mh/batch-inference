#!/usr/bin/env bash
# load-test.sh: load tests the API layer with k6. While three tenants'
# batches run, many simulated clients poll batch status and list batches,
# and the script reports p50/p95/p99 latency for each endpoint.
#
# Usage:   scripts/load-test.sh
# Needs:   Docker with Compose and curl (k6 runs in Docker).
# Options: VUS=50          simulated clients (each pauses 0.1-0.5 s between rounds)
#          DURATION=60s    how long the load runs
#          REQUESTS=2000   requests in each tenant's batch
#
# The batches run on compose.yaml's settings (fake model at 50-500 ms with
# its usual errors), so the API is measured while workers, the scheduler,
# and finishing batches all use the same database. Keep the machine awake
# (on macOS: caffeinate -i scripts/load-test.sh). Output goes to
# results/load-test-<time>/.

set -euo pipefail

VUS=${VUS:-50}
DURATION=${DURATION:-60s}
REQUESTS=${REQUESTS:-2000}
API=http://localhost:8080
K6_IMAGE=grafana/k6:2.2.0

cd "$(dirname "$0")/.."
WORK=$(mktemp -d)
OUT="results/load-test-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
OUT_ABS=$(cd "$OUT" && pwd)

# Prints a step heading.
step() {
	printf '\n==> %s\n' "$1"
}

# Runs one SQL query against the service's database and prints the result.
# Its input is /dev/null so it never reads the caller's input.
sql() {
	docker compose exec -T postgres psql -U postgres -d batch -Atc "$1" </dev/null
}

# Prints a string field from compact JSON on stdin, like "id".
json_string() {
	grep -o "\"$1\":\"[^\"]*\"" | head -1 | cut -d'"' -f4
}

trap 'rm -rf "$WORK"' EXIT

step "Starting the stack"
docker compose up -d --build --wait >"$WORK/compose.log" 2>&1 || { cat "$WORK/compose.log"; exit 1; }

step "Starting three batches of $REQUESTS requests (tenants a, b, c)"
TENANTS="["
for tenant in a b c; do
	awk -v n="$REQUESTS" -v t="$tenant" 'BEGIN { for (i = 0; i < n; i++)
		printf "{\"custom_id\": \"%s-%d\", \"method\": \"POST\", \"url\": \"/v1/chat/completions\", \"body\": {\"model\": \"fake\", \"messages\": [{\"role\": \"user\", \"content\": \"Tenant %s, ticket %d\"}]}}\n", t, i, t, i }' >"$WORK/$tenant.jsonl"
	KEY="sk-dev-tenant-$tenant"
	FILE=$(curl -sf -H "Authorization: Bearer $KEY" -F purpose=batch -F "file=@$WORK/$tenant.jsonl" "$API/v1/files" | json_string id)
	BATCH=$(curl -sf -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
		-d "{\"input_file_id\": \"$FILE\", \"endpoint\": \"/v1/chat/completions\", \"completion_window\": \"24h\"}" \
		"$API/v1/batches" | json_string id)
	echo "tenant_$tenant: $BATCH"
	TENANTS="$TENANTS{\"key\": \"$KEY\", \"batch\": \"$BATCH\"},"
	BATCH_IDS="${BATCH_IDS:-}'$BATCH',"
done
TENANTS="${TENANTS%,}]"
BATCH_IDS="${BATCH_IDS%,}"

step "Load: $VUS clients for $DURATION"
DONE_BEFORE=$(sql "SELECT count(*) FROM requests WHERE batch_id IN ($BATCH_IDS) AND status IN ('succeeded', 'failed')")
docker run --rm --network batch-inference_default \
	-v "$PWD/scripts/k6":/scripts:ro -v "$OUT_ABS":/out \
	-e BASE_URL=http://server:8080 -e TENANTS="$TENANTS" -e VUS="$VUS" -e DURATION="$DURATION" -e OUT=/out \
	"$K6_IMAGE" run --quiet /scripts/api-load.js >"$OUT/k6-output.txt" 2>&1 ||
	{ cat "$OUT/k6-output.txt"; echo "k6 failed (see above)"; exit 1; }
DONE_AFTER=$(sql "SELECT count(*) FROM requests WHERE batch_id IN ($BATCH_IDS) AND status IN ('succeeded', 'failed')")
STILL_RUNNING=$(sql "SELECT count(*) FROM batches WHERE id IN ($BATCH_IDS) AND status = 'in_progress'")

{
	echo "## API load test"
	echo
	echo "Command: \`VUS=$VUS DURATION=$DURATION REQUESTS=$REQUESTS scripts/load-test.sh\`, commit \`$(git rev-parse --short HEAD)\`, run on $(date -u +%Y-%m-%d), k6 $K6_IMAGE."
	echo "$VUS simulated clients for $DURATION, each polling a random tenant's batch status and listing its batches, pausing 0.1–0.5 s between rounds, while three tenants' batches of $REQUESTS requests ran (compose.yaml's settings)."
	echo
	cat "$OUT/k6-table.md"
	echo
	echo "Meanwhile the batches kept running: $((DONE_AFTER - DONE_BEFORE)) requests finished during the load; $STILL_RUNNING of 3 batches were still in progress at the end."
} >"$OUT/results.md"

step "Cancelling the batches (they were only there to keep the workers busy)"
for tenant in a b c; do
	BATCH=$(echo "$TENANTS" | grep -o "sk-dev-tenant-$tenant\", \"batch\": \"[^\"]*\"" | cut -d'"' -f5)
	curl -sf -o /dev/null -X POST -H "Authorization: Bearer sk-dev-tenant-$tenant" "$API/v1/batches/$BATCH/cancel" || true
done

step "Results (also saved to $OUT/results.md)"
cat "$OUT/results.md"
