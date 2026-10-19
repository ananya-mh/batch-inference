#!/usr/bin/env bash
# grafana-screenshot.sh: saves a PNG of the Grafana dashboard over the time
# span of one fairness-experiment run, using headless Chrome in Docker.
#
# Usage:   scripts/grafana-screenshot.sh <run tag> <output.png>
#          The run tag is the batches' "experiment" metadata, like
#          fair-1-1792442151 (scripts/fairness-experiment.sh sets it).
# Needs:   the monitoring profile running (docker compose --profile monitoring up),
#          Docker, and the run's batches in the database.
#
# The window is the run's first submission to its last completion, plus one
# second either side. Times in the image are UTC.

set -euo pipefail

if [ $# -ne 2 ]; then
	echo "usage: $0 <run tag> <output.png>" >&2
	exit 2
fi
TAG=$1
OUT=$2

cd "$(dirname "$0")/.."

# Runs one SQL query against the service's database and prints the result.
sql() {
	docker compose exec -T postgres psql -U postgres -d batch -Atc "$1"
}

FROM=$(sql "SELECT (extract(epoch FROM min(created_at)) * 1000)::bigint - 1000 FROM batches WHERE metadata->>'experiment' = '$TAG'")
TO=$(sql "SELECT (extract(epoch FROM max(completed_at)) * 1000)::bigint + 1000 FROM batches WHERE metadata->>'experiment' = '$TAG'")
if [ -z "$FROM" ] || [ -z "$TO" ]; then
	echo "no finished run with tag $TAG" >&2
	exit 1
fi

OUT_DIR=$(cd "$(dirname "$OUT")" && pwd)
SHOT_DIR=$(mktemp -d)
chmod 777 "$SHOT_DIR" # Chrome runs as a non-root user in its container
trap 'rm -rf "$SHOT_DIR"' EXIT

# --virtual-time-budget gives Grafana's page time to load and draw.
docker run --rm --network batch-inference_default -v "$SHOT_DIR":/out zenika/alpine-chrome:124 \
	--no-sandbox --disable-gpu --hide-scrollbars --window-size=1600,760 --virtual-time-budget=30000 \
	--screenshot=/out/shot.png \
	"http://grafana:3000/d/batch-inference/batch-inference?orgId=1&from=$FROM&to=$TO&kiosk&theme=light" \
	>/dev/null 2>&1
cp "$SHOT_DIR/shot.png" "$OUT_DIR/$(basename "$OUT")"
echo "saved $OUT (run $TAG, $FROM to $TO ms)"
