#!/usr/bin/env bash
# Start the 120x90 / 48-rack / 2304-slot wheeled medium console on its own port.
#
# This is intentionally separate from ops/start_runtime.sh:
#   - ops/start_runtime.sh defaults to medium on 4173 (replaces the legacy page);
#   - this script keeps the legacy 4173 page running and adds the 120 layout
#     side-by-side on 14583 (or PORT=...).
#
# Usage:
#   bash ops/start_medium.sh                 # medium1728 on 14583
#   PORT=14584 DATASET=medium19 bash ops/start_medium.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PORT="${PORT:-14583}"
DATASET="${DATASET:-medium1728}"
OUTPUT_ROOT="${OUTPUT_ROOT:-$ROOT/data/local-runs}"
LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/.runtime/pids"
mkdir -p "$LOG_DIR" "$PID_DIR" "$OUTPUT_ROOT"

export LADE_ENV_DIR="${LADE_ENV_DIR:-$ROOT/.venv}"
if [ ! -x "$LADE_ENV_DIR/bin/python" ]; then
  echo "Python environment unavailable: $LADE_ENV_DIR" >&2
  exit 1
fi

if ss -ltn 2>/dev/null | grep -q ":$PORT "; then
  echo "medium console already listening on http://127.0.0.1:$PORT/ (not replacing it)"
  exit 0
fi

LOG="$LOG_DIR/medium-$PORT.log"
PIDFILE="$PID_DIR/medium-$PORT.pid"

echo "Starting medium live console on :$PORT ($DATASET, 120x90 / 48 racks / 2304 slots)..."
nohup "$ROOT/run_in_lade.sh" python "$ROOT/ops/warehouse_live.py" \
  --source-root "$ROOT" \
  --dist "$ROOT/dist/medium" \
  --registry "$ROOT/config/live-wheeled-datasets.json" \
  --dataset-id "$DATASET" \
  --output-root "$OUTPUT_ROOT" \
  --mode rules \
  --knowledge-backend journal \
  --mobility wheeled \
  --commit-seconds 5 \
  --port "$PORT" >"$LOG" 2>&1 &
PID=$!
echo "$PID" >"$PIDFILE"

for _ in $(seq 1 90); do
  if curl -fsS --noproxy '*' --max-time 2 "http://127.0.0.1:$PORT/api/health" >/dev/null 2>&1; then
    echo "Medium console: http://127.0.0.1:$PORT/"
    echo "Run output:     $OUTPUT_ROOT/<runId>/"
    echo "Log:            $LOG"
    exit 0
  fi
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "medium console exited during startup; see $LOG" >&2
    tail -30 "$LOG" >&2 || true
    exit 1
  fi
  sleep 1
done

echo "medium console did not become healthy; see $LOG" >&2
tail -30 "$LOG" >&2 || true
exit 1
