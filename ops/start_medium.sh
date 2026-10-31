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
#   GLOBAL_WRITER=1 bash ops/start_medium.sh # + plain child RunStat/global writer
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PORT="${PORT:-14583}"
DATASET="${DATASET:-medium1728}"
RAY="${RAY:-0}"
RAY_ADDRESS="${RAY_ADDRESS:-127.0.0.1:26380}"
RAY_ACTOR_CPUS="${RAY_ACTOR_CPUS:-0.1}"
ROBOT_GRAPH="${ROBOT_GRAPH:-0}"
GRAPH_STATE_INTERVAL="${GRAPH_STATE_INTERVAL:-4}"
GLOBAL_WRITER="${GLOBAL_WRITER:-0}"
GLOBAL_WRITER_QUEUE="${GLOBAL_WRITER_QUEUE:-256}"
OUTPUT_ROOT="${OUTPUT_ROOT:-$ROOT/data/local-runs}"
LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/.runtime/pids"
mkdir -p "$LOG_DIR" "$PID_DIR" "$OUTPUT_ROOT"

export LADE_ENV_DIR="${LADE_ENV_DIR:-$ROOT/.venv}"
if [ ! -x "$LADE_ENV_DIR/bin/python" ]; then
  echo "ignoring invalid LADE_ENV_DIR=$LADE_ENV_DIR; using $ROOT/.venv" >&2
  export LADE_ENV_DIR="$ROOT/.venv"
fi
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

if [ "$ROBOT_GRAPH" = "1" ] && [ "$RAY" != "1" ]; then
  echo "ROBOT_GRAPH=1 requires RAY=1" >&2
  exit 2
fi
RAY_PORT="${RAY_ADDRESS##*:}"
if [ "$RAY" = "1" ] && ! ss -ltn 2>/dev/null | grep -q ":$RAY_PORT "; then
  echo "Ray head is not listening on $RAY_ADDRESS; run: bash ops/start_ray.sh" >&2
  exit 1
fi
if { [ "$ROBOT_GRAPH" = "1" ] || [ "$GLOBAL_WRITER" = "1" ]; } && ! ss -ltn 2>/dev/null | grep -q ':47687 '; then
  echo "Neo4j is not listening on 47687; run: bash ops/start_neo4j.sh" >&2
  exit 1
fi

RAY_ARGS=()
if [ "$RAY" = "1" ]; then
  RAY_ARGS=(--ray --ray-address "$RAY_ADDRESS" --ray-actor-cpus "$RAY_ACTOR_CPUS")
  if [ "$ROBOT_GRAPH" = "1" ]; then
    RAY_ARGS+=(--robot-graph --graph-state-interval "$GRAPH_STATE_INTERVAL")
  fi
fi
if [ "$GLOBAL_WRITER" = "1" ]; then
  RAY_ARGS+=(--global-writer --global-writer-queue "$GLOBAL_WRITER_QUEUE")
fi

echo "Starting medium live console on :$PORT ($DATASET, 120x90 / 48 racks / 2304 slots, ray=$RAY)..."
nohup "$ROOT/run_in_lade.sh" python "$ROOT/ops/warehouse_live.py" \
  --source-root "$ROOT" \
  --dist "$ROOT/dist" \
  --registry "$ROOT/config/live-wheeled-datasets.json" \
  --dataset-id "$DATASET" \
  --output-root "$OUTPUT_ROOT" \
  --mode rules \
  --knowledge-backend journal \
  --mobility wheeled \
  --commit-seconds 5 \
  "${RAY_ARGS[@]}" \
  --autostart --initial-speed 60 --port "$PORT" >"$LOG" 2>&1 &
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
