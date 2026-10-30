#!/usr/bin/env bash
# Start the optional multi-run replay service on 14185.
#
# It is read-only: scans data/local-runs and data/runs, never advances a
# simulation, never writes a checkpoint or graph.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PORT="${REPLAY_PORT:-14185}"
LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/.runtime/pids"
LOG="$LOG_DIR/replay.log"
PIDFILE="$PID_DIR/replay.pid"
mkdir -p "$LOG_DIR" "$PID_DIR"

if curl -fsS --noproxy '*' --max-time 1 "http://127.0.0.1:$PORT/api/runs" >/dev/null 2>&1; then
  echo "Replay service already listening on http://127.0.0.1:$PORT/"
  exit 0
fi

export LADE_ENV_DIR="$ROOT/.venv"
echo "Starting replay service on 127.0.0.1:$PORT..."
nohup "$ROOT/run_in_lade.sh" python -m backend.replay_service \
  >"$LOG" 2>&1 9>&- &
PID=$!
echo "$PID" >"$PIDFILE"

for _ in $(seq 1 30); do
  if curl -fsS --noproxy '*' --max-time 1 "http://127.0.0.1:$PORT/api/runs" >/dev/null 2>&1; then
    echo "Replay:  http://127.0.0.1:$PORT/wide.html"
    echo "Log:     $LOG"
    exit 0
  fi
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "replay service exited during startup; see $LOG" >&2
    tail -20 "$LOG" >&2 || true
    exit 1
  fi
  sleep 1
done

echo "replay service did not become ready; see $LOG" >&2
tail -20 "$LOG" >&2 || true
exit 1
