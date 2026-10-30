#!/usr/bin/env bash
# Start the project-local Ray head used by the 120 medium live page.
#
# This script is idempotent: an existing head on RAY_PORT is reused.  It never
# calls `ray stop`, so other project services sharing this head are not killed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RAY_PORT="${RAY_PORT:-26380}"
RAY_NUM_CPUS="${RAY_NUM_CPUS:-12}"
RAY_TEMP_DIR="${RAY_TEMP_DIR:-/tmp/wh-ray}"
MIN_WORKER_PORT="${RAY_MIN_WORKER_PORT:-26400}"
MAX_WORKER_PORT="${RAY_MAX_WORKER_PORT:-26599}"
DASHBOARD_PORT="${RAY_DASHBOARD_PORT:-18265}"
CLIENT_PORT="${RAY_CLIENT_PORT:-20001}"
RAY_BIN="$ROOT/.venv/bin/ray"
LOG="$ROOT/logs/ray.log"
PID_DIR="$ROOT/.runtime/pids"
mkdir -p "$ROOT/logs" "$PID_DIR" "$RAY_TEMP_DIR"

if [ ! -x "$RAY_BIN" ]; then
  echo "Ray CLI unavailable: $RAY_BIN (install ray[default])" >&2
  exit 1
fi

if ss -ltn 2>/dev/null | grep -q ":$RAY_PORT "; then
  echo "Ray head already listening on 127.0.0.1:$RAY_PORT (reused)"
  echo "RAY_ADDRESS=127.0.0.1:$RAY_PORT"
  exit 0
fi

echo "Starting Ray head on 127.0.0.1:$RAY_PORT..."
nohup "$RAY_BIN" start --head \
  --node-ip-address=127.0.0.1 --port="$RAY_PORT" --num-cpus="$RAY_NUM_CPUS" \
  --temp-dir="$RAY_TEMP_DIR" \
  --min-worker-port="$MIN_WORKER_PORT" --max-worker-port="$MAX_WORKER_PORT" \
  --dashboard-port="$DASHBOARD_PORT" --ray-client-server-port="$CLIENT_PORT" \
  --disable-usage-stats >"$LOG" 2>&1 9>&- &

for _ in $(seq 1 60); do
  if ss -ltn 2>/dev/null | grep -q ":$RAY_PORT "; then
    echo "Ray head:      127.0.0.1:$RAY_PORT"
    echo "Dashboard:     http://127.0.0.1:$DASHBOARD_PORT (optional)"
    echo "RAY_ADDRESS=127.0.0.1:$RAY_PORT"
    echo "Log:           $LOG"
    exit 0
  fi
  sleep 1
done

echo "Ray head did not open $RAY_PORT; see $LOG" >&2
tail -30 "$LOG" >&2 || true
exit 1
