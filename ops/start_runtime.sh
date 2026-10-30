#!/usr/bin/env bash
# Start the 4173 warehouse console and the multi-run replay service.
#
# Defaults to the 120x90 medium layout with the wheeled live runtime.
# Legacy Ray console remains available as an explicit rollback path.
#
# Usage:
#   bash ops/start_runtime.sh             # start medium live at 4173
#   bash ops/start_runtime.sh --restart   # stop the current 4173 process first
#   bash ops/start_runtime.sh --ray       # 120 medium + 100 Ray motion actors
#   bash ops/start_runtime.sh --global-writer  # extra plain child process for global stats
#   bash ops/start_runtime.sh --legacy    # start the old Ray/backend.server console
#   bash ops/start_runtime.sh --legacy new
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

STACK="medium"
MODE="resume"
RESTART=0
RAY=0
ROBOT_GRAPH=0
GLOBAL_WRITER=0
for arg in "$@"; do
  case "$arg" in
    medium|--medium) STACK="medium" ;;
    legacy|--legacy) STACK="legacy" ;;
    ray|--ray) RAY=1 ;;
    robot-graph|--robot-graph) ROBOT_GRAPH=1 ;;
    global-writer|--global-writer) GLOBAL_WRITER=1 ;;
    resume|new) MODE="$arg" ;;
    --restart) RESTART=1 ;;
    -h|--help)
      sed -n '1,14p' "$0"
      exit 0
      ;;
    *)
      echo "unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

export LADE_ENV_DIR="${LADE_ENV_DIR:-$ROOT/.venv}"
if [ ! -x "$LADE_ENV_DIR/bin/python" ]; then
  echo "ignoring invalid LADE_ENV_DIR=$LADE_ENV_DIR; using $ROOT/.venv" >&2
  export LADE_ENV_DIR="$ROOT/.venv"
fi
if [ ! -x "$LADE_ENV_DIR/bin/python" ]; then
  echo "Python environment unavailable: $LADE_ENV_DIR" >&2
  exit 1
fi

LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/.runtime/pids"
CHECKPOINT_DIR="$ROOT/data/checkpoints"
mkdir -p "$LOG_DIR" "$PID_DIR" "$CHECKPOINT_DIR" /tmp/wh-ray

if command -v flock >/dev/null 2>&1; then
  exec 9>"$PID_DIR/start_runtime.lock"
  if ! flock -n 9; then
    echo "another start_runtime.sh is already running" >&2
    exit 1
  fi
fi

port_busy() {
  for _ in 1 2 3 4 5; do
    if ss -ltn 2>/dev/null | grep -q ":$1 "; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

port_pid() {
  ss -ltnp 2>/dev/null | sed -n "s/.*:$1 .*pid=\([0-9][0-9]*\).*/\1/p" | head -1
}

port_owned_by() {
  [ -n "${2:-}" ] && [ "$(port_pid "$1")" = "$2" ]
}

wait_port() {
  local port=$1 limit=${2:-60}
  for _ in $(seq 1 "$limit"); do
    port_busy "$port" && return 0
    sleep 1
  done
  return 1
}

wait_http() {
  local url=$1 limit=${2:-30}
  for _ in $(seq 1 "$limit"); do
    if curl -fsS --noproxy '*' --max-time 2 "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

stop_console_if_requested() {
  [ "$RESTART" = "1" ] || return 0
  if ! port_busy 4173; then
    return 0
  fi
  local pid
  pid="$(port_pid 4173)"
  if [ -z "$pid" ]; then
    echo "4173 is busy but its PID is not visible; stop it manually first" >&2
    exit 1
  fi
  if ! tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -qE 'backend\.server|warehouse_live\.py'; then
    echo "4173 is owned by another process (pid $pid); refusing to stop it" >&2
    exit 1
  fi
  echo "Stopping 4173 (pid $pid)..."
  kill -INT "$pid"
  for _ in $(seq 1 15); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "4173 did not stop after 15s; sending SIGTERM"
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 10); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
  fi
  if kill -0 "$pid" 2>/dev/null; then
    echo "4173 did not stop after SIGTERM; sending SIGKILL"
    kill -KILL "$pid" 2>/dev/null || true
    sleep 1
  fi
  if kill -0 "$pid" 2>/dev/null; then
    echo "4173 did not stop; stop it manually" >&2
    exit 1
  fi
}

# 1-2. Neo4j and Ray are only required by the legacy Ray console.
# The medium live page runs one V5 coordinator and a local journal.
if [ "$STACK" = "legacy" ]; then
if port_busy 47687; then
  echo "Neo4j already listening on 47687"
else
  NEO4J_HOME="$ROOT/.runtime/warehouse-neo4j-5.26.32"
  if [ -x "$NEO4J_HOME/bin/neo4j" ]; then
    export JAVA_HOME="$("$LADE_ENV_DIR/bin/python" -c 'from jdk4py import JAVA_HOME; print(JAVA_HOME)')"
    echo "Starting Neo4j..."
    "$NEO4J_HOME/bin/neo4j" start >"$LOG_DIR/neo4j.log" 2>&1 || true
    if ! wait_port 47687 60; then
      echo "Neo4j did not open 47687; see $LOG_DIR/neo4j.log" >&2
      exit 1
    fi
  else
    echo "Neo4j not found at $NEO4J_HOME; start it manually" >&2
    exit 1
  fi
fi

# 2. Ray head (project-specific ports/temp dir)
if port_busy 26380; then
  echo "Ray already listening on 26380"
else
  echo "Starting Ray head..."
  if ! "$LADE_ENV_DIR/bin/ray" start --head \
    --node-ip-address=127.0.0.1 --port=26380 --num-cpus=12 \
    --temp-dir=/tmp/wh-ray \
    --min-worker-port=26400 --max-worker-port=26599 \
    --dashboard-port=18265 --ray-client-server-port=20001 \
    --disable-usage-stats >"$LOG_DIR/ray.log" 2>&1; then
    if port_busy 26380; then
      echo "Ray is already running on 26380; continuing"
    else
      echo "Ray failed to start; see $LOG_DIR/ray.log" >&2
      tail -20 "$LOG_DIR/ray.log" >&2 || true
      exit 1
    fi
  fi
  if ! wait_port 26380 60; then
    echo "Ray did not open 26380; see $LOG_DIR/ray.log" >&2
    exit 1
  fi
fi
fi  # legacy Neo4j/Ray

# 3. 4173 console
if [ "$STACK" = "medium" ]; then
  if [ "$ROBOT_GRAPH" = "1" ] && [ "$RAY" != "1" ]; then
    echo "--robot-graph requires --ray" >&2
    exit 2
  fi
  RAY_PORT="${RAY_ADDRESS:-127.0.0.1:26380}"; RAY_PORT="${RAY_PORT##*:}"
  if [ "$RAY" = "1" ] && ! port_busy "$RAY_PORT"; then
    echo "Ray head is not listening on ${RAY_ADDRESS:-127.0.0.1:26380}; run: bash ops/start_ray.sh" >&2
    exit 1
  fi
  if { [ "$ROBOT_GRAPH" = "1" ] || [ "$GLOBAL_WRITER" = "1" ]; } && ! port_busy 47687; then
    echo "Neo4j is not listening on 47687; run: bash ops/start_neo4j.sh" >&2
    exit 1
  fi
fi

stop_console_if_requested
if port_busy 4173; then
  echo "4173 is already running; use --restart to replace it, or stop it manually" >&2
  exit 1
fi

if [ "$STACK" = "medium" ]; then
  LIVE_DATASET="${LIVE_DATASET:-medium1728}"
  LIVE_OUTPUT_ROOT="${LIVE_OUTPUT_ROOT:-$ROOT/data/local-runs}"
  LIVE_ARGS=(
    --source-root "$ROOT"
    --registry "$ROOT/config/live-wheeled-datasets.json"
    --dataset-id "$LIVE_DATASET"
    --output-root "$LIVE_OUTPUT_ROOT"
    --mode rules
    --knowledge-backend journal
    --mobility wheeled
    --commit-seconds "${COMMIT_SECONDS:-5}"
    --dist "$ROOT/dist/medium"
    --port 4173
  )
  if [ "$RAY" = "1" ]; then
    LIVE_ARGS+=(--ray --ray-address "${RAY_ADDRESS:-127.0.0.1:26380}" --ray-actor-cpus "${RAY_ACTOR_CPUS:-0.1}")
    if [ "$ROBOT_GRAPH" = "1" ]; then
      LIVE_ARGS+=(--robot-graph --graph-state-interval "${GRAPH_STATE_INTERVAL:-4}")
    fi
  fi
  if [ "$GLOBAL_WRITER" = "1" ]; then
    LIVE_ARGS+=(--global-writer --global-writer-queue "${GLOBAL_WRITER_QUEUE:-256}")
  fi
  if [ -n "${MAX_ACTIVE_ROBOTS:-}" ]; then
    LIVE_ARGS+=(--max-active-robots "$MAX_ACTIVE_ROBOTS")
  fi
  echo "Starting medium live 4173 ($LIVE_DATASET, 120x90/48 racks/2304 slots)..."
  nohup "$ROOT/run_in_lade.sh" python "$ROOT/ops/warehouse_live.py" "${LIVE_ARGS[@]}" \
    >"$LOG_DIR/4173.log" 2>&1 9>&- &
  CONSOLE_PID=$!
  echo "$CONSOLE_PID" >"$PID_DIR/4173.pid"

  if ! wait_http "http://127.0.0.1:4173/api/health" 90; then
    echo "4173 medium live page did not become healthy; see $LOG_DIR/4173.log" >&2
    tail -30 "$LOG_DIR/4173.log" >&2 || true
    exit 1
  fi
else
  export WAREHOUSE_NEO4J_URI="${WAREHOUSE_NEO4J_URI:-bolt://127.0.0.1:47687}"
  export WAREHOUSE_NEO4J_USER="${WAREHOUSE_NEO4J_USER:-neo4j}"
  export WAREHOUSE_NEO4J_PASSWORD="${WAREHOUSE_NEO4J_PASSWORD:-}"
  export WAREHOUSE_NEO4J_DATABASE="${WAREHOUSE_NEO4J_DATABASE:-neo4j}"
  export WAREHOUSE_NEO4J_PACKAGE_DIR="${WAREHOUSE_NEO4J_PACKAGE_DIR:-$ROOT/data/generated/warehouse100-day1}"
  export WAREHOUSE_SIMULATION_DATASET_DIR="${WAREHOUSE_SIMULATION_DATASET_DIR:-$ROOT/data/generated/warehouse100-day1}"
  export WAREHOUSE_NEO4J_WRITE_ENABLED="${WAREHOUSE_NEO4J_WRITE_ENABLED:-1}"
  export WAREHOUSE_NEO4J_WRITE_MODE="${WAREHOUSE_NEO4J_WRITE_MODE:-robot-actors}"
  export RAY_AUTH_MODE="${RAY_AUTH_MODE:-disabled}"

  CONSOLE_ARGS=(
    --backend ray
    --ray-address=127.0.0.1:26380
    --ray-actor-cpus=0.1
    --robot-count=100
    --max-active-robots=100
    --simulation-dataset-dir="$WAREHOUSE_SIMULATION_DATASET_DIR"
    --port=4173
  )

  if [ "$MODE" = "new" ]; then
    CHECKPOINT="$CHECKPOINT_DIR/console-csv-ray-$(date +%Y%m%d-%H%M%S).json"
  elif [ -n "${CHECKPOINT:-}" ]; then
    :
  else
    CHECKPOINT="$(find "$CHECKPOINT_DIR" -maxdepth 1 -name 'console-csv-ray-*.json' -printf '%T@ %p\n' 2>/dev/null \
      | sort -nr | head -1 | cut -d' ' -f2-)"
    if [ -z "$CHECKPOINT" ]; then
      CHECKPOINT="$CHECKPOINT_DIR/console-csv-ray-$(date +%Y%m%d-%H%M%S).json"
    fi
  fi

  if [ "$MODE" = "resume" ] && [ -f "$CHECKPOINT" ]; then
    CONSOLE_ARGS+=(--checkpoint-path "$CHECKPOINT" --resume --allow-code-change)
    echo "Resuming legacy 4173 from $CHECKPOINT"
  else
    CONSOLE_ARGS+=(--checkpoint-path "$CHECKPOINT")
    echo "Starting a fresh legacy 4173 run with checkpoint $CHECKPOINT"
  fi

  echo "Starting legacy 4173 console..."
  nohup "$ROOT/run_in_lade.sh" python -m backend.server "${CONSOLE_ARGS[@]}" \
    >"$LOG_DIR/4173.log" 2>&1 9>&- &
  CONSOLE_PID=$!
  echo "$CONSOLE_PID" >"$PID_DIR/4173.pid"

  if ! wait_http "http://127.0.0.1:4173/api/health" 45; then
    echo "4173 did not become healthy; see $LOG_DIR/4173.log" >&2
    tail -20 "$LOG_DIR/4173.log" >&2 || true
    exit 1
  fi
fi
if ! port_owned_by 4173 "$CONSOLE_PID"; then
  owner="$(port_pid 4173)"
  if [ -n "$owner" ] && [ "$owner" != "$CONSOLE_PID" ]; then
    echo "4173 is already served by pid $owner; stopping duplicate $CONSOLE_PID"
    kill "$CONSOLE_PID" 2>/dev/null || true
    sleep 1
    kill -KILL "$CONSOLE_PID" 2>/dev/null || true
    CONSOLE_PID="$owner"
    echo "$CONSOLE_PID" >"$PID_DIR/4173.pid"
  elif port_busy 4173; then
    echo "4173 is already running; continuing without replacing it"
  else
    echo "4173 process $CONSOLE_PID is not serving port 4173" >&2
    tail -20 "$LOG_DIR/4173.log" >&2 || true
    exit 1
  fi
fi

# 4. Multi-run replay service
if port_busy 14185; then
  echo "Replay service already listening on 14185"
else
  echo "Starting replay service..."
  nohup "$ROOT/run_in_lade.sh" python -m backend.replay_service \
    >"$LOG_DIR/replay.log" 2>&1 9>&- &
  REPLAY_PID=$!
  echo "$REPLAY_PID" >"$PID_DIR/replay.pid"
  if ! wait_port 14185 20; then
    echo "Replay service did not open 14185; see $LOG_DIR/replay.log" >&2
    tail -20 "$LOG_DIR/replay.log" >&2 || true
    exit 1
  fi
  if ! port_owned_by 14185 "$REPLAY_PID"; then
    owner="$(port_pid 14185)"
    if [ -n "$owner" ] && [ "$owner" != "$REPLAY_PID" ]; then
      echo "Replay service is already served by pid $owner; stopping duplicate $REPLAY_PID"
      kill "$REPLAY_PID" 2>/dev/null || true
      sleep 1
      kill -KILL "$REPLAY_PID" 2>/dev/null || true
      echo "$owner" >"$PID_DIR/replay.pid"
    elif port_busy 14185; then
      echo "Replay service is already running; continuing"
    else
      echo "Replay process $REPLAY_PID is not serving port 14185" >&2
      tail -20 "$LOG_DIR/replay.log" >&2 || true
      exit 1
    fi
  fi
fi

echo
if [ "$STACK" = "medium" ]; then
  echo "4173 medium live:  http://127.0.0.1:4173/  ($LIVE_DATASET, pid $CONSOLE_PID)"
  echo "Live run output:   $LIVE_OUTPUT_ROOT/<runId>/"
else
  echo "4173 legacy:       http://127.0.0.1:4173/  (pid $CONSOLE_PID)"
fi
echo "Multi-run replay:  http://127.0.0.1:14185/wide.html"
echo "Logs:              $LOG_DIR/4173.log, $LOG_DIR/replay.log"
