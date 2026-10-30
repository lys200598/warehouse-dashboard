#!/usr/bin/env bash
# Start the project-local Neo4j instance (HTTP 47474 / Bolt 47687).
#
# Optional service: needed by kg.html and --robot-graph, not by the 120 page
# itself.  The script never initializes a non-empty database or changes auth.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BOLT_PORT="${WAREHOUSE_NEO4J_BOLT_PORT:-47687}"
NEO4J_HOME="${WAREHOUSE_NEO4J_HOME:-$ROOT/.runtime/warehouse-neo4j-5.26.32}"
LOG="$ROOT/logs/neo4j.log"
mkdir -p "$ROOT/logs" "$ROOT/.runtime"

if ss -ltn 2>/dev/null | grep -q ":$BOLT_PORT "; then
  echo "Neo4j already listening on 127.0.0.1:$BOLT_PORT (reused)"
  echo "Bolt: bolt://127.0.0.1:$BOLT_PORT"
  exit 0
fi

if [ ! -x "$NEO4J_HOME/bin/neo4j" ]; then
  echo "Neo4j not found at $NEO4J_HOME" >&2
  echo "Install it from the manual (section 3) before starting." >&2
  exit 1
fi

export JAVA_HOME="$("$ROOT/.venv/bin/python" -c 'from jdk4py import JAVA_HOME; print(JAVA_HOME)')"
echo "Starting Neo4j from $NEO4J_HOME..."
"$NEO4J_HOME/bin/neo4j" start >"$LOG" 2>&1 || true

for _ in $(seq 1 60); do
  if ss -ltn 2>/dev/null | grep -q ":$BOLT_PORT "; then
    echo "Neo4j Bolt:  bolt://127.0.0.1:$BOLT_PORT"
    echo "Neo4j HTTP:  http://127.0.0.1:47474"
    echo "Log:         $LOG"
    exit 0
  fi
  sleep 1
done

echo "Neo4j did not open $BOLT_PORT; see $LOG" >&2
tail -30 "$LOG" >&2 || true
exit 1
