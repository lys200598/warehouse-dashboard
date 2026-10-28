#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs
for mode in optimized baseline; do
  if [[ "$mode" == optimized ]]; then port=14180; else port=14181; fi
  [[ -f "data/regression/$mode-day/results.json" ]]
  if [[ -f "logs/web-$mode.pid" ]] && kill -0 "$(cat "logs/web-$mode.pid")" 2>/dev/null; then
    echo "$mode viewer already running; preserving it"
    continue
  fi
  nohup ./run_in_lade.sh python -m backend.scheduling_v4.web \
    --scheduler "$mode" --output "data/regression/$mode-day" \
    --dist "dist/v4-$mode" --port "$port" \
    > "logs/web-$mode.log" 2>&1 < /dev/null &
  echo $! > "logs/web-$mode.pid"
done
