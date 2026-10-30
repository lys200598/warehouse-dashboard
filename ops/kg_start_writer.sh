#!/usr/bin/env bash
set -euo pipefail
cd /data/workspace/wz/warehouse-integration-on-lys
if ss -ltn | grep -q ':14591 '; then
  echo '14591 already occupied; not starting another service' >&2
  exit 1
fi
mkdir -p /tmp/wz-kg-evidence
nohup bash ops/kg_dev_env.sh python -m backend.global_writer_api --port 14591 > /tmp/wz-kg-evidence/writer.log 2>&1 < /dev/null &
echo $! > /tmp/wz-kg-evidence/writer.pid
echo "writer pid=$!"
