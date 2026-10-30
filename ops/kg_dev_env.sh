#!/usr/bin/env bash
set -euo pipefail
cd /data/workspace/wz/warehouse-integration-on-lys
export LADE_ENV_DIR=/data/workspace/wz/warehouse-deploy/.venv
export PYTHONPATH=/data/workspace/wz/testdeps-httpx2:$PWD
export RAY_AUTH_MODE=disabled
export PYTHONUNBUFFERED=1
set -a
source /data/workspace/wz/warehouse-wheel-agents-20261029/graph-test/neo4j.env
set +a
export NEO4J_URI=bolt://127.0.0.1:17692
export WAREHOUSE_NEO4J_URI=$NEO4J_URI
export WAREHOUSE_NEO4J_USER=${NEO4J_USER:-neo4j}
export WAREHOUSE_NEO4J_PASSWORD=${NEO4J_PASSWORD:-${WAREHOUSE_NEO4J_PASSWORD:-}}
export WAREHOUSE_RAY_TEST_ADDRESS=127.0.0.1:27380
export WAREHOUSE_KG_TEST_URL=http://127.0.0.1:14591
exec bash run_in_lade.sh "$@"
