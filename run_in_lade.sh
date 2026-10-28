#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_DIR="${LADE_ENV_DIR:-$ROOT/../warehouse-deploy/.venv}"
if [[ -x "$ENV_DIR/bin/python" ]]; then
  source "$ENV_DIR/bin/activate"
elif [[ -f /data/zc/apps/miniconda3/etc/profile.d/conda.sh ]]; then
  source /data/zc/apps/miniconda3/etc/profile.d/conda.sh
  conda activate lade
else
  echo "Python environment unavailable: $ENV_DIR" >&2
  exit 1
fi
exec "$@"
