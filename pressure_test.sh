#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
if [[ -n "${LADE_ENV_DIR:-}" ]]; then
  PY="$LADE_ENV_DIR/bin/python"
elif [[ -x "$ROOT/.venv/bin/python" ]]; then
  PY="$ROOT/.venv/bin/python"
elif [[ -x "$ROOT/../warehouse-deploy/.venv/bin/python" ]]; then
  PY="$ROOT/../warehouse-deploy/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PY="$(command -v python)"
else
  echo '没有找到Python。请安装Python 3.10以上，或设置LADE_ENV_DIR。' >&2
  exit 1
fi
if [[ ! -x "$PY" ]] || ! "$PY" -c 'import sys; sys.exit(sys.version_info < (3,10))'; then
  echo '所选Python不可用或低于3.10，请检查LADE_ENV_DIR。' >&2
  exit 1
fi
exec "$PY" "$ROOT/pressure_test.py" "$@"
