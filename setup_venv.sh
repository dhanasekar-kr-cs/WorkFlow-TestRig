# filename: scripts/setup_venv.sh
#!/usr/bin/env bash
set -euo pipefail

# Why: isolate Python deps per build, avoid clobbering agent-wide site-packages
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-.venv}"

$PYTHON_BIN -m venv "$VENV_DIR"
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

python -m pip install --upgrade pip wheel setuptools
if [[ -f "requirements.txt" ]]; then
  pip install -r requirements.txt
fi

# Optional dev/test requirements
if [[ -f "requirements-dev.txt" ]]; then
  pip install -r requirements-dev.txt
fi

echo "Virtualenv created at $VENV_DIR"
