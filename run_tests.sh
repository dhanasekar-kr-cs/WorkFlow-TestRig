# filename: scripts/run_tests.sh
#!/usr/bin/env bash
set -euo pipefail

# Why: produce machine-readable test reports GoCD can visualize
VENV_DIR="${VENV_DIR:-.venv}"
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

# Ensure pytest + coverage are available even if not in requirements
python -m pip install pytest pytest-cov junitxml

TEST_REPORT_DIR="${TEST_REPORT_DIR:-build/test-reports}"
COVERAGE_DIR="${COVERAGE_DIR:-build/coverage}"
mkdir -p "$TEST_REPORT_DIR" "$COVERAGE_DIR"

pytest \
  --junitxml="$TEST_REPORT_DIR/junit.xml" \
  --cov=. --cov-report=xml:"$COVERAGE_DIR/coverage.xml" \
  --maxfail=1 -q
