#!/usr/bin/env bash
# Local checks mirroring GitHub Actions before push.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

if [[ -d "${ROOT}/.venv/bin" ]]; then
  export PATH="${ROOT}/.venv/bin:${PATH}"
fi

echo "=== Pre-push checks ==="

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "❌ Required command not found: $1"
    exit 1
  fi
}

run_ruff() {
  if command -v ruff >/dev/null 2>&1; then
    ruff "$@"
  elif python3 -m ruff --version >/dev/null 2>&1; then
    python3 -m ruff "$@"
  else
    echo "❌ ruff not found — run: pip install ruff"
    exit 1
  fi
}

run_yamllint() {
  if command -v yamllint >/dev/null 2>&1; then
    yamllint "$@"
  elif python3 -m yamllint --version >/dev/null 2>&1; then
    python3 -m yamllint "$@"
  else
    echo "❌ yamllint not found — run: pip install yamllint"
    exit 1
  fi
}

PYTHON_TARGETS=()
[[ -d flink ]] && PYTHON_TARGETS+=(flink)
[[ -d producers ]] && PYTHON_TARGETS+=(producers)
[[ -d api ]] && PYTHON_TARGETS+=(api)

if [[ ${#PYTHON_TARGETS[@]} -gt 0 ]]; then
  require_cmd python3
  echo "→ ruff check"
  run_ruff check "${PYTHON_TARGETS[@]}"
  echo "→ ruff format --check"
  run_ruff format --check "${PYTHON_TARGETS[@]}"
  echo "→ python syntax (py_compile)"
  find "${PYTHON_TARGETS[@]}" -name '*.py' -print0 | xargs -0 python3 -m py_compile
fi

if [[ -f docker-compose.yml ]] || [[ -d .github/workflows ]]; then
  YAML_FILES=()
  [[ -f docker-compose.yml ]] && YAML_FILES+=(docker-compose.yml)
  if [[ -d .github/workflows ]]; then
    while IFS= read -r -d '' file; do
      YAML_FILES+=("$file")
    done < <(find .github/workflows \( -name '*.yml' -o -name '*.yaml' \) -print0)
  fi
  if [[ ${#YAML_FILES[@]} -gt 0 ]]; then
    echo "→ yamllint"
    run_yamllint -d relaxed "${YAML_FILES[@]}"
  fi
fi

if [[ -f docker-compose.yml ]]; then
  require_cmd docker
  echo "→ docker compose config"
  docker compose -f docker-compose.yml config --quiet
fi

echo "✅ All pre-push checks passed."
