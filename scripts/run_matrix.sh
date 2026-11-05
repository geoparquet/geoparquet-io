#!/usr/bin/env bash
# Shared portable harness lifecycle for both benchmark workflows.
set -euo pipefail
case "$1" in
  save)
    tar --exclude=__pycache__ -cf "$RUNNER_TEMP/profile-harness.tar" \
      scripts/profile_command.py scripts/benchmark_history.py scripts/profile_matrix scripts/run_matrix.sh
    ;;
  checkout)
    revision=$(git rev-parse --verify --end-of-options "$2^{commit}")
    git checkout --detach --force "$revision"
    tar -xf "$RUNNER_TEMP/profile-harness.tar"
    uv sync --all-extras
    ;;
  run)
    uv run python -m scripts.profile_command --command "$2" --size "$3" \
      --runs "$4" --output "$6/matrix.json"
    uv run python -m scripts.benchmark_history --matrix "$6/matrix.json" \
      --version-label "$5" --output "$7"
    ;;
  *) exit 2 ;;
esac
