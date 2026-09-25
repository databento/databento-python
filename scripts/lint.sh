#!/usr/bin/env bash
set -euo pipefail

echo "Running $(uv run --frozen mypy --version)..."
uv run --frozen -- mypy --no-site-packages . "$@"
