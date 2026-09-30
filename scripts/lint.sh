#!/usr/bin/env bash
set -euo pipefail

echo "Running $(uv run --frozen \
    --exclude-newer "7 days" \
    --exclude-newer-package databento-dbn=false \
    -- mypy --version)..."
uv run --frozen \
    --exclude-newer "7 days" \
    --exclude-newer-package databento-dbn=false \
    -- mypy --no-site-packages . "$@"
