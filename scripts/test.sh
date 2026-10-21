#!/usr/bin/env bash
set -euo pipefail

uv run --frozen \
    --exclude-newer "7 days" \
    --exclude-newer-package certifi=false \
    --exclude-newer-package databento-dbn=false \
    -- pytest tests "$@"
