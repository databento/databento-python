#!/usr/bin/env bash
set -euo pipefail

uv sync --all-extras --all-groups \
    --exclude-newer "7 days" \
    --exclude-newer-package databento-dbn=false
