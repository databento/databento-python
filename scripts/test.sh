#!/usr/bin/env bash
set -euo pipefail

uv run --frozen -- pytest tests "$@"
