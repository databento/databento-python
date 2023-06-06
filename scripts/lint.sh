#!/usr/bin/env bash
echo $(mypy --version)
echo Running mypy...
poetry run mypy databento examples tests
