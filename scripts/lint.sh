#! /usr/bin/env bash
echo $(mypy --version)
echo Running mypy...
mypy databento examples tests --config-file mypy.ini
