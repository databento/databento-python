#! /usr/bin/env bash
set -e
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
pip install -r requirements_dev.txt
pip install .
