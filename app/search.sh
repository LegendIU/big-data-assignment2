#!/bin/bash
set -e
source /app/.venv/bin/activate
python /app/query.py "$@"
