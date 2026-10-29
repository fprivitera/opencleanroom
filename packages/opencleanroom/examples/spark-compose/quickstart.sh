#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# From an empty directory to a noisy answer served by Spark. Needs `pip install "opencleanroom[spark]"` and a Spark
# Connect server: `docker compose up -d --wait` in this directory, or OCR_SPARK_URL pointing at another one.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PYTHON="${PYTHON:-python}"
URL="${OCR_SPARK_URL:-sc://localhost:15002}"
DATABASE="${DATABASE:-nyc}"
"$PYTHON" "$HERE/load_taxi.py" "$URL" "$DATABASE"
opencleanroom init
opencleanroom backend add spark-dev --spark "$URL" --database "$DATABASE" --timeout 60
"$PYTHON" - "$HERE/../../../cleanroom-sql/examples/taxi.yaml" > policy.yaml <<'PY'
import sys, yaml
doc = yaml.safe_load(open(sys.argv[1]))
doc["dataset"]["backend"], doc["dataset"]["dialect"] = "spark-dev", "spark"
print(yaml.safe_dump(doc, sort_keys=False))
PY
opencleanroom policy apply policy.yaml
opencleanroom backend materialize nyc_taxi --snapshot "quickstart-$(date +%Y%m%d%H%M%S)"  # a new name each run: re-runnable
KEY=$(opencleanroom consumer create nyc_taxi analytics-team)
opencleanroom query --key "$KEY" "SELECT COUNT(*) FROM trips" --epsilon 0.1 --json | "$PYTHON" -c "import json, sys; print(json.load(sys.stdin)['rows'][0][0])"
