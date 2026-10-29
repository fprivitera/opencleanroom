#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Real data: Chicago Taxi Trips for March 2025 (City of Chicago Data Portal), from an empty directory to noisy answers.
# Needs `pip install opencleanroom` on PATH and network access to data.cityofchicago.org; the month takes about
# ten minutes to download. DAYS=7 fetches a week instead (k in the policy is set for a month: see README.md).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PYTHON="${PYTHON:-python}"
POLICY="${POLICY:-$HERE/../../../cleanroom-sql/examples/chicago_taxi.yaml}"
"$PYTHON" "$HERE/fetch_chicago_taxi.py" --start 2025-03-01 --days "${DAYS:-31}" --out chicago_taxi.duckdb
opencleanroom init
opencleanroom backend add duckdb-chicago --duckdb chicago_taxi.duckdb
opencleanroom policy apply "$POLICY"
KEY=$(opencleanroom consumer create chicago_taxi city-analytics)
opencleanroom query --key "$KEY" "SELECT COUNT(*), AVG(fare) FROM trips" --epsilon 0.2
opencleanroom query --key "$KEY" "SELECT payment_type, COUNT(*) FROM trips GROUP BY payment_type" --epsilon 0.2
opencleanroom query --key "$KEY" "SELECT z.name, COUNT(*) FROM trips t JOIN community_areas z ON t.pickup_community_area = z.area_number GROUP BY z.name" --epsilon 0.3
