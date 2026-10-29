#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# From an empty directory to a noisy answer. Needs `pip install cleanroom-sql[duckdb] opencleanroom` on PATH.
# PYTHON defaults to the python on PATH; the catalog and the DuckDB file land in the current directory.
set -euo pipefail
PYTHON="${PYTHON:-python}"
POLICY="${POLICY:-$(dirname "$0")/../../cleanroom-sql/examples/taxi.yaml}"
"$PYTHON" -c "import duckdb; from cleanroom_sql.taxi import populate; c = duckdb.connect('taxi.duckdb'); populate(c, n_drivers=300); c.close()"
opencleanroom init
opencleanroom backend add duckdb-dev --duckdb taxi.duckdb
opencleanroom policy apply "$POLICY"
KEY=$(opencleanroom consumer create nyc_taxi analytics-team)
opencleanroom query --key "$KEY" "SELECT COUNT(*) FROM trips" --epsilon 0.1 --json | "$PYTHON" -c "import json, sys; print(json.load(sys.stdin)['rows'][0][0])"
