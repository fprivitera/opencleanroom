# SPDX-License-Identifier: Apache-2.0
"""Download Chicago Taxi Trips (City of Chicago open data) into a DuckDB file for a clean-room dataset.

    python fetch_chicago_taxi.py --start 2025-03-01 --days 31 --out chicago_taxi.duckdb

Source: dataset `ajtu-isnz` (Taxi Trips, 2024 onward) and `igwz-8jzy` (community area boundaries) on
data.cityofchicago.org, read through the Socrata CSV API one day at a time. Values become integers, as the
executor's noise works on integers: miles in tenths, money in cents, durations in seconds. The file holds real
trip records keyed by a hashed taxi id; keep it out of version control.
"""
from __future__ import annotations

import argparse
import datetime as dt
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

import duckdb

TRIPS = "https://data.cityofchicago.org/resource/ajtu-isnz.csv"
AREAS = "https://data.cityofchicago.org/resource/igwz-8jzy.csv"
COLUMNS = "trip_id,taxi_id,trip_start_timestamp,trip_seconds,trip_miles,pickup_community_area,dropoff_community_area,fare,tips,payment_type,company"


def get(url: str, params: dict, target: Path, attempts: int = 5) -> None:
    full = f"{url}?{urllib.parse.urlencode(params)}"
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(full, timeout=300) as r:
                target.write_bytes(r.read())
            return
        except OSError:
            if attempt == attempts - 1:
                raise
            time.sleep(5 * (attempt + 1))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--start", type=dt.date.fromisoformat, required=True)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--out", type=Path, default=Path("chicago_taxi.duckdb"))
    a = ap.parse_args()

    raw = Path(tempfile.mkdtemp(prefix="chicago-taxi-"))
    get(AREAS, {"$select": "area_num_1,community", "$limit": 100}, raw / "areas.csv")
    for i in range(a.days):
        day = a.start + dt.timedelta(days=i)
        where = f"trip_start_timestamp between '{day}T00:00:00' and '{day}T23:59:59'"
        started = time.perf_counter()
        get(TRIPS, {"$select": COLUMNS, "$where": where, "$order": "trip_id", "$limit": 200000}, raw / f"trips-{day}.csv")
        print(f"{day}: {sum(1 for _ in open(raw / f'trips-{day}.csv')) - 1} trips in {time.perf_counter() - started:.0f} s", flush=True)

    con = duckdb.connect(str(a.out))
    con.execute("DROP TABLE IF EXISTS trips")
    con.execute("DROP TABLE IF EXISTS community_areas")
    con.execute(f"""CREATE TABLE community_areas AS
        SELECT CAST(area_num_1 AS INTEGER) AS area_number, community AS name FROM read_csv('{raw / "areas.csv"}') ORDER BY 1""")
    con.execute(f"""CREATE TABLE trips AS SELECT
        trip_id, taxi_id,
        CAST(trip_start_timestamp AS DATE) AS trip_date,
        CAST(pickup_community_area AS INTEGER) AS pickup_community_area,
        CAST(dropoff_community_area AS INTEGER) AS dropoff_community_area,
        CAST(trip_seconds AS INTEGER) AS trip_seconds,
        CAST(round(trip_miles * 10) AS INTEGER) AS trip_miles,   -- tenths of a mile
        CAST(round(fare * 100) AS INTEGER) AS fare,              -- cents
        CAST(round(tips * 100) AS INTEGER) AS tips,              -- cents
        payment_type, company
        FROM read_csv('{raw / "trips-*.csv"}', union_by_name = true,
                      types = {{'trip_id': 'VARCHAR', 'taxi_id': 'VARCHAR', 'trip_miles': 'DOUBLE', 'fare': 'DOUBLE', 'tips': 'DOUBLE'}})
        WHERE taxi_id IS NOT NULL""")
    n, taxis = con.execute("SELECT COUNT(*), COUNT(DISTINCT taxi_id) FROM trips").fetchone()
    print(f"{a.out}: {n} trips, {taxis} taxis, {con.execute('SELECT COUNT(*) FROM community_areas').fetchone()[0]} community areas")
    con.close()


if __name__ == "__main__":
    main()
