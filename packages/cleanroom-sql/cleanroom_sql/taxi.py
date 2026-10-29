# SPDX-License-Identifier: Apache-2.0
"""Synthetic NYC taxi dataset in the shape of the 2013 TLC FOIL release.

Tables
  taxi_zones(location_id PK, borough, zone)           public lookup
  drivers(hack_license PK, fleet)                      1 row per driver
  trips(trip_id PK, hack_license FK, pu_location_id FK, fare_amount, trip_distance)

Privacy unit: the driver (hack_license). Amounts are integer cents,
distances integer tenths of a mile, so discrete Laplace applies.
Trips per driver are heavy-tailed on purpose: truncation matters.
"""
from __future__ import annotations

import duckdb

from .schema import Column, Schema, Table

BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR"]
FLEETS = ["medallion", "fleet_A", "fleet_B"]

SCHEMA = Schema(tables={
    "taxi_zones": Table(
        name="taxi_zones", primary_key="location_id",
        columns=(Column("location_id"), Column("borough", type="string"), Column("zone", type="string")),
    ),
    "drivers": Table(
        name="drivers", primary_key="hack_license", privacy_id="hack_license",
        columns=(Column("hack_license", type="string"), Column("fleet", type="string")),
    ),
    "trips": Table(
        name="trips", primary_key="trip_id", privacy_id="hack_license",
        columns=(
            Column("trip_id"), Column("hack_license", type="string"), Column("pu_location_id"),
            Column("fare_amount", lower=0, upper=20_000),  # cents, clamp at $200
            Column("trip_distance", lower=0, upper=500),  # tenths of a mile
        ),
        foreign_keys={
            "hack_license": ("drivers", "hack_license"),
            "pu_location_id": ("taxi_zones", "location_id"),
        },
    ),
})


def empty_connect() -> duckdb.DuckDBPyConnection:
    """The three tables, empty: tests insert exactly the rows they need."""
    con = duckdb.connect()
    con.execute("CREATE TABLE taxi_zones(location_id INT, borough VARCHAR, zone VARCHAR)")
    con.execute("CREATE TABLE drivers(hack_license VARCHAR, fleet VARCHAR)")
    con.execute(
        "CREATE TABLE trips(trip_id INT, hack_license VARCHAR, pu_location_id INT, fare_amount INT, trip_distance INT)"
    )
    return con


def connect(n_drivers: int = 2_000, seed: float = 0.42) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    populate(con, n_drivers, seed)
    return con


def populate(con: duckdb.DuckDBPyConnection, n_drivers: int = 2_000, seed: float = 0.42) -> None:
    """Create and fill the three tables on an open connection (in memory or a file)."""
    con.execute(f"SELECT setseed({seed})")
    con.execute("""
        CREATE TABLE taxi_zones AS
        SELECT i AS location_id,
               CASE WHEN i <= 120 THEN 'Manhattan' WHEN i <= 180 THEN 'Brooklyn'
                    WHEN i <= 240 THEN 'Queens' WHEN i <= 255 THEN 'Bronx'
                    WHEN i <= 262 THEN 'Staten Island' ELSE 'EWR' END AS borough,
               'zone_' || i AS zone
        FROM generate_series(1, 263) t(i)
    """)
    con.execute(f"""
        CREATE TABLE drivers AS
        SELECT 'HL' || lpad(i::VARCHAR, 6, '0') AS hack_license,
               ['medallion','fleet_A','fleet_B'][1 + (i % 3)] AS fleet
        FROM generate_series(1, {n_drivers}) t(i)
    """)
    # heavy tail: most drivers ~30 trips, 2% of drivers ~400 trips
    con.execute("""
        CREATE TABLE trips AS
        WITH per_driver AS (
            SELECT hack_license,
                   CASE WHEN random() < 0.02 THEN 400 ELSE 10 + (random() * 40)::INT END AS n
            FROM drivers
        )
        SELECT row_number() OVER () AS trip_id,
               hack_license,
               1 + (CASE WHEN random() < 0.7 THEN random() * 120 ELSE random() * 263 END)::INT AS pu_location_id,
               (250 + exp(random() * 4.5) * 100)::INT AS fare_amount,
               (1 + exp(random() * 3.5))::INT AS trip_distance
        FROM per_driver, generate_series(1, 400) g(j)
        WHERE j <= n
    """)
