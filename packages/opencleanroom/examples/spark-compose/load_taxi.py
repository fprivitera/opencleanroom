# SPDX-License-Identifier: Apache-2.0
"""Load the synthetic taxi dataset into a Spark Connect server, as the admin would: trusted, outside opencleanroom.

    python load_taxi.py sc://localhost:15002 [database]
"""
import sys

import duckdb
from cleanroom_sql.taxi import populate
from pyspark.sql.connect.session import SparkSession

TYPES = {"taxi_zones": "location_id INT, borough STRING, zone STRING", "drivers": "hack_license STRING, fleet STRING",
         "trips": "trip_id INT, hack_license STRING, pu_location_id INT, fare_amount INT, trip_distance INT"}

url, database = sys.argv[1], (sys.argv[2] if len(sys.argv) > 2 else "nyc")
spark = SparkSession.builder.remote(url).create()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}").collect()
duck = duckdb.connect()
populate(duck, n_drivers=300)
for table, types in TYPES.items():
    rows = [tuple(r) for r in duck.execute(f"SELECT * FROM {table}").fetchall()]
    spark.createDataFrame(rows, schema=types).write.mode("overwrite").saveAsTable(f"{database}.{table}")
print(f"loaded {', '.join(TYPES)} into {database}")
spark.stop()
