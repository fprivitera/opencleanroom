---
status: accepted
---
# Spark is reached through plain Spark Connect; tests run on real engines in Docker

The Spark backend targets Spark Connect (Spark 3.4 or later) through `pyspark-client`, and treats EMR, Glue and Databricks as deployment variants rather than separate connectors. CI runs the official `apache/spark` image as a Spark Connect server, on a 3.5.x and a 4.x line, and compares true aggregates with the DuckDB oracle. When Athena arrives, its adapter is tested against Floci (S3, Glue Data Catalog, Athena API) for the request flow and against a `trinodb/trino` container for dialect equivalence.

## Considered options

- Floci for Spark: rejected. Its EMR, EMR Serverless and Glue job emulation tracks lifecycle state only and never runs Spark.
- Floci alone for Athena: rejected. It executes Athena SQL on DuckDB, so it cannot catch Trino dialect differences such as NULL handling in `GREATEST` and `LEAST`.
- A managed cluster for integration tests: not required for 0.1, since the Spark Connect protocol is the same in Docker.

## Addendum (23 Oct 2026): client and server lines

The client must match the server's major line: `pyspark-client` 4.x cannot run SQL on a 3.5 server (the request reaches it as empty input), so the 3.5 line uses `pyspark[connect]` 3.5 on pandas 2 and NumPy 1 (the `spark35` extra). CI runs the official images `apache/spark:4.1.3` and `apache/spark:3.5.9` through `scripts/spark-connect.sh`.
