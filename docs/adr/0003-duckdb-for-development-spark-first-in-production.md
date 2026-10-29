---
status: accepted
---
# DuckDB for development and tests; Spark is the first production backend

The service needs a backend that runs without a cluster (development, CI, stand-alone use) and a backend for production data. DuckDB serves the first purpose in-process. Spark is the first production backend, reached through a remote protocol, with Athena after it. SQLGlot keeps the rewriter dialect-agnostic, so each backend adds a dialect and an execution adapter, not a new rewriter. Tumult Analytics already targets Spark, so the choice is deliberate: this project differs by being a self-hosted SQL gateway with per-consumer policies, not by the engine it runs on.

## Consequences

- Truncation uses a window function (`ROW_NUMBER() OVER (PARTITION BY privacy_id ...)`), which forces a shuffle of the full table on Spark. Large tables will need a materialized truncated view that the admin refreshes.
- DuckDB results are the test oracle: the same rewritten query must return identical true aggregates on both backends.
