# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow PEP 440; both packages share one version.

## Unreleased

First public pre-release, `0.1.0a1`.

- Library: row-ownership join rule checked by a sensitivity property test, `WHERE` pushed into truncation, `AVG`, expression whitelist, discrete Laplace on `i64`.
- Policies: YAML with JSON Schema, a loader that rejects unsafe policies, `policy check`. δ as part of the dataset's budget is decided (ADR 0011), not yet implemented.
- Control plane: SQLite catalog, CLI, hashed API keys, DuckDB backend, Streamlit console, quickstart.
- Serving: SQLite ledger with atomic reserve and commit, HTTP JSON API, MCP server, budget epochs, `reconcile`.
- Spark Connect backend, query timeout, materialized truncation, benchmark report.
- Example on real data: Chicago Taxi Trips, loaded from the City of Chicago open-data portal at run time, with `k` and clamps chosen on March 2025.
