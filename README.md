# OpenCleanroom: a self-hosted SQL clean room with differential privacy

Rewrites an analytic SQL query into one with finite sensitivity, runs it on DuckDB and adds discrete noise with OpenDP. The noise never enters the SQL: the executor adds it in Python.

**Status: pre-release.** No independent audit yet. A differentially private result with a documented ε is evidence for an anonymity assessment under the GDPR or another law, not a conclusion: that assessment covers the whole deployment. Known limits and how to report a vulnerability: [`SECURITY.md`](SECURITY.md).

Vocabulary is in [`GLOSSARY.md`](GLOSSARY.md); design decisions are in [`docs/adr/`](docs/adr/).

## Layout

| File | Role |
|---|---|
| `packages/cleanroom-sql/cleanroom_sql/schema.py` | Metadata: primary keys, foreign keys, `privacy_id`, clamping bounds. Public tables have `privacy_id=None`. |
| `packages/cleanroom-sql/cleanroom_sql/rewriter.py` | Validates the query, computes sensitivity (row-ownership join rule), pushes `WHERE` into truncation, expands `AVG`, injects truncation (`ROW_NUMBER <= k`) and clamping (`LEAST(GREATEST(...))`). Reads and replaces SQLGlot nodes only. |
| `packages/cleanroom-sql/cleanroom_sql/sqlast.py` | Every SQLGlot node the rewriter builds, in one place, so a SQLGlot upgrade breaks one module and one test file. |
| `packages/cleanroom-sql/cleanroom_sql/session.py` | Trusted executor: ε budget in exact fractions, keyset, OpenDP discrete Laplace on `i64` vectors, check that `meas.map(Δ) <= ε`. |
| `packages/cleanroom-sql/cleanroom_sql/policy.py` | Policy loader: YAML validated by `cleanroom_sql/policy.schema.json`, then by fail-closed semantic rules; builds the `Schema`, `k`, keysets and budgets. |
| `packages/cleanroom-sql/cleanroom_sql/ledger.py` | In-memory budget ledger, one per dataset and shared by every session on a policy: reserve, commit, release in exact fractions. |
| `packages/cleanroom-sql/cleanroom_sql/__main__.py` | `cleanroom-sql policy check <file>`. |
| `packages/cleanroom-sql/examples/` | Four worked policies: `taxi.yaml`, `single_table.yaml`, `two_private_tables.yaml` on synthetic data, and `chicago_taxi.yaml` for real data. |
| `packages/opencleanroom/examples/chicago/` | Chicago Taxi Trips: a loader from the City of Chicago open-data portal, a quickstart, and the measured error. |
| `packages/cleanroom-sql/cleanroom_sql/taxi.py` | Synthetic NYC taxi dataset in the shape of the 2013 TLC release: public `taxi_zones`, `drivers`, `trips`. |
| `packages/opencleanroom/opencleanroom/api.py`, `mcp.py` | HTTP JSON API and MCP server over the `Runtime`: every request under a key's policy, on the shared ledger. |
| `packages/opencleanroom/opencleanroom/backends.py`, `materialize.py` | DuckDB and Spark Connect backends, each behind a select-only guard and a query timeout; materialized tables on Spark. |
| `scripts/spark-connect.sh`, `packages/opencleanroom/examples/spark-compose/` | A Spark Connect server in Docker for tests, and a quickstart on Spark. |
| `packages/cleanroom-sql/demo.py` | Two queries with joins, then budget exhaustion. |
| `packages/cleanroom-sql/red_team.py` | 35 hostile probes. Exits non-zero on FAIL or `??`. |
| `packages/cleanroom-sql/tests/` | rewriting, sensitivity, rejections, accounting, noise scale; policy loader rules, queries under a policy, the `policy check` command, the examples; golden policy outcomes per query shape under two consumers (`tests/golden_policy/`); golden rewritten SQL per query shape in DuckDB, Spark and Postgres (`tests/golden/`, regenerate with `CLEANROOM_SQL_UPDATE_GOLDEN=1`) with Spark/Postgres parse-back; a rewrite latency bound; and a Hypothesis property test that checks the computed sensitivity against neighbouring datasets for every query shape (`CLEANROOM_SQL_HYPOTHESIS_PROFILE=full` runs 1,000 examples per shape). |

## Run

```bash
pip install -e "packages/cleanroom-sql[duckdb,test]" -e "packages/opencleanroom[test]"
cd packages/cleanroom-sql
python demo.py
python -m pytest -q
python red_team.py
cleanroom-sql policy check examples/taxi.yaml   # exit 0, or every fault with its YAML path
cd ../opencleanroom && python -m pytest -q
```

The library itself depends on `sqlglot`, `opendp`, `pyyaml` and `jsonschema` only, pinned exactly (`pyproject.toml`, Python 3.11+). `duckdb` is an extra because the synthetic taxi dataset imports it; `fastmcp`, `starlette` and `uvicorn` belong to the platform package (`opencleanroom`). `requirements/` pins every transitive dependency of each CI job with its hashes (`scripts/lock.sh`); [`CONTRIBUTING.md`](CONTRIBUTING.md) shows how to install exactly that environment.

## Quickstart

From an empty directory to a noisy answer, with both packages installed (`pip install -e "packages/cleanroom-sql[duckdb]" -e packages/opencleanroom`):

```bash
python -c "import duckdb; from cleanroom_sql.taxi import populate; c = duckdb.connect('taxi.duckdb'); populate(c, n_drivers=300); c.close()"
opencleanroom init
opencleanroom backend add duckdb-dev --duckdb taxi.duckdb
opencleanroom policy apply packages/cleanroom-sql/examples/taxi.yaml
KEY=$(opencleanroom consumer create nyc_taxi analytics-team)
opencleanroom query --key "$KEY" "SELECT COUNT(*) FROM trips" --epsilon 0.1
opencleanroom console        # Streamlit: admin and consumer sections (needs opencleanroom[ui])
```

`packages/opencleanroom/examples/quickstart.sh` is the same sequence and runs in CI (`.github/workflows/gates.yml`). Budgets live in the catalog's ledger, shared by every process on that file.

## Real data: Chicago Taxi Trips

`packages/opencleanroom/examples/chicago/quickstart.sh` downloads a month of [Chicago Taxi Trips](https://data.cityofchicago.org/Transportation/Taxi-Trips-2024-/ajtu-isnz) from the City of Chicago open-data portal and serves it under `packages/cleanroom-sql/examples/chicago_taxi.yaml`, with the taxi as privacy unit. The data stays on your machine, never in the repository. Its [README](packages/opencleanroom/examples/chicago/README.md) explains how `k` and the clamps were chosen and what the answers look like: about 1% error on large groups, tens of percent on small ones.

## Query flow

1. `Rewriter.rewrite(sql)`:
   - accepts only a fixed set of node types: tables, columns, aliases, `INNER JOIN … ON col = col`, `COUNT`/`SUM`/`AVG`, comparisons between operands of one declared type, `AND`/`OR`/`NOT`, parentheses, `IS NULL`, `IN (literals)`, `BETWEEN literals`, literals. Everything else is `UnsupportedQuery`: functions, arithmetic, `CAST`, `CASE`, `LIKE`, nested `SELECT`, CTEs, `UNION`, window functions, `DISTINCT`, `HAVING`, `ORDER BY`, `LIMIT`, `MIN`/`MAX`, aggregates in `WHERE`, `SUM`/`AVG` of a column without clamp bounds, joins that are not `FK -> PK`, joins whose `ON` does not bind the joined table, qualified table names;
   - sets the rows one privacy unit can own per table: 0 for a public table, 1 for a keyed table, `k` otherwise;
   - applies the row-ownership join rule: an accepted join keeps every row inside one privacy unit, so `Δ` is the product of the rows owned on each private side, in either join order; a public table on the key side leaves `Δ` unchanged;
   - sets `COUNT` sensitivity to `Δ` and `SUM(col)` sensitivity to `Δ · max(|a|,|b|)`; `AVG(col)` becomes two measurements, `SUM(col)` and `COUNT(col)` over the same rows, with those sensitivities;
   - splits `WHERE` on top-level `AND` and moves each conjunct that reads exactly one truncated table inside that table's truncation subquery, before `ROW_NUMBER`, so the `k` rows kept per privacy unit match the filter; conjuncts on several tables, public tables or keyed tables stay outside;
   - replaces each private, non-keyed table with the truncated subquery.
2. `Session.query(sql, epsilon, keyset)` (or a session from `Session.from_policy(policy, consumer, con)`):
   - `DESCRIBE <table>`, `SHOW COLUMNS FROM <table>` and `SHOW TABLES` never reach the rewriter: they answer from the policy's declared columns and tables (only the consumer's), at no budget cost;
   - under a policy, after rewriting and before any budget moves: every table must be in the consumer's list, every aggregate kind allowed, `ε <= max_epsilon_per_query`, no consumer-supplied keyset, and every `GROUP BY` column must have a declared keyset, else `PolicyViolation`; the keyset is the cartesian product of the declared ones, a `from: table.column` keyset read in clear from the public table;
   - reserves `ε` in the dataset ledger (consumer cap and dataset budget both bound it), otherwise raises `BudgetExhaustedException` without running anything; without a policy the session's own budget applies;
   - splits `ε` evenly across the noisy measurements (sequential composition); `AVG` returns `noisy_sum / max(noisy_count, 1)`, post-processing at no extra cost;
   - runs the rewritten SQL and aligns the rows to the public keyset (absent keys become 0);
   - applies one OpenDP `make_laplace` per measurement (`vector<i64>`, L1, `scale = Δ/ε`); `AVG` rows are `float`, the others `int`;
   - commits `ε` and returns noisy rows, the rewritten SQL, sensitivities and the 95% CI half-width (`b·ln 20`); a failure after the reservation releases it.

## Policy

A policy is one YAML file (`packages/cleanroom-sql/examples/taxi.yaml` is the reference): `dataset` says what is true about the data, `consumers` what each consumer may do.

```yaml
version: 1
dataset:
  name: nyc_taxi
  privacy_unit: {column: hack_license}
  truncation: {k: 50}
  tables:
    taxi_zones: {public: true, primary_key: location_id, columns: {location_id: {type: int}, borough: {type: string}, zone: {type: string}}}
    drivers:    {primary_key: hack_license, columns: {hack_license: {type: string}, fleet: {type: string}}}
    trips:
      primary_key: trip_id              # order_by defaults to it; else a column marked unique: true
      foreign_keys: {hack_license: drivers.hack_license, pu_location_id: taxi_zones.location_id}
      columns:
        trip_id: {type: int}
        hack_license: {type: string}
        pu_location_id: {type: int}
        fare_amount: {type: int, clamp: [0, 20000]}
  budget: {epsilon: "10.0", epoch: "2026-10"}
  keysets:
    taxi_zones.borough: {from: taxi_zones.borough}   # read from the public table
    drivers.fleet: [medallion, fleet_A, fleet_B]       # admin-declared list
consumers:
  analytics-team: {tables: [trips, taxi_zones, drivers], aggregates: [count, sum, avg], max_epsilon_per_query: 0.5, budget: {epsilon: 5.0}}
```

Every column a consumer may reference is declared with a type; clamp bounds go on `int` columns. ε values are read from their decimal text as exact fractions. `cleanroom-sql policy check <file>` validates without a backend; `Session.from_policy(policy, consumer, con)` runs queries under it.

The loader fails closed. It rejects a policy when:

| Rule | YAML path in the error |
|---|---|
| Unknown key, wrong shape, `version` not 1, unknown column type, non-identifier name | the offending key |
| `clamp` on a non-`int` column, lower above upper, or `k × max(abs(a), abs(b))` reaching 2^63 | `dataset.tables.T.columns.C.clamp` |
| `primary_key` not a declared column | `dataset.tables.T.primary_key` |
| A private table without its privacy ID column (the dataset's, or the table's `privacy_id` override) | `dataset.tables.T` |
| A `privacy_id` override not linked by privacy-ID foreign keys to a table carrying `privacy_unit.column` | `dataset.tables.T` |
| `privacy_id` on a public table | `dataset.tables.T.privacy_id` |
| `order_by` neither the primary key nor a column with `unique: true` | `dataset.tables.T.order_by` |
| Foreign key from an undeclared column, from a public table, to an unknown table, or not to its primary key | `dataset.tables.T.foreign_keys.C` |
| Foreign key between private tables that is not the privacy ID on both sides | `dataset.tables.T.foreign_keys.C` |
| Keyset on an undeclared column, or `from:` a column that is not public | `dataset.keysets.T.C` |
| ε not a finite positive decimal (`"0"`, `.inf`, negative) | the ε key |
| Invalid YAML, including a duplicate key | `<root>` (an empty path in `PolicyError.errors`) |
| Consumer table or aggregate unknown | `consumers.N.tables`, `consumers.N.aggregates` |
| Consumer cap above the dataset budget | `consumers.N.budget.epsilon` |
| `max_epsilon_per_query` above the consumer cap, or the dataset budget when there is no cap | `consumers.N.max_epsilon_per_query` |

Schema faults are reported together; when the shape is valid, every semantic fault is reported together.

## Red team

Three layers, all in the gates:

- `packages/cleanroom-sql/red_team.py`: hostile probes against the rewriter and the executor, including known attacks from the literature (error oracles, nested `SELECT`, finite sums, budget channel, multi-group units). Exits non-zero on FAIL or `??`; `KNOWN <limit>` marks one of the known limits in [`SECURITY.md`](SECURITY.md).
- `packages/opencleanroom/red_team_platform.py`: probes against keys, catalog tampering, the read-only DuckDB connection, the ADR 0004 budget rules, concurrency and metadata.
- `packages/cleanroom-sql/tests/test_fuzz_grammar.py`: Hypothesis generates random SQL over the taxi schema; every query is either refused with `UnsupportedQuery`/`PolicyViolation` or satisfies the sensitivity bound on a dataset and its neighbour. `CLEANROOM_SQL_HYPOTHESIS_PROFILE=full` runs thousands of examples, `CLEANROOM_SQL_FUZZ_SEED` reproduces one.

A confirmed finding gets a red-team probe; an accepted one is listed in the known limits of [`SECURITY.md`](SECURITY.md).

## HTTP API and MCP server

Both endpoints run over the catalog: a consumer key selects the policy, the ledger in the catalog file holds the budget, every request lands in the audit log.

```bash
opencleanroom serve --port 8000 --rate 60/60        # HTTP JSON API
curl -s -H "Authorization: Bearer $KEY" -H "Idempotency-Key: q1" \
     -d '{"sql": "SELECT COUNT(*) FROM trips", "epsilon": 0.1}' http://127.0.0.1:8000/v1/query
curl -s -H "Authorization: Bearer $KEY" http://127.0.0.1:8000/v1/schema   # free
curl -s -H "Authorization: Bearer $KEY" http://127.0.0.1:8000/v1/budget   # free
```

Errors are `{"error": <code>, "message": <fixed text>}`: `invalid_key` 401, `unsupported_query` / `policy_violation` / `bad_request` 400, `budget_exhausted` / `rate_limited` 429, `in_progress` 409, `query_failed` 502, `internal` 500. No backend text ever reaches the client. The same `Idempotency-Key` replays a finished query without spending.

| MCP tool | Returns | Spends budget |
|---|---|---|
| `describe_schema` | Tables and columns your policy lets you read, with clamping bounds | no |
| `public_query(sql)` | Rows of public tables only, capped (keysets, lookups) | no |
| `dp_query(sql, epsilon, request_id?)` | Noisy rows, rewritten SQL, sensitivities, 95% CI, remaining budget; GROUP BY keys come from the policy's keysets | yes |
| `budget()` | Remaining epsilon | no |

```bash
OPENCLEANROOM_KEY=$KEY opencleanroom mcp                      # stdio: the key comes from the environment, on this transport only
opencleanroom mcp --transport http --port 8001                # key from the Authorization header
claude mcp add opencleanroom -e OPENCLEANROOM_KEY=$KEY -- opencleanroom --catalog /path/catalog.sqlite mcp
```

### Admin: epochs, audit, ledger

The budget lives in the catalog file (`ledger` table), shared by every process; the `audit` table records every request in the same transaction.

```bash
opencleanroom budget new-epoch nyc_taxi --epoch 2026-11 --snapshot snap-42 --reason "monthly renewal"
opencleanroom policy apply policy.yaml        # the file from git, now with budget.epoch: 2026-11
opencleanroom audit list nyc_taxi [--json]    # one row per request, rejections included
opencleanroom audit reconcile nyc_taxi        # ledger vs audit per epoch; exit 1 on a mismatch
opencleanroom ledger list nyc_taxi            # reservations a crashed worker left (they count as spent)
opencleanroom ledger release 7 --reason "worker killed during the demo"
```

A renewal is a written decision (ADR 0004): `apply` refuses an epoch label that was not recorded with `new-epoch` or that sorts before the current one, so re-applying an old file never moves spending back.

## Spark

A dataset can live on a Spark cluster reached through Spark Connect (ADR 0007); consumers see no difference. The policy names the backend and `dialect: spark`.

```bash
pip install "opencleanroom[spark]"                 # Spark 4.x servers; use [spark35] for a 3.5 server
opencleanroom backend add spark-prod --spark sc://spark-host:15002 --database nyc --timeout 60
opencleanroom policy apply policy.yaml             # dataset.backend: spark-prod, dataset.dialect: spark
opencleanroom backend check-keys nyc_taxi          # declared column types and primary keys hold in the data
opencleanroom backend materialize nyc_taxi --snapshot 2026-10   # materialized tables: the rows truncation keeps, once per epoch
```

- Every statement a session sends is checked against an allow-list of what the rewriter emits ; every query runs under the backend's timeout and a timed-out query is charged like any backend failure.
- A materialized table replaces the truncation window only when the query has no filter on that table, so answers are identical to the window's; `materialize` refuses a dataset whose stored column types differ from the policy or whose declared primary keys repeat. A new epoch or another `k` falls back to the window until `materialize` runs for it; a changed order or column list falls back for the rest of the epoch, since one materialized table is kept per table, epoch and `k`.
- `examples/spark-compose/` starts a server with `docker compose up -d --wait`; its `quickstart.sh` goes from an empty directory to a noisy answer. The equivalence tests (`OCR_SPARK_URL=... pytest tests/test_spark_equivalence.py`) run every query shape on DuckDB and Spark and require identical true aggregates.
- Benchmark: [`docs/benchmarks/spark-truncation.md`](docs/benchmarks/spark-truncation.md).

## Red team

`red_team.py` runs hostile probes against the parser, rewriter and executor. Results after fixes:

| Probe | Result | Note |
|---|---|---|
| Duplicate keys in a keyset | fixed | One user moved two output entries: real L1 `2Δ`, charged `Δ`. The keyset is now deduplicated. |
| Budget float drift (10,000 × 1e-4) | fixed | Exact fractions: the 10,000th query exhausts the budget, the 10,001st is blocked. |
| `epsilon` NaN, inf, string, bool | fixed | Validated before OpenDP is called. |
| `ORDER BY`, columns outside the schema (`_rn`), table functions, CTEs | fixed | Rejected with `UnsupportedQuery`. |
| Multi-statement, `UNION`, subqueries, `FILTER`, `CASE` in `SUM`, self-join | blocked | |
| Singling out with `WHERE hack_license = ...` | safe | Noise scales with `Δ = k`, not with the count. |
| Keyset of the wrong type | harmless | Result is pure noise; nothing leaks. |

Unverified assumptions: the schema is trusted (unique primary keys, real foreign keys, correct `privacy_id`). In the library alone the budget belongs to the `Ledger` object a `Session` is given; the platform binds every session to the ledger in the catalog file.

## Known limits

- **Integers only.** Amounts are in cents and distances in tenths of a mile. The domain is `i64`: OpenDP's default `int` is `i32` and rejects sums of 2^31 or more.
- **Fixed `k`.** No differentially private threshold discovery (`ε_mf`).
- **Public keyset required** for `GROUP BY`; no `(ε, δ)` thresholding.
- **Basic composition only.** No zCDP.
- **No confidence interval for `AVG`.** The result reports the intervals of its noisy `SUM` and `COUNT` only.
- **No seed.** OpenDP does not expose one; the noise test is statistical (mean absolute error ≈ `Δ/ε` over 200 draws).
- **Dialects.** Only DuckDB executes; Spark and Postgres SQL is generated, snapshotted and parsed back, not run. Consumer SQL is always parsed as DuckDB; the backend dialect shapes the output only. The clamp carries an explicit `CASE WHEN col IS NULL` guard because DuckDB, Spark and Postgres all skip NULL inside `GREATEST`/`LEAST`.
- **Synthetic data.** The demo shows truncation bias: 2% of drivers have 400 trips and are cut to `k = 50`. The real TLC download was blocked in the drafting environment.
