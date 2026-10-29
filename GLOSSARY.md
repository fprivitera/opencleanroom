# OpenCleanroom

A self-hosted SQL clean room: an admin connects tables and writes policies, and consumers query them in SQL and receive only differentially private answers.

## Language

### Privacy

**Privacy unit**:
The entity the guarantee protects. Adding or removing every record of one privacy unit does not change an answer in a distinguishable way.
_Avoid_: user, subject, user_id (a column name, not the concept)

**Privacy ID**:
The column that identifies the privacy unit in a private table.

**Public table**:
A table with no privacy ID. Consumers can read it in clear at no budget cost (lookups, dimensions).
_Avoid_: reference table, dimension table

**Keyed table**:
A private table with exactly one row per privacy unit (its primary key is the privacy ID).

**Truncation**:
The cap of `k` rows per privacy unit applied before any aggregation. It introduces bias and makes sensitivity finite.
_Avoid_: contribution bounding, row clipping

**Clamping**:
The restriction of a numeric value to a declared interval `[a, b]`. Not the same thing as truncation.
_Avoid_: clipping, bounding

**Sensitivity**:
The maximum L1 change of the output vector when one privacy unit changes, after truncation and clamping.

**Keyset**:
The public list of `GROUP BY` keys a query reports. Keys absent from the data appear as zero plus noise.
_Avoid_: key domain, known categories

**Budget**:
The amount of ε, and of δ, available for queries on one dataset within one budget epoch. Each query spends part of it, and an exhausted budget blocks further queries until the admin starts a new budget epoch.
_Avoid_: privacy loss (the measure, not the reserve), pool

**Delta (δ)**:
The probability with which a query's privacy guarantee may fail outright. Part of the dataset's budget, set by the admin from zero upward; zero means the pure-ε guarantee (ADR 0011).
_Avoid_: failure probability, slack

**Budget epoch**:
A period during which one budget applies to one snapshot of a dataset. It ends only when the admin records a new one, with a snapshot and a reason; spending is never reset inside it (ADR 0004).
_Avoid_: period, window, reset

**Consumer cap**:
The most ε one consumer may spend within a budget epoch. It draws from the budget and never adds to it.
_Avoid_: consumer budget, quota

**Measurement**:
One noisy value the executor releases: a `COUNT` or a `SUM` with its own sensitivity and share of ε. `AVG` is two measurements, a `SUM` and a `COUNT` over the same rows.
_Avoid_: noisy aggregate, output column

**Policy violation**:
The rejection of a query that is valid SQL but outside the consumer's policy: a table or aggregate not allowed, ε above the per-query maximum, a `GROUP BY` without a declared keyset, or a consumer-supplied keyset. Raised before any budget moves.
_Avoid_: permission error, access denied

**Ledger**:
The record of ε and δ reserved and spent per consumer and in total for one dataset and budget epoch, shared by every entry point. A reservation left by a crash counts as spent until an admin releases it.
_Avoid_: budget tracker, meter

**Request id**:
The idempotency key of one query, chosen by the consumer or generated for it. Unique per dataset and consumer. A finished request id is **replayed**: the stored response returns and nothing is spent; one still reserved is **in progress** and answers with a conflict.
_Avoid_: correlation id, nonce

**Audit log**:
The append-only record of every request: consumer, request id, SQL, rewritten query, ε requested and charged, outcome. It always agrees with the ledger; a disagreement is a fault.
_Avoid_: history, trace

**Materialized table**:
A table that holds exactly the rows truncation keeps for one table, `k` and epoch, created by the platform itself. It is read in place of the truncation only when nothing filters that table first.
_Avoid_: stored copy, first-k copy, physical table, truncated copy

**Rewritten query**:
A consumer's SQL after truncation and clamping. It has finite sensitivity and contains no noise.

### Roles and parts

**Admin**:
The person who connects tables, declares their privacy metadata and writes policies.
_Avoid_: data owner, operator

**Consumer**:
A person, application or LLM agent that sends SQL to the service and receives only noisy results.
_Avoid_: analyst, client, user

**Operator**:
The party that runs the service infrastructure and can read raw data unless technical safeguards prevent it.
_Avoid_: host, provider

**Policy**:
The declarative rules that bound what one consumer may ask: tables, aggregates, per-query ε and budget.
_Avoid_: analysis rule, permissions

**Configured table**:
A table the admin has registered with its privacy metadata, ready to be bound by policies.
_Avoid_: dataset, mapped table

**Backend**:
The engine that executes rewritten queries (DuckDB, Spark, later Athena).
_Avoid_: source, database, connector

**Rewriter**:
The component that turns a consumer's SQL into a rewritten query. It never sees data.

**Executor**:
The trusted component that runs the rewritten query, adds noise and enforces the budget. It is the only component that sees raw aggregates.
_Avoid_: session (the class name, not the role), middleware

**Control plane**:
The part admins use to register configured tables and write policies. It carries no consumer queries.

**Data plane**:
The part that serves consumer queries: policy check, rewriting, execution, noise and budget.
