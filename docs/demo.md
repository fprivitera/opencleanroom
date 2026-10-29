# Demo: from an empty directory to the console

Everything runs locally on the synthetic taxi dataset (300 drivers, heavy-tailed trips). Budgets live in the catalog ledger, shared by every process on the file.

## Set up

```bash
mkdir demo && cd demo
PYTHON=../.venv/bin/python PATH=../.venv/bin:$PATH POLICY=../packages/cleanroom-sql/examples/taxi.yaml \
  bash ../packages/opencleanroom/examples/quickstart.sh      # DuckDB file, catalog, policy, key, one noisy count
opencleanroom consumer revoke nyc_taxi analytics-team          # the script did not print the key; make one you can copy
opencleanroom consumer create nyc_taxi analytics-team          # all tables, count/sum/avg, 0.5 per query, cap 5.0
opencleanroom consumer create nyc_taxi counts-only             # trips + taxi_zones, count only, 0.2 per query
opencleanroom console                                          # http://localhost:8501
```

## What to show

**Admin section**
1. The policies table: `nyc_taxi`, version 1, epoch `2026-10`, budget 10, remaining 10.
2. Upload `packages/cleanroom-sql/examples/taxi.yaml` with `aggregates: [count, max]`: every fault with its YAML path, nothing stored.
3. Upload it with `epsilon: "12.0"`: rejected, "raises the budget … inside epoch 2026-10; start a new epoch (ADR 0004)".
4. Create a key for a name not in the policy: rejected. Revoke `counts-only`, the consumer section stops accepting that key with the same message as a wrong key.

**Consumer section** (paste the `analytics-team` key, click an example, press Run)

The gallery has two columns. "Passes" shows the supported grammar at work; "Blocked on purpose" shows the rewriter and the policy refusing. Each button fills the SQL box and ε and shows a note; the result banner says `accepted` or `blocked: <error type>`.

| Example | What to point at |
|---|---|
| What can I query? / Columns of trips | metadata from the policy, no ε |
| Count of trips | the rewritten SQL: `ROW_NUMBER() OVER (PARTITION BY hack_license …) WHERE _rn <= 50`, sensitivity 50 |
| Count and total fare | two measurements, ε split in two, the clamp `LEAST(GREATEST(fare_amount, 0), 20000)` |
| Average fare | AVG = noisy SUM / noisy COUNT |
| Trips per borough | keys from the public table; small boroughs are pure noise |
| Drivers per fleet | keyset declared inline; keyed table, Δ = 1 |
| Long, cheap trips | two conjuncts pushed inside the truncation subquery |
| Fleet × borough with a filter | three tables, Δ = k (row ownership), keyset 3 × 6 |
| Average fare per borough with counts | three measurements per group |
| MAX, COUNT(DISTINCT), Subquery, CTE, UNION | `UnsupportedQuery`: the grammar is a whitelist |
| GROUP BY without a keyset | `PolicyViolation` (ADR 0005) |
| Hidden cross join | the `ON` must bind the joined table |
| Arithmetic in WHERE, CAST as an oracle | nothing outside the whitelist reaches the backend |
| SUM of an expression | the clamp applies to a column |
| ε above the per-query maximum | `PolicyViolation` from the consumer's policy |
| Table outside the policy | with the `counts-only` key: unknown table |

Then paste the `counts-only` key and run "count and total fare": `PolicyViolation: aggregate sum is not allowed`. Run "count of trips" three times with ε 0.2 and watch remaining ε drop; the cap is the dataset budget here because `counts-only` has no cap.

**Noise intuition**: run "count of trips" five times with ε 0.1; the answers move by about ±500 (scale `Δ/ε = 50 / 0.1`). With ε 0.5 they move by about ±100. Every run spends.

## From the terminal

```bash
KEY=$(cat analytics-team.key)
opencleanroom query --key "$KEY" "SELECT COUNT(*) FROM trips" --epsilon 0.1
opencleanroom query --key "$KEY" "SELECT z.borough, AVG(t.fare_amount), COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough" --epsilon 0.3 --json
opencleanroom policy show nyc_taxi
cleanroom-sql policy check ../packages/cleanroom-sql/examples/two_private_tables.yaml
```

## Reset

Delete the `demo` directory. Nothing else is written.
