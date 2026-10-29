# SPDX-License-Identifier: Apache-2.0
"""Adversarial probes against the PoC. Prints PASS (blocked / safe) or FAIL."""
import math
import sys

from cleanroom_sql import BudgetExhaustedException, Rewriter, Session, UnsupportedQuery
from cleanroom_sql.taxi import SCHEMA, connect

con = connect(n_drivers=300)
K = 50
rw = Rewriter(SCHEMA, k=K)


RESULTS = []


def probe(name, fn, expect_block=True, known=None):
    """known=<limit>: one of the known limits in SECURITY.md; it does not fail the gate, and FAILs if the hole closes."""
    try:
        out = fn()
        if known:
            status = f"KNOWN {known}" if out else f"FAIL (the KNOWN hole {known} closed; update SECURITY.md)"
        else:
            status = "FAIL (accepted)" if expect_block else (f"PASS -> {out}" if out else f"FAIL -> {out}")
    except (UnsupportedQuery, ValueError, KeyError, BudgetExhaustedException) as e:
        status = f"PASS (blocked: {type(e).__name__}: {str(e)[:70]})" if expect_block else f"FAIL (blocked: {e})"
    except Exception as e:
        status = f"?? {type(e).__name__}: {str(e)[:80]}"
    RESULTS.append(status)
    print(f"{name:48} {status}")


R = lambda sql: (lambda: rw.rewrite(sql).sql)

POLICY_YAML = """
version: 1
dataset:
  name: nyc_taxi
  privacy_unit: {column: hack_license}
  truncation: {k: 50}
  tables:
    taxi_zones: {public: true, primary_key: location_id, columns: {location_id: {type: int}, borough: {type: string}, zone: {type: string}}}
    drivers: {primary_key: hack_license, columns: {hack_license: {type: string}, fleet: {type: string}}}
    trips:
      primary_key: trip_id
      foreign_keys: {hack_license: drivers.hack_license, pu_location_id: taxi_zones.location_id}
      columns: {trip_id: {type: int}, hack_license: {type: string}, pu_location_id: {type: int}, fare_amount: {type: int, clamp: [0, 20000]}, trip_distance: {type: int, clamp: [0, 500]}}
  budget: {epsilon: "1.0"}
  keysets: {taxi_zones.borough: {from: taxi_zones.borough}, drivers.fleet: [medallion, fleet_A, fleet_B]}
consumers:
  counts-only: {tables: [trips, taxi_zones], aggregates: [count], max_epsilon_per_query: 0.2}
"""

print("--- parser / rewriter ---")
probe("multi-statement injection", R("SELECT COUNT(*) FROM trips; DROP TABLE trips"))
probe("table function read_csv", R("SELECT COUNT(*) FROM read_csv('/etc/passwd')"))
probe("string table (parquet path)", R("SELECT COUNT(*) FROM 'x.parquet'"))
probe("self-join to multiply rows", R("SELECT COUNT(*) FROM trips a JOIN trips b ON a.hack_license = b.hack_license"))
probe("hidden cross join (ON ignores joined table)", R("SELECT COUNT(*) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license JOIN taxi_zones z ON t.hack_license = d.hack_license"))
probe("EXISTS subquery (nested SELECT)", R("SELECT COUNT(*) FROM trips WHERE EXISTS (SELECT 1 FROM trips t2 WHERE t2.pu_location_id = trips.pu_location_id)"))
probe("ALL subquery (nested SELECT)", R("SELECT COUNT(*) FROM trips WHERE fare_amount > ALL (SELECT fare_amount FROM trips)"))
probe("catalog-qualified table name", R("SELECT COUNT(*) FROM memory.main.drivers"))
probe("duplicate GROUP BY column (keyset blow-up)", R("SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough, z.borough"))
probe("Boenisch oracle: arithmetic overflow in WHERE", R("SELECT COUNT(*) FROM trips WHERE hack_license = 'HL000001' AND fare_amount * 9223372036854775807 > 0"))
probe("Boenisch oracle: CASE + CAST in WHERE", R("SELECT COUNT(*) FROM trips WHERE CASE WHEN hack_license = 'HL000001' AND fare_amount <= 400 THEN CAST('x' AS INT) = 1 ELSE TRUE END"))
probe("function in WHERE (error())", R("SELECT COUNT(*) FROM trips WHERE error('fare=' || fare_amount) IS NULL"))
probe("implicit cast via mixed-type comparison", R("SELECT COUNT(*) FROM trips WHERE hack_license > fare_amount"))
probe("join on non-key column", R("SELECT COUNT(*) FROM trips t JOIN drivers d ON t.pu_location_id = d.hack_license"))
probe("column list on a table alias (positional rename)", R("SELECT COUNT(*) FROM trips t JOIN drivers AS d (fleet, hack_license) ON t.hack_license = d.hack_license"))
probe("GROUP BY column not in SELECT (merged groups)", R("SELECT COUNT(*) FROM trips GROUP BY pu_location_id"))
probe("string column as a predicate (cast error oracle)", R("SELECT COUNT(*) FROM trips WHERE hack_license"))
probe("string column inside NOT/AND as a predicate", R("SELECT COUNT(*) FROM trips WHERE NOT (hack_license) AND fare_amount > 1"))
probe("reference internal _rn column", R("SELECT COUNT(*) FROM trips WHERE _rn > 0"))
probe("SUM of expression (scale bypass)", R("SELECT SUM(fare_amount * 1000) FROM trips"))
probe("SUM with CAST", R("SELECT SUM(CAST(fare_amount AS DOUBLE)) FROM trips"))
probe("SUM FILTER clause", R("SELECT SUM(fare_amount) FILTER (WHERE trip_id = 1) FROM trips"))
probe("COUNT(col) ok but COUNT(DISTINCT)", R("SELECT COUNT(DISTINCT hack_license) FROM trips"))
probe("unbounded column in SUM", R("SELECT SUM(pu_location_id) FROM trips"))
probe("CTE smuggling", R("WITH x AS (SELECT * FROM trips) SELECT COUNT(*) FROM x"))
probe("ORDER BY on aggregate (row-order leak)", R("SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet ORDER BY 2"))
probe("UNION", R("SELECT COUNT(*) FROM trips UNION ALL SELECT COUNT(*) FROM drivers"))
probe("scalar subquery in WHERE", R("SELECT COUNT(*) FROM trips WHERE fare_amount > (SELECT MAX(fare_amount) FROM trips)"))
probe("CASE inside SUM arg", R("SELECT SUM(CASE WHEN hack_license='HL000001' THEN fare_amount END) FROM trips"))
probe("alias shadowing a table name", R("SELECT COUNT(*) FROM trips AS drivers"), expect_block=False)
probe("WHERE pushed inside truncation", lambda: "FROM trips WHERE trip_id > 5) AS _t" in rw.rewrite("SELECT COUNT(*) FROM trips WHERE trip_id > 5").sql, expect_block=False)

print("--- executor / budget ---")
s = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
probe("epsilon = NaN", lambda: s.query("SELECT COUNT(*) FROM trips", epsilon=math.nan).rows)
probe("epsilon = inf", lambda: s.query("SELECT COUNT(*) FROM trips", epsilon=math.inf).rows)
probe("epsilon negative", lambda: s.query("SELECT COUNT(*) FROM trips", epsilon=-1).rows)
probe("epsilon as string", lambda: s.query("SELECT COUNT(*) FROM trips", epsilon="1").rows)


def drip():
    s2 = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
    for _ in range(10_000):
        s2.query("SELECT COUNT(*) FROM trips", epsilon=1e-4)
    assert s2.remaining == 0.0, s2.remaining  # exact decimal accounting: no drift
    s2.query("SELECT COUNT(*) FROM trips", epsilon=1e-4)  # the 10,001st must be blocked


probe("float drift: 10k x 1e-4 exhausts, 10,001st blocked", drip)


def tiny_eps_scale():
    s3 = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
    return s3.query("SELECT COUNT(*) FROM trips", epsilon=1e-300).ci95_halfwidth


probe("epsilon 1e-300 (scale overflow): rejected before OpenDP", tiny_eps_scale)
probe("keyset with duplicate keys", lambda: s.query("SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet", epsilon=0.01, keyset=["fleet_A", "fleet_A"]).rows, expect_block=False)
probe("keyset with wrong type (int for str)", lambda: s.query("SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet", epsilon=0.01, keyset=[1, 2]).rows, expect_block=False)
probe("single-user WHERE (singling out)", lambda: s.query("SELECT COUNT(*) FROM trips WHERE hack_license = 'HL000001'", epsilon=0.01).ci95_halfwidth, expect_block=False)
probe("GROUP BY privacy_id with full keyset", lambda: s.query("SELECT hack_license, COUNT(*) FROM trips GROUP BY hack_license", epsilon=0.01, keyset=["HL000001"]).ci95_halfwidth, expect_block=False)

print("--- known attacks from the literature ---")
from cleanroom_sql import Column, QueryFailed, Schema, Table  # noqa: E402
from cleanroom_sql.taxi import empty_connect  # noqa: E402


def _l1(sql, rows_with, rows_without, schema=SCHEMA, k=3):
    """L1 change of every measurement between a dataset and the one without the victim (no noise, the executor's seam)."""
    def vectors(trips):
        c = empty_connect()
        c.executemany("INSERT INTO taxi_zones VALUES (?, ?, ?)", [(i, b, f"z{i}") for i, b in enumerate(["Manhattan", "Brooklyn", "Queens", "EWR"], 1)])
        c.executemany("INSERT INTO drivers VALUES (?, ?)", [("V", "fleet_A"), ("O", "fleet_B")])
        if trips:
            c.executemany("INSERT INTO trips VALUES (?, ?, ?, ?, ?)", trips)
        return Session(con=c, schema=schema, epsilon=1.0, k=k)._true_vectors(rq, keyset)[1]
    rq = Rewriter(schema, k=k).rewrite(sql)
    keyset = ["Manhattan", "Brooklyn", "Queens", "EWR"] if rq.key_positions else None
    a, b = vectors(rows_with), vectors(rows_without)
    return [(m.sensitivity, sum(abs(x - y) for x, y in zip(u, v))) for m, u, v in zip(rq.measurements, a, b)]


victim = [(i, "V", 1 + i % 4, 100, 10) for i in range(1, 9)]  # one driver, 8 trips across every borough
others = [(100 + i, "O", 1, 100, 10) for i in range(3)]
probe("a unit in every group stays within Δ = k",
      lambda: all(l1 <= d for d, l1 in _l1("SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough", victim + others, others)),
      expect_block=False)


def _dup_public_key():
    """Attack 3: the admin declares taxi_zones.location_id as a primary key but the data has it twice: every trip doubles."""
    sql = "SELECT COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id"
    rq = Rewriter(SCHEMA, k=3).rewrite(sql)
    def vectors(trips):
        c = empty_connect()
        c.executemany("INSERT INTO taxi_zones VALUES (?, ?, ?)", [(1, "Manhattan", "a"), (1, "Manhattan", "b")])
        c.executemany("INSERT INTO drivers VALUES (?, ?)", [("V", "fleet_A")])
        if trips:
            c.executemany("INSERT INTO trips VALUES (?, ?, ?, ?, ?)", trips)
        return Session(con=c, schema=SCHEMA, epsilon=1.0, k=3)._true_vectors(rq, None)[1]
    l1 = abs(vectors([(i, "V", 1, 100, 10) for i in range(1, 4)])[0][0] - vectors([])[0][0])
    return l1 > rq.measurements[0].sensitivity  # the bound is broken: trusted metadata, see SECURITY.md


probe("duplicated public primary key breaks the bound (admin trust)", _dup_public_key, expect_block=False, known="declared keys trusted")


def _probe_query_failed(fn):
    try:
        fn()
    except QueryFailed:
        raise ValueError("QueryFailed (generic, charged)") from None  # counted as blocked by probe()
    return "a value came back"


def _finite_sum():
    """Attack 2: clamp bounds near the i64 edge; the loader bounds k × bound, not the total over units."""
    schema = Schema(tables={"drivers": SCHEMA.table("drivers"), "taxi_zones": SCHEMA.table("taxi_zones"),
                            "trips": Table(name="trips", primary_key="trip_id", privacy_id="hack_license",
                                           columns=(Column("trip_id"), Column("hack_license", type="string"), Column("pu_location_id"),
                                                    Column("fare_amount", lower=0, upper=2**61), Column("trip_distance", lower=0, upper=500)),
                                           foreign_keys=SCHEMA.table("trips").foreign_keys)})
    import duckdb
    c = duckdb.connect()  # BIGINT fares: the synthetic fixture uses INT32
    c.execute("CREATE TABLE taxi_zones(location_id INT, borough VARCHAR, zone VARCHAR)")
    c.execute("CREATE TABLE drivers(hack_license VARCHAR, fleet VARCHAR)")
    c.execute("CREATE TABLE trips(trip_id INT, hack_license VARCHAR, pu_location_id INT, fare_amount BIGINT, trip_distance INT)")
    c.executemany("INSERT INTO drivers VALUES (?, ?)", [(f"D{i}", "fleet_A") for i in range(8)])
    c.executemany("INSERT INTO trips VALUES (?, ?, ?, ?, ?)", [(i, f"D{i}", 1, 2**61, 1) for i in range(8)])
    s = Session(con=c, schema=schema, epsilon=1000.0, k=1)
    return s.query("SELECT SUM(fare_amount) FROM trips", epsilon=100.0)  # total 2^64: must fail closed, charged


probe("a sum beyond i64 is a generic failure, not a value", lambda: _probe_query_failed(_finite_sum))


def _budget_channel():
    """Attack 6: the same query with and without the victim must cost the same and fail with the same text on every path."""
    texts, spends = set(), set()
    for trips in ([(1, "V", 1, 100, 10)], []):
        c = empty_connect()
        c.executemany("INSERT INTO drivers VALUES (?, ?)", [("V", "fleet_A")])
        if trips:
            c.executemany("INSERT INTO trips VALUES (?, ?, ?, ?, ?)", trips)
        s = Session(con=c, schema=SCHEMA, epsilon=1.0, k=3)
        for sql in ("SELECT MAX(fare_amount) FROM trips", "SELECT COUNT(*) FROM trips WHERE hack_license = 'V' AND fare_amount * 2 > 1",
                    "SELECT COUNT(*) FROM trips WHERE hack_license = 'V'"):
            try:
                s.query(sql, epsilon=0.1)
                texts.add((sql, "ok"))
            except Exception as e:
                texts.add((sql, type(e).__name__, str(e)))
        spends.add(s.spent)
    return len(spends) == 1 and len(texts) == 3  # one outcome per query whatever the data


probe("identical cost and text with and without the victim", _budget_channel, expect_block=False)


def _order_by_lie():
    """Attack 17: the admin marks a non-unique column unique and orders truncation by it; is the kept set stable?"""
    schema = Schema(tables={**SCHEMA.tables, "trips": Table(name="trips", primary_key="trip_id", privacy_id="hack_license",
                                                           columns=SCHEMA.table("trips").columns, foreign_keys=SCHEMA.table("trips").foreign_keys,
                                                           order_by="fare_amount")})
    rq = Rewriter(schema, k=2).rewrite("SELECT SUM(trip_distance) FROM trips")
    c = empty_connect()
    c.executemany("INSERT INTO drivers VALUES (?, ?)", [("V", "fleet_A")])
    c.executemany("INSERT INTO trips VALUES (?, ?, ?, ?, ?)", [(i, "V", 1, 5, i) for i in range(1, 6)])  # same fare, distinct distances
    runs = {tuple(Session(con=c, schema=schema, epsilon=1.0, k=2)._true_vectors(rq, None)[1][0]) for _ in range(5)}
    return len(runs) == 1  # stable on this engine; still a bias the admin owns


probe("truncation order on a lied-about unique column is stable here", _order_by_lie, expect_block=False)

print("--- policy ---")
from cleanroom_sql import loads_policy  # noqa: E402

ps = Session.from_policy(loads_policy(POLICY_YAML), "counts-only", con)
probe("policy: table outside the consumer's policy", lambda: ps.query("SELECT COUNT(*) FROM drivers", epsilon=0.1).rows)
probe("policy: aggregate outside the policy", lambda: ps.query("SELECT SUM(fare_amount) FROM trips", epsilon=0.1).rows)
probe("policy: epsilon above max_epsilon_per_query", lambda: ps.query("SELECT COUNT(*) FROM trips", epsilon=0.5).rows)
probe("policy: consumer-supplied keyset", lambda: ps.query("SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough", epsilon=0.1, keyset=["Manhattan"]).rows)
probe("policy: GROUP BY on a column without a keyset", lambda: ps.query("SELECT pu_location_id, COUNT(*) FROM trips GROUP BY pu_location_id", epsilon=0.1).rows)
probe("policy: keyset from the policy works", lambda: len(ps.query("SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough", epsilon=0.1).rows), expect_block=False)

bad = [r for r in RESULTS if r.startswith(("FAIL", "??"))]
known = [r for r in RESULTS if r.startswith("KNOWN")]
print(f"\n{len(RESULTS)} probes, {len(bad)} failing, {len(known)} known")
sys.exit(1 if bad else 0)
