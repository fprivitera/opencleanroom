# SPDX-License-Identifier: Apache-2.0
import math

import pytest
from cleanroom_sql import BudgetExhaustedException, Rewriter, Session, UnsupportedQuery
from cleanroom_sql.taxi import BOROUGHS, SCHEMA, connect, empty_connect

K = 50


@pytest.fixture(scope="module")
def con():
    return connect(n_drivers=300)


@pytest.fixture
def rw():
    return Rewriter(SCHEMA, k=K)


# -- rewriting ----------------------------------------------------------------

def test_truncation_and_clamping_are_injected(rw, con):
    rq = rw.rewrite("SELECT COUNT(*), SUM(fare_amount) FROM trips")
    assert "ROW_NUMBER() OVER (PARTITION BY hack_license ORDER BY trip_id)" in rq.sql
    assert f"_rn <= {K}" in rq.sql
    assert "GREATEST(fare_amount, 0)" in rq.sql and "20000" in rq.sql
    con.execute(rq.sql).fetchall()  # valid DuckDB


def test_truncation_bounds_rows_per_driver(rw, con):
    heavy = con.execute(
        "SELECT hack_license FROM trips GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1"
    ).fetchone()[0]
    assert con.execute(f"SELECT COUNT(*) FROM trips WHERE hack_license='{heavy}'").fetchone()[0] > K
    rq = rw.rewrite(f"SELECT COUNT(*) FROM trips WHERE hack_license = '{heavy}'")
    assert con.execute(rq.sql).fetchone()[0] == K


def test_public_tables_are_not_truncated(rw):
    rq = rw.rewrite(
        "SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough"
    )
    assert rq.sql.count("ROW_NUMBER") == 1 and "JOIN taxi_zones AS z" in rq.sql


# -- sensitivity --------------------------------------------------------------

def test_single_table_sensitivity(rw):
    rq = rw.rewrite("SELECT COUNT(*), SUM(fare_amount), SUM(trip_distance) FROM trips")
    assert [a.sensitivity for a in rq.aggregates] == [K, K * 20_000, K * 500]


def test_public_join_keeps_sensitivity(rw):
    rq = rw.rewrite("SELECT COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id")
    assert rq.rows_per_user == K


def test_private_keyed_join_rule(rw):
    # Row ownership: a driver's trips join only that driver's one keyed row, so Δ = K
    rq = rw.rewrite("SELECT COUNT(*) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license")
    assert rq.rows_per_user == K


# -- rejections ---------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "SELECT COUNT(DISTINCT hack_license) FROM trips",
    "SELECT MAX(fare_amount) FROM trips",
    "SELECT fare_amount, COUNT(*) FROM trips GROUP BY hack_license",
    "SELECT COUNT(*) FROM trips t JOIN drivers d ON t.trip_id = d.hack_license",
    "SELECT COUNT(*) FROM trips t LEFT JOIN drivers d ON t.hack_license = d.hack_license",
    "SELECT COUNT(*) FROM trips GROUP BY hack_license HAVING COUNT(*) > 3",
    "SELECT COUNT(*) FROM (SELECT * FROM trips) s",
    "SELECT COUNT(*) FROM trips AS drivers WHERE drivers.fleet = 'x'",
])
def test_rejected(rw, sql):
    with pytest.raises(UnsupportedQuery):
        rw.rewrite(sql)


# -- executor -----------------------------------------------------------------

def test_group_by_requires_keyset(con):
    s = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
    with pytest.raises(ValueError):
        s.query("SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet", epsilon=0.1)


def test_output_follows_keyset_not_data(con):
    s = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
    keyset = BOROUGHS + ["Atlantis"]
    res = s.query(
        "SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough",
        epsilon=0.5, keyset=keyset,
    )
    assert [r[0] for r in res.rows] == keyset
    assert all(r[1] >= 0 for r in res.rows)  # counts post-processed to >= 0


def test_epsilon_accounting_matches_opendp(con):
    s = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
    res = s.query("SELECT COUNT(*), SUM(fare_amount) FROM trips", epsilon=0.4)
    assert res.epsilon_per_measurement == pytest.approx(0.2)
    assert res.sensitivities == (K, K * 20_000)
    assert res.ci95_halfwidth[0] == pytest.approx(K / 0.2 * math.log(20))
    assert s.spent == pytest.approx(0.4)


def test_noise_scale_is_plausible(con):
    """Mean absolute error of a count over repeats ≈ Laplace scale b = Δ/ε."""
    s = Session(con=con, schema=SCHEMA, epsilon=1000.0, k=K)
    truth = con.execute(Rewriter(SCHEMA, K).rewrite("SELECT COUNT(*) FROM trips").sql).fetchone()[0]
    errs = [abs(s.query("SELECT COUNT(*) FROM trips", epsilon=1.0).rows[0][0] - truth) for _ in range(200)]
    mae, b = sum(errs) / len(errs), K / 1.0
    assert 0.6 * b < mae < 1.4 * b


def test_budget_exhausted(con):
    s = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
    s.query("SELECT COUNT(*) FROM trips", epsilon=0.7)
    with pytest.raises(BudgetExhaustedException):
        s.query("SELECT COUNT(*) FROM trips", epsilon=0.5)
    assert s.spent == pytest.approx(0.7)  # the blocked query consumed nothing


# -- red team -----------------------------------------------------------------

def test_duplicate_keyset_is_deduplicated(con):
    s = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
    res = s.query("SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet", epsilon=0.1, keyset=["fleet_A", "fleet_A"])
    assert [r[0] for r in res.rows] == ["fleet_A"]


@pytest.mark.parametrize("eps", [math.nan, math.inf, 0, -1, "1", True])
def test_degenerate_epsilon_rejected(con, eps):
    s = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
    with pytest.raises((ValueError, BudgetExhaustedException)):
        s.query("SELECT COUNT(*) FROM trips", epsilon=eps)


def test_exact_budget_accounting(con):
    s = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
    for _ in range(10):
        s.query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert s.remaining == 0.0
    with pytest.raises(BudgetExhaustedException):
        s.query("SELECT COUNT(*) FROM trips", epsilon=1e-9)


@pytest.mark.parametrize("sql", [
    "SELECT COUNT(*) FROM trips WHERE _rn > 0",
    "SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet ORDER BY 2",
    "SELECT COUNT(*) FROM read_csv('/etc/passwd')",
    "WITH x AS (SELECT * FROM trips) SELECT COUNT(*) FROM x",
])
def test_red_team_rejections(rw, sql):
    with pytest.raises(UnsupportedQuery):
        rw.rewrite(sql)


def test_sums_beyond_int32_are_supported(con):
    """OpenDP's T=int is i32 and rejects values >= 2**31; the vector domain must be i64."""
    from cleanroom_sql.rewriter import Aggregate
    from cleanroom_sql.session import Session as S
    meas = S._mechanism(Aggregate(0, "sum", "fare_amount", 1_000_000), eps=1.0)
    out = meas([3_000_000_000, -3_000_000_000])
    assert abs(out[0] - 3_000_000_000) < 50_000_000


def test_sum_over_all_null_group_is_zero():
    """SUM over a group with no non-NULL value returns NULL in SQL; the executor must treat it as 0."""
    c = empty_connect()
    c.execute("INSERT INTO drivers VALUES ('HL1', 'fleet_A')")
    c.execute("INSERT INTO trips VALUES (1, 'HL1', 1, NULL, NULL)")
    s = Session(con=c, schema=SCHEMA, epsilon=1.0, k=K)
    rq = Rewriter(SCHEMA, k=K).rewrite("SELECT SUM(fare_amount) FROM trips")
    assert s._true_vectors(rq, None) == ([()], [[0]])
    s.query("SELECT SUM(fare_amount) FROM trips", epsilon=0.1)  # and the noisy path does not crash


# -- WHERE pushdown -------------------------------------------------------------

def test_where_applies_before_truncation():
    """The k rows kept per privacy unit are rows that match the filter."""
    c = empty_connect()
    c.execute("INSERT INTO drivers VALUES ('HL1', 'fleet_A')")
    c.executemany("INSERT INTO trips VALUES (?, 'HL1', 1, 100, 10)", [(i,) for i in range(1, 11)])
    rq = Rewriter(SCHEMA, k=3).rewrite("SELECT COUNT(*) FROM trips WHERE trip_id > 5")
    assert c.execute(rq.sql).fetchone()[0] == 3


def test_where_conjuncts_on_other_tables_stay_outside(rw):
    rq = rw.rewrite(
        "SELECT COUNT(*) FROM trips t "
        "JOIN drivers d ON t.hack_license = d.hack_license "
        "JOIN taxi_zones z ON t.pu_location_id = z.location_id "
        "WHERE t.trip_distance > 5 AND d.fleet = 'fleet_A' AND z.borough = 'Manhattan' AND t.fare_amount < z.location_id"
    )
    sql = rq.sql
    assert "FROM trips WHERE trip_distance > 5) AS _t" in sql  # inside the truncation subquery, unqualified
    outer = sql[sql.index("AS t"):]
    assert "d.fleet = 'fleet_A'" in outer and "z.borough = 'Manhattan'" in outer and "t.fare_amount < z.location_id" in outer
    assert rq.rows_per_user == K  # sensitivity untouched by pushdown


# -- AVG ----------------------------------------------------------------------

def test_avg_is_two_measurements(rw, con):
    rq = rw.rewrite("SELECT COUNT(*), AVG(fare_amount) FROM trips")
    assert [a.kind for a in rq.aggregates] == ["count", "avg"]
    assert [(m.kind, m.sensitivity) for m in rq.measurements] == [("count", K), ("sum", K * 20_000), ("count", K)]
    s = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
    res = s.query("SELECT COUNT(*), AVG(fare_amount) FROM trips", epsilon=0.3)
    assert res.epsilon_per_measurement == pytest.approx(0.1)
    assert res.sensitivities == (K, K * 20_000, K)
    assert len(res.rows) == 1 and len(res.rows[0]) == 2


@pytest.mark.parametrize("sql", [
    "SELECT AVG(pu_location_id) FROM trips",  # no clamp bounds
    "SELECT AVG(DISTINCT fare_amount) FROM trips",
    "SELECT AVG(fare_amount * 2) FROM trips",
    "SELECT SUM(pu_location_id) FROM trips",  # same rule as SUM
])
def test_avg_rejected(rw, sql):
    with pytest.raises(UnsupportedQuery):
        rw.rewrite(sql)


def test_avg_is_finite_when_noisy_count_is_not_positive():
    c = empty_connect()
    s = Session(con=c, schema=SCHEMA, epsilon=100.0, k=K)
    for _ in range(20):
        v = s.query("SELECT AVG(fare_amount) FROM trips", epsilon=0.01).rows[0][0]
        assert math.isfinite(v)


def test_avg_error_scale_is_plausible(con):
    """AVG error ≈ noise of the SUM over the true count: (Δ_sum/ε_sum) / N, plus a small count term."""
    s = Session(con=con, schema=SCHEMA, epsilon=1000.0, k=K)
    rq = Rewriter(SCHEMA, K).rewrite("SELECT AVG(fare_amount) FROM trips")
    true_sum, n = con.execute(rq.sql).fetchone()
    truth = true_sum / n
    errs = [abs(s.query("SELECT AVG(fare_amount) FROM trips", epsilon=1.0).rows[0][0] - truth) for _ in range(200)]
    mae = sum(errs) / len(errs)
    b = (K * 20_000 / 0.5) / n  # ε split in two: the SUM term dominates
    assert 0.6 * b < mae < 1.4 * b


def test_avg_counts_follow_select_order_with_aliases(rw):
    """COUNT(col) must pair with its own AVG even when an alias nests the first AVG deeper."""
    c = empty_connect()
    c.execute("INSERT INTO drivers VALUES ('HL1', 'fleet_A')")
    c.executemany("INSERT INTO trips VALUES (?, 'HL1', 1, ?, ?)", [(1, 100, None), (2, 200, None), (3, None, 10)])
    rq = rw.rewrite("SELECT AVG(fare_amount) AS a, AVG(trip_distance) FROM trips")
    assert rq.sql.index("COUNT(fare_amount)") < rq.sql.index("COUNT(trip_distance)")
    s = Session(con=c, schema=SCHEMA, epsilon=1.0, k=K)
    assert s._true_vectors(rq, None)[1] == [[300], [2], [10], [1]]


@pytest.mark.parametrize("sql", [
    "SELECT COUNT(*) FROM trips WHERE SUM(fare_amount) > 1",
    "SELECT COUNT(*) FROM trips WHERE AVG(fare_amount) > 1",
])
def test_aggregate_in_where_rejected(rw, sql):
    with pytest.raises(UnsupportedQuery):
        rw.rewrite(sql)


# -- row-ownership join rule ----------------------------------------------------

@pytest.mark.parametrize("sql, expected", [
    ("SELECT COUNT(*) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license", K),
    ("SELECT COUNT(*) FROM drivers d JOIN trips t ON d.hack_license = t.hack_license", K),
    ("SELECT COUNT(*) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license "
     "JOIN taxi_zones z ON t.pu_location_id = z.location_id", K),
    ("SELECT COUNT(*) FROM drivers d JOIN trips t ON d.hack_license = t.hack_license "
     "JOIN taxi_zones z ON t.pu_location_id = z.location_id", K),
    ("SELECT COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id", K),
    ("SELECT COUNT(*) FROM taxi_zones z JOIN trips t ON t.pu_location_id = z.location_id", K),  # public base
    ("SELECT COUNT(*) FROM drivers", 1),
    ("SELECT COUNT(*) FROM taxi_zones", 0),  # no private table: nothing to protect
])
def test_join_sensitivity_is_rows_owned_by_one_privacy_unit(rw, sql, expected):
    assert rw.rewrite(sql).rows_per_user == expected


def test_join_to_private_table_not_keyed_by_its_privacy_id_is_rejected():
    """The FK is the privacy ID of one side only: rows leave the privacy unit, so no product rule (story 13)."""
    from cleanroom_sql import Column, Schema, Table
    schema = Schema(tables={
        "accounts": Table(name="accounts", primary_key="account_id", privacy_id="owner",
                          columns=(Column("account_id"), Column("owner"))),
        "events": Table(name="events", primary_key="event_id", privacy_id="account_id",
                        columns=(Column("event_id"), Column("account_id")),
                        foreign_keys={"account_id": ("accounts", "account_id")}),
    })
    with pytest.raises(UnsupportedQuery):
        Rewriter(schema, k=K).rewrite("SELECT COUNT(*) FROM events e JOIN accounts a ON e.account_id = a.account_id")


@pytest.mark.parametrize("sql", [
    # ON repeats an earlier condition and never mentions the joined table: a hidden cross join
    "SELECT COUNT(*) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license "
    "JOIN taxi_zones z ON t.hack_license = d.hack_license",
    "SELECT COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id "
    "JOIN drivers d ON t.pu_location_id = z.location_id",
    # both sides on the joined table: a filter, not a join
    "SELECT COUNT(*) FROM trips t JOIN drivers d ON d.hack_license = d.hack_license",
])
def test_join_on_must_bind_the_joined_table(rw, sql):
    with pytest.raises(UnsupportedQuery):
        rw.rewrite(sql)


# -- input dialect and NULL-propagating clamp ------------------------------------

def test_null_value_contributes_to_neither_sum_nor_count():
    c = empty_connect()
    c.execute("INSERT INTO drivers VALUES ('HL1', 'fleet_A')")
    c.executemany("INSERT INTO trips VALUES (?, 'HL1', 1, ?, NULL)", [(1, 100), (2, None)])
    rq = Rewriter(SCHEMA, k=K).rewrite("SELECT SUM(fare_amount), AVG(fare_amount) FROM trips")
    s = Session(con=c, schema=SCHEMA, epsilon=1.0, k=K)
    assert s._true_vectors(rq, None)[1] == [[100], [100], [1]]


@pytest.mark.parametrize("dialect", ["spark", "postgres"])
def test_consumer_sql_is_parsed_as_duckdb_for_every_backend(dialect):
    """Backtick identifiers parse in Spark but not in DuckDB: the input dialect is DuckDB whatever the backend."""
    import sqlglot
    sql = "SELECT COUNT(*) FROM `trips`"
    sqlglot.parse_one(sql, read="spark")  # Spark would accept it
    with pytest.raises(UnsupportedQuery):
        Rewriter(SCHEMA, k=K, dialect=dialect).rewrite(sql)
    rq = Rewriter(SCHEMA, k=K, dialect=dialect).rewrite("SELECT COUNT(*) FROM trips")
    sqlglot.parse_one(rq.sql, read=dialect)


# -- order_by and query facts ----------------------------------------------------

def test_default_order_by_is_primary_key():
    assert SCHEMA.table("trips").order_by == "trip_id"


def test_rewritten_query_reports_tables_and_group_columns(rw):
    rq = rw.rewrite(
        "SELECT z.borough, COUNT(*) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license "
        "JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough"
    )
    assert rq.tables == ("drivers", "taxi_zones", "trips")
    assert rq.group_columns == (("taxi_zones", "borough"),)


def test_column_type_defaults_to_int_and_bound_needs_int():
    from cleanroom_sql import Column
    assert Column("x").type == "int"
    with pytest.raises(ValueError):
        Column("name", lower=0, upper=1, type="string").bound  # noqa: B018  (the property must raise)


def test_schema_rejects_unknown_type_and_undeclared_order_by():
    from cleanroom_sql import Column, Table
    with pytest.raises(ValueError):
        Column("x", type="weird")
    with pytest.raises(ValueError):
        Table("t", "pk", (Column("pk"), Column("pid")), privacy_id="pid", order_by="nope")


# -- security review, 22 Oct 2026 -------------------------------------------------

@pytest.mark.parametrize("sql", [
    "SELECT COUNT(*) FROM trips WHERE EXISTS (SELECT 1 FROM trips t2 WHERE t2.pu_location_id = trips.pu_location_id)",
    "SELECT COUNT(*) FROM trips WHERE fare_amount > ALL (SELECT fare_amount FROM trips)",
    "SELECT COUNT(*) FROM trips WHERE fare_amount IN (SELECT fare_amount FROM trips)",
    "SELECT COUNT(*) FROM trips WHERE fare_amount > ANY (SELECT 1)",
    "SELECT COUNT(*), (SELECT 1) FROM trips",
    "SELECT COUNT(*) FROM memory.main.drivers",
    "SELECT COUNT(*) FROM main.trips",
    "SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough, z.borough",
])
def test_nested_selects_qualified_tables_and_duplicate_group_by_are_rejected(rw, sql):
    with pytest.raises(UnsupportedQuery):
        rw.rewrite(sql)


def test_backend_error_is_generic_and_charged():
    """A failing backend must not leak its message, and the failed query spends its ε (no free oracle)."""
    from cleanroom_sql.session import QueryFailed

    class Leaky:
        def execute(self, sql):
            raise RuntimeError("Invalid Input Error: fare=1234")
    s = Session(con=Leaky(), schema=SCHEMA, epsilon=1.0, k=K)
    with pytest.raises(QueryFailed) as e:
        s.query("SELECT COUNT(*) FROM trips", epsilon=0.3)
    assert "1234" not in str(e.value) and "fare" not in str(e.value)
    assert s.spent == pytest.approx(0.3)


def test_tiny_epsilon_is_rejected_before_opendp(con):
    s = Session(con=con, schema=SCHEMA, epsilon=1.0, k=K)
    with pytest.raises(ValueError):
        s.query("SELECT COUNT(*) FROM trips", epsilon=1e-320)
    assert s.spent == 0


# -- expression whitelist --------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "SELECT COUNT(*) FROM trips WHERE fare_amount * 9223372036854775807 > 0",   # arithmetic: overflow oracle
    "SELECT COUNT(*) FROM trips WHERE error('fare=' || fare_amount) IS NULL",    # function
    "SELECT COUNT(*) FROM trips WHERE sqrt(fare_amount - 10000) = 1",
    "SELECT COUNT(*) FROM trips WHERE CAST(hack_license AS INT) > 0",            # cast failure oracle
    "SELECT COUNT(*) FROM trips WHERE CASE WHEN fare_amount > 1 THEN 1 ELSE 0 END = 1",
    "SELECT COUNT(*) FROM trips WHERE hack_license LIKE 'HL%'",
    "SELECT COUNT(*) FROM trips WHERE current_setting('home_directory') IS NOT NULL",
    "SELECT COUNT(*) FROM trips WHERE fare_amount IN (trip_distance, 3)",       # IN with a column
    "SELECT COUNT(*) FROM trips WHERE -fare_amount > 0",                          # negated column
    "SELECT COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id + 0",
])
def test_expressions_outside_the_whitelist_are_rejected(rw, sql):
    with pytest.raises(UnsupportedQuery):
        rw.rewrite(sql)


@pytest.mark.parametrize("sql", [
    "SELECT COUNT(*) FROM trips WHERE fare_amount BETWEEN 1 AND 5 AND pu_location_id IN (1, 2, 3)",
    "SELECT COUNT(*) FROM trips WHERE NOT (fare_amount < -5 OR fare_amount IS NULL)",
    "SELECT d.fleet AS f, COUNT(*) AS n FROM trips t JOIN drivers d ON t.hack_license = d.hack_license GROUP BY d.fleet",
])
def test_supported_expressions_still_pass(rw, sql):
    rw.rewrite(sql)


@pytest.mark.parametrize("sql", [
    "SELECT COUNT(*) FROM trips WHERE hack_license > fare_amount",            # string column vs int column: implicit cast
    "SELECT COUNT(*) FROM trips WHERE hack_license = 5",
    "SELECT COUNT(*) FROM trips WHERE fare_amount = 'abc'",
    "SELECT COUNT(*) FROM trips WHERE fare_amount IN ('a', 2)",
    "SELECT COUNT(*) FROM trips t ASOF JOIN drivers d ON t.hack_license = d.hack_license",
    "SELECT COUNT(*) FROM trips t JOIN drivers d USING (hack_license)",
])
def test_type_mixing_and_exotic_joins_are_rejected(rw, sql):
    with pytest.raises(UnsupportedQuery):
        rw.rewrite(sql)
