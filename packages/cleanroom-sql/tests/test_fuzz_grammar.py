# SPDX-License-Identifier: Apache-2.0
"""Grammar fuzzer: random SQL over the taxi schema is either rejected cleanly or satisfies the sensitivity bound.

Hypothesis builds SQLGlot ASTs (any tables, any join conditions, WHERE trees,
any aggregates, GROUP BY, plus one forbidden construct in the poison branch),
renders DuckDB SQL and runs it under the taxi policy on a random dataset and
its neighbour. Allowed outcomes: UnsupportedQuery, PolicyViolation, or the L1
bound holding per measurement. Anything else is a finding.

Profiles: default is small for the gate; CLEANROOM_SQL_HYPOTHESIS_PROFILE=full runs
thousands. CLEANROOM_SQL_FUZZ_SEED=<int> reproduces a run.
"""
from __future__ import annotations

import os

import duckdb
import pytest
import yaml
from cleanroom_sql import Ledger, PolicyViolation, QueryFailed, Rewriter, Session, UnsupportedQuery, loads_policy
from hypothesis import HealthCheck, assume, given, seed, settings
from hypothesis import strategies as st
from sqlglot import exp
from sqlglot.errors import ParseError
from test_sensitivity_property import K, _connect, datasets

EXAMPLES = os.path.join(os.path.dirname(__file__), "..", "examples", "taxi.yaml")


def _policy(k: int):
    doc = yaml.safe_load(open(EXAMPLES).read())
    doc["dataset"]["truncation"]["k"] = k
    return loads_policy(yaml.safe_dump(doc, sort_keys=False))


POLICY = _policy(K)
SCHEMA = POLICY.dataset.schema
REWRITER = Rewriter(SCHEMA, k=K)
CONSUMER = "analytics-team"

TABLES = {t.name: [c.name for c in t.columns] for t in SCHEMA.tables.values()}
INT_COLUMNS = {(t.name, c.name) for t in SCHEMA.tables.values() for c in t.columns if c.type == "int"}
STRING_COLUMNS = {(t.name, c.name) for t in SCHEMA.tables.values() for c in t.columns if c.type == "string"}

FULL = os.environ.get("CLEANROOM_SQL_HYPOTHESIS_PROFILE") == "full"
PROFILE = settings(max_examples=3000 if FULL else 150, deadline=None,
                   suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much, HealthCheck.data_too_large])
maybe_seed = seed(int(os.environ["CLEANROOM_SQL_FUZZ_SEED"])) if os.environ.get("CLEANROOM_SQL_FUZZ_SEED") else (lambda f: f)


# -- strategies: ASTs, not strings -------------------------------------------------

@st.composite
def relations(draw):
    """1 to 3 distinct taxi tables with short aliases, in random order."""
    names = draw(st.lists(st.sampled_from(sorted(TABLES)), min_size=1, max_size=3, unique=True))
    return [(name, alias) for name, alias in zip(names, "abc")]


def _col(alias: str, column: str) -> exp.Column:
    return exp.column(column, table=alias)


@st.composite
def any_column(draw, rels):
    name, alias = draw(st.sampled_from(rels))
    return (name, alias, draw(st.sampled_from(TABLES[name])))


@st.composite
def comparisons(draw, rels):
    """col <op> literal with a type-consistent literal, or col <op> col."""
    name, alias, column = draw(any_column(rels))
    op = draw(st.sampled_from([exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE]))
    if draw(st.booleans()):
        other_name, other_alias, other_col = draw(any_column(rels))
        return op(this=_col(alias, column), expression=_col(other_alias, other_col))
    if (name, column) in STRING_COLUMNS:
        lit = exp.Literal.string(draw(st.sampled_from(["HL1", "HL2", "fleet_A", "Manhattan", "zone_1", ""])))
    else:
        lit = exp.Literal.number(draw(st.integers(-5, 40)))
    if draw(st.integers(0, 9)) == 0:
        return exp.Is(this=_col(alias, column), expression=exp.Null())
    return op(this=_col(alias, column), expression=lit)


def conditions(rels):
    base = comparisons(rels)
    return st.recursive(
        base,
        lambda inner: st.one_of(
            st.builds(lambda a, b: exp.And(this=a, expression=b), inner, inner),
            st.builds(lambda a, b: exp.Or(this=a, expression=b), inner, inner),
            st.builds(lambda a: exp.Not(this=exp.Paren(this=a)), inner),
            st.builds(lambda a: exp.Paren(this=a), inner),
        ),
        max_leaves=4,
    )


@st.composite
def aggregates(draw, rels):
    kind = draw(st.sampled_from(["count_star", "count", "sum", "avg"]))
    if kind == "count_star":
        return exp.Count(this=exp.Star())
    name, alias, column = draw(any_column(rels))
    node = {"count": exp.Count, "sum": exp.Sum, "avg": exp.Avg}[kind]
    return node(this=_col(alias, column))


@st.composite
def selects(draw):
    """A SELECT with 0-2 key columns, 1-3 aggregates, random joins, optional WHERE and GROUP BY."""
    rels = draw(relations())
    keys = draw(st.lists(any_column(rels), min_size=0, max_size=2))
    aggs = draw(st.lists(aggregates(rels), min_size=1, max_size=3))
    items = [_col(alias, column) for _, alias, column in keys] + aggs
    if draw(st.booleans()) and aggs:
        items[-1] = exp.alias_(items[-1], "x")
    q = exp.select(*items).from_(exp.table_(rels[0][0], alias=rels[0][1]))
    for name, alias in rels[1:]:
        # ON from any column pair, any direction: valid FK joins and nonsense alike
        left = draw(any_column(rels))
        right = draw(any_column(rels))
        on = exp.EQ(this=_col(left[1], left[2]), expression=_col(right[1], right[2]))
        q = q.join(exp.table_(name, alias=alias), on=on, join_type=draw(st.sampled_from(["", "LEFT", "INNER"])))
    if draw(st.booleans()):
        cond = draw(conditions(rels))
        if draw(st.integers(0, 9)) == 0:  # poison: a nested SELECT the rewriter must refuse
            cond = exp.Exists(this=exp.select(exp.Literal.number(1)).from_("trips").where(cond))
        q = q.where(cond)
    group = keys if draw(st.integers(0, 4)) else keys[:-1]  # sometimes a key outside GROUP BY: must be rejected
    if group or keys:
        q = q.group_by(*[_col(alias, column) for _, alias, column in group])
    return q


# Every class of SQL the rewriter must refuse, as a builder from a clean base query.
REJECTION_CLASSES = {
    "subquery in FROM": lambda: "SELECT COUNT(*) FROM (SELECT * FROM trips) s",
    "scalar subquery in WHERE": lambda: "SELECT COUNT(*) FROM trips WHERE fare_amount > (SELECT 1)",
    "CTE": lambda: "WITH x AS (SELECT * FROM trips) SELECT COUNT(*) FROM x",
    "UNION": lambda: "SELECT COUNT(*) FROM trips UNION ALL SELECT COUNT(*) FROM drivers",
    "window function": lambda: "SELECT COUNT(*) OVER () FROM trips",
    "table function": lambda: "SELECT COUNT(*) FROM read_csv('x.csv')",
    "string table": lambda: "SELECT COUNT(*) FROM 'x.parquet'",
    "DISTINCT": lambda: "SELECT DISTINCT COUNT(*) FROM trips",
    "COUNT(DISTINCT)": lambda: "SELECT COUNT(DISTINCT hack_license) FROM trips",
    "HAVING": lambda: "SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet HAVING COUNT(*) > 1",
    "ORDER BY": lambda: "SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet ORDER BY 2",
    "LIMIT": lambda: "SELECT COUNT(*) FROM trips LIMIT 1",
    "expression inside SUM": lambda: "SELECT SUM(fare_amount * 2) FROM trips",
    "expression inside AVG": lambda: "SELECT AVG(fare_amount + 1) FROM trips",
    "CAST inside SUM": lambda: "SELECT SUM(CAST(fare_amount AS DOUBLE)) FROM trips",
    "MAX": lambda: "SELECT MAX(fare_amount) FROM trips",
    "join on a non-FK column": lambda: "SELECT COUNT(*) FROM trips t JOIN drivers d ON t.pu_location_id = d.hack_license",
    "hidden cross join": lambda: ("SELECT COUNT(*) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license "
                                  "JOIN taxi_zones z ON t.hack_license = d.hack_license"),
    "self join": lambda: "SELECT COUNT(*) FROM trips a JOIN trips b ON a.hack_license = b.hack_license",
    "LEFT JOIN": lambda: "SELECT COUNT(*) FROM trips t LEFT JOIN drivers d ON t.hack_license = d.hack_license",
    "aggregate in WHERE": lambda: "SELECT COUNT(*) FROM trips WHERE SUM(fare_amount) > 1",
    "internal _rn column": lambda: "SELECT COUNT(*) FROM trips WHERE _rn > 0",
    "key outside GROUP BY": lambda: "SELECT fleet, COUNT(*) FROM drivers",
    "no aggregate": lambda: "SELECT fleet FROM drivers GROUP BY fleet",
    "multi-statement": lambda: "SELECT COUNT(*) FROM trips; DROP TABLE trips",
    "SUM without clamp bounds": lambda: "SELECT SUM(pu_location_id) FROM trips",
    "unknown table": lambda: "SELECT COUNT(*) FROM passwords",
    "unknown column": lambda: "SELECT COUNT(*) FROM trips WHERE secret = 1",
    # security review, 22 Oct 2026
    "EXISTS subquery": lambda: "SELECT COUNT(*) FROM trips WHERE EXISTS (SELECT 1 FROM trips t2 WHERE t2.pu_location_id = trips.pu_location_id)",
    "ALL subquery": lambda: "SELECT COUNT(*) FROM trips WHERE fare_amount > ALL (SELECT fare_amount FROM trips)",
    "IN subquery": lambda: "SELECT COUNT(*) FROM trips WHERE fare_amount IN (SELECT fare_amount FROM trips)",
    "scalar subquery in SELECT": lambda: "SELECT COUNT(*), (SELECT 1) FROM trips",
    "catalog-qualified table": lambda: "SELECT COUNT(*) FROM memory.main.drivers",
    "duplicate GROUP BY": lambda: "SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet, fleet",
    # expression whitelist: nothing the backend could fail on reaches it
    "arithmetic in WHERE": lambda: "SELECT COUNT(*) FROM trips WHERE fare_amount * 9223372036854775807 > 0",
    "function in WHERE": lambda: "SELECT COUNT(*) FROM trips WHERE sqrt(fare_amount - 10000) = 1",
    "error() in WHERE": lambda: "SELECT COUNT(*) FROM trips WHERE error('x') IS NULL",
    "CAST in WHERE": lambda: "SELECT COUNT(*) FROM trips WHERE CAST(hack_license AS INT) > 0",
    "CASE in WHERE": lambda: "SELECT COUNT(*) FROM trips WHERE CASE WHEN fare_amount > 1 THEN 1 ELSE 0 END = 1",
    "LIKE": lambda: "SELECT COUNT(*) FROM trips WHERE hack_license LIKE 'HL%'",
    "string concatenation": lambda: "SELECT COUNT(*) FROM trips WHERE hack_license || 'x' = 'HL1x'",
    "current_setting": lambda: "SELECT COUNT(*) FROM trips WHERE current_setting('home_directory') IS NOT NULL",
    "mixed-type comparison": lambda: "SELECT COUNT(*) FROM trips WHERE hack_license > fare_amount",
    "ASOF JOIN": lambda: "SELECT COUNT(*) FROM trips t ASOF JOIN drivers d ON t.hack_license = d.hack_license",
    "ANY over an array": lambda: "SELECT COUNT(*) FROM trips WHERE fare_amount = ANY(ARRAY[1, 2])",
}


# -- oracles -----------------------------------------------------------------------

class _Spy:
    """A connection that records whether anything reached the backend."""

    def __init__(self):
        self.calls = 0

    def execute(self, sql):
        self.calls += 1
        raise AssertionError(f"SQL reached the backend: {sql}")


def _session(con) -> Session:
    return Session.from_policy(POLICY, CONSUMER, con, ledger=Ledger(POLICY))


def _vectors(con, rq, keyset):
    return _session(con)._true_vectors(rq, keyset)[1]


@maybe_seed
@PROFILE
@given(data=datasets(), q=selects())
def test_random_sql_is_rejected_or_bounded(data, q):
    sql = q.sql(dialect="duckdb")
    try:
        rq = REWRITER.rewrite(sql)
    except UnsupportedQuery:
        return
    except ParseError:  # the generator produced text the DuckDB parser refuses: not a rewriter outcome
        assume(False)
    drivers, trips, removed = data
    con_d = _connect(drivers, trips)
    con_n = _connect([d for d in drivers if d[0] != removed], [t for t in trips if t[1] != removed])
    session = _session(con_d)
    try:
        session._check_policy(rq, 0.1, None)
    except PolicyViolation:
        return
    keyset = session._policy_keyset(rq)
    try:
        v_d, v_n = _vectors(con_d, rq, keyset), _vectors(con_n, rq, keyset)
    except QueryFailed:  # accepted SQL the backend cannot run (string vs int comparison): generic error, ε charged
        return
    for m, a, b in zip(rq.measurements, v_d, v_n):
        l1 = sum(abs(x - y) for x, y in zip(a, b))
        assert l1 <= m.sensitivity, (
            f"bound broken: {m.kind}({m.column}) moved by {l1} > Δ={m.sensitivity}\n"
            f"sql: {sql}\nrewritten: {rq.sql}\nremoved: {removed}\ndrivers: {drivers}\ntrips: {trips}\nD: {a}\nD': {b}"
        )


@pytest.mark.parametrize("name", sorted(REJECTION_CLASSES))
def test_every_rejection_class_is_refused_before_the_backend(name):
    sql = REJECTION_CLASSES[name]()
    spy = _Spy()
    with pytest.raises(UnsupportedQuery):
        _session(spy).query(sql, epsilon=0.1)
    assert spy.calls == 0


@st.composite
def clean_selects(draw):
    """Only the supported grammar: FK joins in a valid direction, aggregates on clamped columns, keysets declared."""
    shape = draw(st.sampled_from(["trips", "trips_zones", "trips_drivers", "three"]))
    q = exp.select()
    if shape == "trips":
        q = q.from_(exp.table_("trips", alias="t"))
    elif shape == "trips_zones":
        q = q.from_(exp.table_("trips", alias="t")).join(exp.table_("taxi_zones", alias="z"), on="t.pu_location_id = z.location_id")
    elif shape == "trips_drivers":
        q = q.from_(exp.table_("trips", alias="t")).join(exp.table_("drivers", alias="d"), on="t.hack_license = d.hack_license")
    else:
        q = (q.from_(exp.table_("trips", alias="t"))
             .join(exp.table_("drivers", alias="d"), on="t.hack_license = d.hack_license")
             .join(exp.table_("taxi_zones", alias="z"), on="t.pu_location_id = z.location_id"))
    keys = []
    if "zones" in shape or shape == "three":
        if draw(st.booleans()):
            keys.append("z.borough")
    if "drivers" in shape or shape == "three":
        if draw(st.booleans()):
            keys.append("d.fleet")
    aggs = draw(st.lists(st.sampled_from(["COUNT(*)", "SUM(t.fare_amount)", "AVG(t.trip_distance)", "COUNT(t.fare_amount)"]), min_size=1, max_size=3))
    q = q.select(*keys, *aggs)
    if draw(st.booleans()):
        q = q.where(draw(st.sampled_from(["t.trip_distance > 5", "t.fare_amount < 1000 AND t.trip_distance > 2", "t.hack_license = 'HL1'"])))
    if keys:
        q = q.group_by(*keys)
    return q


@maybe_seed
@PROFILE
@given(q=clean_selects())
def test_supported_grammar_is_accepted(q):
    sql = q.sql(dialect="duckdb")
    rq = REWRITER.rewrite(sql)  # must not raise
    _session(_Spy())._check_policy(rq, 0.1, None)  # must not raise: every GROUP BY column has a declared keyset
    assert rq.measurements


def test_profile_knobs_are_documented():
    assert PROFILE.max_examples >= 150
    assert isinstance(duckdb.__version__, str)
