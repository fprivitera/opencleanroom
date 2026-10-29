# SPDX-License-Identifier: Apache-2.0
"""Queries under a policy: table, aggregate and ε rules; policy keysets; one shared ledger per dataset."""
from __future__ import annotations

from fractions import Fraction

import pytest
from cleanroom_sql import BudgetExhaustedException, Ledger, PolicyViolation, Session, UnsupportedQuery, loads_policy
from cleanroom_sql.taxi import BOROUGHS, connect
from test_policy import _policy


def _with_counts_only(d):
    d["consumers"]["counts-only"] = {"tables": ["trips", "taxi_zones"], "aggregates": ["count"], "max_epsilon_per_query": 0.2}


@pytest.fixture(scope="module")
def con():
    return connect(n_drivers=300)


@pytest.fixture
def policy():
    return loads_policy(_policy(_with_counts_only))


@pytest.fixture
def analytics(policy, con):
    return Session.from_policy(policy, "analytics-team", con)


@pytest.fixture
def counts_only(policy, con):
    return Session.from_policy(policy, "counts-only", con)


# -- policy check at query time -------------------------------------------------

def test_query_inside_policy_is_accepted(counts_only):
    res = counts_only.query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert len(res.rows) == 1 and res.sensitivities == (50,)


def test_table_outside_policy_rejected(counts_only):
    # The consumer's rewriter does not know the table: same error as a table that does not exist (no schema oracle)
    with pytest.raises(UnsupportedQuery, match="drivers"):
        counts_only.query("SELECT COUNT(*) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license", epsilon=0.1)


def test_aggregate_outside_policy_rejected(counts_only):
    with pytest.raises(PolicyViolation, match="sum"):
        counts_only.query("SELECT SUM(fare_amount) FROM trips", epsilon=0.1)


def test_epsilon_above_per_query_max_rejected(counts_only):
    with pytest.raises(PolicyViolation, match="max_epsilon_per_query"):
        counts_only.query("SELECT COUNT(*) FROM trips", epsilon=0.3)


def test_group_by_without_declared_keyset_rejected(analytics):
    with pytest.raises(PolicyViolation, match="keyset"):
        analytics.query("SELECT pu_location_id, COUNT(*) FROM trips GROUP BY pu_location_id", epsilon=0.1)


def test_caller_keyset_rejected_under_policy(analytics):
    with pytest.raises(PolicyViolation, match="keyset"):
        analytics.query("SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet", epsilon=0.1, keyset=["fleet_A"])


def test_unsupported_query_wins_over_policy_violation(counts_only):
    with pytest.raises(UnsupportedQuery):
        counts_only.query("SELECT MAX(fare_amount) FROM drivers", epsilon=0.1)


def test_rejected_query_spends_nothing(counts_only):
    with pytest.raises(PolicyViolation):
        counts_only.query("SELECT SUM(fare_amount) FROM trips", epsilon=0.1)
    assert counts_only.spent == 0


# -- keysets from the policy ------------------------------------------------------

def test_policy_keyset_from_public_column(analytics):
    res = analytics.query(
        "SELECT z.borough, COUNT(*) FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY z.borough",
        epsilon=0.1,
    )
    assert sorted(r[0] for r in res.rows) == sorted(BOROUGHS)


def test_inline_keyset_and_cartesian_product(analytics):
    res = analytics.query(
        "SELECT d.fleet, z.borough, COUNT(*) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license "
        "JOIN taxi_zones z ON t.pu_location_id = z.location_id GROUP BY d.fleet, z.borough",
        epsilon=0.1,
    )
    assert len(res.rows) == 3 * 6
    assert {r[0] for r in res.rows} == {"medallion", "fleet_A", "fleet_B"}


# -- ledger ------------------------------------------------------------------------

def test_exact_cap_accounting(policy, con):
    def m(d):
        _with_counts_only(d)
        d["consumers"]["counts-only"]["budget"] = {"epsilon": "1.0"}
    s = Session.from_policy(loads_policy(_policy(m)), "counts-only", con)
    for _ in range(10):
        s.query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert s.remaining == 0
    with pytest.raises(BudgetExhaustedException):
        s.query("SELECT COUNT(*) FROM trips", epsilon=0.1)


def test_two_consumers_share_dataset_budget(con):
    def m(d):
        _with_counts_only(d)
        d["dataset"]["budget"]["epsilon"] = "0.5"
        d["consumers"]["analytics-team"]["budget"]["epsilon"] = "0.5"
        d["consumers"]["analytics-team"]["max_epsilon_per_query"] = 0.3
        d["consumers"]["counts-only"]["max_epsilon_per_query"] = 0.3
    policy = loads_policy(_policy(m))
    ledger = Ledger(policy)
    a = Session.from_policy(policy, "analytics-team", con, ledger=ledger)
    b = Session.from_policy(policy, "counts-only", con, ledger=ledger)
    a.query("SELECT COUNT(*) FROM trips", epsilon=0.3)
    with pytest.raises(BudgetExhaustedException):
        b.query("SELECT COUNT(*) FROM trips", epsilon=0.3)
    b.query("SELECT COUNT(*) FROM trips", epsilon=0.2)
    assert ledger.spent_total == Fraction(1, 2) and ledger.remaining("counts-only") == 0


def test_backend_failure_is_charged_and_generic(policy, con):
    """A backend failure after the policy check spends the ε (no free oracle) and hides the backend's text."""
    from cleanroom_sql import QueryFailed

    class Broken:
        def execute(self, sql):
            raise RuntimeError("backend down: secret=42")
    s = Session.from_policy(policy, "counts-only", Broken())
    with pytest.raises(QueryFailed) as e:
        s.query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert "42" not in str(e.value)
    assert s.spent == 0.2 and s.remaining == 9.8  # charged the per-query maximum of counts-only, not the 0.1 asked


def test_unknown_consumer_rejected(policy, con):
    with pytest.raises(KeyError):
        Session.from_policy(policy, "nobody", con)


# -- review findings ---------------------------------------------------------------

def test_sessions_on_one_policy_share_the_ledger_by_default(policy, con):
    """ADR 0004: the dataset budget bounds every consumer together, without the caller threading a ledger."""
    a = Session.from_policy(policy, "analytics-team", con)
    b = Session.from_policy(policy, "counts-only", con)
    assert a.ledger is b.ledger
    a.query("SELECT COUNT(*) FROM trips", epsilon=0.5)
    assert b.spent == 0 and b.remaining == 9.5  # dataset budget 10, no cap on counts-only


def test_ledger_rejects_non_positive_reservation(policy):
    ledger = Ledger(policy)
    with pytest.raises(ValueError):
        ledger.reserve("analytics-team", Fraction(-5))
    with pytest.raises(ValueError):
        ledger.reserve("analytics-team", Fraction(0))


def test_executor_refuses_a_keyset_ref_to_a_private_table(policy, con):
    from cleanroom_sql import KeysetRef
    s = Session.from_policy(policy, "analytics-team", con)
    s.keysets = {**s.keysets, ("drivers", "fleet"): KeysetRef("drivers", "fleet")}
    with pytest.raises(PolicyViolation, match="public"):
        s.query("SELECT fleet, COUNT(*) FROM drivers GROUP BY fleet", epsilon=0.1)


def test_ledger_update_policy_keeps_spend_and_takes_new_caps(con):
    """A re-apply inside the same epoch never resets spending (ADR 0004); new caps apply from now on."""
    p1 = loads_policy(_policy(_with_counts_only))
    ledger = Ledger(p1)
    s = Session.from_policy(p1, "analytics-team", con, ledger=ledger)
    s.query("SELECT COUNT(*) FROM trips", epsilon=0.5)
    assert ledger.spent("analytics-team") == Fraction(1, 2)

    def lowered(d):
        _with_counts_only(d)
        d["dataset"]["budget"]["epsilon"] = "0.4"          # below what is spent
        d["consumers"]["analytics-team"]["budget"]["epsilon"] = "0.4"
        d["consumers"]["analytics-team"]["max_epsilon_per_query"] = 0.1
        d["consumers"]["newcomer"] = {"tables": ["trips"], "aggregates": ["count"], "max_epsilon_per_query": 0.1}
    p2 = loads_policy(_policy(lowered))
    ledger.update_policy(p2)
    assert ledger.spent("analytics-team") == Fraction(1, 2)   # kept
    assert ledger.remaining("analytics-team") < 0                # blocked until a new epoch
    assert ledger.spent("newcomer") == 0 and ledger.remaining("newcomer") < 0  # dataset budget already exceeded
    with pytest.raises(BudgetExhaustedException):
        Session.from_policy(p2, "analytics-team", con, ledger=ledger).query("SELECT COUNT(*) FROM trips", epsilon=0.1)


def test_ledger_reserve_is_thread_safe(policy):
    """20 threads each reserve 0.1 under a budget of 1.0: exactly 10 succeed, never 11."""
    import threading
    def m(d):
        d["dataset"]["budget"]["epsilon"] = "1.0"
        d["consumers"]["analytics-team"]["budget"]["epsilon"] = "1.0"
        d["consumers"]["analytics-team"]["max_epsilon_per_query"] = 0.1
    ledger = Ledger(loads_policy(_policy(m)))
    ok, barrier = [], threading.Barrier(20)
    def worker():
        barrier.wait()
        try:
            ok.append(ledger.reserve("analytics-team", Fraction(1, 10)))
        except BudgetExhaustedException:
            pass
    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(ok) == 10 and ledger.spent_total == Fraction(1)


def test_ledger_remaining_for_a_consumer_dropped_by_a_reapply(con):
    p1 = loads_policy(_policy(_with_counts_only))
    ledger = Ledger(p1)
    Session.from_policy(p1, "counts-only", con, ledger=ledger).query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    ledger.update_policy(loads_policy(_policy()))  # counts-only dropped
    assert ledger.spent("counts-only") == Fraction(1, 10)      # still counted in the dataset total
    assert ledger.remaining("counts-only") == ledger.budget - ledger.spent_total  # no cap, no KeyError


# -- metadata queries: DESCRIBE and SHOW TABLES answer from the policy, cost nothing -------------

def test_describe_lists_declared_columns_and_spends_nothing(counts_only):
    rows = counts_only.describe("trips")
    assert [r[0] for r in rows] == ["trip_id", "hack_license", "pu_location_id", "fare_amount", "trip_distance"]
    assert ("fare_amount", "int", "[0, 20000]") in rows and ("trip_id", "int", "") in rows
    assert counts_only.spent == 0


def test_describe_refuses_a_table_outside_the_policy(counts_only):
    with pytest.raises(PolicyViolation, match="drivers"):
        counts_only.describe("drivers")


def test_show_tables_lists_the_consumer_tables_with_their_kind(counts_only):
    assert counts_only.tables() == [("taxi_zones", "public"), ("trips", "private, truncated to 50 rows per privacy unit")]


def test_query_routes_describe_and_show_tables(counts_only):
    from cleanroom_sql.session import is_metadata_query
    assert is_metadata_query("DESCRIBE trips") == ("describe", "trips")
    assert is_metadata_query("  show tables ") == ("tables", None)
    assert is_metadata_query("SELECT COUNT(*) FROM trips") is None


def test_show_columns_is_an_alias_of_describe():
    from cleanroom_sql.session import is_metadata_query
    assert is_metadata_query("SHOW COLUMNS FROM trips") == ("describe", "trips")
    assert is_metadata_query("show columns in trips;") == ("describe", "trips")
    assert is_metadata_query("SHOW COLUMNS FROM trips WHERE 1=1") is None  # anything beyond the table name is not metadata


def test_rewriter_under_a_policy_sees_only_the_consumer_tables(counts_only):
    """Unknown and forbidden tables fail the same way: no schema oracle across the policy boundary."""
    with pytest.raises(UnsupportedQuery) as forbidden:
        counts_only.query("SELECT COUNT(*) FROM drivers", epsilon=0.1)
    with pytest.raises(UnsupportedQuery) as unknown:
        counts_only.query("SELECT COUNT(*) FROM nope", epsilon=0.1)
    assert str(forbidden.value).replace("drivers", "X") == str(unknown.value).replace("nope", "X")


def test_keyset_product_is_capped(policy, con):
    s = Session.from_policy(policy, "analytics-team", con)
    s.keysets = {**s.keysets, ("trips", "pu_location_id"): list(range(2000)), ("trips", "trip_id"): list(range(2000))}
    with pytest.raises(PolicyViolation, match="keyset"):
        s.query("SELECT pu_location_id, trip_id, COUNT(*) FROM trips GROUP BY pu_location_id, trip_id", epsilon=0.1)
    assert s.spent == 0


# -- adversarial review findings ---------------------------------------------------

def test_backend_failure_charges_the_per_query_maximum(policy, con):
    """A data-dependent failure is a bit of information: it costs as much as the largest query the policy allows."""
    from cleanroom_sql import QueryFailed

    class Broken:
        def execute(self, sql):
            raise RuntimeError("boom")
    s = Session.from_policy(policy, "counts-only", Broken())  # max_epsilon_per_query 0.2, no cap
    with pytest.raises(QueryFailed):
        s.query("SELECT COUNT(*) FROM trips", epsilon=0.001)
    assert s.spent == 0.2


def test_mechanism_failure_is_generic_and_charged(con):
    """OpenDP refusing a value must not leak the true aggregate (clamp 2^57 makes the sum exceed i64 but passes the scale guard)."""
    from cleanroom_sql import QueryFailed
    def m(d):
        d["dataset"]["tables"]["trips"]["columns"]["fare_amount"]["clamp"] = [2**57, 2**57 + 1]
        d["dataset"]["truncation"]["k"] = 50
        d["dataset"]["budget"]["epsilon"] = "1000"
        d["consumers"]["analytics-team"]["budget"]["epsilon"] = 1000
        d["consumers"]["analytics-team"]["max_epsilon_per_query"] = 100
    p = loads_policy(_policy(m))
    s = Session.from_policy(p, "analytics-team", con)
    with pytest.raises(QueryFailed) as e:
        s.query("SELECT SUM(fare_amount) FROM trips", epsilon=100)  # scale fits the guard; the true sum does not fit i64
    assert "representable" not in str(e.value) and not any(ch.isdigit() for ch in str(e.value))
    assert s.spent == 100


def test_keyset_ref_product_is_capped_before_building(policy, con, monkeypatch):
    import cleanroom_sql.session as sess
    from cleanroom_sql import KeysetRef
    monkeypatch.setattr(sess, "MAX_KEYSET_SIZE", 1000)
    s = Session.from_policy(policy, "analytics-team", con)
    s.keysets = {**s.keysets, ("trips", "pu_location_id"): KeysetRef("taxi_zones", "location_id"), ("trips", "trip_id"): list(range(500))}
    with pytest.raises(PolicyViolation, match="keyset"):
        s.query("SELECT pu_location_id, trip_id, COUNT(*) FROM trips GROUP BY pu_location_id, trip_id", epsilon=0.1)


def test_keyset_from_a_public_table_outside_the_consumer_tables_works(con):
    """counts-only may GROUP BY trips.pu_location_id with a keyset read from taxi_zones even if it cannot query taxi_zones."""
    def m(d):
        _with_counts_only(d)
        d["consumers"]["counts-only"]["tables"] = ["trips"]
        d["dataset"]["keysets"]["trips.pu_location_id"] = {"from": "taxi_zones.location_id"}
    s = Session.from_policy(loads_policy(_policy(m)), "counts-only", con)
    res = s.query("SELECT pu_location_id, COUNT(*) FROM trips GROUP BY pu_location_id", epsilon=0.1)
    assert len(res.rows) == 263
