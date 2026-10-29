# SPDX-License-Identifier: Apache-2.0
"""One contract, two ledgers: the library's in-memory Ledger and the SQLite ledger must behave the same.

The SQLite ledger also persists, counts crashed reservations as spent, and carries request ids and audit rows.
"""
from __future__ import annotations

import subprocess
import sys
import threading
from fractions import Fraction
from pathlib import Path

import pytest
import yaml
from cleanroom_sql import BudgetExhaustedException, Ledger, Session, loads_policy
from cleanroom_sql.taxi import connect
from opencleanroom import catalog
from opencleanroom.ledger_store import SqliteLedger

TAXI_YAML = (Path(__file__).resolve().parents[2] / "cleanroom-sql" / "examples" / "taxi.yaml").read_text()


def _policy_text(**changes) -> str:
    doc = yaml.safe_load(TAXI_YAML)
    for dotted, value in changes.items():
        node = doc
        *parents, last = dotted.split(".")
        for p in parents:
            node = node.setdefault(p, {})
        node[last] = value
    return yaml.safe_dump(doc, sort_keys=False)


SMALL = {"dataset.budget.epsilon": "1.0", "consumers.analytics-team.budget.epsilon": "1.0",
         "consumers.analytics-team.max_epsilon_per_query": 0.1, "consumers.counts-only.max_epsilon_per_query": 0.1,
         "consumers.counts-only.tables": ["trips"], "consumers.counts-only.aggregates": ["count"]}


@pytest.fixture(scope="module")
def con():
    return connect(n_drivers=50)


@pytest.fixture(params=["memory", "sqlite"])
def make_ledger(request, tmp_path):
    if request.param == "memory":
        return lambda policy: Ledger(policy)
    cat = tmp_path / "catalog.sqlite"
    catalog.init(cat)
    return lambda policy: SqliteLedger(cat, policy.dataset.name, policy.dataset.epoch, policy)


def test_exact_cap(make_ledger, con):
    p = loads_policy(_policy_text(**SMALL))
    ledger = make_ledger(p)
    s = Session.from_policy(p, "analytics-team", con, ledger=ledger)
    for _ in range(10):
        s.query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert ledger.remaining("analytics-team") == 0
    with pytest.raises(BudgetExhaustedException):
        s.query("SELECT COUNT(*) FROM trips", epsilon=0.1)


def test_two_consumers_share_the_dataset_budget(make_ledger, con):
    p = loads_policy(_policy_text(**SMALL))
    ledger = make_ledger(p)
    a = Session.from_policy(p, "analytics-team", con, ledger=ledger)
    b = Session.from_policy(p, "counts-only", con, ledger=ledger)
    for _ in range(6):
        a.query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    for _ in range(4):
        b.query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    with pytest.raises(BudgetExhaustedException):
        b.query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert ledger.spent_total == Fraction(1)


def test_update_policy_keeps_spend(make_ledger, con):
    p1 = loads_policy(_policy_text(**SMALL))
    ledger = make_ledger(p1)
    Session.from_policy(p1, "analytics-team", con, ledger=ledger).query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    p2 = loads_policy(_policy_text(**{**SMALL, "dataset.budget.epsilon": "0.05", "consumers.analytics-team.budget.epsilon": "0.05",
                                      "consumers.analytics-team.max_epsilon_per_query": 0.05, "consumers.counts-only.max_epsilon_per_query": 0.05}))
    ledger.update_policy(p2)
    assert ledger.spent("analytics-team") == Fraction(1, 10) and ledger.remaining("analytics-team") < 0


def test_reserve_is_thread_safe(make_ledger):
    p = loads_policy(_policy_text(**SMALL))
    ledger = make_ledger(p)
    ok, barrier = [], threading.Barrier(20)

    def worker():
        barrier.wait()
        try:
            ok.append(ledger.reserve("analytics-team", Fraction(1, 10)))
        except BudgetExhaustedException:
            pass
    ts = [threading.Thread(target=worker) for _ in range(20)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert len(ok) == 10 and ledger.spent_total == Fraction(1)


def test_reserve_rejects_non_positive(make_ledger):
    ledger = make_ledger(loads_policy(_policy_text(**SMALL)))
    with pytest.raises(ValueError):
        ledger.reserve("analytics-team", Fraction(0))


# -- SQLite only: persistence, crash semantics, requests and audit ----------------------

@pytest.fixture
def cat(tmp_path):
    path = tmp_path / "catalog.sqlite"
    catalog.init(path)
    return path


def _sqlite(cat, **changes) -> tuple[SqliteLedger, object]:
    p = loads_policy(_policy_text(**{**SMALL, **changes}))
    return SqliteLedger(cat, p.dataset.name, p.dataset.epoch, p), p


def test_spend_survives_a_new_ledger_object(cat, con):
    ledger, p = _sqlite(cat)
    Session.from_policy(p, "analytics-team", con, ledger=ledger).query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    again, _ = _sqlite(cat)
    assert again.spent("analytics-team") == Fraction(1, 10)


def test_crashed_reservation_counts_as_spent_until_released(cat):
    ledger, p = _sqlite(cat)
    code = f"""
import sys
from cleanroom_sql import loads_policy
from opencleanroom.ledger_store import SqliteLedger
from fractions import Fraction
p = loads_policy(open({str(Path(cat).with_name('policy.yaml'))!r}).read())
SqliteLedger({str(cat)!r}, p.dataset.name, p.dataset.epoch, p).reserve("analytics-team", Fraction(3, 10))
sys.exit(0)  # dies before commit
"""
    Path(cat).with_name("policy.yaml").write_text(_policy_text(**SMALL))
    subprocess.run([sys.executable, "-c", code], check=True, cwd=Path(__file__).resolve().parents[1])
    assert ledger.spent("analytics-team") == Fraction(3, 10)
    stuck = ledger.reserved_rows()
    assert len(stuck) == 1
    ledger.release_reservation(stuck[0].id, reason="worker crashed")
    assert ledger.spent("analytics-team") == 0
    assert any(a.outcome == "released_by_admin" for a in ledger.audit_rows())


def test_request_view_writes_an_audit_row_per_request(cat, con):
    ledger, p = _sqlite(cat)
    view = ledger.for_request("req-1", "SELECT COUNT(*) FROM trips")
    s = Session.from_policy(p, "analytics-team", con, ledger=view)
    s.query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    view.finish("ok", None, '{"rows": [[1]]}')
    rows = ledger.audit_rows()
    assert len(rows) == 1 and rows[0].request_id == "req-1" and rows[0].outcome == "ok"
    assert rows[0].epsilon_charged == Fraction(1, 10) and rows[0].epsilon_requested == Fraction(1, 10)
    assert ledger.lookup("req-1", "analytics-team") == ("committed", '{"rows": [[1]]}')
    assert ledger.lookup("req-1", "counts-only") is None


def test_request_id_is_unique_per_consumer(cat):
    ledger, p = _sqlite(cat)
    ledger.for_request("dup", "q").reserve("analytics-team", Fraction(1, 10))
    with pytest.raises(ValueError):
        ledger.for_request("dup", "q").reserve("analytics-team", Fraction(1, 10))
    ledger.for_request("dup", "q").reserve("counts-only", Fraction(1, 10))  # another consumer, fine
    assert ledger.lookup("dup", "analytics-team") == ("reserved", None)


def test_penalty_charge_is_its_own_committed_row_and_counted_in_audit(cat, con):
    ledger, p = _sqlite(cat)
    view = ledger.for_request("fail-1", "q")
    rid = view.reserve("analytics-team", Fraction(1, 100))
    view.commit(rid)
    view.charge_up_to("analytics-team", Fraction(9, 100))  # up to the per-query maximum 0.1
    view.finish("query_failed", "query_failed", None)
    assert ledger.spent("analytics-team") == Fraction(1, 10)
    assert ledger.audit_rows()[0].epsilon_charged == Fraction(1, 10)


def test_crashed_reservation_stays_visible_after_a_new_epoch(cat):
    ledger, p = _sqlite(cat)
    ledger.for_request("oldcrash", "q", "analytics-team").reserve("analytics-team", Fraction(1, 10))
    later, _ = _sqlite(cat, **{"dataset.budget.epoch": "2099-01"})
    assert later.spent("analytics-team") == 0  # the old epoch's ε is not summed into the new one
    assert [r.request_id for r in later.reserved_rows()] == ["oldcrash"]


def test_released_live_row_cannot_be_committed_by_its_old_worker_at_another_epsilon(cat):
    """Admin releases a row that is still live; the retry reuses it at a smaller ε; the old worker's commit must fail."""
    ledger, p = _sqlite(cat)
    v1 = ledger.for_request("R", "q", "analytics-team")
    rid = v1.reserve("analytics-team", Fraction(1, 2))
    ledger.release_reservation(rid, "looked crashed")
    v2 = ledger.for_request("R", "q", "analytics-team")
    assert v2.reserve("analytics-team", Fraction(1, 100)) == rid  # the released row is reused
    with pytest.raises(ValueError):
        v1.commit(rid)
    v2.commit(rid)
    assert ledger.spent("analytics-team") == Fraction(1, 100)
    with pytest.raises(ValueError):
        ledger.for_request("S", "q", "analytics-team").commit(rid)  # not this view's reservation at all


def test_reconcile_is_per_consumer(cat, con):
    import sqlite3
    ledger, p = _sqlite(cat)
    Session.from_policy(p, "analytics-team", con, ledger=ledger.for_request("a1", "q", "analytics-team")).query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    Session.from_policy(p, "counts-only", con, ledger=ledger.for_request("c1", "q", "counts-only")).query("SELECT COUNT(*) FROM trips", epsilon=0.1)
    assert all(r.ok for r in ledger.reconcile()) and [r.consumer for r in ledger.reconcile()] == ["analytics-team", "counts-only"]
    with sqlite3.connect(cat) as c:  # move the audit charge of one consumer onto the other: the per-epoch total is unchanged
        c.execute("UPDATE audit SET consumer = 'counts-only' WHERE consumer = 'analytics-team'")
    assert [r.ok for r in ledger.reconcile()] == [False, False]


def test_a_reapplied_policy_reaches_a_ledger_opened_before_it(tmp_path):
    """Cap and per-query maximum are re-read from the catalog, so an open ledger and its request views follow a re-apply."""
    import yaml
    from cleanroom_sql import loads_policy
    from opencleanroom import catalog
    from opencleanroom.ledger_store import SqliteLedger
    from test_catalog import _policy_text
    cat = tmp_path / "c.sqlite"
    catalog.init(cat)
    catalog.add_backend(cat, "duckdb-dev", "duckdb", str(tmp_path / "x.duckdb"))
    catalog.apply_policy(cat, _policy_text())
    stored = catalog.get_policy(cat, "nyc_taxi")
    ledger = SqliteLedger(cat, "nyc_taxi", stored.epoch, loads_policy(stored.text))
    view = ledger.for_request("r1", "SELECT 1", "analytics-team")
    doc = yaml.safe_load(stored.text)
    doc["consumers"]["analytics-team"].update(budget={"epsilon": "0.2"}, max_epsilon_per_query="0.2")
    catalog.apply_policy(cat, yaml.safe_dump(doc, sort_keys=False))
    for held in (ledger, view):
        assert held.caps["analytics-team"] == Fraction(1, 5)
        assert held.rules["analytics-team"].max_epsilon_per_query == Fraction(1, 5)
