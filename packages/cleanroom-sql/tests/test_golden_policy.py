# SPDX-License-Identifier: Apache-2.0
"""One query under two policies: the outcome (accepted with its rewritten SQL, or rejected with the message) is a snapshot.

Files live in tests/golden_policy/<shape>.<consumer>.txt; CLEANROOM_SQL_UPDATE_GOLDEN=1 regenerates them.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
from cleanroom_sql import PolicyViolation, Session, UnsupportedQuery, load_policy
from cleanroom_sql.taxi import empty_connect
from test_sensitivity_property import SHAPES

GOLDEN = Path(__file__).parent / "golden_policy"
POLICY = Path(__file__).resolve().parents[1] / "examples" / "taxi.yaml"
CONSUMERS = ["analytics-team", "counts-only"]


def _outcome(shape: str, consumer: str) -> str:
    session = Session.from_policy(load_policy(POLICY), consumer, empty_connect())
    try:
        return "accepted\n" + session.query(SHAPES[shape][0], epsilon=0.1).rewritten_sql
    except (PolicyViolation, UnsupportedQuery) as e:  # a forbidden table is unknown to the consumer's rewriter
        return "rejected\n" + str(e)


@pytest.mark.parametrize("consumer", CONSUMERS)
@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_outcome_matches_golden(shape, consumer):
    outcome = _outcome(shape, consumer)
    path = GOLDEN / f"{shape}.{consumer}.txt"
    if os.environ.get("CLEANROOM_SQL_UPDATE_GOLDEN"):
        path.write_text(outcome + "\n")
    assert path.exists(), f"missing snapshot {path.name}: run CLEANROOM_SQL_UPDATE_GOLDEN=1 pytest"
    assert outcome == path.read_text().rstrip("\n"), f"policy outcome changed for {shape} ({consumer})"


def test_no_orphan_policy_snapshots():
    expected = {f"{s}.{c}.txt" for s in SHAPES for c in CONSUMERS}
    assert {p.name for p in GOLDEN.glob("*.txt")} == expected


def test_the_two_policies_disagree_on_at_least_one_shape():
    outcomes = {(s, c): _outcome(s, c).split("\n", 1)[0] for s in SHAPES for c in CONSUMERS}
    assert any(outcomes[(s, "analytics-team")] != outcomes[(s, "counts-only")] for s in SHAPES)
