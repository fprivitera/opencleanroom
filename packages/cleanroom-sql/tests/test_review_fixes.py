# SPDX-License-Identifier: Apache-2.0
"""Query forms the rewriter must refuse because the sensitivity proof or the result alignment would not hold."""
from __future__ import annotations

import pytest
from cleanroom_sql import Rewriter, UnsupportedQuery
from cleanroom_sql.taxi import SCHEMA

REFUSED = {
    "column list on an alias": "SELECT COUNT(*) FROM trips t JOIN drivers AS d (fleet, hack_license) ON t.hack_license = d.hack_license",
    "GROUP BY column not selected": "SELECT COUNT(*) FROM trips GROUP BY pu_location_id",
    "key selected twice": "SELECT pu_location_id, pu_location_id, COUNT(*) FROM trips GROUP BY pu_location_id",
    "string column as predicate": "SELECT COUNT(*) FROM trips WHERE hack_license",
    "string column under NOT and AND": "SELECT COUNT(*) FROM trips WHERE NOT (hack_license) AND fare_amount > 1",
    "int literal as predicate": "SELECT COUNT(*) FROM trips WHERE fare_amount > 1 OR 1",
    "IS with a non-NULL operand": "SELECT COUNT(*) FROM trips WHERE fare_amount IS 5",
}


@pytest.mark.parametrize("sql", REFUSED.values(), ids=REFUSED.keys())
def test_refused(sql):
    with pytest.raises(UnsupportedQuery):
        Rewriter(SCHEMA, k=50).rewrite(sql)


def test_key_positions_follow_group_by_order():
    rq = Rewriter(SCHEMA, k=50).rewrite(
        "SELECT t.pu_location_id, d.fleet, COUNT(*) FROM trips t JOIN drivers d ON t.hack_license = d.hack_license "
        "GROUP BY d.fleet, t.pu_location_id")
    assert rq.group_columns == (("drivers", "fleet"), ("trips", "pu_location_id"))
    assert rq.key_positions == (1, 0)  # keyset order is GROUP BY order; positions say where each key sits in a row
