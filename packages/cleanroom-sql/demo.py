# SPDX-License-Identifier: Apache-2.0
"""End-to-end demo on the synthetic NYC taxi dataset."""
from cleanroom_sql import BudgetExhaustedException, Session
from cleanroom_sql.taxi import BOROUGHS, FLEETS, SCHEMA, connect

con = connect()
session = Session(con=con, schema=SCHEMA, epsilon=2.0, k=50)


def show(title, sql, keyset, eps):
    print(f"\n=== {title} (ε={eps}) ===")
    res = session.query(sql, epsilon=eps, keyset=keyset)
    truth = {tuple(r[:1]): r for r in con.execute(sql).fetchall()}
    print("rewritten SQL:\n" + res.rewritten_sql)
    print(f"\nL1 sensitivities: {res.sensitivities}  95% CI ±{tuple(round(h) for h in res.ci95_halfwidth)}")
    print(f"{'key':14} {'private':>22} {'true (untruncated)':>22}")
    for row in res.rows:
        t = truth.get(row[:1], (row[0], *([None] * (len(row) - 1))))
        print(f"{str(row[0]):14} {str(row[1:]):>22} {str(tuple(t[1:])):>22}")
    print(f"budget: spent {session.spent:.2f} / {session.epsilon}")


show(
    "Fares by pickup borough (join on public lookup)",
    """SELECT z.borough, COUNT(*) AS trips, SUM(t.fare_amount) AS fare_cents
       FROM trips t JOIN taxi_zones z ON t.pu_location_id = z.location_id
       GROUP BY z.borough""",
    keyset=BOROUGHS, eps=1.0,
)

show(
    "Fares by fleet (join on private keyed table)",
    """SELECT d.fleet, SUM(t.fare_amount) AS fare_cents
       FROM trips t JOIN drivers d ON t.hack_license = d.hack_license
       WHERE t.trip_distance > 20
       GROUP BY d.fleet""",
    keyset=FLEETS, eps=0.5,
)

print("\n=== Budget exhaustion ===")
try:
    session.query("SELECT COUNT(*) FROM trips", epsilon=1.0)
except BudgetExhaustedException as e:
    print("blocked:", e)
