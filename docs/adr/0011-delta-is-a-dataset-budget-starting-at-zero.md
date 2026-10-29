---
status: accepted
---
# δ is part of the dataset's budget, set by the admin from zero upward

The budget of a dataset is a pair (ε, δ), not ε alone. The admin sets δ once per dataset in the policy, next to ε; it defaults to 0, any value from 0 up to but excluding 1 is accepted, and it is never set per table or per consumer. δ = 0 is the pure-ε guarantee every release so far gives, so existing policies keep their meaning unchanged. In 0.1 every mechanism is pure ε (discrete Laplace, ADR 0002) and spends δ = 0: a positive δ is a reserve that later mechanisms draw from, such as `(ε, δ)` thresholding of keys (ADR 0005) or Gaussian noise. δ composes like ε: the ledger sums the δ of committed queries per dataset and epoch, a reservation that would exceed δ is refused, and ADR 0004 applies to the pair, so δ can be lowered inside an epoch but raised only in a new one.

## Considered options

- δ per table: rejected. The guarantee belongs to the privacy unit, who appears across every private table of the dataset; a join spends from all of them at once, so a table cannot own a share of δ.
- δ per consumer, as a cap like ε's consumer cap: not now. Colluding consumers add their δ exactly as they add their ε, so the dataset bound is the one that holds; a consumer δ cap can come later without changing this decision.
- A fixed maximum such as 1e-5: rejected as a hard rule. A safe δ depends on the number of privacy units, which the policy does not know. The guidance is δ well below 1/N for N privacy units; the loader enforces only 0 ≤ δ < 1.

## Consequences

- The ledger's `delta` column, written as `'0'` since the ledger moved into the catalog, becomes the δ spent per entry; no migration.
- When composition moves to zCDP, ε and δ of the dataset become the target of a conversion, recorded as its own ADR.
