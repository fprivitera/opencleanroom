---
status: accepted
---
# The admin declares k, clamp bounds and keysets; nothing is discovered from private data

In 0.1, `k` and the clamp bounds are static values in the policy file, and every `GROUP BY` needs a keyset from a public table or an admin-declared list. Discovering `k` with a differentially private sub-budget (`ε_mf`) or keys with `(ε, δ)` thresholding would charge budget on every query, add δ to the accounting and complicate composition. In the proof of concept truncation bias, not noise, dominated the error, and the choice of `k` that sets that bias belongs to the data owner.

## Consequences

- Queries without a public keyset are rejected, not approximated.
- Thresholding and `ε_mf` stay possible later; both would arrive as new ADRs because they change the accounting.
