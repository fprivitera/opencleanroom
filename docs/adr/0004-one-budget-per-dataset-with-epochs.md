---
status: accepted
---
# One budget per dataset, spent within explicit budget epochs

The guarantee belongs to the dataset, so ε is one persisted budget per dataset, not per consumer: budgets per consumer alone do not bound colluding consumers. Consumer caps are optional and draw from the budget. Spending is never reset. The admin starts a new budget epoch, tied to a snapshot ID and a written reason, and past epochs stay readable for audit. Within an epoch the budget can be lowered but never raised; a policy commit that raises it is rejected, because it would be a renewal without a record.

## Considered options

- Budgets per consumer only: rejected, colluding consumers add their ε.
- Automatic periodic renewal: rejected, total privacy loss grows linearly with time and nobody decides it.
- A reset command that zeroes the spent ε: rejected, it erases the evidence the ledger exists to keep.

## Addendum (23 Oct 2026): renewals are recorded

A renewal is recorded before it is applied: `opencleanroom budget new-epoch <dataset> --epoch <label> --snapshot <id> --reason <text>` writes the `epoch_log` row, then the policy file with the new label is applied from git. `apply` refuses a label that is not recorded or that sorts before the current one. Labels are plain text (`[A-Za-z0-9][A-Za-z0-9._-]*`) and compare as text: zero-pad months (`2026-09`, not `2026-9`) and keep one scheme per dataset, or a later label can sort before the current one and lock the dataset out of renewals until an admin picks a label that sorts after.
