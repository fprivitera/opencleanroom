---
status: accepted
---
# SQLite holds the catalog and ledger; 0.1 runs one gateway process

The catalog is derived from policy files in git, so the only state that must be durable is the ledger, and one SQLite file with `BEGIN IMMEDIATE` reservations gives atomic, exact accounting without a server. The price is one writing process: 0.1 supports a single gateway instance and documents it. Postgres arrives with the first need for replicas, and no storage abstraction is built before then.
