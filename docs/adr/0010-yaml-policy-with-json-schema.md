---
status: accepted
---
# Policies are YAML files validated by a JSON Schema, then by the loader

A policy is a YAML file with two sections, `dataset` and `consumers`, in the layout of the technical design §2. The library validates it in two passes: a JSON Schema shipped inside the package (`dpsql/policy.schema.json`, draft 2020-12, `additionalProperties: false` everywhere) rejects unknown keys and wrong shapes, then the loader applies the semantic rules the rewriter and executor rely on (clamps on int columns, foreign keys to primary keys, private joins on the privacy ID, unique truncation order, public keysets, consumer caps inside the dataset budget). Every fault is reported in one run with its YAML path. ε values are read from their decimal text as exact fractions.

This adds `pyyaml` (safe loader only, refusing duplicate keys) and `jsonschema` to the library's runtime dependencies, pinned exactly like `sqlglot` and `opendp`. Amends ADR 0009's dependency list.

## Considered options

- TOML with the standard library: rejected, no comments-friendly nested tables for per-column metadata, and admins asked for YAML.
- Python policy objects only: rejected, nothing an admin can review or diff without reading code.
- Pydantic models: rejected, a framework dependency for what a JSON Schema and forty lines of checks cover; the schema file is also usable by editors and other tools.

## Consequences

- The schema is the contract; a new key needs a schema change and a loader rule, never just one of the two.
- `dpsql policy check <file>` runs the loader without a backend, so a policy is checked before deployment.
