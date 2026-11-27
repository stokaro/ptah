# CockroachDB

Start with `internal/builtindialects/cockroachdb.go`. The PostgreSQL-family
reader, planner, and migration services live in `internal/dbschema/postgres`,
`internal/planner/dialects/postgres`, and `internal/migratorext/postgres`.
CockroachDB extensions live under `dialect/cockroachdb`.

- Identify the CockroachDB release and session settings. PostgreSQL-compatible
  syntax and catalogs do not establish PostgreSQL transaction guarantees or
  feature support.
- Inspect the exact schema operation's transaction restrictions. Most schema
  changes do not have general PostgreSQL-style multi-statement atomicity.
  Preserve partial-commit diagnostics and the existing rollback-survivor
  handling instead of treating a rollback as proof that nothing changed.
- Track the applicable schema-change job and final catalog state when the
  changed path depends on asynchronous work. Do not add blind retries for a
  failed or timed-out schema statement.
- Distinguish engine-generated hidden keys from user-declared keys. Preserve
  declared key columns and index identities when catalog representations
  include hidden columns or other engine metadata.
- Verify grants, sequences, index qualification, and cleanup against
  CockroachDB's own catalogs. Keep shared PostgreSQL behavior behind the
  selected target's capabilities.

Use affected family unit tests and the CockroachDB cases under
`integration/dbschema/postgres`. The hidden-key, index-name, and sequence-key
tests are useful entry points. Include PostgreSQL controls when changing shared
code, and exercise persisted failure state when migration semantics change.

Source: CockroachDB [online schema changes and transaction limitations](https://docs.cockroachlabs.com/docs/stable/online-schema-changes).
Select the matching release documentation before applying a limitation or
exception to a capability.
