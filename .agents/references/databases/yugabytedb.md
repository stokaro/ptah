# YugabyteDB

Start with `internal/builtindialects/yugabytedb.go`. Ptah's target is the YSQL
API. The reader, planner, and migration services use
`internal/dbschema/postgres`, `internal/planner/dialects/postgres`, and
`internal/migratorext/postgres`.

- Identify the YugabyteDB release separately from the PostgreSQL compatibility
  version in its banner. Use the descriptor's release parsing and capability
  selection instead of treating the banner as plain PostgreSQL.
- Verify whether transactional data definition language (DDL) is enabled on
  the actual server. Its feature flag and release-specific restrictions matter.
  A shared transactional migrator path does not prove that schema statements
  roll back together. Preserve the existing rollback-survivor handling.
- Check statement, savepoint, concurrent-schema-change, and replication
  restrictions against that release. A transaction retry recipe is not
  permission to replay a schema change with an unknown durable outcome.
- Preserve supported index properties, null semantics, types, and expression
  spellings through readback. PostgreSQL support for a clause is not proof of
  YSQL support in every YugabyteDB release.
- Test the real YSQL catalog and connection scope when changing shared
  PostgreSQL-family readers. Keep unavailable coverage distinct from absence.

Use the affected family unit tests and YugabyteDB live cases under
`integration/`, including
`integration/dbschema/yugabytedb_string_constant_forms_live_test.go`.
For transaction changes, prove the persisted effects of rollback and failure
on the configured server. A PostgreSQL pass cannot supply that evidence.

Sources: YugabyteDB [transactional DDL](https://docs.yugabyte.com/stable/architecture/transactions/transactional-ddl/)
and [transaction support](https://docs.yugabyte.com/stable/explore/transactions/).
Read the release-specific feature status and settings before claiming support.
