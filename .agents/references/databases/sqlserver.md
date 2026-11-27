# SQL Server

Start with `internal/builtindialects/sqlserver.go`. Catalog and planner
packages use `mssql`: `internal/dbschema/mssql`,
`internal/planner/dialects/mssql`, and `engine/builtin/internal/dialects/mssql`.
Migration services use `internal/migratorext/sqlserver`.

- Record server version, edition, compatibility level, collation, and session
  settings relevant to the change. Azure SQL defaults are not evidence for
  a standalone SQL Server installation.
- Preserve schema-qualified identity, bracketed or quoted names, and catalog
  visibility under the connected principal. Do not assume every object is in
  `dbo` or treat an inaccessible catalog object as absent.
- Treat a column default as a named constraint where the catalog represents
  it that way. Replacement must address the actual constraint identity,
  including server-generated names. Keep computed and identity columns distinct.
- Check the exact alteration's dependency and locking requirements.
  `ALTER TABLE` can acquire schema-modification locks. Online index options
  depend on version and edition and do not mean that no locks are required.
- For execution changes, observe transaction state and persisted effects
  after errors or cancellation. A client cancellation or rollback request
  alone does not establish completion or permit replay.

Use the affected owner's tests and the SQL Server cases in
`integration/dbschema`, including `sqlserver_default_change_live_test.go`,
plus `integration/dbschema/mssql` for writer behavior. Preserve separate
success, refusal, and failure-cleanup evidence for the changed path.

Sources: Microsoft [default constraints](https://learn.microsoft.com/en-us/sql/relational-databases/tables/specify-default-values-for-columns?view=sql-server-ver17)
and [ALTER TABLE](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-table-transact-sql?view=sql-server-ver17).
Select the documentation view and applicability section for the actual target.
