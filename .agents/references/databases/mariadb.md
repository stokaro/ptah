# MariaDB

Start with `internal/builtindialects/mariadb.go` and
`engine/builtin/internal/dialects/mariadb`. Catalog and planning owners are
`internal/dbschema/mysql` and `internal/planner/dialects/mysql`. Shared code
must select MariaDB's capabilities and grammar explicitly.

- Confirm MariaDB's release and storage engine. MySQL version numbers,
  online-alter support tables, and catalog layouts are not MariaDB contracts.
- Check implicit commits before grouping data definition language (DDL) with
  other work. A failing DDL statement can leave preceding work committed;
  retain progress and uncertain-outcome information.
- Choose the alteration's algorithm and lock level from MariaDB's own
  operation support. Some versions support online `COPY` operations. Neither
  the word `COPY` nor `LOCK=NONE` establishes the complete locking behavior.
- Preserve sequences, generated columns, defaults, and type spellings through
  the selected reader and planner. `integration/dbschema/mariadb_sequence_live_test.go`
  shows why accepted rendering alone does not prove a sequence round trip.
- Inspect the selected database, identifier case settings, definers, and
  effective roles for catalog or permission changes. Keep uninspected state
  distinct from an absent object.

Run the changed shared owner's tests, then the relevant MariaDB live case
under `integration/dbschema` or `integration/dbschema/mysql`. Include a MySQL
control when changing shared selection, SQL generation, or catalog handling.
Read server state after failure for execution changes.

Sources: MariaDB [implicit commits](https://mariadb.com/docs/server/reference/sql-statements/transactions/sql-statements-that-cause-an-implicit-commit),
[ALTER TABLE](https://mariadb.com/docs/server/reference/sql-statements/data-definition/alter/alter-table),
and [metadata locking](https://mariadb.com/docs/server/reference/sql-statements/transactions/metadata-locking).
Check each page's version conditions against the actual server.
