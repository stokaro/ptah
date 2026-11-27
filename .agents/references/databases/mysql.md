# MySQL

Start with `internal/builtindialects/mysql.go`. Catalog code lives in
`internal/dbschema/mysql`, planning in `internal/planner/dialects/mysql`,
and migration services in `internal/migratorext/mysql`. MySQL extensions live
under `dialect/mysql`; the reader and planner also serve MariaDB.

- Record the server version, table storage engine, SQL mode, character set,
  collation, and `lower_case_table_names` when they affect the changed path.
  Preserve explicit values and identifier semantics through readback.
- Distinguish atomic data definition language (DDL) from transactional DDL.
  MySQL's atomic DDL does not make a sequence of schema statements rollbackable
  with the surrounding transaction. Check implicit commits and record the
  applied prefix after failure.
- Select `ALGORITHM` and `LOCK` from the exact operation, engine, and version.
  MySQL 8.4 permits only `LOCK=DEFAULT` with `ALGORITHM=INSTANT`. Do not append
  `LOCK=NONE` to every online alteration. Online execution can still wait for
  metadata locks; test refusal and timeout behavior when those paths change.
- Preserve generated expressions, enum members, index prefix lengths,
  visibility, and named constraints where supported. Do not normalize a
  schema to a preferred application style.
- Inspect grants, definers, and the effective session role when changing
  visibility or permissions. Test the relevant MySQL and MariaDB branches
  separately when the shared reader or planner changes.

Use the affected owner's tests and `integration/dbschema/mysql`. Other live
cases live in `integration/dbschema` and `integration/migrateup`, including
index failure and drift transaction modes. Read back the committed state
after an injected failure; a rollback call alone proves no schema result.

Sources: MySQL 8.4 [atomic DDL](https://dev.mysql.com/doc/refman/8.4/en/atomic-ddl.html)
and [online DDL](https://dev.mysql.com/doc/refman/8.4/en/innodb-online-ddl.html).
Check the target version's operation-specific support table.
