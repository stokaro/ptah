# SQLite

Start with `internal/builtindialects/sqlite.go`, `internal/dbschema/sqlite`,
`internal/planner/dialects/sqlite`, and `internal/migratorext/sqlite`.
Table extensions live under `dialect/sqlite`.

- Identify the actual SQLite library version and connection settings. Check
  supported `ALTER TABLE` operations before selecting direct alteration or a
  table rebuild. Preserve attached-database scope and identifier resolution.
- `PRAGMA foreign_keys` is connection-scoped and changing it inside a
  transaction has no effect. Apply any required change outside the transaction
  on the same connection used for the work, and restore the prior setting
  after success or failure.
- For a rebuild, follow SQLite's documented sequence: create the replacement,
  copy the intended columns, drop the original, then rename the replacement.
  Renaming the original first can redirect dependent objects incorrectly.
- Preserve or reconstruct affected indexes, triggers, and views. Check foreign
  keys before committing when required by the rebuild procedure. Verify both
  data and dependent objects after applying the plan.
- Keep declared types, defaults, generated columns, `STRICT`, `WITHOUT ROWID`,
  and virtual-table ownership where the target supports them. Do not repair
  unsupported schema changes by editing `sqlite_schema` directly.

Use the affected owner's tests and the SQLite cases in `integration/`.
`integration/dbschema/sqlite_table_identity_live_test.go` exercises the real
library through a temporary file. For connection-scope changes, include
failure cleanup and a subsequent operation on the reused connection.

Sources: SQLite [ALTER TABLE and rebuild procedure](https://sqlite.org/lang_altertable.html)
and [foreign-key PRAGMA](https://sqlite.org/pragma.html#pragma_foreign_keys).
The manual may describe a newer library than the selected Go dependency.
