# Oracle Database

Start with `internal/builtindialects/oracle.go`, `internal/dbschema/oracle`,
`internal/planner/dialects/oracle`, `engine/builtin/internal/dialects/oracle`,
and `internal/migratorext/oracle`.

- Record the server version, compatibility settings, connected user, and
  current schema. Verify the catalog views and privileges for the operation;
  a restricted catalog result is not an authoritative inventory of all objects.
- Preserve quoted identifiers and schema namespaces. Ordinary unquoted names
  fold to uppercase; a quoted mixed-case name can identify a different object.
  Use Ptah's structured identity rules instead of uppercasing every catalog name.
- Account for implicit commits around data definition language (DDL). A DDL
  failure can leave preceding work committed. Do not promise migration-wide
  rollback based on a surrounding transaction or connection error.
- Keep sequences, identity clauses, default expressions, synonyms, types,
  and dependency order through parse, plan, and readback. Do not replace an
  owner-qualified object with an unqualified name for convenience.
- Check replacement and cleanup effects on dependent objects and grants.
  Validate the resulting catalog state, including invalid or inaccessible
  objects where the changed operation can leave them.

Use the affected owner's unit tests and the Oracle cases under `integration/`.
`integration/oracle_sequence_identity_e2e_test.go` and
`integration/dbschema/oracle_synonym_live_test.go` exercise real boundaries.
For execution changes, include failure-state readback on the actual Oracle
version and report unavailable live evidence separately.

Sources: Oracle 26 [object names](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Database-Object-Names-and-Qualifiers.html)
and [SQL transaction effects](https://docs.oracle.com/en/database/oracle/oracle-database/26/cncpt/sql.html).
Use the corresponding manual for the tested server and compatibility level.
