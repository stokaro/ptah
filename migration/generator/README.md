# Migration generator

The migration generator package provides functionality to automatically generate both UP and DOWN migration files by comparing the desired database schema (from Go entities) with the current database state.

## Features

- **Automatic Schema Comparison**: Compares Go entity definitions with current database schema
- **Bidirectional Migrations**: Generates both UP and DOWN migration files
- **Multiple Database Support**: Works with PostgreSQL, MySQL, and MariaDB
- **Schema-Scoped Introspection**: Restricts PostgreSQL reads to selected schemas
- **Proper File Naming**: Uses timestamp-based naming convention for migration files
- **Embedded Field Support**: Handles embedded structs in Go entities

## Usage

### Basic usage

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "go.5x5.cz/ptah/migration/generator"
)

func main() {
    opts := generator.GenerateMigrationOptions{
        GoEntitiesDir: "./entities",           // Directory containing Go entities
        DatabaseURL:   "postgres://user:pass@localhost/db", // Database connection
        MigrationName: "add_user_table",       // Optional: defaults to "migration"
        OutputDir:     "./migrations",         // Directory to save migration files
        Schemas:       []string{"auth", "billing", "public"}, // Optional PostgreSQL schema allow-list
        ShadowDatabaseURL: "postgres://user:pass@localhost/shadow", // Optional pre-write verification DB
    }

    // Bound the initial database connection so a stuck host fails fast.
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    files, err := generator.GenerateMigration(ctx, opts)
    if err != nil {
        log.Fatal(err)
    }

    // Check if any migration was generated (nil means no changes detected)
    if files == nil {
        fmt.Println("No schema changes detected - no migration needed")
        return
    }

    fmt.Println("Generated migration files:")
    for _, pair := range files.Files {
        fmt.Printf("UP:      %s\n", pair.UpFile)
        fmt.Printf("DOWN:    %s\n", pair.DownFile)
        fmt.Printf("Version: %d\n", pair.Version)
    }
}
```

`MigrationFiles.Files` is the authoritative result. It contains every
generated pair in apply order, including each pair's optional safety report
and transaction mode.

### Planning before publication

`GenerateMigration` plans and writes in one call. Use `PlanMigration` when a
caller must finish database cleanup or another pre-publication step before any
migration file appears:

```go
plan, err := generator.PlanMigration(ctx, opts)
if err != nil {
    return err
}
if plan == nil {
    return nil
}

files, err := plan.WriteFiles()
if err != nil {
    return err
}
for _, pair := range files.Files {
    fmt.Printf("published %s and %s\n", pair.UpFile, pair.DownFile)
}
```

Planning does not write migration artifacts. One `WriteFiles` call consumes the
plan; call it once after all surrounding work has succeeded. A second call is
reported rather than retried, because the contents the plan verifies against and
the version it chose both describe the directory as it was before the first
attempt — the honest retry is a fresh `PlanMigration`.

The plan binds the migration directory while it is being built and holds that
binding until its publication attempt returns, so it is a claim on a filesystem
object rather than on a pathname. `WriteFiles` acquires the shared cross-process
directory lock, rejects the plan if migration SQL or integrity metadata changed
before publication, and rejects it if the pathname no longer names the object
the plan holds — a substitute holding exactly the planned files, and a directory
removed and recreated at the same pathname, are both a different destination. It
never renumbers and publishes a plan derived from stale history. The version the
plan chooses is scanned through the bound directory too, so it avoids colliding
with the files it will be published beside rather than with whatever the
pathname resolved to while the version was being picked.

`WriteFiles` releases the migration directory handles before it returns, on the
failure paths as well as the successful one, so the plan's hold on the directory
ends at a moment the caller controls. A plan that is never published at all
releases them when it is collected.

Holding the directory open is not a lock on it. On Unix another process renames
or removes the held directory exactly as it always could, and the guarantee is
that publication stays bound to the retained object and refuses once
revalidation shows the pathname no longer names it. On Windows an open handle is
stronger and the rename or removal may be refused or deferred until the plan
releases the directory.

A migration directory containing a symbolic link that points outside itself — a
shared migration linked in from another directory — is refused: everything in
the directory is read, checksummed and published through the object the run
opened, so bytes that live elsewhere cannot be part of it. The refusal names the
entry and the rule. A link whose target is inside the migration directory
resolves normally and is unaffected. The applier already refused such a
directory, so publishing into one produced a directory `ptah migrations up`
would not read; the generator no longer creates that situation.

It renders every up/down file and requested safety report before publishing
the artifacts as one batch. A filename collision leaves no partial new files.
Set `ReportFormat` to `json` or `html` to publish one
`<version>_<name>.safety.<format>` file beside each migration pair. The
resulting `MigrationFilePair.ReportFile` names the published artifact.

Use `WriteFilesContext` when lock acquisition must honor cancellation or an
operation deadline. Callers can branch on
`generator.ErrMigrationDirectoryChanged` when another process changed the
migration history after planning, and on `generator.ErrMigrationPlanInUse`
when the same plan is already being published.

### Migration process

The generator follows this process:

1. **Parse Go Entities**: Scans the specified directory for Go structs with schema annotations
2. **Read Current Database Schema**: Connects to the database and introspects the current schema
3. **Calculate Differences**: Compares the desired schema with the current database state
4. **Generate UP Migration**: Creates SQL statements to transform current schema to desired schema
5. **Generate DOWN Migration**: Creates SQL statements to reverse the changes (rollback)
6. **Shadow Verification (Optional)**: Replays prior migrations plus the candidate on a disposable database before files are written
7. **Save Files**: Writes both migration files with proper naming convention

### Shadow database verification

Set `ShadowDatabaseURL` to verify generated migrations before they are written:

```go
opts := generator.GenerateMigrationOptions{
    GoEntitiesDir:      "./entities",
    DatabaseURL:        "postgres://localhost:5432/app_dev",
    MigrationName:      "add_user_table",
    OutputDir:          "./migrations",
    ShadowDatabaseURL:  "postgres://localhost:5432/app_shadow",
}
```

The shadow database is treated as disposable and must identify a different
live database realm from the target. Ptah verifies that separation before any
destructive work. The generator drops all objects in the shadow database,
applies every existing migration from `OutputDir`, applies the candidate
migration, re-introspects the database, and compares the result against the Go
schema. If the replayed schema differs, generation aborts before writing files:

```text
shadow check failed: missing column users.email
```

Ptah also runs an `up -> down -> up` round-trip on the candidate migration and
aborts if either direction fails.

Candidate generation and `VerifyBaselineShadow` preserve structured shadow
diagnostics. Use `errors.As` rather than parsing the display message:

```go
var shadowErr *generator.ShadowVerificationError
if errors.As(err, &shadowErr) {
    fmt.Printf("shadow stage: %s\n", shadowErr.Result.Stage)
    for _, mismatch := range shadowErr.Result.Mismatches {
        fmt.Printf("%s: %s\n", mismatch.Kind, mismatch.Message)
    }
}
```

Operational failures also unwrap to the underlying database or migration
error. Structural schema mismatches carry the complete, deterministically
ordered mismatch list without an underlying error. Baseline verification keeps
the `baseline shadow check failed:` display prefix while exposing the same
typed result. It shares stage names with candidate verification at common
boundaries, can additionally report `target-introspect`, `reset-schemas`, and
`drop-metadata`, and never reports the candidate-only `round-trip-down` or
`round-trip-up` stages.

### File naming convention

Migration files follow the pattern:
```text
<timestamp>_<migration_name>.<up|down>.sql
```

Examples:
- `1703123456_add_user_table.up.sql`
- `1703123456_add_user_table.down.sql`

### Supported schema changes

The generator can handle:

- **Table Operations**: CREATE, DROP, ALTER TABLE
- **Column Operations**: ADD COLUMN, DROP COLUMN, ALTER COLUMN
- **Index Operations**: CREATE INDEX, DROP INDEX
- **Enum Operations**: CREATE TYPE, DROP TYPE, ALTER TYPE (PostgreSQL)
- **Constraint Operations**: ADD/DROP foreign keys, unique constraints

### Example generated migration

**UP Migration** (`1703123456_add_user_table.up.sql`):
```sql
-- Migration generated from schema differences
-- Generated on: 2023-12-21T10:30:56Z
-- Direction: UP

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_email ON users(email);
```

**DOWN Migration** (`1703123456_add_user_table.down.sql`):
```sql
-- Migration rollback
-- Generated on: 2023-12-21T10:30:56Z
-- Direction: DOWN

DROP INDEX IF EXISTS idx_users_email;
DROP TABLE IF EXISTS users;
```

### Error handling

The generator will return an error if:

- Database connection fails
- Go entity parsing fails
- File system operations fail
- SQL generation fails

**Note:** When no schema changes are detected, the generator returns `nil, nil` (not an error). This is considered a successful no-op operation.

### Integration with migration system

The generated files are compatible with the ptah migration system and can be executed using:

```bash
go run ./cmd migrations up --db-url postgres://user:pass@localhost/db --migrations-dir ./migrations --lock-timeout 3s --statement-timeout 30s
```

For PostgreSQL, MySQL, and MariaDB targets, generated migrations containing `ALTER TABLE` automatically include:

```sql
-- +ptah lock_timeout=3s
-- +ptah statement_timeout=30s
```

Review these defaults before production deployment and adjust them per migration when a longer rollout window is intentional.

## Configuration options

### `GenerateMigrationOptions`

The `GenerateMigrationOptions` struct provides comprehensive configuration for migration generation:

```go
type GenerateMigrationOptions struct {
    // GoEntitiesDir is the directory to scan for Go entities
    GoEntitiesDir string

    // GoEntitiesFS is the filesystem to use for reading entities (optional, defaults to os.DirFS)
    // Useful for embedded filesystems or testing with virtual filesystems
    GoEntitiesFS fs.FS

    // DatabaseURL is the connection string for the database
    DatabaseURL string

    // DBConn is the database connection (optional, if not provided, a new connection will be created)
    // Useful for reusing existing connections or custom connection management
    DBConn *dbschema.DatabaseConnection

    // MigrationName is the name for the migration (optional, defaults to "migration")
    MigrationName string

    // OutputDir is the directory where migration files will be saved (always real filesystem)
    OutputDir string

    // AllowedOutputRoot confines the whole writer transaction when accepting
    // user-supplied paths
    AllowedOutputRoot string

    // CompareOptions are the options to use when comparing schemas
    CompareOptions *config.CompareOptions

    // Schemas restricts database introspection to the listed schemas when the
    // connected dialect supports schema scoping.
    Schemas []string

    // ShadowDatabaseURL enables pre-write verification on a disposable database.
    ShadowDatabaseURL string
}
```

### Field details

- `GoEntitiesDir`: Directory to scan for Go entities (required)
- `DatabaseURL`: Database connection string (required)
- `DBConn`: Existing database connection (optional; used instead of `DatabaseURL` when set)
- `MigrationName`: Name for the migration (optional, defaults to "migration")
- `OutputDir`: Directory where migration files will be saved (required)
- `AllowedOutputRoot`: Project or workspace root the output directory must stay inside (optional). It is opened, not merely compared against: the root, the migration directory and its parent are bound once and every read, create, checksum commit and rollback of the run goes through those handles, so replacing the directory or an ancestor after the path was validated cannot move the write outside the root
- `CompareOptions`: Schema comparison options (optional)
- `Schemas`: PostgreSQL schema allow-list for database introspection (optional)
- `ShadowDatabaseURL`: Disposable database URL for pre-write migration replay and round-trip checks; it must identify a different live database realm from the target (optional)

### PostgreSQL concurrent indexes

When Ptah generates a new PostgreSQL index on an existing table whose
introspected row estimate is greater than zero, it emits `CREATE INDEX
CONCURRENTLY` and marks that migration file with `-- +ptah no_transaction`.
This avoids PostgreSQL's `CREATE INDEX CONCURRENTLY cannot run inside a
transaction block` failure and reduces write blocking on populated tables.

If a generated change set contains both ordinary transactional DDL and
concurrent index builds, Ptah writes separate migration file pairs: the
transactional migration first, then the `no_transaction` concurrent-index
migration at the next version. Rollbacks naturally run in the reverse order, so
the index is dropped before prerequisite schema changes are reverted.

Ptah only enables this policy when the live target capabilities include
PostgreSQL `CREATE INDEX CONCURRENTLY`. PostgreSQL-wire engines such as
YugabyteDB and CockroachDB keep regular `CREATE INDEX` output when their
capability preset disables the keyword.

### Database URL examples

- PostgreSQL: `postgres://user:password@localhost:5432/database`
- MySQL: `mysql://user:password@localhost:3306/database`
- MariaDB: `mariadb://user:password@localhost:3306/database`

## Best practices

1. **Review Generated SQL**: Always review the generated migration files before applying them
2. **Test Migrations**: Test both UP and DOWN migrations in a development environment
3. **Backup Data**: Always backup your database before running migrations in production
4. **Version Control**: Commit migration files to version control
5. **Sequential Application**: Apply migrations in the correct order based on timestamps

## How the reverse plan handles each category

A DOWN migration is planned from the UP diff with the added and removed
collections exchanged and the pre-change database schema supplied as the target.
Three rules decide what happens to each field of the diff, and every field has
exactly one of them:

- **Added and removed are exchanged** where both sides carry the same kind of
  value and the reverse operation is the inverse of the forward one: tables,
  enums, indexes, extensions, functions, sequences, domains, composite types,
  ranges, views, materialized views, triggers, RLS policies, RLS enablement,
  roles, grants, grant options and constraints. A created view is dropped on
  rollback; a dropped view is recreated from the pre-change schema.
- **Modified entries are carried across, not exchanged.** A modification is not
  the inverse of itself. The planner re-renders a modified object from the schema
  it is given, and the DOWN direction is given the pre-change database schema, so
  carrying the entry across is what restores the prior definition. The recorded
  `old -> new` description is flipped so the diff reads truthfully in the DOWN
  direction, and a view's recorded prior body is exchanged for the UP
  migration's target body, because that is the state the database is in when the
  rollback runs.
- **Derived entries are rebuilt** from the pre-change database rather than
  swapped, because a rollback must restore the prior body rather than the new
  one. This covers the table-qualified constraint collections. The recorded
  catalog identifier semantics are copied rather than reversed: they describe the
  catalog the diff was measured against, which has no direction.

No category is dropped from the rollback. Two tests hold that claim up, and they
check different things. A reflection test over `SchemaDiff` fails when a field is
added and the reverse builder does not read it, because adding a field to a
struct literal is silent about the fields the literal omits. A second test
renders the rollback and requires the modified view, materialized view and
trigger to appear in it: reaching the reversed diff is not the same as reaching
the file, because a modified object is re-rendered from the pre-change schema by
a name lookup, and a lookup that misses renders nothing at all.

### View modifications choose per view, and the direction settles the rest

PostgreSQL accepts `CREATE OR REPLACE VIEW` only when the new query produces the
old column list with columns appended to the end. Measured on PostgreSQL 17.10
against a view over `(id bigint, email text, age integer)`:

| change to the view | PostgreSQL |
| --- | --- |
| append a trailing column | accepted |
| drop the appended column | `ERROR: cannot drop columns from view` |
| rename a column | `ERROR: cannot change name of view column "id" to "uid"` |
| change a column type | `ERROR: cannot change data type of view column "id"` |
| change only the predicate | accepted |

A projected column's type is fixed by the relation it reads, which the select
list does not say, so the relations are compared as well: swapping
`SELECT id FROM b` for `SELECT id FROM a` keeps the column name and still fails
with `cannot change data type of view column "id"`.

That comparison folds letter case only outside quoted identifiers. PostgreSQL
folds an unquoted identifier to lower case and keeps a quoted one exactly, so
`"Foo"` and `"foo"` are two relations; folding them together answered "the
relations did not change" and produced a `CREATE OR REPLACE VIEW` PostgreSQL
17.10 refuses with `cannot change data type of view column "id" from bigint to
text` — `psql -v ON_ERROR_STOP=1 -f` exits 3. The
conservative side of the same rule is that `"foo"` and `foo`, which *are* the
same relation, compare as changed and cost a drop and recreate. Telling those
apart needs a real parse of the `FROM` clause, because `"join"` is a relation
where `join` is a keyword.

Both statements cost something. The replace can be refused at execution time. The
drop always applies and carries `CASCADE`, which takes dependent views and
materialized views with it along with the privileges granted on the view. So Ptah
decides per view:

| what Ptah can prove | UP | DOWN |
| --- | --- | --- |
| the new column list appends to the old one, over the same relations | replace | replace |
| the column list moved, or the relations changed | drop and recreate | drop and recreate |
| neither — a `WITH` prefix, a `SELECT *` projection, a top-level set operation, or no prior body at all | replace | drop and recreate |

The last row is the only place direction enters. Going forward, an undecidable
body is usually a predicate-only edit where the column list never moves and the
replace is accepted; if it is not, PostgreSQL refuses the statement and the
migration stops having destroyed nothing. A rollback cannot be answered that way,
because it is already running during the incident it was meant to end, so it
takes the statement that always applies.

Wherever the drop path runs, the plan rebuilds every view and materialized view
the `CASCADE` takes, transitively, in dependency order. What it cannot rebuild is
anything Ptah does not declare.

That rebuild list is read off the declared bodies, and it reads their **code**:
a name spelled inside a string literal, a dollar-quoted string, a line comment
or a block comment refers to nothing and does not join the list. The distinction
is not cosmetic, because every name on the list is answered with a statement —
a materialized view is dropped with `CASCADE` and recreated. Measured on
PostgreSQL 17.10, a materialized view whose body is
`SELECT 'base_view' AS label, count(*) AS total FROM accounts` was dropped when
`base_view` was modified, and the drop took a hand-made dependent view, a unique
index on the materialized view and the `SELECT` granted on it; none of the three
is declared, so nothing put them back.

## Limitations

- **Data Loss Warning**: DOWN migrations may result in data loss (e.g., dropping columns/tables)
- **Complex Changes**: Some complex schema changes may require manual intervention
- **Database-Specific Features**: Some database-specific features may not be fully supported in reverse migrations
- **Materialized View Contents**: A rollback that recreates a materialized view
  recreates its definition, and PostgreSQL populates it as part of the `CREATE`
  — Ptah does not emit `WITH NO DATA`. The contents are therefore recomputed
  from the base tables as they stand at rollback time, not restored: a snapshot
  that had deliberately not been refreshed is silently refreshed, and the
  rollback pays for the whole query. Anything attached to the old materialized
  view rather than to its definition — its indexes, and the privileges granted
  on it — is gone with the drop.
- **Dependents of a Recreated View**: `DROP ... CASCADE` takes dependent objects
  with it. Every view and materialized view Ptah declares is rebuilt after the
  view it reads, but an object Ptah does not declare — a hand-made view, a rule,
  or a privilege granted on the view itself — is not, and is lost with the drop.
- **Views Read Under Two Names**: A modified object is found in the pre-change
  schema by name, and the two sides do not always spell it the same way: the diff
  records the name the Go schema uses while the introspected schema qualifies it
  with the schema or database it was read from. The lookup accepts either
  spelling, but only where one candidate carries the unqualified name. Two views
  of the same name in different schemas are ambiguous, and the modification is
  skipped rather than guessed at.
