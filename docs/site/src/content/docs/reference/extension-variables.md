---
title: Extension environment variables
description: The PTAH_* booleans that turn on behavior the pinned Atlas community CLI has no counterpart for, one entry per variable.
type: reference
audience:
  - "all-users"
readerQuestion: "Which `PTAH_*` variables enable compatibility extensions?"
goal: "Look up the `PTAH_*` variables that enable compatibility extensions."
sourceOfTruth:
  - "cmd"
  - "core"
  - "migration"
generated: false
overlaps: []
disposition: keep
---

You want a capability Ptah models and the community CLI does not, on the
`ptah-compat` surface rather than on a native `ptah` verb. Each one is reached
through a `PTAH_*` boolean environment variable, and this page is the inventory:
what each variable's default is, what setting it turns on, and why the default
is what it is.

They are environment variables rather than flags on purpose, and
[Capabilities retained
deliberately](../../atlas/strict-ce-mode/#capabilities-retained-deliberately)
carries that argument. Native `ptah` verbs always emit everything Ptah models,
with no switch to set.

Every variable below is a boolean, and they all read the same way: leaving it
unset selects the default described here, a valid boolean is honored, and
anything else — including an exported empty value — fails the command before it
does any work, naming the variable and the value you typed. The accepted
spellings and the error shape are documented once, in
[Boolean environment variables](../configuration/#boolean-environment-variables).

The value is read on every run of the command that owns it, not only on the runs
that would have used the enabled behavior, so `PTAH_ATLAS_LINT_ALL_VERSIONS=yes`
in a CI environment file fails the next run rather than the next run that
happens to omit `--latest`.

## At a glance

Each entry below carries the default, the reason for it, and what the pinned
community CLI does on the same input.

| Variable | Set to `1` |
| --- | --- |
| [`PTAH_ATLAS_INSPECT_ALL_BLOCKS`](#ptah_atlas_inspect_all_blocks) | Emit every PostgreSQL block, unreferenced ones included |
| [`PTAH_POSTGRES_INSPECT_ALL_ROLES`](#ptah_postgres_inspect_all_roles) | Describe every role on the server |
| [`PTAH_ALLOW_RESERVED_ROLE_NAMES`](#ptah_allow_reserved_role_names) | Plan a reserved role name |
| [`PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`](#ptah_sqlite_allow_virtual_table_drop) | Plan a virtual-table drop |
| [`PTAH_SQLITE_ALLOW_UNREGISTERED_VIRTUAL_MODULE`](#ptah_sqlite_allow_unregistered_virtual_module) | Compare an unregistered module's storage |
| [`PTAH_ALLOW_EXTERNAL_SCHEMA`](#ptah_allow_external_schema) | Evaluate `data "external_schema"` |
| [`PTAH_ATLAS_LINT_WITHOUT_DEV_URL`](#ptah_atlas_lint_without_dev_url) | Lint with no dev database |
| [`PTAH_STRICT_DIR_QUERY`](#ptah_strict_dir_query) | Refuse an unknown `--dir` query key |
| [`PTAH_ATLAS_LINT_ALL_VERSIONS`](#ptah_atlas_lint_all_versions) | Lint the whole directory |
| [`PTAH_ATLAS_PLAN_LINT_FAIL_ON_ERROR`](#ptah_atlas_plan_lint_fail_on_error) | Let a plan-lint error decide the exit code |
| [`PTAH_SCHEMA_DIFF_TEMPLATE_HELPERS`](#ptah_schema_diff_template_helpers) | Register the shared `--format` helpers |
| [`PTAH_HCL_MERGE_REDECLARATIONS`](#ptah_hcl_merge_redeclarations) | Merge repeated blocks again |
| [`PTAH_HCL_STRICT_REDECLARATIONS`](#ptah_hcl_strict_redeclarations) | Refuse four more repeated block kinds |
| [`PTAH_HCL_SCHEMA_SCOPED_ENUMS`](#ptah_hcl_schema_scoped_enums) | Key `enum` blocks by qualified name |

## `PTAH_ATLAS_INSPECT_ALL_BLOCKS`

By default, `ptah-compat schema inspect`
leaves an `extension`, `sequence` or `policy` block out of PostgreSQL HCL
output when nothing else in the document depends on it, and reports each
omission on standard error. For an extension, "depends on" is measured against
what the catalog says the extension supplies — `isn` supplies the type `isbn` —
rather than against its name, and against what the catalog resolved for the
document's indexes, since a GIN index over an `integer` column needs `btree_gin`
and prints no word of it. Set it to `1` and every block Ptah models is
emitted: the output describes the database in full, and the community CLI
refuses it.

## `PTAH_POSTGRES_INSPECT_ALL_ROLES`

By default, a PostgreSQL read describes
only the roles the inspected schemas use, because roles are cluster-wide and a
description of one database is not a place to list another tenant's roles. Each
read reports on standard error how many managed roles it left out. Set it to
`1` and every role Ptah manages on the server is described again, which is what
you need to reproduce one cluster's roles in another. It widens the description
only: comparison already treats undescribed roles as present, so the planned
statements are identical either way. Reserved `pg_` names and the bootstrap
`postgres` superuser are outside it in both directions.

## `PTAH_ALLOW_RESERVED_ROLE_NAMES`

By default, a desired schema that
declares a reserved PostgreSQL role is refused before anything is compared or
planned, naming the role and the rule, because Ptah manages neither the `pg_`
roles nor the bootstrap `postgres` superuser in either direction and the
declaration would otherwise become a `CREATE ROLE` the server rejects at
SQLSTATE 42939 or 42710. Set it to `1` and the declaration is planned instead,
as it was before the refusal existed. That is worth having on a cluster
bootstrapped under a name other than `postgres`, where `CREATE ROLE "postgres"`
succeeds.

## `PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`

By default, a comparison whose
database side holds a SQLite virtual table is refused before anything is
compared, naming the table and its module. No desired-state format can declare a
virtual table, so its absence is not a request to drop it, and planning the
removal deletes the index and everything in it. Measured on the pinned community
binary v1.3.0, `schema diff` plans that drop plus one per shadow table and
exits 0.

- Set it to `1` and the removal is planned as before.
- `--exclude <table>` is the other direction: the table is kept and the rest of
  the schema converges.
- A malformed value refuses every SQLite comparison command and public
  migration-generator call before filesystem path resolution, schema source
  loading, database connection, or SQL. Non-SQLite operations ignore it.
- The opt-in covers only the removal. A desired ordinary table colliding with a
  live virtual one stays refused however it is set, because the planner cannot
  convert one kind into the other.
- `schema inspect` compares nothing and is unaffected.

See [SQLite](../../databases/sqlite/) for the whole picture.

## `PTAH_SQLITE_ALLOW_UNREGISTERED_VIRTUAL_MODULE`

By default, a comparison
whose database side holds a virtual table using a module this build does not
register is refused before anything is compared, naming the table, the module,
and the modules this build does register. SQLite marks a module's shadow tables
as `shadow` only while the module is loaded, so without it that module's private
storage is described as ordinary user tables — and a desired state that does not
name them reads as a request to drop them. Measured on `fts3` and `fts4`:
excluding the virtual table left the storage in the comparison and
`ptah schema apply` dropped all of it at exit 0, after which `MATCH` answered
`SQL logic error`. The `fts5` control, whose module this build does register,
reported a synced schema and changed nothing.

- Set it to `1` and the comparison proceeds against the module's storage as the
  ordinary tables it appears to be, accepting those drops. This is what Ptah did
  before the refusal existed.
- Excluding the virtual table is **not** an escape here and is not suggested:
  the tables at risk are the module's own storage, not the table an operator
  would name, and Ptah cannot list them without the module.
- It is separate from `PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`, which permits
  dropping a virtual table Ptah can see. Neither implies the other.
- Adding such a table has no opt-in. A plan carrying
  `CREATE VIRTUAL TABLE ... USING fts4` fails on this build with
  `no such module: fts4`, and no value of a variable makes a module exist. This
  fires only where that statement would actually be planned — virtual on the
  desired side, absent from the database — so two databases that both already
  hold the same `fts4` index compare normally under the opt-in.
- A read is never refused. `ptah db read` and `schema inspect` print a note
  naming the table and module, and leave standard output and the exit code
  alone.
- A project that skips table drops is not asked for it. With
  `diff { skip { drop_table = true } }` in the project file — or
  `diff.skip: [drop_table]` in `ptah.yaml` — every table drop and the dependent
  removals it carries are deleted from the diff before any SQL is rendered, so
  the refusal, which is a claim about a `DROP TABLE`, does not fire. What still
  fires is a rebuild: a desired state that NAMES one of the module's storage
  tables and describes it differently is refused under the policy too, because
  `skip drop_table` filters removals rather than modifications and SQLite
  converges a modification by dropping and recreating the table.
- A change SQLite can make in place is not asked for it either. A table whose
  only change is a column the desired state adds is planned as
  `ALTER TABLE ... ADD COLUMN`, which drops and rebuilds nothing, so a narrowed
  comparison such as `--include users` against a database holding an `fts4`
  index runs at exit 0 and prints that one statement. Remove or change a column
  on the same table, or change a constraint, and it is a rebuild again and
  refused again.

See [SQLite](../../databases/sqlite/) for the whole picture.

## `PTAH_ALLOW_EXTERNAL_SCHEMA`

By default, `atlas.hcl`
`data "external_schema"` is not evaluated, because it runs a
repository-controlled program. Set it to `1` and the data source is evaluated,
matching the native `--allow-external-schema` flag.

## `PTAH_ATLAS_LINT_WITHOUT_DEV_URL`

By default, `ptah-compat migrate lint` requires `--dev-url`, because the community CLI marks
it required and exits 1 without it. Ptah's analyzers read the migration files
and need no database, so set it to `1` and the run proceeds with no dev database
and reports what the static analysis finds. Native `ptah migrations lint` needs
no opt-in.

## `PTAH_STRICT_DIR_QUERY`

By default, a `--dir` URL query key other than
`format` is ignored, exactly as the community CLI ignores it, and named on
standard error so a misspelled `?fromat=goose` does not quietly read the
directory in the wrong layout. Set it to `1` and such a key is a refusal
instead, for a pipeline that wants a typo to stop the run. The value is read on
every run of the eight verbs that accept a `--dir` query — `apply`, `diff`,
`hash`, `lint`, `new`, `set`, `status` and `validate` — whether or not the URL
carries a query at all, so `PTAH_STRICT_DIR_QUERY=nope` in a CI environment file
fails the next run rather than the next typo.
`migrate checkpoint`, `down`, `edit`, `rebase`, `rm`
and `test` refuse a `--dir` query outright, so neither the note nor this
variable applies there.

## `PTAH_ATLAS_LINT_ALL_VERSIONS`

By default, `ptah-compat migrate lint`
refuses a run that names no scope, because the community CLI refuses it:
`--latest`, `--git-base` or an `atlas.hcl` `lint` block supplying one is
required, and without it the answer is
`Error: --latest or --git-base is required` at exit 1, before the migration
directory is read and before `--dev-url` is contacted. Set it to `1` and the
whole directory is linted instead, which is what Ptah's own linter does. Native
`ptah migrations lint` needs no scope and ignores the variable.

## `PTAH_ATLAS_PLAN_LINT_FAIL_ON_ERROR`

By default, `ptah-compat schema plan
lint` reports and does not gate: a plan carrying a destructive change is
described on stdout and the command exits 0. A plan is a document an operator
reviews and approves, and a report that refuses on their behalf is one they
cannot approve anything with. Set it to `1` when a pipeline needs the report to
gate, and an error-severity finding exits 1 with the report still on stdout.

Either way the command states on stderr what the report covers: Ptah's rule set
is its own and does not name every hazard a schema change can carry, so a report
without findings describes the rules rather than the plan. It is an environment
variable rather than a flag for the same reason the two above are — the
conformance `cli-surface` tier asserts flag parity with the pinned binary, and an
environment variable is invisible to the help surface.

## `PTAH_SCHEMA_DIFF_TEMPLATE_HELPERS`

By default, `ptah-compat schema diff
--format` registers one helper, `sql`, which is what the pinned community binary
offers there; `{{ json . }}` exits 1 with `function "json" not defined`. Set the
variable to `1` and the shared helper set `schema apply` already registers
becomes available on this verb too, so `{{ json . }}` renders a
document carrying `From`, `To` and `Changes`. The default stays narrow because
registering more would let ptah-compat accept a template the community binary
refuses. Native `ptah schema diff` needs no variable: `--format json` emits a
machine-readable diff there already.

## `PTAH_HCL_MERGE_REDECLARATIONS`

By default, an HCL schema document that
declares one object twice is refused, naming the kind and the object. Before the
refusal existed the second declaration was folded into the first and the run
reported success, so a file declaring `table "users"` twice was read as one
table while the community CLI exits 1 on it with
`pq: relation "users" already exists`. Set the variable to `1` and the merge
comes back, on both the compatibility surface and native `ptah` verbs.

Which kinds refuse is measured rather than chosen: a repeat is refused where the
community CLI refuses the same document.

- Refused: `table`, `column`, `index`, a named `check` or `constraint`,
  `foreign_key`, `enum`, `sequence`, `domain`, `composite`, `range`,
  `extension`, `trigger`, `policy`.
- Exempt under every setting: `schema`, because a directory of HCL files is one
  document and its files each open with the same block; `function`, because two
  blocks sharing a name can be two legal overloads; `permission`, which renders
  a GRANT the engine accepts twice; and `data`, which declares no database
  object.

## `PTAH_HCL_STRICT_REDECLARATIONS`

By default, a repeated `view`,
`materialized`, `role` or `unique` block is read at exit 0, because the
community CLI reads it at exit 0: it drops the first three unread and merges two
`unique` blocks sharing a label into one. Refusing them is above the drop-in
floor rather than on it, so it is opt-in. Set the variable to `1` and each of
the four is refused within one document, which is the rule an HCL schema
*directory* already applies across its files. The kinds refused by default are
still refused with it set; the four exceptions above are still exempt.

## `PTAH_HCL_SCHEMA_SCOPED_ENUMS`

By default, two `enum` blocks sharing a bare
name are one object however they are spelled, because the community CLI keys
enums by their bare name and answers `duplicate enum "mood"` at exit 1 for
`enum "mood"` in two schemas and for the two-label `enum "public" "mood"` /
`enum "other" "mood"` alike. Set the variable to `1` and they are keyed by their
qualified name, which is what `public.mood` and `other.mood` are. This is the
setting under which `ptah-compat schema inspect` of a database holding one enum
name in two schemas can be applied again: the document names both, and reading
it back re-renders it byte for byte. The community CLI has no such setting and
refuses the document its own inspect writes for that database.

## One shape has no Atlas-readable form at all

Suppression can only leave out a block nothing else names. A **sequence behind a
column default** is named, so the block stays and the document is not readable
by the community CLI:

```sql
CREATE SEQUENCE order_seq;
CREATE TABLE orders (id integer NOT NULL DEFAULT nextval('order_seq'::regclass));
```

This is not a gap Ptah can close. Measured on PostgreSQL 17, the community CLI's
own inspect of that database emits
`default = sql("nextval('order_seq'::regclass)")` with no `sequence` block, and
then cannot read its own output back: `pq: relation "order_seq" does not exist`.
There is no faithful description of that database the CLI can read — not Ptah's
and not its own. Ptah keeps the sequence, so the document is at least readable
by Ptah and true about the database, and says so on standard error. Dropping the
column's default to make the file readable would describe a database you do not
have, which is the one outcome worse than a refusal.

So `ptah-compat schema inspect` is not a promise that every PostgreSQL database
produces community-CLI-readable HCL. It is a promise that the output is always
self-consistent, that nothing disappears without being reported, and that the
full description is one environment variable away.

## Next steps

- The argument these variables implement:
  [Capabilities retained deliberately](../../atlas/strict-ce-mode/#capabilities-retained-deliberately).
- The boolean grammar and the strict-compatibility selector:
  [Configuration](../configuration/#boolean-environment-variables).
- The per-verb flag surface that reads them:
  [Atlas-compatible commands](../atlas-commands/).
