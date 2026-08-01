---
title: Atlas schema commands
description: Inspect, diff, apply, plan, format, clean, and test schemas with the Atlas-style ptah-compat schema verbs.
---

You want Atlas-style declarative schema work — inspect a live database, diff
schema files, apply or plan desired-schema changes — through Ptah. This page
covers the `ptah-compat schema` verbs with runnable examples. Every invocation
on this page uses the separate `ptah-compat` drop-in binary; the install steps
plus the flag translation rules are on the
[Atlas compatibility overview](../overview/).

## Command behavior

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah-compat schema inspect` | Inspects a live database, a local schema file, a migration directory, or an `env://` reference (non-database sources evaluated on the `--dev-url` dev database) and writes Atlas-shaped HCL, SQL, JSON, or custom-template output, including split/write file exports. |
| `ptah-compat schema apply` | Diffs a desired-state source (schema files, a database URL, a migration directory, or an `env://` reference) against a live database and applies the planned SQL after confirmation. |
| `ptah-compat schema plan` | Saves the declarative plan as a fingerprinted local plan file for a later `schema apply --plan`. |
| `ptah-compat schema diff` | Diffs two desired-state sources (schema files, database URLs, migration directories, or `env://` references) and prints migration SQL. |
| `ptah-compat schema fmt` | Formats local `.hcl` files using HCL canonical layout. |
| `ptah-compat schema clean` | Plans and applies destructive cleanup of user-owned schema objects. |
| `ptah-compat schema test [paths]` | Forwards to `ptah schema test` with Ptah-native YAML test cases. |
| `ptah-compat schema push` | Atlas CE boundary stub; the native `ptah schema push` to any OCI registry is the open replacement. |

Per-verb status detail — Atlas differences, waivers, and the inputs that fail
explicitly — is on [Atlas-compatible commands](../../reference/atlas-commands/).

## Inspect a schema source

`ptah-compat schema inspect` accepts a `--url` inspection source and writes
machine-oriented schema output without native Ptah status banners. The default
format is Atlas-compatible HCL. The source is a live database URL, a local
schema file (`.hcl`, `.yaml`, `.yml`, or `.sql`), a migration directory (a
directory containing `atlas.sum`), or an `env://` reference into the evaluated
`atlas.hcl` environment.

```bash
ptah-compat schema inspect --url "$DATABASE_URL" > schema.hcl
ptah-compat schema inspect --url "$DATABASE_URL" --format sql > schema.sql
ptah-compat schema inspect --url "$DATABASE_URL" --format json > schema.json
```

:::caution[A dev database executes SQL for real]
Whatever you pass as `--dev-url` runs the SQL Ptah is evaluating, so point it
at a disposable database. This matters most when the SQL came from a plan file
someone else wrote — see
[The replay is not a sandbox](#the-replay-is-not-a-sandbox).
:::

Non-database sources require `--dev-url`, mirroring Atlas dev-database
normalization: the dev database is reset destructively, the source is
materialized on it (schema files executed, migration directories replayed),
and the result is introspected. Inspecting a file without `--dev-url` fails
with Atlas's `--dev-url cannot be empty` message.

```bash
ptah-compat schema inspect \
  --url file://schema.sql \
  --dev-url "$DEV_DATABASE_URL" > schema.hcl
```

`--schema` / `-s` narrows inspection when the underlying database reader supports
schema scoping. `--format`
accepts Atlas-style Go templates with `.MarshalHCL`, `hcl`, `sql`, `json`,
`base64url`, `mermaid`, `split`, and `write`. Split-write exports are
supported for HCL and SQL output with the documented Atlas split strategies:
per object (the default: one file per object under per-type directories, with
a `main.sql` `atlas:import` entry point for SQL), `"schema"` (one file per
schema), and `"type"` (one file per object type), plus an optional
file-extension argument:

```bash
ptah-compat schema inspect \
  --url "$DATABASE_URL" \
  --format '{{ hcl . | split | write "schema" }}'

ptah-compat schema inspect \
  --url "$DATABASE_URL" \
  --format '{{ sql . | split "type" | write "schema" }}'

ptah-compat schema inspect \
  --url "$DATABASE_URL" \
  --format '{{ hcl . | split "schema" ".pg.hcl" | write "schema" }}'
```

Rendering plans the output files first, and one writer applies the plan:
duplicate output paths, paths that escape the output directory, collisions
between a planned file and a planned directory, and destinations that already
exist as directories fail explicitly before anything is written. Unsupported
split modes and unsafe extension arguments fail at render time. The pinned
Atlas CE binary rejects the `split`, `write`, and `hcl` template functions as
non-community features, so these exports are an open Ptah extension that
follows the documented Atlas behavior.

`--exclude` accepts repeated or comma-separated Atlas-style glob patterns,
including `[type=...]` selectors, and removes matching resources from HCL,
SQL, JSON, and custom-template output. Field-level exclude selector support
includes the Atlas-documented `*[type=extension].version` form, including
schema-qualified globs such as `public.*[type=extension].version`.

Other field-level selectors, and type selectors on non-final pattern segments,
fail explicitly before any database is contacted. Schema-qualified function
and enum filters remain limited by Ptah's current introspection model, which
does not retain schema names for those resource types yet. Exporter blocks
remain an explicit gap.

### Select what is inspected with `--include`

`--include` positively selects which top-level resources survive inspection,
with the same selector engine as [`schema apply` and `schema
diff`](#scope-the-comparison-with---schema-and---include): `--schema` names
the schema universe, `--include` picks resources inside it, and `--exclude`
subtracts from the result. Repeated and comma-separated values union.
Selectors that match nothing render no objects; an empty value carries no
selection, so inspection stays unfiltered.

```bash
ptah-compat schema inspect --url "sqlite://app.db" --include users
ptah-compat schema inspect --url "postgres://localhost/app" \
  --schema public --include 'app_*' --exclude app_scratch
```

Child resources — columns, indexes, constraints, triggers, policies, grants —
ride along with their parent and cannot be selected on their own, in either
the `[type=column]` or the literal-dot `table.column` spelling; both fail
before any database is contacted. Glob metacharacters — `*`, `?`, and
character classes — still match a dot, so `table*column` and `table[.]column`
are not caught by that check and select nothing instead
([`stokaro/ptah#979`](https://github.com/stokaro/ptah/issues/979)). An
identifier that itself contains a dot is selected as `main."my.table"` or
`a\.b\.c`; the bare `a.b.c` spelling is refused as ambiguous. A selection
that keeps an object whose dependency it dropped is refused rather than
rendered, so inspected output never references an object it omits:

```text
error: the --schema/--include selection drops objects that selected objects depend on:
  - table "main.posts" depends on table "main.users" via a foreign key, but "main.users" is not selected
add the missing objects to the selection or exclude the dependent objects
```

The flag is not part of the pinned Atlas CE inspect surface: CE v1.2.0 rejects
`schema inspect --include` with `Error: unknown flag: --include`. It is
registered on the licensed Atlas build, where its behavior differs from
Ptah's in two measured ways, both documented in
[the comparison](../comparison/#schema-inspect---include).

## Apply a desired schema

`ptah-compat schema apply` accepts a live database `--url` and a `--to`
desired state: one or more local schema file URLs, one directly connectable
database URL whose live schema becomes the desired state, one migration
directory (a `file://` directory containing `atlas.sum`) replayed on the
required `--dev-url` dev database, or one `env://<attribute>` reference
(`src`, `schema.src`, `url`, `dev`, `migration.dir`) resolved through the
evaluated `atlas.hcl` env.

All `--to` values must be one source kind, and unsupported schemes such as
`atlas://` fail before the target database is contacted.

With `--env`, Ptah can read `env.url`, `env.src`, `env.schema.src`, `env.dev`,
`env.exclude`, `env.schema.mode`, `format.schema.apply`, and supported `diff`
policy from the selected `atlas.hcl` environment, including local variable
defaults, locals, `getenv`, `file`, `fileset`, `format`, `jsonencode`, and
`data.hcl_schema.<name>.url` references. Explicit CLI flags still take
precedence.

Ptah reads the current database schema, diffs it against the desired local
schema files, prints the planned SQL, and applies it after interactive
confirmation. Use `--dry-run` to print the plan without applying it, or
`--auto-approve` to skip the prompt explicitly.

Use `--tx-mode=file` or `--tx-mode=all` to execute the generated plan in one
transaction, or `--tx-mode=none` to execute statements without transaction
wrapping. With `--edit`, the planned SQL opens in `$VISUAL` or `$EDITOR`
before the plan is shown and approved, and the edited SQL is what gets
applied.

For Atlas script compatibility, `schema apply` also accepts the hidden
`--file/-f` alias for local HCL or SQL paths and maps it to the same
local desired-schema loading path as `--to`. `--file` and `--to` are mutually
exclusive.

```bash
ptah-compat schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run
```

Expected output includes:

```text
Planned schema changes:
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "email" TEXT NOT NULL UNIQUE,
  "name" TEXT
);
```

An `atlas.hcl` environment can carry the same inputs:

```hcl
data "hcl_schema" "app" {
  paths = fileset("schema/*.hcl")
}

env "local" {
  url = getenv("DATABASE_URL")
  dev = getenv("DEV_DATABASE_URL")
  schema {
    src = data.hcl_schema.app.url
    mode {
      funcs = false
    }
  }
  format {
    schema {
      apply = "{{ sql . \"  \" }}"
    }
  }
}
```

```bash
ptah-compat schema apply --env local --dry-run
```

`--dev-url` must match the target database dialect. For migration-directory
`--to` sources it names the dev database the directory is replayed on and is
required.

Before a non-dry-run apply touches the target, the generated plan is rehearsed
on the dev database: Ptah resets the dev database, recreates the target's
introspected current schema on it, and executes the exact ordered plan
statements — including SQL edited through `--edit` — under the same
transaction mode as the target apply. A failed rehearsal refuses the apply and
leaves the target unchanged.

The dev database must not be the target itself or a database-URL `--to`
desired state (it is reset destructively), must be directly connectable (no
`docker://`), and must use the same schema scope as the target on
scope-parameterized dialects such as SQL Server.

`--lock-timeout` bounds how long the apply waits for the session advisory lock
(`ptah_schema_apply`) that serializes concurrent schema applies against one
target database. The lock is acquired before target inspection and planning,
held through simulation, confirmation, and execution, and released on every
exit path including cancellation.

An empty value waits indefinitely; an elapsed timeout fails the apply before
the target is inspected. PostgreSQL (`pg_advisory_lock`), MySQL and MariaDB
(`GET_LOCK`), and SQL Server (`sp_getapplock`) use real database locks.

SQLite, ClickHouse, CockroachDB, YugabyteDB, and Spanner have no advisory-lock
semantics: the apply proceeds without a lock, and an explicitly passed
`--lock-timeout` prints a note on stderr.

`--exclude` accepts repeated or comma-separated Atlas-style glob patterns,
including `[type=...]` selectors. Ptah applies the filter to both the current
live schema and the desired local schema files before planning, so excluded
objects are ignored rather than dropped.

Disabled `schema.mode` values are mapped to the same resource-exclusion system
for object kinds represented in Ptah's schema IR. `diff.skip.drop_table = true`
removes table drops from supported local plans. For non-dry-run PostgreSQL
`schema apply` plans that actually emit `CREATE INDEX CONCURRENTLY`,
`diff.concurrent_index.create = true` requires `--tx-mode none`;
`diff.concurrent_index.drop` and `diff.skip.drop_schema` fail explicitly.

### Scope the comparison with `--schema` and `--include`

`--schema/-s` and `--include` positively select what both comparison sides
see, on `schema apply` and `schema diff` alike:

- `--schema` names define the schema universe. Repeated and comma-separated
  values union deterministically. On PostgreSQL-family targets the names are
  schema namespaces and unqualified objects belong to the connection's default
  schema (`public`). On MySQL and MariaDB a schema is a database, and because
  a Ptah connection is bound to one database, only the connected database's
  name selects anything. SQLite has the single schema `main`.
- `--include` picks top-level resources inside that universe with Atlas-style
  glob selectors, optionally constrained with `[type=...]`. Selectable types:
  `table`, `view`, `materialized_view`, `function`, `enum`, `extension`,
  `sequence`, `domain`, `composite_type`, `range`, and `role`. Repeated and
  comma-separated selectors union deterministically. Children of a selected
  table — columns, indexes, constraints, triggers, policies, grants, and seed
  data — ride along with it, and support objects the selection depends on
  (enums and other types used by kept columns, sequences owned by kept
  tables, roles named by kept grants, owning schemas) are retained.
  Child-resource selectors such as `[type=column]` or `[type=index]`, field
  selectors, and unknown resource types are rejected loudly because Ptah
  cannot project a partial parent faithfully. The literal-dot spelling of the
  same thing is rejected too: a selector names a resource as `name` or
  `schema.name`, so a pattern with a deeper path such as `main.users.email`
  or `main.users.*` fails instead of silently selecting nothing.

#### Selecting an identifier that contains a dot

Selector depth counts separators **outside quotes**, so a quoted identifier
holding a dot stays a valid depth-one selector:

```bash
ptah-compat schema diff … --include 'main."my.table"'   # qualified
ptah-compat schema diff … --include '*."my.table"'      # any schema
```

The **bare** spelling of a dotted name is refused, because `a.b.c` cannot be
told apart from the `schema.table.column` form the depth rule exists to
reject:

```text
$ ptah-compat schema diff … --include 'a.b.c'
error: unsupported Atlas include selector "a.b.c": selectors name top-level
resources as "name" or "schema.name", and a deeper pattern names a child
resource that rides along with its parent
```

Spell it one of two unambiguous ways instead — escape the dots, or quote the
identifier:

```bash
ptah-compat schema diff … --include 'a\.b\.c'
ptah-compat schema diff … --include 'main."a.b.c"'
```

This is a deliberate trade of one ambiguous spelling for two exact ones.
Removing the need for it requires checking the selection's outcome rather than
its shape, tracked in
[`stokaro/ptah#979`](https://github.com/stokaro/ptah/issues/979).

#### What the depth check does not catch

It covers the literal-dot spelling only. Every glob metacharacter that can
stand for a dot escapes it — `*`, `?`, and a character class — so all three of
these reach past a top-level resource and report a synced schema instead of
failing:

```bash
ptah-compat schema diff … --include 'main.users*email'
ptah-compat schema diff … --include 'main.users?email'
ptah-compat schema diff … --include 'main.users[.]email'
```

Closing the whole class needs the outcome-based check in
[`stokaro/ptah#979`](https://github.com/stokaro/ptah/issues/979).
- `--exclude` and disabled `schema.mode` values subtract from the positive
  selection afterward. The composition order is fixed: schema universe first,
  include selection inside it, exclusion last.

The same projection is applied to the current database state and the desired
schema, so out-of-scope objects are invisible to the comparison and are never
created, modified, or dropped. A selected object that depends on an object the
selection dropped — a foreign key to an unselected table, a function calling
an unselected function, a view or trigger body referencing an unselected
relation, a column using an excluded enum — refuses the plan with an explicit
cross-scope diagnostic instead of emitting incomplete SQL:

```text
error: apply --schema/--include to desired schema: the --schema/--include selection drops objects that selected objects depend on:
  - table "scope_groups" depends on table "scope_users" via a foreign key, but "scope_users" is not selected
add the missing objects to the selection or exclude the dependent objects
```

A selection that matches nothing is not an error: the comparison sees two
empty projections and reports a synced schema. Malformed selectors fail during
validation, before any database is contacted.

`--format` accepts Atlas-style Go templates over the planned apply changes. The
supported template surface includes the `sql` helper and `.MarshalSQL`:

```bash
ptah-compat schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run \
  --format '{{ sql . "  " }}'
```

## Save and execute plan files

`ptah-compat schema plan` is the open local replacement for Atlas's Pro
registry-gated plan workflow, and it speaks Atlas's plan-file format.

It computes the same declarative plan `schema apply` would generate — from the
`--from` target database to the local `--to` schema files — and saves it as a
local plan file. The default format is the Atlas `.plan.hcl` shape: one
`plan "<name>"` block with `from`/`to` fingerprints and the migration SQL in a
heredoc, named with an Atlas-style UTC timestamp. The written file parses in
Atlas's own plan reader; the `from`/`to` values are Ptah's sha256
fingerprints, which the official binary parses but cannot verify against its
own base64 hashes (those have no local recipe — in either direction). An
`--output` path ending in `.json` writes the native JSON plan
(`format_version` 1) instead, which additionally records per-statement safety
severity, the dialect, and exclude patterns. Without
`--save`/`--output`/`--dry-run`, the plan document prints to stdout, and
`--auto-approve` is accepted for CLI compatibility.

`schema apply --plan file://<path>` accepts both formats, detected by
content — including `.plan.hcl` files written by the licensed Atlas binary:

- A **JSON plan** executes after verifying the live database still matches
  the plan's recorded source fingerprint; a drifted database refuses with a
  stale-plan error instead of running reviewed SQL against unreviewed state.
  `--to` is optional.
- An **Atlas-format plan** requires `--to`, exactly like the official binary
  (`the flag "to" is required to verify the provided plan`). Its hashes are
  re-verified with Ptah's own machinery: the plan is replayed on a dev
  database starting from the target's current schema, and the reached state
  must equal the `--to` desired state under Ptah's schema diff before the
  target is touched. SQLite targets get a throwaway dev database
  automatically; every other dialect requires `--dev-url`. A Ptah-written
  `.plan.hcl` keeps native sha256 fingerprints, so it gets the stale-plan
  check too — but the replay runs either way, because the fingerprint shape
  is public and must never be able to switch a verification off.

### The replay is not a sandbox

Before replaying, Ptah refuses statements that match a **deny-list of known
escape constructs** — `ATTACH`/`DETACH`, `VACUUM INTO`,
storage-directory pragmas, `load_extension`, routine bodies and dynamic SQL
calling file-access or `dblink` functions, `LOAD DATA INFILE`,
`SELECT ... INTO OUTFILE`/`DUMPFILE`, `LOAD_FILE`, `ENGINE=FEDERATED`,
`CREATE SERVER`, `INSTALL PLUGIN`/`COMPONENT`, `DATA`/`INDEX DIRECTORY`,
`COPY ... PROGRAM` or `COPY` with a file path, `dblink`, `postgres_fdw`,
`file_fdw`, the SQL Server `xp_`/`sp_addlinkedserver`/`OPENROWSET` family, and
ClickHouse's remote table engines:

```text
error: pre-planned migration was refused before it reached the dev database: statement 1 uses ATTACH, which attaches another SQLite database file to the session ...
```

An anonymous `DO` block is not itself refused — it is the standard PostgreSQL
idiom for idempotent DDL, and a foreign plan is full of them — but what its
body does is scanned like any other statement.

**That list is best-effort and not exhaustive, and the replay is not a
sandbox.** SQL dialects offer many ways to address something other than the
connected database — server-side language extensions, foreign-data wrappers,
storage-engine options, loadable modules, and engine-specific pragmas and
functions — and new ones arrive with new engine versions. Treat the deny-list
as a tripwire for honest mistakes and known tricks, not as a security
boundary against a hostile plan author.

The practical rule: **a `--dev-url` must point at a database you are willing
to have a foreign plan file execute arbitrary SQL against.** Use a disposable
dev database, not one that shares a server, credentials, or filesystem with
anything you care about. The lint is a tripwire in front of it, not a wall
around it.

### Where enforcement is real

The ephemeral SQLite dev database — the one Ptah creates for SQLite targets,
which you never opt into — is the exception, and it does not rely on the lint
at all. It is a throwaway file in a private temporary directory, removed when
the command exits, and its session is restricted at the engine level before
any plan SQL runs:

- `ATTACH` and `DETACH` are refused by SQLite itself, so plan SQL cannot reach
  another database file — including the real target.
- `VACUUM INTO` is refused by the same restriction, so it cannot write a
  database copy to an arbitrary path.
- Native extensions cannot be loaded.

What the engine does **not** stop, and the lint therefore still has to: the
storage-directory pragmas (`temp_store_directory`, `data_store_directory` —
the first is process-global in SQLite) and `PRAGMA writable_schema`. The last
one has a consequence worth stating: a plan that sets `writable_schema` could
edit the dev database's catalog directly, so the "converges to `--to`" verdict
is not tamper-proof against the very document being verified. The verdict is a
good-faith check, not an adversarial one.

Ptah verifies the restriction is in force before rehearsing, and refuses to
rehearse if it is not, so this cannot fail silently. These are engine
refusals: they hold for statements the lint never recognized, including ones
built by string concatenation at run time.

The restriction keys on the **dev** database's dialect, not on who supplied
it, so an operator-supplied SQLite `--dev-url` gets exactly the same engine
refusals. For any dialect other than SQLite, none of the above applies: that
database executes the plan's SQL for real, with whatever credentials and
network reach you gave it.

The verification also runs under `--dry-run`, so a plan received from someone
else can be checked without committing to apply it.

After every `--plan` apply with a desired state available, the end state is
verified again on the target — the semantic end-state verification Atlas
performs — and a mismatch fails loudly. There is no flag to disable it.

```bash
# Compute and save the plan for review (or --save for ./<timestamp>.plan.hcl).
ptah-compat schema plan \
  --from "$DATABASE_URL" \
  --to file://schema.sql \
  --output add-orders.plan.hcl

# Later, execute exactly the reviewed plan; drift refuses loudly.
ptah-compat schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --plan file://add-orders.plan.hcl \
  --auto-approve
```

Expected output of the plan step ends with:

```text
Plan saved to file://add-orders.plan.hcl
```

If the target database changed after the plan was saved, apply refuses before
touching the target. For a plan with native fingerprints the stale-plan error
names both fingerprints:

```text
error: pre-planned migration is stale: the target database schema does not match the plan's source fingerprint (plan sha256:..., database sha256:...); the database changed since the plan was computed, so re-run `schema plan` against the current database and review the fresh plan
```

For an Atlas-authored plan the drift surfaces semantically, and the plan is
not applied to the target:

```text
error: pre-planned migration does not converge to the desired state: replaying the plan on the dev database, starting from the target's current schema, left the following schema drift against --to (the plan was not applied to the target):
...
```

## Diff schema files

`ptah-compat schema diff` accepts a desired-state source on each side: one or
more local `--from`/`--to` schema file URLs, one directly connectable database
URL whose live schema is introspected, one migration directory (a `file://`
directory containing `atlas.sum`) replayed on the required `--dev-url` dev
database, or one `env://<attribute>` reference (`src`, `schema.src`, `url`,
`dev`, `migration.dir`) resolved through the evaluated `atlas.hcl` env.

All URLs of one flag must be one source kind. The SQL dialect is pinned by
`--dev-url` first, then by `--from` and `--to` database URLs; local schema
files alone still require `--dev-url` for dialect selection. With `--env`,
Ptah can read `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`,
`format.schema.diff`, and supported `diff` policy from `atlas.hcl`.

The current implementation does not execute Atlas's dev-database simulation;
the dev URL selects the dialect and hosts migration-directory replays.

```bash
ptah-compat schema diff \
  -f file://old.hcl \
  --to file://schema.hcl \
  --dev-url "sqlite://dev.db"
```

With an `old.hcl` declaring only a `users` table and a `schema.hcl` adding a
`posts` table, expected output includes:

```text
CREATE TABLE "main"."posts" (
  "id" INTEGER PRIMARY KEY,
  "user_id" INTEGER NOT NULL
);
```

`--format` accepts Atlas-style Go templates over Ptah's local diff report. The
supported template surface includes the `sql` helper and `.MarshalSQL`:

```bash
ptah-compat schema diff \
  --from file://old.hcl \
  --to file://schema.hcl \
  --dev-url "sqlite://dev.db" \
  --format '{{ sql . "  " }}'
```

Unsupported schemes — `atlas://` registry URLs, `docker://` as a desired
state, and anything Ptah cannot connect to directly — fail during validation,
before any database is contacted. A migration-directory source without
`--dev-url` fails the same way. Non-Atlas-CE flags such as `--tx-mode` are
rejected as unknown.

`--exclude` and disabled `schema.mode` values filter both local `--from` and
`--to` schema files before diffing, and `--schema`/`--include` positively
scope both sides with the same selection semantics, composition order, and
cross-scope dependency diagnostics as `schema apply` (see [Scope the
comparison with `--schema` and
`--include`](#scope-the-comparison-with---schema-and---include)).

A diff whose change needs a dialect-specific rebuild plan — for example adding
a column to a SQLite table — fails with an explicit error instead of emitting
SQL the dialect cannot run in place.

## Format schema files

`ptah-compat schema fmt` rewrites local `.hcl` files into HCL canonical layout:

```bash
ptah-compat schema fmt schema.hcl
```

## Clean a database

`ptah-compat schema clean` plans and applies destructive cleanup of user-owned
schema objects. Preview first:

```bash
ptah-compat schema clean --url "$DATABASE_URL" --dry-run
```

Against a SQLite database containing one `users` table, expected output
includes:

```text
Planned cleanup changes: 1
- DROP TABLE IF EXISTS "users"
[DRY RUN] No changes were applied.
```

:::danger
Without `--dry-run`, cleanup drops the listed objects after confirmation
(`--auto-approve` skips the prompt). There is no undo.
:::

## Format template fields

| Command | Format data fields |
| --- | --- |
| `ptah-compat schema inspect --format` | `.Realm`, `.Schema`, `.MarshalHCL`, `.MarshalSQL`, `.MarshalJSON`, plus `hcl`, `sql`, `json`, `base64url`, `mermaid`, `split`, and `write` template helpers. |
| `ptah-compat schema apply --format` | `.Changes`, `.MarshalSQL`, plus the `sql` helper for the planned SQL statements. |
| `ptah-compat schema diff --format` | `.Changes`, `.MarshalSQL`, plus the `sql` helper for generated migration SQL. |
| `ptah-compat schema clean --format` | `.Env.Driver`, `.Env.URL`, `.DryRun`, `.Applied`, `.Objects`, and `.Changes`. |

The shared report shape and URL redaction rules are described on the
[Atlas compatibility overview](../overview/#format-reports-and-redaction).

## Next steps

- Managing migration directories on this surface:
  [Atlas migrate commands](../migrate-commands/).
- Doing direct changes with a native-first flow:
  [Apply schema changes directly](../../direct/apply/).
- Checking the supported `atlas.hcl` inputs these commands read:
  [Atlas project config](../project-config/).
