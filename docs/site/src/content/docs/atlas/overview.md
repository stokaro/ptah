---
title: Atlas compatibility overview
description: How the ptah-compat drop-in binary replaces the Atlas CLI, and how Atlas-style flags translate to native Ptah concepts.
---

You run scripts, CI jobs, or habits built around the Atlas CLI and want to know
how Ptah fits them. Ptah ships a separate drop-in binary, `ptah-compat`, that
presents the Atlas-compatible command surface. This page explains how to
install it, how Atlas-style flags translate to Ptah concepts, and where the
compatibility boundary is — before you pick up the per-command usage pages.

## The drop-in binary

The main `ptah` binary is a purely native CLI — it has no Atlas-style command
paths. The separate `ptah-compat` binary is a binary-level drop-in replacement
for the Atlas CLI, built for scripts that need Atlas-style root commands:

```bash
go install go.5x5.cz/ptah/cmd/ptah-compat@latest

ptah-compat migrate apply --url "$DATABASE_URL" --dir ./migrations
```

Command examples on the Atlas compatibility pages are written as
`ptah-compat <command> ...` — the name the binary ships under.

What the two binaries share is capabilities, not command lines. A generally
useful capability you reach through `ptah-compat` is reachable through native
`ptah` as well, under native names and flags: `ptah-compat migrate apply` and
`ptah migrations up`, or `ptah-compat schema inspect` and `ptah schema inspect`.
Atlas-specific machinery has no native twin at all. See
[Capability parity, not interface parity](../comparison/#capability-parity-not-interface-parity).

Use the native tree for new Ptah-authored work and the compat binary for
existing Atlas scripts; the per-verb mapping is listed in the
[Atlas-compatible commands reference](../../reference/atlas-commands/).

## Strict Community Edition mode

The normal `ptah-compat` process keeps every Atlas Pro-like and best-effort
capability Ptah implements. That is the migration surface for existing Atlas
pipelines, so it is also the default.

For a pinned Atlas Community Edition oracle or conformance run, select the
separate strict policy before starting the process:

```bash
PTAH_ATLAS_STRICT_COMPAT=1 ptah-compat schema inspect --help
```

Strict mode changes command construction, not only runtime validation. Its help
tree exposes CE commands and flags, and gated verbs use a Ptah-owned diagnostic
that tells the operator to unset `PTAH_ATLAS_STRICT_COMPAT` for the full
compatibility surface. It never links to an Atlas installer. Ptah's generic
`PTAH_<FLAG>` environment twins are disabled. A present extension variable is
rejected before help, version, argument handling, configuration, filesystem, or
database work. The selector has no CLI flag, so it cannot change the surface
being measured.

This validation targets known Ptah flag bindings and feature toggles. It does
not reserve the whole `PTAH_*` namespace: values read explicitly by an
`atlas.hcl` `getenv` expression remain project inputs in strict mode.

Strict mode also refuses authored or inspected content that CE cannot represent
safely. This includes Pro-only schema objects and extended `atlas.hcl`
evaluation. Strict schema workflows refuse YAML sources and an authored
`schema apply` lint policy that the CE path cannot enforce.

Commands that execute, convert, or replay migration bodies refuse Atlas txtar,
Ptah directives, and SQL templates; checksum-only reads preserve those bytes.
A live Pro-only object stops schema inspect, apply, diff, or clean before
output, comparison, or mutation. Inspect, apply planning, and database-backed
or replayed schema- and migration-diff sources supplement the ordinary reader
with a read-only inventory of catalog-only kinds in the selected schema scope.
Cleanup validates the writer's full destruction inventory, including dependent
objects and the same PostgreSQL catalog kinds absent from the schema reader.

Strict mode never emulates a CE behavior that would silently drop authored
data, hide a live object, or corrupt state. Default mode retains every listed
extension. Deliberate safety and correctness improvements remain enabled and
are listed in [Retained divergences](../retained-divergences/).

Do not enable strict mode in ordinary migrated Pro pipelines. To verify both
contracts, run CE parity tests with the variable set and Pro-retention tests
with it absent. Native `ptah` does not read this variable.

### Installing under the name `atlas`

For a byte-level drop-in with existing scripts that call an executable named
`atlas`, install the binary under that name:

```bash
# Build it under the name your scripts expect:
go build -o atlas ./cmd/ptah-compat

# Or install it and link the atlas name:
go install go.5x5.cz/ptah/cmd/ptah-compat@latest
install_dir="$(go env GOPATH)/bin"
ln -sf "$install_dir/ptah-compat" "$install_dir/atlas"
```

The binary adopts the name it is invoked as, so usage, help, and error output
read `atlas migrate apply ...` when the executable is named `atlas`.

## Translation model

Implemented Atlas-compatible commands either execute dedicated Atlas-shaped
behavior or translate Atlas-style flags into the closest native Ptah command
model. Unsupported flags fail clearly instead of being ignored.

| Atlas flag style | Native Ptah concept |
| --- | --- |
| `--url` | `--db-url` |
| `--dir` | `--migrations-dir` |
| `atlas.hcl` env | Project config IR for supported `ptah-compat ... --env` defaults |
| `--config`, `-c` | Local Atlas project config path for `schema` and `migrate` commands |
| `--var name=value` | Atlas HCL variable override for supported local expressions |
| Atlas revision table mode | Ptah revision format and table settings |

Atlas project flags are persistent on the Atlas-compatible `schema` and
`migrate` command groups, so both of these forms are valid:

```bash
ptah-compat migrate --config project.hcl --env local hash
ptah-compat migrate hash --config project.hcl --env local
```

The supported `atlas.hcl` subset those flags read is documented in
[Atlas project config](../project-config/).

Atlas OSS shorthand aliases are part of the compatibility surface. Ptah accepts
`-u` for `--url`, `-c` for `--config`, `-s` for `--schema` on Atlas commands
that register schema selection, and `-f` for `schema diff --from`. `schema apply`
also accepts Atlas's hidden `--file/-f` input alias for local HCL or SQL paths;
prefer the native `ptah schema apply` verb in new Ptah-authored scripts.

## Utility commands

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah-compat version` | Prints Ptah build information. |
| `ptah-compat license` | Prints Ptah MIT license and license-clean Atlas compatibility notice. |
| `ptah-compat completion <shell>` | Generates Cobra completion output for the Atlas-compatible command tree. |

## Format reports and redaction

Atlas-compatible `--format` reports use the Atlas data shape. URL fields render
as redacted URL strings in Go templates such as `{{ .Env.URL }}`, but
`{{ json . }}` emits an Atlas-like URL object with `Scheme`, `User`, `Host`,
`Path`, `RawQuery`, `Fragment`, `RawPath`, `RawFragment`, `ForceQuery`,
`OmitHost`, and, for SQLite URLs, `Schema`. Query keys that look like
passwords, tokens, secrets, or API keys are replaced with `xxxxx`; URL userinfo
passwords are removed.

The per-command template fields are listed on
[Atlas migrate commands](../migrate-commands/#format-template-fields) and
[Atlas schema commands](../schema-commands/#format-template-fields).

## Compatibility boundaries

Some Atlas Pro commands and flags are bound to Atlas Cloud services Ptah
intentionally has no counterpart for. Those paths are recorded waivers or Atlas
CE boundary stubs: they are registered so scripts fail in the right namespace,
and they reject execution loudly with their rationale instead of being silently
ignored. The largest examples are `migrate push` and `schema push` (the Atlas
Registry protocol is proprietary and account-bound; the native
`ptah migrations push` and `ptah schema push` verbs publish to any
[OCI registry](../../operate/oci-registry/) instead) and the `schema plan`
registry sub-verbs. The per-command pages name each waiver where it appears.

## Compatibility never costs you a capability

Ptah models things the Atlas community CLI does not. PostgreSQL extensions,
sequences and row-level security policies are the clearest examples: that CLI
answers `postgres: extensions are not supported by this version` and refuses a
schema file that declares any of them, while Ptah reads, diffs and applies all
three.

Being a drop-in for that CLI never means giving those up.

The compatibility surface **defaults** to what the community CLI accepts, so
output you hand back to it stays readable. What that default leaves out is
reported rather than dropped in silence — you are told what was omitted and
why, so a compatibility-shaped inspect never describes less of your database
than it found without saying so.

The fuller behavior stays available on the same `ptah-compat` surface through a
`PTAH_*` environment variable. It is an environment variable rather than a flag
on purpose: the compatibility binary's flags are held to parity with the pinned
community CLI, so a flag Atlas does not have would break the very drop-in
promise it was added to serve. Native `ptah` verbs always emit everything Ptah
models, with no switch to set.

This matters most if you are coming from Atlas **Pro** rather than CE. The
compatibility surface is the migration path for Pro scripts and configuration
too, not only CE ones — a capability you could only reach by rewriting your
pipeline against native `ptah` verbs would not be a migration path at all.

The rule runs the other way too. A capability built for Atlas compatibility
does not stay on the compatibility surface: where it is generally useful for
schemas or migrations, native `ptah` reaches it under native names.

### The variables

Every variable below is a boolean, and they all read the same way: leaving it
unset selects the default described here, a valid boolean is honored, and
anything else — including an exported empty value — fails the command before it
does any work, naming the variable and the value you typed. The accepted
spellings and the error shape are documented once, in
[Boolean environment variables](../../reference/configuration/#boolean-environment-variables).

The value is read on every run of the command that owns it, not only on the runs
that would have used the enabled behavior, so `PTAH_ATLAS_LINT_ALL_VERSIONS=yes`
in a CI environment file fails the next run rather than the next run that
happens to omit `--latest`.

**`PTAH_ATLAS_INSPECT_ALL_BLOCKS`** — by default, `ptah-compat schema inspect`
leaves an `extension`, `sequence` or `policy` block out of PostgreSQL HCL
output when nothing else in the document depends on it, and reports each
omission on standard error. For an extension, "depends on" is measured against
what the catalog says the extension supplies — `isn` supplies the type `isbn` —
rather than against its name, and against what the catalog resolved for the
document's indexes, since a GIN index over an `integer` column needs `btree_gin`
and prints no word of it. Set it to `1` and every block Ptah models is
emitted: the output describes the database in full, and the community CLI
refuses it.

**`PTAH_POSTGRES_INSPECT_ALL_ROLES`** — by default, a PostgreSQL read describes
only the roles the inspected schemas use, because roles are cluster-wide and a
description of one database is not a place to list another tenant's roles. Each
read reports on standard error how many managed roles it left out. Set it to
`1` and every role Ptah manages on the server is described again, which is what
you need to reproduce one cluster's roles in another. It widens the description
only: comparison already treats undescribed roles as present, so the planned
statements are identical either way. Reserved `pg_` names and the bootstrap
`postgres` superuser are outside it in both directions.

**`PTAH_ALLOW_RESERVED_ROLE_NAMES`** — by default, a desired schema that
declares a reserved PostgreSQL role is refused before anything is compared or
planned, naming the role and the rule, because Ptah manages neither the `pg_`
roles nor the bootstrap `postgres` superuser in either direction and the
declaration would otherwise become a `CREATE ROLE` the server rejects at
SQLSTATE 42939 or 42710. Set it to `1` and the declaration is planned instead,
as it was before the refusal existed. That is worth having on a cluster
bootstrapped under a name other than `postgres`, where `CREATE ROLE "postgres"`
succeeds.

**`PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`** — by default, a comparison whose
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

**`PTAH_SQLITE_ALLOW_UNREGISTERED_VIRTUAL_MODULE`** — by default, a comparison
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

See [SQLite](../../databases/sqlite/) for the whole picture.

**`PTAH_ALLOW_EXTERNAL_SCHEMA`** — by default, `atlas.hcl`
`data "external_schema"` is not evaluated, because it runs a
repository-controlled program. Set it to `1` and the data source is evaluated,
matching the native `--allow-external-schema` flag.

**`PTAH_ATLAS_LINT_WITHOUT_DEV_URL`** — by default,
`ptah-compat migrate lint` requires `--dev-url`, because the community CLI marks
it required and exits 1 without it. Ptah's analyzers read the migration files
and need no database, so set it to `1` and the run proceeds with no dev database
and reports what the static analysis finds. Native `ptah migrations lint` needs
no opt-in.

**`PTAH_STRICT_DIR_QUERY`** — by default, a `--dir` URL query key other than
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

**`PTAH_ATLAS_LINT_ALL_VERSIONS`** — by default, `ptah-compat migrate lint`
refuses a run that names no scope, because the community CLI refuses it:
`--latest`, `--git-base` or an `atlas.hcl` `lint` block supplying one is
required, and without it the answer is
`Error: --latest or --git-base is required` at exit 1, before the migration
directory is read and before `--dev-url` is contacted. Set it to `1` and the
whole directory is linted instead, which is what Ptah's own linter does. Native
`ptah migrations lint` needs no scope and ignores the variable.

**`PTAH_HCL_MERGE_REDECLARATIONS`** — by default, an HCL schema document that
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

**`PTAH_HCL_STRICT_REDECLARATIONS`** — by default, a repeated `view`,
`materialized`, `role` or `unique` block is read at exit 0, because the
community CLI reads it at exit 0: it drops the first three unread and merges two
`unique` blocks sharing a label into one. Refusing them is above the drop-in
floor rather than on it, so it is opt-in. Set the variable to `1` and each of
the four is refused within one document, which is the rule an HCL schema
*directory* already applies across its files. The kinds refused by default are
still refused with it set; the four exceptions above are still exempt.

**`PTAH_HCL_SCHEMA_SCOPED_ENUMS`** — by default, two `enum` blocks sharing a bare
name are one object however they are spelled, because the community CLI keys
enums by their bare name and answers `duplicate enum "mood"` at exit 1 for
`enum "mood"` in two schemas and for the two-label `enum "public" "mood"` /
`enum "other" "mood"` alike. Set the variable to `1` and they are keyed by their
qualified name, which is what `public.mood` and `other.mood` are. This is the
setting under which `ptah-compat schema inspect` of a database holding one enum
name in two schemas can be applied again: the document names both, and reading
it back re-renders it byte for byte. The community CLI has no such setting and
refuses the document its own inspect writes for that database.

### One shape has no Atlas-readable form at all

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

## Parity expectations

Ptah is not documented as a full Atlas OSS replacement until the external
conformance reports and the comparison gap register support that claim. Use
[Conformance](../conformance/) for current evidence and
[Comparison](../comparison/) for tracked product, coverage, and
documentation gaps.

## Next steps

- Running migration directories with Atlas verbs:
  [Atlas migrate commands](../migrate-commands/).
- Inspecting, diffing, and applying schemas with Atlas verbs:
  [Atlas schema commands](../schema-commands/).
- Evaluating the compatibility claims first:
  [Conformance](../conformance/).
