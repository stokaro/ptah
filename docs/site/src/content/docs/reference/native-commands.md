---
title: Native commands
description: Complete verb table for the native ptah command tree.
---

This page lists every native `ptah` verb with its purpose. Native commands use
Ptah-owned spellings; Atlas aliases are intentionally absent from the `ptah`
binary. The Atlas-compatible surface — the separate `ptah-compat` drop-in
binary — has its own page: [Atlas-compatible commands](../atlas-commands/).
Use `ptah <command> --help` for the exact flag set in an installed binary.

## Desired schema: `ptah schema`

| Command | Purpose |
| --- | --- |
| `ptah schema render` | Render desired-schema SQL to stdout from Go, YAML, HCL, SQL, or external-command inputs; write progress and dependency diagnostics to stderr. |
| `ptah schema annotations` | Export Ptah Go annotation metadata as a JSON Schema document. |
| `ptah schema compare` | Compare desired schema with a live database. |
| `ptah schema drift` | Check live database drift against desired schema. |
| `ptah schema apply` | Apply a desired schema directly to a database, with an advisory lock, optional dev-database rehearsal, and interactive approval (`--auto-approve` for scripts). |
| `ptah schema plan` | Save the declarative apply plan as a fingerprinted local plan file; `ptah schema apply --plan` executes it only while the target still matches the recorded fingerprint. |
| `ptah schema inspect` | Inspect a live database, a local schema file, an `oci://` schema artifact, or an Atlas-format migration directory as machine-clean HCL, SQL, or JSON; `--out-dir`/`--split` export files. |
| `ptah schema diff` | Diff two arbitrary schema states (files, database URLs, or migration directories) into migration SQL or JSON. |
| `ptah schema fmt` | Format HCL schema files canonically; `--check` is a no-write CI gate. |
| `ptah schema export` | Export a schema to HCL, an OpenAPI 3.0 component schema, a GraphQL SDL, or a Protobuf Edition 2023 definition. |
| `ptah schema push` | Publish a lossless canonical desired schema to an OCI registry. |
| `ptah schema pull` | Pull a canonical desired schema from an OCI registry. |
| `ptah schema test` | Apply a desired schema from Go annotations, a SQL or HCL file, or a live database to a throwaway database and run declarative seed/SQL/assert cases against it. |

Pass an explicit `--dialect` when the output must be executable by one target.
Without it, `schema render` attempts the built-in review targets and emits
labeled output only if every target can render the schema. An unsupported
feature fails atomically with empty standard output. The combined output is a
review artifact, not one executable SQL script.

### Rendering for a specific server

`schema render` never connects, so a bare `--dialect` renders against that
dialect's default capability preset — the newest release line Ptah has measured.
Add `--server-version` to render for the server you actually run:

```bash
ptah schema render --root-dir ./models --dialect mysql --server-version 8.0.42
```

The flag requires `--dialect`, since without one the command renders every
supported target and a single server version cannot describe all of them. A
value that names no server is refused with exit code `2` rather than quietly
ignored, and a value that resolves to something other than an exact measured
release line is applied and announced on stderr as a `warning:` line. Accepted
shapes are a dotted version such as `17` or `8.0.42` and a server banner such as
`PostgreSQL 16.3 (Debian)`, `10.11.6-MariaDB`, or `CockroachDB CCL v25.4.0`.

A banner naming a different server product than `--dialect` is refused too:
`--dialect mysql --server-version 10.11.6-MariaDB` is a contradiction, not a
request, and it would otherwise render MySQL DDL against MariaDB capabilities.
Pass the banner with the dialect it names.

`ptah sql lint` spells the same contract `--version`.

### Schema file paths

Native `--schema-file` inputs use the process working directory as their path
boundary. A relative path must resolve inside that directory after symbolic
links are followed. Ptah accepts a path that temporarily contains `..` when its
resolved destination is still inside the boundary, and refuses a symbolic link
whose destination is outside it.

Pass an absolute pathname when you intentionally read a schema file outside the
working directory, such as `--schema-file /srv/schemas/app.sql`. Absolute
pathnames retain their existing reach and are not confined to the working
directory.

## Migration lifecycle: `ptah migrations`

| Command | Purpose |
| --- | --- |
| `ptah migrations plan` | Print migration SQL from desired/live schema differences. |
| `ptah migrations generate` | Generate migration files from desired/live schema differences; `--replay` derives the current state by replaying the directory on a `--dev-url` database instead of introspecting `--db-url`. |
| `ptah migrations create` | Create empty migration files for manual SQL; `--edit` opens them in `$VISUAL`/`$EDITOR` (or `--editor`) and refreshes `atlas.sum` for Atlas-format directories. |
| `ptah migrations import` | Convert a golang-migrate, Goose, Flyway, Liquibase, or dbmate migration directory into Ptah's native format, auto-detecting the source tool unless `--from` is set. |
| `ptah migrations baseline` | Record existing migrations as applied in the revision table without executing their SQL; `--shadow-db` verifies the baselined history reproduces the target schema. |
| `ptah migrations set` | Move the revision boundary to an arbitrary version in both directions without executing SQL: everything through `--version` is recorded applied, rows above it are removed. |
| `ptah migrations up` | Run pending migrations; a hashed directory (`ptah.sum` or `atlas.sum`) verifies before anything executes, `--limit N` applies only the first N, and `--allow-dirty` explicitly requests a verified retry that skips only an unchanged committed source prefix. |
| `ptah migrations down` | Roll back migrations. |
| `ptah migrations status` | Show migration status. |
| `ptah migrations ls` | List the migration files a directory holds, oldest version first, without contacting a database; `--short` collapses a reversible pair onto its version and `--latest` keeps only the newest migration. |
| `ptah migrations show` | Print the SQL a migration directory stores, without contacting a database; repeat `--version` to print several bodies in the order asked for, and `--direction` selects which half of a reversible pair is printed. |
| `ptah migrations repair` | Repair migration revision metadata under the migration advisory lock after a dirty or partial state; `--resume-from` verifies the committed prefix, then executes the remaining statements of whichever body left the row dirty — up statements before marking the version applied, or down statements before removing the revision. |
| `ptah migrations hash` | Write or update migration-directory integrity. |
| `ptah migrations validate` | Validate migration-directory integrity and, optionally, SQL execution by cleaning and replaying migrations on `--dev-url`. |
| `ptah migrations lint` | Lint migration files and, with `--dev-url`, clean and replay migrations on a directly connectable dev database before static reporting. |
| `ptah migrations test` | Run declarative YAML cases with migrate/apply-schema/seed/SQL/assert steps against a throwaway database, exiting non-zero on any failure; `--migrations-schema` places the revision table a `migrate_to` step writes. |
| `ptah migrations edit` | Edit a migration's SQL (via `$EDITOR` or `--up-file`/`--down-file`) and rewrite the integrity file, refusing already-applied migrations unless `--force`. |
| `ptah migrations rebase` | Move a migration to the end of history by re-timestamping it, and rewrite the integrity file, refusing already-applied migrations unless `--force`. |
| `ptah migrations rm` | Delete a migration's up/down pair and rewrite the integrity file, refusing already-applied migrations unless `--force`. |
| `ptah migrations checkpoint` | Squash migration history into a cumulative-schema checkpoint by replaying the directory on a `--shadow-db`; `--dir-format` selects the ptah pair (default) or the Atlas single-file convention, and `--qualifier`, `--migration-lock-timeout` and `--edit` shape the replay and the written files. |
| `ptah migrations data` | Generate a reversible data migration from declarative reference-data drift against a live database. |
| `ptah migrations push` | Publish a migration directory to an OCI registry. |
| `ptah migrations pull` | Pull and reconstruct a migration directory from an OCI registry. |

## Live databases: `ptah db`

| Command | Purpose |
| --- | --- |
| `ptah db read` | Read a live database as executable SQL on stdout; write connection status and failures to stderr. |
| `ptah db capabilities` | Report the capability profile Ptah resolves for a live database: the preset it plans with, how that preset was reached, the support level of the release line, and every capability key with its value there. Modifies no schema object. |
| `ptah db drop-all` | Drop all schema objects in a live database. |

### Reading a server's capability profile

`ptah db capabilities` connects, reads the server's own version surface, and
prints what Ptah resolved from it. It executes no DDL and modifies no schema
object. Beyond the metadata queries any Ptah connection makes — the server's
version string, and the session's current schema or database — it adds one
statement, `SERVERPROPERTY('ProductVersion')`, and only on SQL Server. Opening
the connection is still opening a connection: a `sqlite://` URL naming a file
that does not exist creates that file, exactly as any other command reaching the
same URL would.

```bash
ptah db capabilities --db-url "$DATABASE_URL"
```

Expected output includes:

```text
Dialect:            postgres
Server version:     18.4
Server product:     postgres
Banner:             PostgreSQL 18.4 (Debian 18.4-1.pgdg13+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
Capability preset:  Postgres17 (postgres)
Preset source:      version-ladder
Support level:      certified
Release line:       18
```

`Banner` is the version string the server reported verbatim; `Server version`
and `Server product` are what Ptah parsed out of it. The three differ where it
matters most: a MariaDB reached over a `mysql://` URL reports dialect `mysql`,
product `mariadb`, and a preset resolved on the MariaDB ladder. Below the block the command
prints the reason for the support level, the non-boolean behavior values —
identifier limit, enum modeling, foreign key reference — and every capability
key marked `supported` or `unsupported`. A version that selected no exact
measured release line adds a `Note:` line naming what was planned instead.

| Flag | Value |
| --- | --- |
| `--db-url` | Database URL. Required. |
| `--format` | `text` (the default) or `json`. |
| `--connect-timeout` | Maximum time to wait for the initial connection, default `10s`; `0` disables the timeout. |

The output answers four questions that a refused or unexpected operation
otherwise leaves open:

- **Which capability set is in force.** `Capability preset` names it, and the
  dialect in parentheses names whose resolution produced the set — a version
  ladder where the dialect has one, the dialect default or a banner match
  otherwise. It is not always the dialect the URL connected as.
- **Why that set.** `Preset source` names the rule that chose it:
  `version-ladder` when the version selected an exact measured release line,
  `unmeasured-line` when the dialect's ladder answered but no measured line
  matched, `newer-than-measured` when the server is newer than the newest line
  Ptah has measured, `dialect-default` when the dialect has no ladder at all,
  and `unrecognized-banner` when the version string named no server.
- **What Ptah promises about the release line.** `Support level` and
  `Release line`, with the reason underneath. A server on a line Ptah declares
  nothing about reports `best-effort` and is not refused: capabilities are
  resolved for it and the operations they allow are performed.
- **What this server can do.** Every registered capability key appears, present
  or absent, so an absent capability is distinguishable from one this build does
  not know about.

`--format json` carries everything the text form shows, plus each capability
key's documentation string, as a stable sorted document. Two runs against an
unchanged server produce identical bytes, so a diff of them reports a change
that happened rather than a reordering.

## Registries and SQL files: `ptah oci`, `ptah sql`

| Command | Purpose |
| --- | --- |
| `ptah oci tags` | List the tags a repository carries. |
| `ptah oci resolve` | Resolve a mutable tag to the immutable digest it names. |
| `ptah oci inspect` | Report what an artifact declares, without downloading it. |
| `ptah oci referrers` | List direct referrer metadata attached to an OCI artifact. |
| `ptah oci fetch` | Download the payload of metadata attached to an artifact. |
| `ptah oci tag` | Move an alias onto an artifact that already exists. |
| `ptah oci copy` | Copy an artifact between repositories without rebuilding it. |
| `ptah oci capabilities` | Report what the registry behind a reference supports. |
| `ptah oci reindex` | Republish attachments the registry's referrers index does not list. |
| `ptah oci verify` | Check an artifact against a verification policy before it is consumed. |
| `ptah sql lint` | Lint standalone SQL files. |

## Top-level verbs

| Command | Purpose |
| --- | --- |
| `ptah introspect` | Generate annotated Go models from a live database. |
| `ptah seed` | Apply environment-scoped SQL seed files. |
| `ptah viz` | Render desired schema diagrams as Mermaid, DOT, or SVG. |
| `ptah version` | Print Ptah build information. |
| `ptah license` | Print license, copyright, and Atlas-compatibility attribution. |
| `ptah completion <shell>` | Generate shell completion output for the native `ptah` command tree. |

## OCI transport behavior

These native commands resolve an `oci://` reference, each through the flag
named beside it:

| Native command | OCI source flag | Artifact |
| --- | --- | --- |
| `ptah migrations up` | `--migrations-dir` | migration directory |
| `ptah migrations status` | `--migrations-dir` | migration directory |
| `ptah migrations ls` | `--migrations-dir` | migration directory |
| `ptah migrations show` | `--migrations-dir` | migration directory |
| `ptah migrations down` | `--migrations-dir` | migration directory |
| `ptah migrations lint` | `--dir` | migration directory |
| `ptah migrations validate` | `--dir` | migration directory |
| `ptah schema render` | `--schema-file` | desired schema |
| `ptah schema export` | `--schema-file` | desired schema |
| `ptah schema inspect` | `--schema-file` | desired schema |
| `ptah schema compare` | `--schema-file` | desired schema |
| `ptah schema drift` | `--schema-file` | desired schema |
| `ptah schema plan` | `--schema-file` | desired schema |
| `ptah schema apply` | `--schema-file` | desired schema |
| `ptah schema push` | `--schema-file` | desired schema |
| `ptah migrations plan` | `--schema-file` | desired schema |
| `ptah migrations generate` | `--schema-file` | desired schema |

`migrations lint` can attach its canonical report with `--attach`, and a plan
with exactly one OCI schema source can attach its canonical safety report. Use
digest pins for reproducible runs and reserve `--plain-http` for an explicitly
trusted local registry. See [OCI registry
artifacts](../../operate/oci-registry/).

Every command that resolves an `oci://` source registers `--plain-http`, and
neither that pairing nor the table above is maintained by hand: a test walks the
built command tree for `--schema-file`, `--dir` and `--migrations-dir`, requires
every command whose value reaches the OCI loader to register the flag, drives it
at a registry to prove the value reaches the client rather than merely parsing,
and then requires the rows here to be exactly that set. A verb that starts or
stops resolving the scheme fails that test until this table says so.

`migrations validate --dir oci://...` answers the integrity question on the
artifact itself, with no database and nothing executed. Over a tag a successful
run adds the movable-tag qualifier on standard error, naming the digest the tag
resolved to; a digest-pinned reference prints nothing extra. `migrations hash`
refuses `oci://` by design, because it writes the integrity file back into the
directory it hashed and a registry artifact is immutable.

`ptah oci referrers <oci-reference>` lists direct attachment descriptors.
`--type` accepts `all`, `lint`, `plan`, or `deployment`; `--format` accepts
`text` or `json`. Unqualified subjects resolve to `:latest`, tags resolve to
their current manifest, and digest subjects remain immutable. Docker
credentials and HTTPS are the defaults; `--plain-http` is only for an
explicitly trusted local registry. The command lists metadata; `ptah oci fetch`
returns the payload.

`ptah oci resolve <oci-reference>` prints the pinned reference a tag currently
names, so a pipeline can record the digest once and pass it to every later step
instead of resolving the tag again at each one. `--format json` adds the
descriptor's media type and size.

`ptah oci inspect <oci-reference>` reads the manifest and stops there: artifact
type, subject, annotations, and each file layer's name, media type, size, and
digest, without downloading the files. It also reports how each referrer was
discovered. Ptah writes both the standard referrers index and its own
content-derived durable tag, and a referrer reported as `durable-tag` was
returned by the second mechanism alone — Ptah finds it and another OCI client
may not. `--no-referrers` skips that lookup.

`ptah oci fetch <oci-reference>` returns the bytes of an attached report, which
is how Ptah reads back the lint, plan, and deployment reports Ptah published.
Selection never guesses: one candidate is fetched, several are refused with the
digests printed. Narrow with `--type`, or name one with `--digest`. The same
rule governs the files inside the chosen referrer — one is written, several
require `--file`. `--output` writes to a path instead of standard output.

`ptah oci tags <oci-reference>` lists the aliases a repository carries, which
is the view that says which of them exist before a promotion moves one.

`ptah oci tag <oci-reference> <tag>...` moves an alias onto an artifact that
already exists. Promotion through a push re-derives content that was already
reviewed, so what arrives in production is an artifact equal to the reviewed
one rather than the same one; moving the alias keeps the manifest digest
identical by construction, because nothing is built and nothing is uploaded.
Aliases move one at a time, and the ones already applied are named when a later
one fails.

`ptah oci copy <source> <destination>` copies an artifact between repositories
with its digest preserved. `--recursive` carries the artifact's referrers with
it; without it the copy arrives with its lint results, plans, deployment
reports, and signatures left behind in the source repository, which is how a
promotion loses the evidence it was promoted on. A digest destination is
refused, because a digest names content that already exists.

`ptah oci capabilities <oci-reference>` asks the registry whether it answers
the referrers API. Ptah publishes referrers both through the standard index and
through its own content-derived tag and merges them on read, so its own
discovery is robust whatever the registry does — and where the API is absent, a
referrer Ptah published is one another OCI client may never find. The question
is put with the client pinned to the API so a success cannot have come from the
tag-schema fallback, and a failure to ask is reported as an error rather than
folded into a no.

This does not implement the Atlas Cloud command paths. The Atlas-compatible
`migrate push` and `schema push` remain Atlas community-edition boundary
stubs in the `ptah-compat` binary, and Atlas-compatible apply commands do not
expose the native OCI transport flags.

## External desired-schema inputs

The native desired-schema consumers — `schema render`, `schema compare`,
`schema drift`, `migrations plan`, and `migrations generate` — accept external
commands that emit SQL, HCL, or YAML. Each command also reads
`ptah.yaml external_schema` when `--schema-cmd` is not set and
`--allow-external-schema` explicitly permits config-sourced execution. See
[Configuration](../configuration/) for the `external_schema` block.
