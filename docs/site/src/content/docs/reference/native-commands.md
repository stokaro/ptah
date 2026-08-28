---
title: Native commands
description: Every command path the native ptah binary answers to, and the notes a verb table cannot hold.
---

This page is the lookup reference for the native `ptah` surface: every command
path the binary answers to, and the per-verb detail a table cannot carry. Native
commands use Ptah-owned spellings; Atlas aliases are intentionally absent from
the `ptah` binary. The Atlas-compatible surface — the separate `ptah-compat`
drop-in binary — has its own page:
[Atlas-compatible commands](../atlas-commands/).

## Every native verb

The index below is generated from the `ptah` command tree, so it names every
command path the binary answers to and no path it does not. Rows are sorted,
which keeps a namespace's verbs together; which verb to reach for first is
[Choose a workflow](../../start/choose-a-workflow/). Use `ptah <command> --help`
for the exact flag set in an installed binary.

A row marked `group` is a namespace rather than an operation. Running one with
no verb prints its help, with one exception: `ptah assist` on its own starts a
conversation, which [A model of your own](#a-model-of-your-own) describes.

The tree is walked the way cobra finishes building it, so `ptah help` and the
four `ptah completion` shells have rows: they ship, and an index built from the
verbs `cmd/` registers by hand would leave them out. Two spellings stay outside
it. `__complete` and `__completeNoDesc` ship and exit `0`, and no walk reaches
them, because cobra registers them from an unexported function that removes them
again unless the invocation being parsed is one of the two. They answer a shell
rather than a person.

<!-- BEGIN GENERATED NATIVE COMMANDS -->
| Command | What it does | Notes |
| --- | --- | --- |
| `ptah assist` | Work with Ptah through a model you supply | group |
| `ptah assist context` | Prints what a question would send to a model provider and sends nothing; it opens neither a database nor an endpoint, and writes no file | — |
| `ptah assist explain` | Opens no connection of its own; it asks a model a question and lets the model call Ptah's own tools, each of which carries its own classification | — |
| `ptah assist provider` | Inspect and test the model providers this machine can reach | group |
| `ptah assist provider list` | Lists the provider profiles configured locally, and opens neither a database nor an endpoint | — |
| `ptah assist provider test` | Measures a provider profile by calling the model endpoint it names; no database is involved | — |
| `ptah assist sessions` | List, read and remove saved Ptah Assist conversations | group |
| `ptah assist sessions delete` | Removes one saved conversation file from the project; no database is opened, and the audit log is a separate file it does not touch | — |
| `ptah assist sessions list` | Lists the conversations saved under the project's .ptah directory; neither a database nor a model endpoint is opened | — |
| `ptah assist sessions prune` | Removes saved conversation files untouched for longer than a given age; no database is opened, and the audit log is left alone | — |
| `ptah assist sessions show` | Prints one saved conversation from disk, including what Ptah's tools answered during it; nothing is opened to do so | — |
| `ptah completion` | Generate the autocompletion script for the specified shell | group |
| `ptah completion bash` | Writes a bash completion script to stdout, generated from the command tree; it opens no database and writes no file | — |
| `ptah completion fish` | Writes a fish completion script to stdout, generated from the command tree; it opens no database and writes no file | — |
| `ptah completion powershell` | Writes a PowerShell completion script to stdout, generated from the command tree; it opens no database and writes no file | — |
| `ptah completion zsh` | Writes a zsh completion script to stdout, generated from the command tree; it opens no database and writes no file | — |
| `ptah db` | Work with live database schemas | group |
| `ptah db capabilities` | Reads the server's version and catalogs to report the capability profile Ptah resolves | — |
| `ptah db drop-all` | Drops every schema object in the database it is given | — |
| `ptah db read` | Introspects the database and prints what it found | — |
| `ptah help` | Prints the help text of the verb it names, or of the root when it names none; the verb itself is not run and nothing is opened | — |
| `ptah inference` | Plan, run and cut over embedding-generation migrations | group |
| `ptah inference backfill` | Reads the source, sends it to the embedding endpoint the specification names, and writes vectors and checkpoints into the target database | — |
| `ptah inference catchup` | Rereads the source rows recorded as changed and writes their vectors, which sends that text to the embedding endpoint | — |
| `ptah inference cutover` | Moves the pointer queries read to a different generation, and refuses when the pointer is not where the plan it was built from expects | — |
| `ptah inference evaluate` | Searches the generation with queries from a corpus, which sends those queries to the embedding endpoint; the database is only read | — |
| `ptah inference plan` | Resolves a specification against the database and prints what would happen; nothing is created and nothing is written | — |
| `ptah inference prepare` | Creates the run's own tables and, under the outbox mode, a companion table and two triggers on the source | — |
| `ptah inference retire` | Drops a generation's index and column; it is the one verb here that cannot be undone | — |
| `ptah inference rollback` | Moves the pointer queries read back to a previous generation, when that generation is still measurably one you can go back to | — |
| `ptah inference status` | Prints a run's phase, progress and watermarks from the run-state tables | — |
| `ptah inference verify` | Reads the source and the generation and reports what a cutover would rest on; it writes nothing | — |
| `ptah introspect` | Reads a live database and writes annotated Go models to disk; the database is only read | — |
| `ptah license` | Prints license and attribution text compiled into the binary | — |
| `ptah mcp` | Opens no connection of its own; it serves other operations to an MCP client, and each of those carries its own classification | — |
| `ptah migrations` | Manage migration plans, files, and revision state | group |
| `ptah migrations baseline` | Records existing migrations as applied in the target's tracking table, and replays the directory into a disposable shadow database to verify it reproduces the schema | — |
| `ptah migrations checkpoint` | Squashes history into a checkpoint, replaying the directory into an ephemeral shadow database; the target is not touched | — |
| `ptah migrations create` | Writes an empty up and down migration file pair for someone to fill in by hand | — |
| `ptah migrations data` | Reads reference and seed data from the target and writes a migration file for the drift | — |
| `ptah migrations down` | Rolls back migrations against the target, after replaying and verifying the rollback plan in an ephemeral shadow database | — |
| `ptah migrations edit` | Rewrites a migration file and re-hashes the directory; the target is read to check whether the migration has been applied | — |
| `ptah migrations generate` | Writes migration files from schema differences; the dev database it replays into "is reset destructively" and the shadow database verifies the result | — |
| `ptah migrations hash` | Writes the directory's integrity file, so a later run can tell a hand-edited migration from an intact one | — |
| `ptah migrations import` | Converts another tool's migration directory into Ptah's format on disk | — |
| `ptah migrations lint` | Lints migration files; the dev database it names is cleaned and replayed into | — |
| `ptah migrations ls` | Lists the migration files in a directory, reading nothing but the directory | — |
| `ptah migrations plan` | Reads the target and prints the migration SQL the difference implies, writing nothing | — |
| `ptah migrations pull` | Downloads a migration directory from an OCI registry and writes it to disk | — |
| `ptah migrations push` | Uploads a migration directory from disk to an OCI registry | — |
| `ptah migrations rebase` | Moves a migration to the end of history and updates the target's tracking table | — |
| `ptah migrations repair` | Rewrites revision metadata in the target's tracking table | — |
| `ptah migrations rm` | Deletes a migration, re-hashes the directory and updates the target's tracking table | — |
| `ptah migrations set` | Sets the revision boundary in the target's tracking table to a named version | — |
| `ptah migrations show` | Prints the SQL of one or more migration files, reading nothing but the files | — |
| `ptah migrations status` | Reads the target's tracking table and reports which migrations are applied | — |
| `ptah migrations tag` | Records, lists or removes a tag in the target's tracking table; two of the three write | — |
| `ptah migrations test` | Runs declarative test cases against the database named by `--db-url`, whose own help calls it a "Throwaway database URL": the cases run raw SQL and apply schemas there | — |
| `ptah migrations up` | Runs pending migrations against the target | — |
| `ptah migrations validate` | Validates the directory against its integrity file; the dev database it names is "used to clean and replay migrations for SQL validation" | — |
| `ptah oci` | Inspect Ptah artifacts in OCI registries | group |
| `ptah oci capabilities` | Asks the registry behind a reference which features it supports, and prints them | — |
| `ptah oci copy` | Copies an artifact between two registry repositories without rebuilding it | — |
| `ptah oci fetch` | Downloads the payload of metadata attached to an OCI artifact and writes it to disk | — |
| `ptah oci inspect` | Reports what an OCI artifact declares in its manifest, without downloading the payload | — |
| `ptah oci login` | Checks a registry credential and stores it; it touches no database and writes only Ptah's own credential file | — |
| `ptah oci logout` | Removes the credential Ptah stored for a registry, leaving a Docker-placed one alone | — |
| `ptah oci referrers` | Asks a registry which metadata artifacts refer to one subject and prints them | — |
| `ptah oci reindex` | Republishes attachments a registry's referrers index does not list, so a later query finds them | — |
| `ptah oci resolve` | Asks a registry which immutable digest a mutable tag currently names | — |
| `ptah oci tag` | Asks a registry to move a tag onto an artifact it already holds; nothing is uploaded | — |
| `ptah oci tags` | Asks a registry for the tags one repository carries and prints them | — |
| `ptah oci verify` | Checks an artifact against a verification policy before anything consumes it | — |
| `ptah project` | Read a project file and report what Ptah makes of it | group |
| `ptah project adopt` | Classifies every construct a project file declares as exact, compat-only or unsupported; `--check` reports that and writes nothing, the bare verb rewrites the compat-only spellings and refuses a project declaring anything unsupported, and `--preflight` also reads the revision history in the project's database, writing nothing there | — |
| `ptah project inspect` | Reads a project file and reports which of its settings Ptah acts on and which it read and ignored; it opens no database | — |
| `ptah schema` | Work with desired schema definitions | group |
| `ptah schema annotations` | Exports the Go annotation metadata compiled into the binary, as JSON or a JSON Schema | — |
| `ptah schema apply` | Applies a desired schema to the target; the dev database is where "the plan is rehearsed on before touching the target" | — |
| `ptah schema approve` | Signs a saved plan file with an SSH key and writes the signature beside it | — |
| `ptah schema compare` | Reads the target and reports the difference; on Oracle alone it creates a probe table in the dev database and drops it again, to learn how that engine spells a declared generated-column expression | — |
| `ptah schema diff` | Diffs two arbitrary schema states; a non-database source is materialized by replaying it into the dev database | — |
| `ptah schema drift` | Reads the target and reports how it differs from the desired schema | — |
| `ptah schema export` | Converts one desired-schema source format into another on disk; no database is opened | — |
| `ptah schema fmt` | Rewrites HCL schema files in the repository into canonical form; no database is opened | — |
| `ptah schema inspect` | Reads a schema source and prints it; the dev database it names "is reset destructively" | — |
| `ptah schema lineage` | Traces which base columns feed each view column, from the desired schema alone | — |
| `ptah schema plan` | Saves a fingerprinted apply plan; the dev database is where the plan is rehearsed | — |
| `ptah schema pull` | Downloads a desired-schema document from an OCI registry and writes it to disk | — |
| `ptah schema push` | Uploads a desired-schema document from disk to an OCI registry | — |
| `ptah schema render` | Renders the desired schema as SQL with no connection at all; the dialect comes from a flag | — |
| `ptah schema security` | Reads the target's roles, grants and policies and reports security findings | — |
| `ptah schema serve` | Serves a live read-only view of the schema over HTTP; it opens a listener, which is an exposure of its own even though the database is only read | — |
| `ptah schema stats` | Counts the objects in the target and emits them as OpenMetrics | — |
| `ptah schema test` | Runs declarative test cases against the database named by `--db-url`, whose own help calls it a "Throwaway database URL": measured on PostgreSQL 17.11, a case with an apply_schema step created a table there and an exec step inserted into it | — |
| `ptah schema validate` | Reports structural problems in a desired schema without a database | — |
| `ptah schema verify-approval` | Checks a saved plan's signature against an allowed-signers file | — |
| `ptah seed` | Applies environment-scoped SQL seed files to the database it is given | — |
| `ptah sql` | Work with standalone SQL files | group |
| `ptah sql lint` | Lints standalone SQL files on disk and reports findings; no database is opened | — |
| `ptah version` | Prints the version, commit and build date compiled into the binary | — |
| `ptah viz` | Renders diagrams from a desired schema; no database is opened | — |
<!-- END GENERATED NATIVE COMMANDS -->

## Sources and output formats

An index cell is the command's own one-line description — the line
`ptah --help` prints beside it — so it is short by construction, and the verbs
that accept several inputs or write several formats cannot state the set there.
The set is here. For the flags that select within it, `ptah <command> --help` is
authoritative and [Command flags](../command-flags/) is the same inventory as a
page.

| Verb | Reads | Writes |
| --- | --- | --- |
| `ptah schema render` | Go annotations, YAML, HCL, SQL, or an external command | SQL on stdout; progress and dependency diagnostics on stderr |
| `ptah schema inspect` | a live database, a local schema file, an `oci://` schema artifact, or an Atlas-format migration directory | HCL, SQL, or JSON; `--out-dir` and `--split` write files instead of stdout |
| `ptah schema diff` | two schema states, each a file, a database URL, or a migration directory | migration SQL or JSON |
| `ptah schema export` | any desired-schema source | HCL, an OpenAPI 3.0 component schema, a GraphQL SDL, a Protobuf Edition 2023 definition, or reference documentation as Markdown or a self-contained HTML page |
| `ptah schema test` | a desired schema from Go annotations, a SQL or HCL file, or a live database | a throwaway database the declarative seed, SQL and assert cases run against |
| `ptah migrations import` | a golang-migrate, Goose, Flyway, Liquibase, or dbmate directory, detected unless `--from` names one | Ptah's native migration format |
| `ptah viz` | a desired schema | Mermaid, DOT, or SVG; `--security` marks the tables the schema security rules find |

## Rendering for a specific server

Pass an explicit `--dialect` when the output must be executable by one target.
Without it, `schema render` attempts the built-in review targets and emits
labeled output only if every target can render the schema. An unsupported
feature fails atomically with empty standard output. The combined output is a
review artifact, not one executable SQL script.

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

## Schema file paths

Native `--schema-file` inputs use the process working directory as their path
boundary. A relative path must resolve inside that directory after symbolic
links are followed. Ptah accepts a path that temporarily contains `..` when its
resolved destination is still inside the boundary, and refuses a symbolic link
whose destination is outside it.

Pass an absolute pathname when you intentionally read a schema file outside the
working directory, such as `--schema-file /srv/schemas/app.sql`. Absolute
pathnames retain their existing reach and are not confined to the working
directory.

## Reading a server's capability profile

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

- **What this server decided rather than its release line.** A version alone
  does not settle every key. Where the server's own configuration answers
  differently, the profile says so in its own section (see below).

`--format json` carries everything the text form shows, plus each capability
key's documentation string, as a stable sorted document. Two runs against an
unchanged server produce identical bytes, so a diff of them reports a change
that happened rather than a reordering.

### When configuration, not the version, decides a capability

MySQL 8.4 reads its foreign-key reference policy from
`restrict_fk_on_non_standard_key`. Two servers on that release therefore answer
opposite ways about what a foreign key may point at, and the release line can
only be right about one of them. `ptah db capabilities` pins a session, reads
the set that session resolves, and names every key where the two differ:

```text
Behavior:
  identifier_limit       64 characters
  enum_modeling          inline
  foreign_key_reference  indexed

Set by this server rather than by its release line:
  foreign_keys_require_indexed_reference  supported    (the release line answers unsupported)
  foreign_keys_require_unique_reference   unsupported  (the release line answers supported)
  read from this server's session settings, not from its release line
```

That is the same MySQL 8.4 release as the profile above, started with
`--restrict-fk-on-non-standard-key=OFF`. `foreign_key_reference` is read from
those keys, so it moves with them — `unique` on a default server, `indexed`
here.

The section is absent when the version decided everything, which is the
ordinary case: a server whose configuration matches its release line renders
exactly what it rendered before the refinement existed. In `--format json` the
same information is the `configuration_refinements` array, omitted when empty.

The refinement reports; it does not change the server. Ptah reads settings and
plans by them, and never enables a database feature or writes a server
variable.

## What a machine reads, and from where

Every verb that has a machine-readable format writes that document to **stdout**,
whatever the outcome, and keeps stderr for diagnostics. That includes the
expected-negative cases: `ptah schema drift --format json` writes its report to
stdout and exits 1 when there is drift, exactly as
`ptah migrations status --json --exit-code` writes its report to stdout and
exits 1 when migrations are pending.

The distinction a caller needs is the exit code, not the stream: `0` success,
`1` an expected negative result, `2` a command or usage error. A parser reading
stdout therefore gets a document in every case except `2`.

## What an inference verb does not imply

No `ptah inference` verb implies another. A backfill finishing does not mean
the corpus is right; verification passing does not mean anything has cut over; and
cutting over does not make the old generation disposable. The task-oriented
walkthrough is [Inference migrations](../../operate/inference-migrations/).

## An AI client, over MCP

`ptah mcp` speaks the Model Context Protocol on stdin and stdout, so an AI
client or coding agent can drive Ptah's operations directly. It is not a command
to run by hand: a client starts it and speaks the protocol to it.

Eight reading tools, each forwarding to the operation that already owns the work:

| tool | answers |
| --- | --- |
| `describe_session` | what this session may do, and what it can reach |
| `validate_schema` | structural problems in a declared schema, for one dialect, with no database |
| `render_schema` | the DDL a declared schema becomes, in dependency order |
| `schema_lineage` | which base columns feed each view column |
| `search_docs` | what Ptah's own documentation says about a question, with the document and heading it came from |
| `read_database` | the schema a live database currently holds |
| `inference_plan` | what changing an embedding model would do, what blocks it, and what text would leave the database |
| `inference_status` | how far a generation run has got, and why a cutover is blocked |

Three of Ptah's own reading verbs are deliberately absent: `schema inspect`,
`schema diff` and `migrations lint`. Each needs a scratch database that Ptah
resets destructively, and a destructive capability must not sit behind a
read-only name on a surface an agent drives without reading flag documentation.
They return when a later phase can supply that database out of band rather than
from the caller.

`--workspace` adds three artifact tools -- `read_artifact`, `preview_patch`
and `apply_patch` -- confined to the directories `--migrations-dir`,
`--schema-dir` and `--tests-dir` name.
Writing stays refused until `--allow-write` names an artifact class, and a named
class asks for approval per patch unless `--auto-approve` says otherwise.
Applying anything to a database is unavailable on this surface at any setting.
[AI agents over MCP](../../operate/ai-agents/) is the guide.

Credentials are the client's to supply. The server holds none, stores none, and
sends nothing anywhere: it runs locally and talks to whatever the caller names.

The transport is stdio. A remote one brings authentication, which is a security
surface this release does not open.

## A model of your own

`ptah assist` talks to a model you choose, through a key you hold: a hosted API,
a gateway your organization runs, or one running on this machine. There is no
Ptah account and no Ptah-hosted model.

`ptah assist` holds a conversation and `ptah assist explain` asks one question:
either way the model calls Ptah's tools, and every call goes through the same
surface `ptah mcp` serves, over an in-memory transport. The answer is the
model's words and the tool trace is what Ptah did; `--trace` prints both, and a
run where no tool answered says so.

Conversations are saved to `.ptah/sessions/` in the project, one JSON object per
line; `--ephemeral` keeps no record and `--resume` continues an earlier one.
`ptah assist explain --format jsonl` prints those same records to stdout as they
happen, so a program reading a pipe and a program reading a saved session are
reading one format.

`ptah assist context` prints exactly what a question would send to the provider
and sends nothing, built by the same code that builds the real request. Every
run then reports how many bytes of project content actually went.

`ptah assist provider list` reports
the profiles this machine can reach, including ones inferred from a key already
in your environment, and `ptah assist provider test` measures whether one works
-- reachable, credential accepted, model served, and tool calling available,
which is the capability the workflow requires. The check sends nothing about
your project.

A profile carries a credential REFERENCE such as `env:OPENAI_API_KEY`; a key
written into configuration is refused, and Ptah stores none.
[Ptah Assist and your own model](../../operate/ai-assist/) is the guide.

## A live view

`ptah schema serve` runs an HTTP server that renders the declared schema and how
a live database differs from it, re-reading both on every request. Every route
answers `GET` and `HEAD` and nothing else, and no code path writes to the
database. Nothing about it becomes a dependency of a migration.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--addr` | `127.0.0.1:7070` | Listen address. The address is read back from the listener, so `127.0.0.1:0` prints the port that was chosen. |
| `--refresh` | `30s` | Self-reload interval as a Go duration. `0` serves a page that does not reload. |
| `--title` | `Schema dashboard` | Heading, browser title and sidebar name. |
| `--root-dir` | working directory | Go annotation tree to read the declared schema from. Repeatable. |
| `--schemas` | connection default | Comma-separated schemas the database read is limited to. |
| `--db-url` | none | Database to compare against. Required. |

[Serve a live schema view](../../schema/serve/) is the guide.

## Schema documentation

`ptah schema export` carries two targets that write a schema as a document for a
person: `--to markdown` and `--to html`. Both read the desired schema from any
source and connect to no database.

| Target | Output | Diagram |
| --- | --- | --- |
| `markdown` | One Markdown document written to stdout, or to `--out`. | none |
| `html` | One self-contained HTML file: the styling and the diagram sit inside it, and it names no resource outside itself. | inline SVG, laid out by Ptah |

`--title`, `--include-tables` and `--exclude-tables` apply to both.
[Generate schema documentation](../../schema/document/) is the guide.

## Plan approval

`ptah schema approve` signs a saved plan file with an SSH key,
`ptah schema verify-approval` checks that signature against an OpenSSH
`allowed_signers` file, and `ptah schema apply --plan <path> --require-approval`
refuses a plan that does not verify. `ssh-keygen` performs both halves, in the
`ptah-plan` namespace, over the plan document byte for byte; Ptah implements no
cryptography of its own.

Three outcomes stay distinct, because they call for different actions:

| Outcome | Meaning | Exit |
| --- | --- | --- |
| unapproved | no signature beside the plan — nobody reviewed it | `2` |
| unverifiable | the plan changed after approval, an unlisted key signed it, or `ssh-keygen` could not run | `2` |
| approved | verified, and the signer is named on stdout | `0` |

| Flag | Verb | Meaning |
| --- | --- | --- |
| `--plan` | `approve`, `verify-approval` | Path to the plan file. |
| `--key` | `approve` | Private key `ssh-keygen` signs with. |
| `--allowed-signers` | `verify-approval`, `apply` | Approver list. Default `./.ptah/allowed_signers`, resolved against the working directory. |
| `--signer` | `verify-approval` | Require the approval to belong to one named principal. |
| `--require-approval` | `apply` | Refuse a `--plan` that carries no verifying approval. |

[Plan and approve changes](../../direct/plan-and-approve/) is the guide.

## Column lineage

`ptah schema lineage` derives column-to-column dependencies from the view and
materialized-view bodies a schema declares. The analysis is static and local: it
reads the schema Ptah already models and contacts nothing.

A body it cannot fully resolve is reported under `undecided` with a reason
rather than omitted, because "nothing reads this column" and "I could not tell"
call for different decisions.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--schema-file` | none | Schema file to read. Repeatable. |
| `--root-dir` | working directory | Go annotation tree to read instead. Repeatable. |
| `--dialect` | `postgres` | How the source is parsed. Not a render target. |
| `--format` | `table` | `table` or `json`. |

The run exits `0` whether or not any view landed in `undecided`, so a gate reads
the `undecided` key of the JSON output.
[Trace view column lineage](../../schema/lineage/) is the guide.

## Schema object counts

`ptah schema stats` reads a live database and writes one OpenMetrics gauge per
object kind, ending with a literal `# EOF` line. The family list is fixed rather
than derived from the target, so every run emits the same names in the same
order and a family the reader found none of is reported at `0`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--db-url` | none | Database to read. Required; a `ptah.yaml` `url:` does not satisfy it. |
| `--schemas` | connection default | Comma-separated schemas. Selects what is counted on the PostgreSQL family, and adds a `schemas` label carrying the flag's value as one string on every engine. |

Every gauge is named `ptah_schema_<kind>` and carries a `dialect` label. The
`HELP` line of each family names what it counts;
[Count schema objects](../../schema/stats/) is the guide and shows the complete
set.

## Schema security findings

`ptah schema security` reads a live database and reports findings over what it
declares — who holds which privilege, which tables are reachable with no
row-level policy, which routines run as their owner.
[Report schema security findings](../../schema/security/) is the guide; this
section is the per-code and per-engine detail behind a report.

The rules:

| Code | Finding | Severity |
| --- | --- | --- |
| `PRV01` | a table whose privileges reach a role, with row-level security not enabled | info |
| `PRV02` | a `SECURITY DEFINER` routine, which runs with its owner's privileges | info |
| `PRV03` | a privilege granted to `PUBLIC`, which every current and future role holds | warning |
| `ROL01` | a role holding privileges on an object it was not observed using | warning |
| `ROL03` | two roles held by one member that grant nearly the same privileges | info |
| `ROL04` | a role that cannot log in and that nobody holds | info |
| `OWN01` | objects owned by a role that can log in | info |

`ROL01` needs something no catalog keeps. A privilege is not use: a role holding
`SELECT` on a table it has never read looks identical, in every catalog Ptah
reads, to one that reads it hourly. PostgreSQL's `pg_stat_user_tables` counts
scans without attributing them to a role, `pg_stat_statements` attributes to a
`userid` but is an optional extension that records statement text rather than an
object graph, MySQL and MariaDB keep no equivalent, and SQL Server's
`sys.dm_db_index_usage_stats` attributes nothing to a principal.

So the observation is supplied rather than read. `--role-usage <file>` takes a
JSON list of what something else saw — an audit stream, `pg_stat_statements`, a
proxy log. Without the flag `ROL01` reports itself skipped; with it, every grant
to a named role on an object absent from the list is reported.

The severity is `warning` rather than `error` because the signal is a window,
and a window is evidence of absence only for as long as it covers — a quarterly
job that did not run inside it holds a privilege the rule will name. Grants to
`PUBLIC` are left to `PRV03`, which has the better remedy for them.

`ROL03` and `ROL04` read the server's role graph — who holds which role — so they
run against a live database and report themselves skipped anywhere that graph was
not read. The PostgreSQL family reads it from `pg_auth_members`, MySQL from
`mysql.role_edges`, MariaDB from `mysql.roles_mapping`, and SQL Server from
`sys.database_role_members` — where the fixed roles every database ships with,
and `public`, are excluded on the role side, because a membership nobody wrote
is not a finding.

`OWN01` reads ownership from the PostgreSQL family and SQL Server. MySQL has no
object owner to read — a routine has a `DEFINER` and a table has nothing — so the
rule reports itself skipped there rather than answering from a concept the engine
does not have.

`OWN01` reports **one finding per owning role**, not one per object: an owner is
not a grant, so whoever holds that role's password can drop or alter everything
it owns and no revoke takes that away. It commonly fires — PostgreSQL makes the
creating role the owner — which is why it is `info` and why it is aggregated: a
row per table would bury every other finding.

Two details decide whether it answers at all:

- **The login flag is read from the catalog** beside the owner rather than looked
  up in the described roles, so the rule still answers where the owner is a role
  Ptah does not describe — the bootstrap superuser owns everything on a default
  PostgreSQL database. On SQL Server the same question is
  `authentication_type_desc`: `dbo` reports `INSTANCE`, while `guest` and a user
  created `WITHOUT LOGIN` report `NONE` and are nobody's password.
- **An object with no owner of its own** is owned by its schema's owner on SQL
  Server, which is what most objects are, so the read resolves through the schema
  rather than skipping them.

An edge granted **with admin option** is the right to grant a role onward rather
than evidence somebody uses it, and neither rule counts it. That is measured
rather than stylistic: `CREATE ROLE` on MariaDB inserts an admin edge from the
creator by itself, so counting those would make `ROL04` unable to fire on any
MariaDB server and would make `ROL03` name the creator on every one. The cost is
stated: an explicit `WITH ADMIN OPTION` grant to a real user is ignored too, so
`ROL04` can name a role one administrator could also use — a finding a reader
can dismiss beats one that never appears. `ROL03` reports a pair when they share at least two privileges and at
least half of the smaller role's set; one shared privilege is a coincidence on
any real schema. A login role with no members is not `ROL04`: it is its own
principal, and reporting every application account would bury the rule.

Severities are the ones the rest of Ptah speaks: `info` reports and never
blocks, `warning` asks for review, `error` blocks. `--fail-on error` (the
default), `any`, or `none` decides the exit code, spelled the way
`ptah migrations lint` spells it. No rule here is error-severity, so the default reports without
failing; `--fail-on any` gates on every finding.

`--format json` emits the same findings as a document, each with its structured
detail — the privileges, the roles, the routine's language — so a pipeline can
group or diff them without parsing prose.

A rule that cannot run is listed under `Not checked here:` with its reason,
because a rule that quietly did not run is indistinguishable from one that found
nothing. Row-level security has no meaning on a target that does not model it,
so `PRV01` reports itself skipped on MySQL and SQLite.

PostgreSQL's own `USAGE` on schema `public` to `PUBLIC` is excluded by name,
because a finding present in every database is one a reader learns to skip.
`CREATE` on that schema is not excluded — PostgreSQL 15 revoked it from `PUBLIC`
by default, so a database that grants it is stating something.

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
| `ptah schema validate` | `--schema-file` | desired schema |
| `ptah schema export` | `--schema-file` | desired schema |
| `ptah schema inspect` | `--schema-file` | desired schema |
| `ptah schema lineage` | `--schema-file` | desired schema |
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
discovery works whether or not the registry answers that API — and where the
API is absent, a referrer Ptah published is one another OCI client may never
find. The question is put with the client pinned to the API so a success cannot
have come from the tag-schema fallback, and a failure to ask is reported as an
error rather than folded into a no.

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
