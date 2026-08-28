# The agent surface: what each verb does to a database

Ptah has one command tree and two audiences. A person at a terminal reads a
verb's name and its `--help`; an autonomous agent is handed a list of operations
and drives them. The second audience needs a fact the first can infer from
context: **which databases can this verb change, and which can it destroy.**

This document answers that for every verb, and it is generated. The rows below
come from walking the command tree of the binary this repository builds, joined
with a classification written down in
[`internal/agentsurface`](../internal/agentsurface). Neither half may drift from
the other: a verb the classification does not name fails the build, and so does
one whose class says it touches no database while it registers a flag that names
one.

It exists because the previous inventory could not be kept true. ADR 0002 §1.1
enumerated the surface from the built binary on 2026-08-21 and §1.2 classified
the readers by which of them register `--dev-url`. A record of a decision is
never edited — that is the rule this repository's ADRs are written under — so
when the binary grew nine verbs, the table could only go stale, and it did.
[ADR 0005](adr/0005-agent-surface-inventory.md) records why the inventory moved
here and what the classification is measured against.

## The two axes

**Target** is the database the verb is pointed at, by `--db-url`.

| value | meaning |
| --- | --- |
| none | no connection to a target is opened |
| reads | the target is read and not written |
| **writes** | the target is changed |

**Second database** is the other one some verbs take: a dev, shadow or
throwaway database that is not the target, named by `--dev-url` or
`--shadow-db`.

| value | meaning |
| --- | --- |
| none | no second database is taken |
| probes | objects are created there and dropped again; what was there survives |
| **rewrites** | a directory or a plan is replayed into it, or it is cleaned first; what was there does not survive |

The second axis is separate from the first because the two are independent, and
conflating them is the failure this document exists to prevent: `schema inspect`
reads its target and its dev database "is reset destructively", in the flag's
own words. A reader who stopped at the verb's name would call it safe.

## What this does not answer

Only databases. Several verbs below have no database at risk and still do
something an agent-exposure decision has to weigh: `introspect` and `schema fmt`
write files in the working tree, `schema push` and `oci copy` publish to a
registry, `assist provider test` reaches a model endpoint, and `schema serve`
opens a network listener. Artifact mutation is decided by
[ADR 0004](adr/0004-constrained-artifact-mutation.md); this table is not a
verdict on any of it.

## What the walk sees

Five rows are cobra's rather than Ptah's: `help`, and `completion` for bash,
fish, powershell and zsh. Nothing in the native command tree registers them:
cobra adds them in `ExecuteC`, after the program has finished assembling its own
tree, so they are absent from what `cmd/root` returns and present in what the
binary answers to. The walk calls the same two initializers before it measures, which is what
gives every leaf `ptah --help` reaches a row here. The compatibility binary
differs: `cmd/atlas` registers `completion` itself, so that Atlas-style group
help can rewrite it, and `help` is the only name that arrives that late there.

Two spellings are not rows, and no walk can make them rows. `__complete` and its
alias `__completeNoDesc` are the protocol a shell uses to ask a running binary
what to suggest next. cobra registers them from an unexported initializer that
also removes the command again unless the invocation being parsed is that
command, so they are unreachable from a constructed tree and hidden from
`--help`. Measured, `ptah __complete ""` prints the top-level verb names and
exits 0; it reads no database and takes no database flag. A row for either would
be hand-written, and a hand-written row is the thing the generated table exists
to rule out — so they are named here, where a claim nothing measures belongs.

## Every verb

<!-- BEGIN GENERATED AGENT SURFACE -->
| verb | target | second database | flags | what it does |
| --- | --- | --- | --- | --- |
| `assist context` | none | none | — | prints what a question would send to a model provider and sends nothing; it opens neither a database nor an endpoint, and writes no file |
| `assist explain` | none | none | — | opens no connection of its own; it asks a model a question and lets the model call Ptah's own tools, each of which carries its own classification |
| `assist provider list` | none | none | — | lists the provider profiles configured locally, and opens neither a database nor an endpoint |
| `assist provider test` | none | none | — | measures a provider profile by calling the model endpoint it names; no database is involved |
| `assist sessions delete` | none | none | — | removes one saved conversation file from the project; no database is opened, and the audit log is a separate file it does not touch |
| `assist sessions list` | none | none | — | lists the conversations saved under the project's .ptah directory; neither a database nor a model endpoint is opened |
| `assist sessions prune` | none | none | — | removes saved conversation files untouched for longer than a given age; no database is opened, and the audit log is left alone |
| `assist sessions show` | none | none | — | prints one saved conversation from disk, including what Ptah's tools answered during it; nothing is opened to do so |
| `completion bash` | none | none | — | writes a bash completion script to stdout, generated from the command tree; it opens no database and writes no file |
| `completion fish` | none | none | — | writes a fish completion script to stdout, generated from the command tree; it opens no database and writes no file |
| `completion powershell` | none | none | — | writes a PowerShell completion script to stdout, generated from the command tree; it opens no database and writes no file |
| `completion zsh` | none | none | — | writes a zsh completion script to stdout, generated from the command tree; it opens no database and writes no file |
| `db capabilities` | reads | none | `--db-url` | reads the server's version and catalogs to report the capability profile Ptah resolves |
| `db drop-all` | **writes** | none | `--db-url` | drops every schema object in the database it is given |
| `db read` | reads | none | `--db-url` | introspects the database and prints what it found |
| `help` | none | none | — | prints the help text of the verb it names, or of the root when it names none; the verb itself is not run and nothing is opened |
| `inference backfill` | **writes** | none | `--db-url` | reads the source, sends it to the embedding endpoint the specification names, and writes vectors and checkpoints into the target database |
| `inference catchup` | **writes** | none | `--db-url` | rereads the source rows recorded as changed and writes their vectors, which sends that text to the embedding endpoint |
| `inference cutover` | **writes** | none | `--db-url` | moves the pointer queries read to a different generation, and refuses when the pointer is not where the plan it was built from expects |
| `inference evaluate` | reads | none | `--db-url` | searches the generation with queries from a corpus, which sends those queries to the embedding endpoint; the database is only read |
| `inference index` | **writes** | none | `--db-url` | builds the generation's vector index concurrently, which writes an index into the target database and drops an invalid leftover before rebuilding it |
| `inference plan` | reads | none | `--db-url` | resolves a specification against the database and prints what would happen; nothing is created and nothing is written |
| `inference prepare` | **writes** | none | `--db-url` | creates the run's own tables and, under the outbox mode, a companion table and two triggers on the source |
| `inference retire` | **writes** | none | `--db-url` | drops a generation's index and column; it is the one verb here that cannot be undone |
| `inference rollback` | **writes** | none | `--db-url` | moves the pointer queries read back to a previous generation, when that generation is still measurably one you can go back to |
| `inference status` | reads | none | `--db-url` | prints a run's phase, progress and watermarks from the run-state tables |
| `inference verify` | reads | none | `--db-url` | reads the source and the generation and reports what a cutover would rest on; it writes nothing |
| `introspect` | reads | none | `--db-url` | reads a live database and writes annotated Go models to disk; the database is only read |
| `license` | none | none | — | prints license and attribution text compiled into the binary |
| `mcp` | none | none | — | opens no connection of its own; it serves other operations to an MCP client, and each of those carries its own classification |
| `migrations baseline` | **writes** | **rewrites** | `--db-url`, `--shadow-db` | records existing migrations as applied in the target's tracking table, and replays the directory into a disposable shadow database to verify it reproduces the schema |
| `migrations checkpoint` | none | **rewrites** | `--shadow-db` | squashes history into a checkpoint, replaying the directory into an ephemeral shadow database; the target is not touched |
| `migrations create` | none | none | — | writes an empty up and down migration file pair for someone to fill in by hand |
| `migrations data` | reads | none | `--db-url` | reads reference and seed data from the target and writes a migration file for the drift |
| `migrations down` | **writes** | **rewrites** | `--db-url`, `--shadow-db` | rolls back migrations against the target, after replaying and verifying the rollback plan in an ephemeral shadow database |
| `migrations edit` | reads | none | `--db-url` | rewrites a migration file and re-hashes the directory; the target is read to check whether the migration has been applied |
| `migrations generate` | reads | **rewrites** | `--db-url`, `--dev-url`, `--shadow-db` | writes migration files from schema differences; the dev database it replays into "is reset destructively" and the shadow database verifies the result |
| `migrations hash` | none | none | — | writes the directory's integrity file, so a later run can tell a hand-edited migration from an intact one |
| `migrations import` | none | none | — | converts another tool's migration directory into Ptah's format on disk |
| `migrations lint` | none | **rewrites** | `--dev-url` | lints migration files; the dev database it names is cleaned and replayed into |
| `migrations ls` | none | none | — | lists the migration files in a directory, reading nothing but the directory |
| `migrations plan` | reads | none | `--db-url` | reads the target and prints the migration SQL the difference implies, writing nothing |
| `migrations pull` | none | none | — | downloads a migration directory from an OCI registry and writes it to disk |
| `migrations push` | none | none | — | uploads a migration directory from disk to an OCI registry |
| `migrations rebase` | **writes** | none | `--db-url` | moves a migration to the end of history and updates the target's tracking table |
| `migrations repair` | **writes** | none | `--db-url` | rewrites revision metadata in the target's tracking table |
| `migrations rm` | **writes** | none | `--db-url` | deletes a migration, re-hashes the directory and updates the target's tracking table |
| `migrations set` | **writes** | none | `--db-url` | sets the revision boundary in the target's tracking table to a named version |
| `migrations show` | none | none | — | prints the SQL of one or more migration files, reading nothing but the files |
| `migrations status` | reads | none | `--db-url` | reads the target's tracking table and reports which migrations are applied |
| `migrations tag` | **writes** | none | `--db-url` | records, lists or removes a tag in the target's tracking table; two of the three write |
| `migrations test` | **writes** | none | `--db-url` | runs declarative test cases against the database named by `--db-url`, whose own help calls it a "Throwaway database URL": the cases run raw SQL and apply schemas there |
| `migrations up` | **writes** | none | `--db-url` | runs pending migrations against the target |
| `migrations validate` | none | **rewrites** | `--dev-url` | validates the directory against its integrity file; the dev database it names is "used to clean and replay migrations for SQL validation" |
| `oci capabilities` | none | none | — | asks the registry behind a reference which features it supports, and prints them |
| `oci copy` | none | none | — | copies an artifact between two registry repositories without rebuilding it |
| `oci fetch` | none | none | — | downloads the payload of metadata attached to an OCI artifact and writes it to disk |
| `oci inspect` | none | none | — | reports what an OCI artifact declares in its manifest, without downloading the payload |
| `oci login` | none | none | — | checks a registry credential and stores it; it touches no database and writes only Ptah's own credential file |
| `oci logout` | none | none | — | removes the credential Ptah stored for a registry, leaving a Docker-placed one alone |
| `oci referrers` | none | none | — | asks a registry which metadata artifacts refer to one subject and prints them |
| `oci reindex` | none | none | — | republishes attachments a registry's referrers index does not list, so a later query finds them |
| `oci resolve` | none | none | — | asks a registry which immutable digest a mutable tag currently names |
| `oci tag` | none | none | — | asks a registry to move a tag onto an artifact it already holds; nothing is uploaded |
| `oci tags` | none | none | — | asks a registry for the tags one repository carries and prints them |
| `oci verify` | none | none | — | checks an artifact against a verification policy before anything consumes it |
| `project adopt` | reads | none | — | classifies every construct a project file declares as exact, compat-only or unsupported; `--check` reports that and writes nothing, the bare verb rewrites the compat-only spellings and refuses a project declaring anything unsupported, and `--preflight` also reads the revision history in the project's database, writing nothing there |
| `project inspect` | none | none | — | reads a project file and reports which of its settings Ptah acts on and which it read and ignored; it opens no database |
| `schema annotations` | none | none | — | exports the Go annotation metadata compiled into the binary, as JSON or a JSON Schema |
| `schema apply` | **writes** | **rewrites** | `--db-url`, `--dev-url` | applies a desired schema to the target; the dev database is where "the plan is rehearsed on before touching the target" |
| `schema approve` | none | none | — | signs a saved plan file with an SSH key and writes the signature beside it |
| `schema compare` | reads | probes | `--db-url`, `--dev-url` | reads the target and reports the difference; on Oracle alone it creates a probe table in the dev database and drops it again, to learn how that engine spells a declared generated-column expression |
| `schema diff` | none | **rewrites** | `--dev-url` | diffs two arbitrary schema states; a non-database source is materialized by replaying it into the dev database |
| `schema drift` | reads | none | `--db-url` | reads the target and reports how it differs from the desired schema |
| `schema export` | none | none | — | converts one desired-schema source format into another on disk; no database is opened |
| `schema fmt` | none | none | — | rewrites HCL schema files in the repository into canonical form; no database is opened |
| `schema inspect` | reads | **rewrites** | `--db-url`, `--dev-url` | reads a schema source and prints it; the dev database it names "is reset destructively" |
| `schema lineage` | none | none | — | traces which base columns feed each view column, from the desired schema alone |
| `schema plan` | reads | **rewrites** | `--db-url`, `--dev-url` | saves a fingerprinted apply plan; the dev database is where the plan is rehearsed |
| `schema pull` | none | none | — | downloads a desired-schema document from an OCI registry and writes it to disk |
| `schema push` | none | none | — | uploads a desired-schema document from disk to an OCI registry |
| `schema render` | none | none | — | renders the desired schema as SQL with no connection at all; the dialect comes from a flag |
| `schema security` | reads | none | `--db-url` | reads the target's roles, grants and policies and reports security findings |
| `schema serve` | reads | none | `--db-url` | serves a live read-only view of the schema over HTTP; it opens a listener, which is an exposure of its own even though the database is only read |
| `schema stats` | reads | none | `--db-url` | counts the objects in the target and emits them as OpenMetrics |
| `schema test` | **writes** | none | `--db-url` | runs declarative test cases against the database named by `--db-url`, whose own help calls it a "Throwaway database URL": measured on PostgreSQL 17.11, a case with an apply_schema step created a table there and an exec step inserted into it |
| `schema validate` | none | none | — | reports structural problems in a desired schema without a database |
| `schema verify-approval` | none | none | — | checks a saved plan's signature against an allowed-signers file |
| `seed` | **writes** | none | `--db-url` | applies environment-scoped SQL seed files to the database it is given |
| `sql lint` | none | none | — | lints standalone SQL files on disk and reports findings; no database is opened |
| `version` | none | none | — | prints the version, commit and build date compiled into the binary |
| `viz` | none | none | — | renders diagrams from a desired schema; no database is opened |
<!-- END GENERATED AGENT SURFACE -->

## The verbs no database is at risk from

This is where an agent-exposure decision starts, not where it ends: read the
section above about what this does not answer before treating a row here as a
permission.

<!-- BEGIN GENERATED DATABASE-SAFE VERBS -->
| verb | why no database is at risk |
| --- | --- |
| `assist context` | prints what a question would send to a model provider and sends nothing; it opens neither a database nor an endpoint, and writes no file |
| `assist explain` | opens no connection of its own; it asks a model a question and lets the model call Ptah's own tools, each of which carries its own classification |
| `assist provider list` | lists the provider profiles configured locally, and opens neither a database nor an endpoint |
| `assist provider test` | measures a provider profile by calling the model endpoint it names; no database is involved |
| `assist sessions delete` | removes one saved conversation file from the project; no database is opened, and the audit log is a separate file it does not touch |
| `assist sessions list` | lists the conversations saved under the project's .ptah directory; neither a database nor a model endpoint is opened |
| `assist sessions prune` | removes saved conversation files untouched for longer than a given age; no database is opened, and the audit log is left alone |
| `assist sessions show` | prints one saved conversation from disk, including what Ptah's tools answered during it; nothing is opened to do so |
| `completion bash` | writes a bash completion script to stdout, generated from the command tree; it opens no database and writes no file |
| `completion fish` | writes a fish completion script to stdout, generated from the command tree; it opens no database and writes no file |
| `completion powershell` | writes a PowerShell completion script to stdout, generated from the command tree; it opens no database and writes no file |
| `completion zsh` | writes a zsh completion script to stdout, generated from the command tree; it opens no database and writes no file |
| `db capabilities` | reads the server's version and catalogs to report the capability profile Ptah resolves |
| `db read` | introspects the database and prints what it found |
| `help` | prints the help text of the verb it names, or of the root when it names none; the verb itself is not run and nothing is opened |
| `inference evaluate` | searches the generation with queries from a corpus, which sends those queries to the embedding endpoint; the database is only read |
| `inference plan` | resolves a specification against the database and prints what would happen; nothing is created and nothing is written |
| `inference status` | prints a run's phase, progress and watermarks from the run-state tables |
| `inference verify` | reads the source and the generation and reports what a cutover would rest on; it writes nothing |
| `introspect` | reads a live database and writes annotated Go models to disk; the database is only read |
| `license` | prints license and attribution text compiled into the binary |
| `mcp` | opens no connection of its own; it serves other operations to an MCP client, and each of those carries its own classification |
| `migrations create` | writes an empty up and down migration file pair for someone to fill in by hand |
| `migrations data` | reads reference and seed data from the target and writes a migration file for the drift |
| `migrations edit` | rewrites a migration file and re-hashes the directory; the target is read to check whether the migration has been applied |
| `migrations hash` | writes the directory's integrity file, so a later run can tell a hand-edited migration from an intact one |
| `migrations import` | converts another tool's migration directory into Ptah's format on disk |
| `migrations ls` | lists the migration files in a directory, reading nothing but the directory |
| `migrations plan` | reads the target and prints the migration SQL the difference implies, writing nothing |
| `migrations pull` | downloads a migration directory from an OCI registry and writes it to disk |
| `migrations push` | uploads a migration directory from disk to an OCI registry |
| `migrations show` | prints the SQL of one or more migration files, reading nothing but the files |
| `migrations status` | reads the target's tracking table and reports which migrations are applied |
| `oci capabilities` | asks the registry behind a reference which features it supports, and prints them |
| `oci copy` | copies an artifact between two registry repositories without rebuilding it |
| `oci fetch` | downloads the payload of metadata attached to an OCI artifact and writes it to disk |
| `oci inspect` | reports what an OCI artifact declares in its manifest, without downloading the payload |
| `oci login` | checks a registry credential and stores it; it touches no database and writes only Ptah's own credential file |
| `oci logout` | removes the credential Ptah stored for a registry, leaving a Docker-placed one alone |
| `oci referrers` | asks a registry which metadata artifacts refer to one subject and prints them |
| `oci reindex` | republishes attachments a registry's referrers index does not list, so a later query finds them |
| `oci resolve` | asks a registry which immutable digest a mutable tag currently names |
| `oci tag` | asks a registry to move a tag onto an artifact it already holds; nothing is uploaded |
| `oci tags` | asks a registry for the tags one repository carries and prints them |
| `oci verify` | checks an artifact against a verification policy before anything consumes it |
| `project adopt` | classifies every construct a project file declares as exact, compat-only or unsupported; `--check` reports that and writes nothing, the bare verb rewrites the compat-only spellings and refuses a project declaring anything unsupported, and `--preflight` also reads the revision history in the project's database, writing nothing there |
| `project inspect` | reads a project file and reports which of its settings Ptah acts on and which it read and ignored; it opens no database |
| `schema annotations` | exports the Go annotation metadata compiled into the binary, as JSON or a JSON Schema |
| `schema approve` | signs a saved plan file with an SSH key and writes the signature beside it |
| `schema compare` | reads the target and reports the difference; on Oracle alone it creates a probe table in the dev database and drops it again, to learn how that engine spells a declared generated-column expression |
| `schema drift` | reads the target and reports how it differs from the desired schema |
| `schema export` | converts one desired-schema source format into another on disk; no database is opened |
| `schema fmt` | rewrites HCL schema files in the repository into canonical form; no database is opened |
| `schema lineage` | traces which base columns feed each view column, from the desired schema alone |
| `schema pull` | downloads a desired-schema document from an OCI registry and writes it to disk |
| `schema push` | uploads a desired-schema document from disk to an OCI registry |
| `schema render` | renders the desired schema as SQL with no connection at all; the dialect comes from a flag |
| `schema security` | reads the target's roles, grants and policies and reports security findings |
| `schema serve` | serves a live read-only view of the schema over HTTP; it opens a listener, which is an exposure of its own even though the database is only read |
| `schema stats` | counts the objects in the target and emits them as OpenMetrics |
| `schema validate` | reports structural problems in a desired schema without a database |
| `schema verify-approval` | checks a saved plan's signature against an allowed-signers file |
| `sql lint` | lints standalone SQL files on disk and reports findings; no database is opened |
| `version` | prints the version, commit and build date compiled into the binary |
| `viz` | renders diagrams from a desired schema; no database is opened |
<!-- END GENERATED DATABASE-SAFE VERBS -->
