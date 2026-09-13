---
title: FAQ
description: Short answers to what to use, what not to expect, and which guide to open next.
type: troubleshooting
audience:
  - "all-users"
readerQuestion: "What should I use for this task, what should I not expect, and which guide explains it?"
goal: "Choose the right path for a task and recognize the limit behind a surprising result."
sourceOfTruth:
  - "cmd"
  - "internal/cli"
  - "docs/site/src/content/docs"
generated: false
searchAliases:
  - "squash"
  - "compact migrations"
  - "slow fresh database"
  - "already exists"
  - "existing database"
  - "partial migration"
  - "dirty state"
  - "stuck migration"
  - "table was dropped"
  - "not reversible"
  - "checksum mismatch"
  - "drift ignored"
  - "no such command"
  - "which license"
  - "commercial support"
  - "report a bug"
overlaps: []
disposition: keep
tableOfContents: false
---

Italicized guide names become internal links at integration time.

## My application is not written in Go. Is Ptah for me? {#not-a-go-project}

Yes. The CLI does not care what language your application is written in, and a
desired schema can be SQL, YAML, HCL, or DBML. Go annotations and the Go API are
additional ways in, not a condition of use. See [Choose a workflow](../start/choose-a-workflow/) and [Desired schema and schema sources](../concepts/desired-schema-and-sources/).

## I want to describe the final schema instead of writing each `ALTER TABLE`. Which workflow do I pick? {#declare-or-alter}

Use the desired schema as the source of change. Ptah either generates versioned
migrations from it for review and later deployment, or builds and applies a plan
directly against the database. The choice turns on whether individual SQL
migrations have to stay part of the project history. See [Choose a workflow](../start/choose-a-workflow/).

## I prefer writing migrations by hand. Do I have to declare a schema at all? {#hand-written-migrations}

No. `ptah migrations create` writes empty migration files you fill with your own
SQL. Generated and hand-written migrations share one apply mechanism and can sit
in the same directory. See [Generate migrations](../versioned/generate/#write-a-migration-by-hand).

## I already run a database in production. How do I adopt it without recreating tables? {#adopt-existing-database}

Start by inspecting the live schema and checking the description you get back.
For the versioned workflow, `ptah migrations baseline` records the matching
history as already applied without running its SQL against the existing
database. See [Adopt an existing database](../start/adopt-an-existing-database/).

## I pointed `ptah migrations up` at my existing database and it failed on the first object. Why? {#up-fails-on-existing-object}

Native `migrations up` has no adoption gate, so nothing refuses a database it did
not create, and the first `CREATE` for an object that already exists fails. Adopt
the database with `ptah migrations baseline` first; `--shadow-db` checks that the
baselined history reproduces the schema it was pointed at. See [Adopt an existing database](../start/adopt-an-existing-database/).

## I want to generate migrations in CI without giving CI access to production. Is that possible? {#generate-without-production}

Yes. In replay mode Ptah rebuilds the source schema from migration history in a
separate disposable database and compares that with the desired schema, so
generation needs no production access. See [Generate migrations](../versioned/generate/#generate-without-the-target-database-replay).

## We already have Goose, Flyway, Liquibase, dbmate, or golang-migrate migrations. Do we rewrite the history? {#import-from-another-tool}

Use `ptah migrations import` for the supported formats. Converting the files and
putting an existing database under management are separate jobs; the second one
still needs a baseline step. Check the notes for your source format in [Import from another tool](../versioned/import/).

## After importing from Flyway, a repeatable migration no longer re-runs when I edit the file. Why? {#flyway-repeatable}

Import converts it to an ordinary one-time migration on a reserved slot ordered
after every versioned file, because the native format has no reapply semantics to
convert it into. Make later changes as new migrations, or move that object to a
desired-schema workflow. See [Import from another tool](../versioned/import/#a-repeatable-becomes-a-one-time-migration).

## My Liquibase changelog was rejected on import. What is supported? {#liquibase-changelog-format}

Formatted-SQL changelogs (`--changeset` / `--rollback`) import; XML, YAML, and
JSON changelogs are refused with a message rather than half-converted. Convert
the changelog to formatted SQL, or leave that history where it is and adopt the
database with baseline instead. See [Import from another tool](../versioned/import/).

## Can I replace Atlas with Ptah without rewriting every command and config at once? {#replace-atlas-gradually}

`ptah-compat` presents an Atlas-compatible surface for a gradual move. Full
parity is not claimed: check the commands and configuration constructs you
actually use, and either replace an unsupported step with a native command or
leave it in the old workflow for now. See [Atlas compatibility](../atlas/overview/) and [Migrate from Atlas](../atlas/adoption/).

## `--allow-dirty` did not do what I read about. Why? {#allow-dirty-two-meanings}

One spelling covers two unrelated safety questions. On `ptah migrations up` it
asks for a verified retry of a migration body that failed part-way; on
`ptah-compat migrate apply` it asks to adopt a database that already holds
objects this history did not create. Neither implies the other, so read the flag
against the command it was typed after. See [Apply migrations](../versioned/apply/).

## Why does `ptah` reject a command that works in `ptah-compat`, and the other way round? {#native-vs-compat-surface}

They are different command surfaces, on purpose. Atlas spellings live only in
`ptah-compat`, and native namespaces (`ptah schema`, `ptah db`, `ptah
migrations`) live only in `ptah`. Check which binary the documentation page you
are reading describes. See [Native commands](../reference/native-commands/) and [Atlas-compatible commands](../reference/atlas-commands/).

## I want to split the schema across several files and commands. Can I combine them? {#split-schema-across-files}

Yes. Commands that accept composite sources merge several sources into one
desired schema, which lets you keep parts separate. Ownership of shared objects
still has to be agreed between those parts. See [Composite desired schema](../schema/composite/).

## I passed a second file to override the first and got a conflict instead. Why? {#composite-conflict}

A composite schema is not a last-file-wins mechanism. Matching definitions merge,
and contradicting definitions of the same object are refused. Remove the
duplicated ownership, or prepare the final description for that environment
before handing it to Ptah. See [Composite desired schema](../schema/composite/).

## One database holds tables from several services. How do I manage only mine? {#shared-database-many-services}

Set an explicit management scope through the schema selection and object filters
the command offers. Do not pass one service's description as the full desired
state of a shared database: objects outside the description can land in a drop
plan. See [Compare and drift](../direct/compare-and-drift/) and [Apply a desired schema](../direct/apply/).

## I removed a table from the file so Ptah would stop managing it, and now it plans a `DROP`. Why? {#removed-table-plans-drop}

In a desired-schema workflow, an absent object can mean the object should not
exist inside the managed scope. To leave the table in place and unmanaged, change
the scope rather than deleting its description, and read the plan before applying
it. See [Apply a desired schema](../direct/apply/).

## Can one schema description target both PostgreSQL and SQLite? {#one-schema-two-engines}

For constructs both engines can express, yes. A shared description does not make
engine capabilities equal, so check the rendering and the capability profile for
each target and keep engine-specific parts separate. See [Dialects and capabilities](../concepts/dialects-and-capabilities/).

## Ptah understands a construct but refuses to apply it to my database. Why not skip it? {#capability-refused-not-skipped}

Skipping can change the meaning of the schema without saying so, for example by
dropping a constraint you rely on. Use a construct with the semantics you need
that the target engine supports, or reconsider the engine. See [Troubleshooting](../operate/troubleshooting/#a-dialect-capability-is-unsupported).

## My database is listed as supported. Does that mean all of its SQL is supported? {#supported-engine-not-all-sql}

No. Support depends on the server version, the object type, and the operation:
reading, comparing, generating, or applying. Check the specific capability in the
support matrix and with `ptah db capabilities`, not the presence of an engine
name in a list. See [Database support matrix](../databases/support-matrix/).

## A statement runs fine in my database, but Ptah will not take it as a desired-schema file. What now? {#sql-runs-but-not-a-source}

A desired-schema file is read as a description of structure, not as an arbitrary
SQL script. Express a change the schema model cannot carry as a hand-written
versioned migration and test it against the target engine. See [SQL schema](../schema/sql/) and
[Generate migrations](../versioned/generate/#write-a-migration-by-hand).

## I already changed my local database, and now `migrations generate` produces nothing. Why? {#generate-produces-nothing}

Generation compares the desired schema with a chosen source state. If the local
database already matches it, there is no difference to record. Generate against a
database at the previous version, or restore the source state by replaying
history. See [Generate migrations](../versioned/generate/).

## There are too many migrations and a fresh database takes too long to build. Can I squash them? {#squash-migrations}

Yes, with `ptah migrations checkpoint`. A checkpoint carries the resulting
schema, so a new database starts from it and continues with later migrations
instead of replaying the whole chain. See [Checkpoints](../versioned/checkpoints/).

## I added a checkpoint and it is not applied on an existing database. Is that a bug? {#checkpoint-not-applied}

No. A checkpoint is for the initial build of a database that has no applied
history. A database that has been through migrations skips the checkpoint and
continues on its normal chain. See [Checkpoints](../versioned/checkpoints/#how-a-checkpoint-applies).

## After creating a checkpoint, can I delete the earlier migrations? {#delete-old-migrations}

Do not read a checkpoint as permission to delete history. Existing databases may
still need old migrations they have not run, applied history is tied to revision
metadata, and speeding up new databases does not require removing the old files.
See [Checkpoints](../versioned/checkpoints/) and [Maintain migration history](../versioned/maintain-history/).

## A database started from a checkpoint has the tables but not the initial data from the old migrations. Why? {#checkpoint-has-no-data}

A checkpoint captures the resulting schema, not table contents. Data a new
database needs has to have its own initialization path, such as seed files or
managed reference data. See [Checkpoints](../versioned/checkpoints/), [Seed data](../operate/seed-data/), and [Reference data](../versioned/reference-data/).

## Can I roll a checkpoint-built database back to a point inside the old history? {#checkpoint-rollback-boundary}

Not by rolling that history back: those individual migrations never ran on this
database. Reaching a state before the checkpoint boundary means restoring or
rebuilding through the matching history. The supported rollback boundary is in
[Checkpoints](../versioned/checkpoints/#rollback-boundary).

## I fixed an already-applied migration and Ptah will not run it again. How do I ship the fix? {#fix-an-applied-migration}

Write a new corrective migration. Editing an applied file does not turn it into a
new operation, and it makes the published history mean different things in
different environments. See [Maintain migration history](../versioned/maintain-history/).

## Two branches each added a migration and the order broke after the merge. What now? {#merge-broke-migration-order}

For a migration that is neither published nor applied, `ptah migrations rebase`
moves it to the end; check the resulting sequence afterwards. For history that
has already been distributed, establish what each environment has first, because
renaming files can give one operation two identities. See [Maintain migration history](../versioned/maintain-history/#move-a-migration-to-the-end-rebase).

## Rebase was allowed against my database. Does that make it safe? {#rebase-allowed-not-safe}

Not necessarily. The check speaks for the database you named; it does not prove
that nobody applied the migration or pulled it from a registry. Treat published
history as shared even when your local database has not seen it yet. See
[Maintain migration history](../versioned/maintain-history/#applied-is-not-the-same-as-published).

## The import produced down files. Are all my migrations reversible now? {#import-down-files}

No. Where the source tool carried no rollback SQL, import can leave a placeholder
file. Review those versions and prepare a real recovery path before you rely on
`down`. See [Import from another tool](../versioned/import/).

## I want test data locally without managing it like a production catalog. What do I use? {#seed-versus-reference-data}

Use environment-scoped seed files for initial filling. Declarative reference data
answers a different question: keeping the contents of tables Ptah genuinely
manages at a declared state. See [Seed data](../operate/seed-data/) and [Reference data](../versioned/reference-data/).

## I want countries, statuses, or similar lookup tables in Git, updated by migrations. Is that supported? {#lookup-tables-in-git}

Yes. Managed reference data compares declared rows with the database and
generates a migration for the difference. Check the supported way to declare the
rows and the gates around updates and deletes in [Reference data](../versioned/reference-data/).

## I removed a row from reference data and Ptah wants to delete it from the database. Why? {#reference-data-row-removed}

For a managed lookup table the declaration is the desired contents, so an absent
row can mean a deleted row. Do not put user data, or tables the application also
writes, under that mode. See [Reference data](../versioned/reference-data/).

## I need to fill a new column in a large existing table. Will schema generation do it? {#backfill-a-new-column}

Changing structure and transforming data are separate jobs. A small backfill fits
in a hand-written SQL migration; a large, long-running one belongs in a separate
process with batching and resumption, sequenced against the schema and
application changes. See [Generate migrations](../versioned/generate/#write-a-migration-by-hand).

## If a down migration exists, can I skip the backup? {#down-is-not-a-backup}

No. Down SQL can restore structure, but it cannot bring back data that earlier
operations destroyed. Treat rollback as a transition you designed in advance, not
as a general undo. See [Roll back migrations](../versioned/rollback/).

## I rolled back the application, or a Git commit. Does the database follow? {#app-rollback-and-database}

No. Source state and database state move independently. Decide whether you need a
database rollback or a corrective migration forward, and check the remaining
schema against the application version you are running. See [Roll back migrations](../versioned/rollback/).

## A migration failed and part of its changes stayed. Why did the transaction not save me? {#partial-migration-transaction}

The outcome depends on the transaction mode and on what the engine can carry
transactionally for those statements. Do not assume every migration is atomic:
check the execution mode and the actual state of the database first. See [Apply migrations](../versioned/apply/#execution-controls).

## I set `--tx-mode all` and a statement still committed on its own. Why? {#tx-mode-all-limits}

Some statements cannot run inside a transaction on the target engine, and some
engines do not carry DDL transactionally at all. The mode asks for one
transaction; it cannot grant the engine a capability it lacks. See [Apply migrations](../versioned/apply/#what---tx-mode-all-cannot-carry).

## After a failure Ptah reports a dirty state and refuses the next migrations. What do I do? {#dirty-state-after-failure}

Look at the failing version, the statements that ran, and the actual state of the
database through the migration diagnostics. Then choose the fix and the intended
repair or resume path: clearing the dirty flag does not undo a partial execution.
See [Maintain migration history](../versioned/maintain-history/#repair-a-dirty-revision-state).

## The process was killed and Ptah will not continue the migration on its own. Why not re-run the SQL? {#why-not-rerun-the-sql}

In some non-transactional failures it is unknown whether the last statement
committed, so a re-run can perform the operation twice. Establish the result in
the database first, then reconcile the metadata with it. See [Maintain migration history](../versioned/maintain-history/#repair-a-dirty-revision-state).

## I ran `repair` or `set` and the database structure did not change. Why? {#repair-and-set-change-nothing}

`set` moves the recorded history boundary without running SQL, and `repair`
without a resume reconciles the record of the failing migration. Neither replaces
fixing the database itself; both should record a state you have already
established. See [Maintain migration history](../versioned/maintain-history/).

## How do I stop an accidental table or column drop in CI? {#block-destructive-changes-in-ci}

Use the destructive-change check at plan or generation time, plus the separate
checks at apply time. A warning or a safety report is not a refusal: turn the
blocking policy on explicitly. See [Generate migrations](../versioned/generate/#failure-modes) and
[Lint and gate unsafe SQL](../versioned/lint/).

## Ptah marked a change safe. Does that mean no locking and no broken application? {#safe-is-not-zero-downtime}

No. The classification is not a promise of zero downtime or of compatibility with
every reader and writer. For live systems, judge locking, data volume, and
rollout order separately, and stage the application and schema change when they
are not compatible. See [Generate migrations](../versioned/generate/) and [Apply migrations](../versioned/apply/).

## Before adding a constraint I want to know the data satisfies it. Can I stop the migration early? {#pre-migration-assertions}

Use pre-migration assertions for conditions you can check in the data. They
complement SQL analysis: a statement can be correct and still fail because
existing rows do not allow it. See [Integrity and safety](../versioned/integrity-and-safety/#pre-migration-checks).

## The dry run passed. Why test migrations against a separate database as well? {#dry-run-versus-replay}

A dry run shows the intended actions; it does not execute the whole sequence or
reach the intermediate states. Replay or shadow verification runs the real
execution, and that needs a disposable database. See [Apply migrations](../versioned/apply/) and
[Generate migrations](../versioned/generate/#verify-on-a-shadow-database).

## Can I use my working dev database as the shadow database and save myself one server? {#shadow-database-must-be-disposable}

Only if its contents can genuinely be destroyed. The shadow and replay workflows
clear the database to get a reproducible run of the history, which rules out a
shared dev, staging, or production database. See [Generate migrations](../versioned/generate/#verify-on-a-shadow-database).

## I want one person to approve the SQL and another to apply exactly what was approved. How? {#separate-approval-from-apply}

Save the plan, review it, and use the signed approval at apply time. That
separates agreeing to a specific piece of SQL from permission to run whatever
plan the tool builds later. See [Plan and approve changes](../direct/plan-and-approve/).

## The plan is approved and Ptah says it is stale. Why? {#plan-is-stale}

A plan is bound to the source state it was built from. If the database moved
after that, the earlier approval no longer describes the current conditions, so
build and review a new plan. See [Apply a desired schema](../direct/apply/#failure-modes).

## Two deployment jobs can run migrations at the same time. How do I avoid the race? {#concurrent-deployment-jobs}

Use one agreed migration workflow and a shared lock for a given target. Ptah's
migration lock serializes clients that use the same lock name; it cannot stop
other SQL from a client that ignores it. See [Apply migrations](../versioned/apply/#execution-controls).

## `migrations status` says everything is applied, but someone dropped a column by hand. Why did it not notice? {#status-versus-drift}

Migration status answers a question about recorded history, not about whether the
live schema matches an expectation. For the second question use `ptah schema
drift` with the desired state. See [Apply migrations](../versioned/apply/#check-status) and [Compare and drift](../direct/compare-and-drift/).

## `schema compare` shows differences and CI still passes. Why? {#compare-exit-code}

`compare` succeeds by default even when the diff is non-empty. Use `compare
--exit-code`, or `schema drift`, which fails on drift by default, and keep found
drift and a failed check separate when you read the result. See [Compare and drift](../direct/compare-and-drift/).

## My check returns 1 and I cannot tell a finding from a failure. What do the codes mean? {#exit-codes-1-and-2}

`1` is an expected negative result, such as drift, lint findings, or pending
migrations. `2` is a command or usage error: bad flags, a connection failure, a
parse failure. Treat them differently in CI. See [Exit codes](../reference/exit-codes/).

## I turned on object ignores and drift disappeared. Does the database match the schema now? {#ignores-hide-drift}

It matches within the check you selected, not across the database. Review the
filters and the severity threshold: they can keep differences out of a blocking
result without removing the differences. See [Compare and drift](../direct/compare-and-drift/).

## Ptah sees a difference but prints no SQL to fix it. Is it drift or not? {#drift-without-sql}

It is drift. A difference does not stop existing because no supported planning
operation covers it. Check the reported category and the engine capabilities; a
hand-written migration or a change to the desired schema may be needed. See
[Compare and drift](../direct/compare-and-drift/#compare-the-difference-as-sql).

## A merge, or an edited comment, broke the checksum. Do I run `hash` and move on? {#checksum-broke-after-merge}

Read the diff first: the checksum covers file contents, not only the meaning of
the SQL. Restore accidental changes, and re-hash only for edits you decided to
keep. Do not use it to paper over a change to shared history. See [Integrity and safety](../versioned/integrity-and-safety/).

## Why did Ptah apply migrations with no `ptah.sum` at all? I expected a mandatory check. {#missing-sum-file}

On the native surface a missing integrity file is allowed, while a file that
exists is verified. Use `--verify-sum` where the file has to be present and
checked. See [Integrity and safety](../versioned/integrity-and-safety/#a-directory-that-was-never-hashed).

## I want to ship migrations without checking out the whole repository. Can I? {#ship-migrations-without-checkout}

Yes. Publish the migration directory as an OCI artifact and hand the reference to
a supported apply or check command. Pin a digest for a reproducible deployment.
See [OCI registry artifacts](../operate/oci-registry/).

## The artifacts are in a private registry. Do I need a Ptah account? {#private-registry-credentials}

No. Ptah uses your registry's credentials, through a Docker credential store or
`ptah oci login`. Credentials do not belong in the OCI reference itself. See [OCI registry artifacts](../operate/oci-registry/#authenticate-securely).

## Docker is not installed. Does that rule out a private OCI registry? {#oci-login-without-docker}

No. `ptah oci login` is a separate authentication path that does not need Docker
installed. Which registry you use and whether a Docker daemon runs are different
questions. See [OCI registry artifacts](../operate/oci-registry/#authenticate-securely).

## Staging and production point at the same tag and got different migrations. How? {#same-tag-different-content}

A tag is a mutable pointer. Promote one verified digest between environments
rather than expecting a tag to hold the same contents across two deployments. See
[OCI registry artifacts](../operate/oci-registry/#choose-a-reference).

## The digest and the checksum match. Does that mean a trusted publisher produced the artifact? {#digest-is-not-authenticity}

No. They establish identity and internal consistency, not who published the
bytes. `ptah oci verify` enforces a policy before an artifact is consumed, and
its `require_signature` checks that a signature is attached without running any
cryptography. Keep signing and cryptographic verification with cosign or
Notation, and use registry access controls. See [OCI registry artifacts](../operate/oci-registry/#identity-integrity-and-authenticity).

## Can I use Ptah without a cloud service and without sending my schema to someone else's registry? {#ptah-without-a-cloud-service}

Yes. The schema and migration workflows work against local files and your own
database, and OCI is an optional delivery route through a registry you choose. AI
features and inference need their own review, because they can call external
services you configure. See [OCI registry artifacts](../operate/oci-registry/) and [Inference migrations](../inference/overview/).

## I configured one connection and the command used another. Where do I look? {#wrong-connection-used}

Check the selected environment, the environment variables, and the explicit CLI
flags, which take precedence. Diagnose the connection with a read-only command
using the same parameters before repeating a schema change. See [Troubleshooting](../operate/troubleshooting/#database-connection-fails). `ptah project` reports what Ptah makes of a project
file.

## The command is in the documentation and my binary does not know it. What do I check? {#command-not-in-my-binary}

Line up the binary version, the documentation version, and the command surface:
`ptah` and `ptah-compat` have different command paths. The `edge` documentation
does not describe any released version. See [Install Ptah](../start/install/) and [Native commands](../reference/native-commands/).

## I want an ER diagram and SVG output asks for Graphviz. Can I avoid installing it? {#svg-needs-graphviz}

Yes. Choose Mermaid or DOT output and use a viewer that reads it. Graphviz is
needed for the SVG rendering path, not for visualization in general. See
[Troubleshooting](../operate/troubleshooting/#svg-output-says-graphviz-is-required).

## I want to embed schema work in my own Go tool instead of shelling out to the CLI. Is that supported? {#embed-ptah-in-go}

Yes, for part of the surface. Use the documented public API: being able to import
a package does not make it a stable public contract. See [Public Go API](../extend/public-api/) and
[Reusable components](../extend/components/).

## I want an AI agent to prepare migrations without letting it touch production. How do I split the rights? {#agent-without-production-access}

In MCP and Assist, configure file access and the permitted write classes
separately. On the agent surface, inspecting a database classified `production`
is denied and no flag on that surface widens it; give the agent verified schema
artifacts for analysis and run production changes through the normal review and
deploy path. See [Configure agent permissions](../operate/ai-agent-permissions/).

## I granted write access and in CI the agent still asks for approval and stops. Why? {#agent-auto-approve}

Granting a class of operations and allowing it without a prompt are separate
settings. For a trusted non-interactive job, add `--auto-approve` for the class
you need; it grants no database access and lifts none of the other gates. See
[Configure agent permissions](../operate/ai-agent-permissions/#run-non-interactively).

## I want to change embedding model without rewriting every live vector at once. Can Ptah help? {#change-embedding-model}

For PostgreSQL with pgvector, use inference migrations: a new generation is built
beside the active one, verified, and activated as a separate step. Supported
endpoint APIs and limitations are in [Inference migrations](../inference/overview/).

## I need a model server, or inference on a GPU. Is that Ptah's job? {#ptah-does-not-run-models}

No. Ptah manages the persisted result of inference; it does not run or host
models. Stand up an embedding service with a supported API and connect it as a
provider. See [Inference migrations](../inference/overview/#who-does-what).

## The embedding backfill finished and the application still reads the old generation. Why? {#backfill-then-cutover}

Finishing a backfill does not verify or switch anything. Verification, cutover,
and retiring the old generation are separate decisions; continue the lifecycle
rather than deleting the old data by hand. See [Inference migrations](../inference/overview/#what-a-migration-looks-like).

## I want to manage the schema through Kubernetes and GitOps. Is there an operator? {#is-there-an-operator}

Yes. Ptah Operator takes a desired schema from an OCI artifact, plans the change,
and manages applying and re-checking it. It is a separate project with its own
documentation, releases, and support matrix. See [Kubernetes operator](../operate/kubernetes-operator/) and the
operator documentation.

## We use versioned migrations. Can the operator run `up` for us? {#operator-and-versioned-migrations}

Not today. `PtahSchema` reconciles a desired schema, and no resource applies a
migration directory. Deploy a versioned workflow with `ptah migrations up`
against a pinned artifact from your pipeline or a Job of your own. See
[Kubernetes operator](../operate/kubernetes-operator/#what-it-does-not-deploy).

## I want to contribute. Where do I start? {#how-to-contribute}

Start with `CONTRIBUTING.md` in `github.com/stokaro/ptah`, which covers what
makes a report actionable and what a change has to pass. `AGENTS.md` beside it
is the authority on the working rules the CI gates enforce. Open an issue before
a change that alters behavior, so the design is settled before the diff exists.

## I found a bug. What makes the report actionable? {#reporting-a-bug}

The exact command, the `ptah version` output, the engine and server version, and
the smallest schema or migration that reproduces it. Say which binary you ran:
`ptah` and `ptah-compat` differ on purpose, so the surface is part of the
report.

## I have a feature suggestion. How should I frame it? {#feature-suggestion}

Describe the task and the outcome you need rather than the flag you have in
mind, because the need often already has a spelling. Ptah is pre-GA and the
surface still moves, so a suggestion that names a workflow is easier to place
than one that names an option.

## What license is Ptah under? {#what-license}

MIT, copyright Denis Voytyuk. `ptah license` prints the license, the copyright,
the source location, and the Atlas-compatibility attribution notice.

## Can I use Ptah in a closed-source commercial product? {#commercial-use}

Yes. MIT permits use, modification, and redistribution, including inside
proprietary software, as long as the copyright notice and the license text
travel with copies you distribute. Nothing in Ptah asks you to publish your
schema or your application.

## Does the Atlas compatibility work put anything non-MIT in the source tree? {#atlas-license-boundary}

No. The Ptah tree is implementation-clean and MIT. Atlas-derived Apache-2.0
fixture material lives in the separate `ptah-atlas-conformance` repository,
which tests Ptah without Ptah importing it. See [License boundary](../atlas/license-boundary/).

## Is commercial support available? {#commercial-support}

Commercial enquiries go to ask (at) stokaro.com. Keep bug reports and feature
requests on the issue tracker instead, where they stay public and get labeled.
