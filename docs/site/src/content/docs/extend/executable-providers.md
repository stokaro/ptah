---
title: Test an executable provider
description: Select a trusted experimental provider for native schema changes and versioned migrations.
type: how-to
sourceMode: source-neutral
audience:
  - "go-developer"
readerQuestion: "How do I test an independently built database provider through the native CLI?"
goal: "Inspect a provider database, apply a saved schema plan, and verify convergence."
sourceOfTruth:
  - "internal/cli/internal/providercommand"
  - "internal/providerselect"
  - "internal/providerhost"
  - "internal/providerinstall"
  - "internal/providerrelease"
  - "internal/providercatalog"
  - "provider-sdk"
  - "integration/providercli/provider_cli_integration_test.go"
  - "integration/providercli/provider_error_reasons_e2e_test.go"
  - "integration/providercli/provider_report_counts_e2e_test.go"
  - "integration/providercli/provider_native_verbs_e2e_test.go"
  - "integration/providercli/provider_migrations_e2e_test.go"
  - "integration/providercli/provider_migration_units_e2e_test.go"
  - "integration/providercli/provider_lint_cli_e2e_test.go"
  - "integration/providercli/provider_devclean_e2e_test.go"
  - "integration/providercli/provider_drop_all_e2e_test.go"
  - "integration/providercli/provider_sql_lint_e2e_test.go"
  - "integration/providercli/provider_data_e2e_test.go"
  - "internal/embedrpc"
  - "integration/providercli/provider_inference_e2e_test.go"
  - "integration/providercli/provider_inference_mcp_e2e_test.go"
  - "integration/providercli/provider_planning_policy_e2e_test.go"
  - "integration/providercli/provider_text_units_e2e_test.go"
  - "integration/providercli/provider_common_objects_e2e_test.go"
  - "integration/providercli/provider_entry_keys_e2e_test.go"
  - "integration/providercli/provider_hcl_claims_e2e_test.go"
  - "integration/providercli/provider_hcl_platform_e2e_test.go"
  - "integration/providercli/provider_index_kinds_e2e_test.go"
  - "integration/providercli/provider_declarations_e2e_test.go"
owns:
  - cli-ptah-providers-list
  - cli-ptah-providers-check
  - cli-ptah-providers-install
generated: false
overlaps: []
disposition: keep
---

Use an explicitly trusted provider executable for native schema changes and
versioned migrations. You need a provider built against the experimental SDK,
its expected identity and SHA-256 checksum, and a disposable database address
accepted by that provider.

Every native command that names a database or a dialect selects a provider:
it takes `--db-url`, `--dev-url`, `--dialect` or `--env`, and it declares
`--provider-config` and `--provider-engine`. That covers the `schema`, `db`,
`migrations`, `seed`, `sql lint`, `viz`, `introspect` and `inference` verbs and
`project adopt`. `schema export` and `schema pull`, which load schema files with
the selected provider's source syntax, declare them too. `ptah mcp` serves the
database `--database-url` names through the selection. `ptah assist` selects no
provider, because it runs the agent tools in process, and `project inspect`
opens no database.

The protocol is experimental. The adapter accepts common tables and columns
whose provider declares `schema.common-table-identity.v1`; other semantics need
additional protocol support. Providers without this feature can still start,
inspect, and run reviewed SQL migrations through their declared services.
Providers can declare `schema.prepare-tables` to prepare captured tables offline
with their own rules. This preserves authored values and inspection knowledge.

Providers can declare `schema.project-table-creations` for their own offline
CREATE prediction. Computed values stay within captured owner identities and
preserve exclusions. Native CREATE planning must replace the host graph step
with provider operations that retain its dependencies.

Providers can declare `schema.columns` with `schema.project-table-creations`
for common-column comparison and readback. The host sends complete captured
parents and its selected column pairs. The provider must return every requested
type, default, nullability, and sequence receipt. Empty literal defaults remain
distinct from absent defaults. Effective nullability can differ from the source
without rewriting the authored field. The same owner predicts complete columns
for file-to-file comparison. Missing facts refuse the operation; they do not
select a built-in parser or a live spelling probe. Domain identities and
generated expressions remain separately owned.

Providers can negotiate `schema.observed-columns` to return complete common
column facts during inspection. The host preserves type metadata, default
presence, generated expressions, identity and sequence settings, comments, and
constraints. Each column has one observed record with its exact name and
position. Legacy scalar fields cannot accompany that record, and missing or
contradictory facts refuse the whole snapshot. Native facets remain attached to
the same column and carry only their separately owned settings.

Providers can negotiate `schema.observed-indexes` with
`schema.observed-columns` to report the common secondary indexes of each
inspected table. The list is complete: an index it does not name is absent, so
the host compares declared indexes with it. The comparison reads each index's
uniqueness, key columns and covered columns, and an index whose definition
differs under the same name is dropped and built again. The build settings stay
the provider's to compare. An index the common model cannot describe stays in
the provider's native facets and out of the list.

The host cannot read a provider's spelling of an index's kind, so it compares
the kind only when the provider negotiates `schema.index-methods` beside
`schema.observed-indexes` and `schema.prepare-tables`. Table preparation then
states the kind of each declared index, spelled exactly as inspection reports a
held index's method. The host compares the two spellings, and a change of kind
alone drops the index and builds it again. The plan keeps the type the source
wrote. Without the feature, the host compares no kind, and a change of kind
alone plans nothing.

A provider that owns the table lifecycle without
`schema.common-table-identity.v1` also receives altered tables. The planning
request carries the captured desired and current state of each altered table,
and every added or dropped column is a step the provider must claim. An added
column reaches the provider with its full definition, including defaults,
comments, collation, and facets. A column step reads its table and creates or
drops one column of it. The host refuses a plan that leaves such a step
unclaimed, and the error names the table and the column. A removed table
reaches the provider with its captured state, and the drop is a step the
provider must claim in the same way.

With `schema.observed-indexes`, a created table's declaration carries its
indexes. An index added to or dropped from a table the plan keeps makes that
table an altered parent, and the change is a step the provider must claim. The
step reads the table and creates or drops one index of it. It carries no
operand: the provider reads the declared index from the desired table and the
held one from the current table. A removal comes before the additions, so a
changed index is dropped before it is created again under the same name.
Where the engine's identifier rules scope an index name to its table, the
plan creates its new tables first and then drops and renames the indexes of
the tables it keeps; where a schema holds index names, the removals come
first, so a new table may take a dropped index's name. Without the feature,
index changes are refused.

With `schema.observed-primary-keys`, a read reports each table's primary key in
key order, and the host compares it with the declared key, members and order.
A provider that owns its tables and lists `planning.primary-key-changes`
receives a key change as an altered table: the desired table carries the
declared key and the current table the held one as its `PRIMARY KEY`
constraint, and no common step stands for the change. The provider plans it,
for example as a rebuild, or refuses it in its own words. Without the feature
the host refuses a key change before it plans, naming the table.

With `planning.index-renames` as well, a provider renames an index in place.
When its session reports `index_rename`, the comparison pairs an index the
table holds with one the declaration states under another name and with one
definition. The pair reaches the provider as one step that names both names in
`renamed_index`, reads the table, drops the held name and creates the declared
one; the provider claims it with an operation that keeps the index. A provider
reports `index_rename` only to a host that lists the feature, and the host
refuses a session that reports it from an engine without the feature. Without
the capability, a renamed index is a drop and a create.

With `schema.observed-index-facets` as well, inspection reports an index's own
settings, such as a YDB index's partitioning, as facets of the index. A source
states the same settings as a facet of the index its index directive, index
entry or index block declares, so the host compares one owner's facet on both
sides and plans a difference as a change of the index's table. The host treats
every facet of such an index as one the provider's rename keeps, and compares a
renamed index's facets under the name the declaration gives it. An HCL export
writes an index facet into the index block, and a whole number a provider
writes for a text attribute is written bare, as the reader reads it back.

An engine that reports its indexes and whose session states
`unique_constraints` as `false` holds a declared UNIQUE constraint, and a
column's UNIQUE, as a global unique index, as YDB does. The host then reads
each one as that index before it compares and plans, under the constraint's
name, or `<table>_<columns>_key` for one that has none, which is the name the
provider gives the index it creates. A UNIQUE over the table's key folds into the key. The
provider receives the index in the table it creates or alters, and never the
constraint. A session that does not state the key leaves a UNIQUE a
constraint.

A service that declares parent assessment
also receives changes to existing columns in the complete table capture. It
must return a parent receipt, including when its own rules find no change.
Missing receipts and refusals discard the plan.

Common-object support carries table, column and index comments. A provider that
owns the table lifecycle and lists the `COMMENT` kind receives the comment of a
table it creates, of a column it adds and of an index it creates as a comment
step after its own creation, and a changed index comment as one comment step
with the comment the index ends with. The comment of a column it drops from a
table the plan keeps is removed with a comment step before the removal, as the
comment of an index it drops is removed: an engine that keeps comments apart
from the object would otherwise keep it. A comment step reads its table and
changes the comment alone, so a service that orders comments after its own
steps of the table leaves out a step that only reads and drops; the host places
the removals after the comments.

A provider that lists the `GRANT` kind receives a grant or revocation on a
table it creates or alters as a step after its own change, and the step reads
the table and changes the table's access alone. These services do not grant
common SQL rendering.

The common steps of a planning request include ordering markers beside the steps
that change a table. A marker has no parent, no effects and no added column. It
stands for the start or end of a phase of the plan, such as
`common/prerequisite-creations`, and a provider orders its own steps against it.
A lifecycle service that matches common steps to its tables must skip the
markers: a step without a parent belongs to no table, and refusing it refuses
every plan. A step with effects and a parent is a table change the service
either claims or leaves to the host.

Every value and native operation Ptah holds for an engine carries the engine's
feature list as its target profile, and a comparison or a plan accepts one
profile per batch. That includes a value decoded from a YAML or Go source. A
provider that lists the same features in the session it opens and in the
inspection snapshot it returns (`target_features`) as in its engine description
gets one profile for observed, desired and planned values. A provider that
leaves them empty refuses every comparison between an inspected facet and a
declared one with "comparison requires one captured target profile".

An inspected catalog retains the opened session's database binding, including
a selected dev realm, even when the selected scope is empty. Comparison and
planning receive that binding so they can resolve absolute object references.

With negotiated `database.context` and `values.context`, the selected provider
resolves the target address without opening a database. Its receipt separates
the execution database from the root of authored absolute references. Source
loads bind owned values before scope selection and dependency collection;
SQL parsing receives the authored root. Object identities and target scopes
stay unchanged. Without an address, parsing retains values that need context,
and dependency consumers refuse them until binding succeeds.

Captured models and their conversions retain this context. A bound model cannot
be rebound to another target. Saved-plan target digests include both roots, and
native operation dispatch checks the captured context against the session.
Neither the captured context nor the digest contains the connection address.

`schema.validate` separately checks captured declarations offline and returns
typed diagnostics. It requires neither SQL rendering nor a database session.
The host rejects declaration families it cannot transport completely; a valid
declaration does not establish that its migration can be planned or applied.

`schema.declarations` gives `ptah schema render` an offline creation path for
provider-owned objects. The host validates the whole schema, sends complete
captured tables to their creation planners, and asks declaration planners for
standalone objects and assigned table children. This path requires
`schema.validate` and uses the caller's capability facts. It opens no database
session and makes no claim about objects that already exist.

The host joins every contribution with its common creation steps before asking
for SQL text. A grant or a revocation on an object a declaration planner
creates comes after every other statement, because the planner may create the
object after the common steps; the other grants keep their place. A missing
receipt, an unclaimed table creation, a dependency cycle, or a provider refusal
returns no SQL prefix. A provider-only operation without a text form cannot
appear in a rendered schema.

Declared rows (`//ptah:schema:data`) and seed files reach a provider through
four services. The host keeps the row diff, the order across tables, protected
tables and the saved plan.

- `data.type-rows` types the declared and the live rows into one canonical form.
- `data.read-rows` reads the live rows in key order.
- `data.render-writes` spells every write. A plan and a data migration hold the
  statements as text, so Ptah asks for the text form when the provider lists
  `render.text-units`, and a write the provider can render only as a native
  operation is refused.
- `data.seeds` runs each `ptah seed` file and records it in a ledger of its own
  in one step. It reports a key conflict, which `--idempotent` tolerates, and
  refuses a file before it runs when the engine cannot run one of its
  statements in a transaction. An outcome the provider cannot confirm is
  reported as such, and the file is not sent again.

A provider without these features refuses declared rows and seed files: the
host does not write its own `INSERT` text or `schema_seeds` table for it. An
engine's `query_dialect` is how the query builder, `core/query`, spells
statements for it. The host registers it with `query.RegisterDialect` when it
starts the provider, and writes and reads the engine's names with its quoting
everywhere else. With `paths`, a qualified name is one path, `dir/t`, and Ptah's
schema is the directory that holds the object.

`engine.default-capabilities` lets a provider state the capability facts its
engine is planned with offline: a render, a generation or an export with no
database, and each release line's for a `--server-version`. Ptah has no preset
for an engine only a provider serves, so without the facts every fact is
unknown offline, and a provider that refuses to plan against an unknown fact
refuses the command. A command that connects plans with the facts the session
states.

A version selects the line it equals, or the longest line it starts with
followed by a dot, so `26.2.1.14` selects `26.2`; a build branch such as
`stable-25-4-1` selects the line of its major and minor. When every line is a
release, a version that selects none is placed on the lines as a built-in
dialect's version is placed on its ladder: one newer than every line is planned
with the newest, one older or between two lines with the line it falls on, and
a warning says what was planned. A version that is no release, such as `trunk`,
is refused.

For a provider that states no offline facts, the facts of the first session
that states any stand in for them for the rest of the command. The conversion
of a source after the connection and the routing of a provider statement in
`migrations generate` then read the server's answers.

`sql.scan` lets a provider scan the SQL text of its engine. Ptah reads SQL text
without parsing it in many places: the lineage of view bodies, the indentation
of the `sql` template function of `ptah-compat`, placeholder rewriting,
statement splitting and the scans of migration bodies. The shared rules read
the SQL standard's strings and quoted names. An engine whose strings or quoted
names differ, as YQL's `"..."` strings and `@@...@@` strings do, lists the
feature. While the provider is selected, Ptah asks `ScanSQL` for the tokens of
each text of the engine it reads, and caches them by text. A scan that fails
reads the rest of the text as unknown, never by the shared rules.

`lint.migrations` lets a provider own the lint rules of its engine. The provider
declares the rule catalog in its description, splits migration files with its own
lexing, and returns findings for the rule codes the host enables. A result that
is not complete is an error, never a clean run. The host keeps output formats,
severity thresholds, and suppression. It sends the target's capabilities with
each check: a dev database's session states them, and a `--server-version`, or
a `server-version` in `.ptah-lint.yaml`, selects a release line the provider
declares facts for (`engine.default-capabilities`), placed on the lines as
above. A version that is no release, such as a floating build, is refused, not
dropped.

A run that names no dialect is split by the host without one. The host answers
the check for the provider, with no finding of its rules, because the request
cannot tell the provider that the SQL may be another engine's. The provider
still annotates that split. A lint run takes an engine the selected provider
serves as its `--dialect`, and reads it from a dev URL the provider serves.
A dialect the lint policy names, in `.ptah-lint.yaml` or in the project
file, selects the engine's provider as `--dialect` would, an installed one
included, so the policy is read and the run linted with that provider's
rules; `sql lint` reads the policy in the working directory the same way. It
replays the migrations on that dev database when the engine declares the dev
database services below, and refuses the dev database at the clean check when
it declares no `database.dev-cleanup`.

`lint.transaction-owner` tells an engine's rules who opens each file's
transaction, so a rule can tell a `BEGIN` that would nest inside the migrator's
transaction from a file's only transaction. `--runner golang-migrate` names a
tool that opens none, and every other run names a migrator that wraps each file.
An engine that does not list the feature judges every file as one its migrator
wraps.

`lint.statement-facts` lets `sql lint` read a provider's SQL. To a host that
lists the feature, the provider's split says what each statement is, which
table a statement creates without a primary key, and which capability keys a
statement needs. `sql lint` reads those facts instead of parsing the file:
DDL001 for a missing primary key, CAP001 for a key the target's capabilities
answer false, SQL002 for a statement that changes no schema object, and SQL004
for a schema statement no fact examines. Ptah declares capabilities only for
the release lines of its own engines, so on another engine SQL004 lists each
requirement as unexamined. A provider whose split does not say what each
statement is is refused.

`database.dev` lets a provider create throwaway databases for its engine. The host
sends the operator's request as typed and never names an image, in either form:
`docker://<engine>/...` or `docker+<engine>://...`. The provider
answers with a lease and an address, or with a typed refusal that says whether the
request is unsupported or its runtime is unavailable. The host releases the lease
when it is done, and the provider removes the leases it still holds when its
process ends. An engine that also declares `database.dev-realms` gets a realm of
the run's own inside the database a `--dev-url` names, created and removed by
the provider, for a database the operator cannot create. A `parallel` case of
`ptah migrations test` or `ptah schema test` gets a realm of its own the same
way, removed when the case ends.

A provider's dev database is cleaned and guarded by the provider, so
`schema apply --dev-url` rehearses a plan on it, `migrations lint --dev-url`
replays a migration directory on it, and `ptah-compat schema plan lint` and
`schema plan validate` replay a saved plan on it:

- `database.dev-cleanup` lists what the dev database holds and empties it. The
  host judges the database clean when the list is empty, refuses one that is
  not, and treats a cleanup whose outcome the provider cannot confirm as not
  clean.
- `database.dev-replay-guard` judges each statement the host runs on the dev
  database and refuses one whose effects reach past it. Without the guard the
  host runs nothing there.
- The host tells the dev database from the target by the database each session
  reports, since it reads no provider address.

`ptah db drop-all` and `ptah-compat schema clean` empty a database the provider
serves through the same `database.dev-cleanup`. The plan is the provider's
list, each object under the provider's name for its kind, such as
`table "users"` or `topic "events" in schema "app"`, and it carries no SQL,
because the provider runs its own statements. The provider then removes the
whole list or nothing. A provider that does not declare the feature for the
engine is refused, and so is a database that holds an object the provider
cannot remove, dry run included; the refusal names the object and the
provider's reason. A plan that `--include` or `--exclude` narrowed is refused
before anything is dropped, since the provider cannot remove part of the list.

`embeddings.store` lets a provider keep the generations of `ptah inference`.
The host lists the feature, and each engine that lists it declares the
consistency modes it builds, `immutable` today. Ptah keeps the lifecycle: it
reads the run state with the provider's token, decides, and sends back only the
records the decision changed. A provider that finds another token writes
nothing, and Ptah reads and decides again. The provider owns the layout of the
run state, the generation's storage, the scans, the vector writes, the index
and the searches, and refuses what its engine cannot hold. `inference describe
--provider-engine <engine>` asks it what a generation needs without a database.
A provider without the feature serves every other command, and `ptah
inference` refuses its engine by name.

The `inference_plan` and `inference_status` tools of `ptah mcp` read the same
store when the operator started the server with a provider flag. The flags are
the server's, never a tool argument.

`units.statement-timeout` carries a migration's statement timeout to the
provider with each unit, and the provider stops a unit that runs longer. A
migration's lock timeout is refused: the protocol has nothing that bounds a lock
wait.

`units.migration-split` lets the provider divide a migration body into the
units its engine runs; see [Run versioned migrations](#run-versioned-migrations).

`models.compare-counts` lets a provider's comparison say how many objects it
compared, so a drift report's inventory lists its kinds beside the ones Ptah
counts. A comparison counts only for a request whose `target_features` list the
feature. Ptah lists it for every comparison of an engine that offers it, also for
a batch that captures no value. A provider without the feature has no entry for
its kinds, and the report never counts them as zero. `provider-sdk/README.md`
says what each count means.

`planning.policy` hands the provider's planner the operator's planning policy:
whether to rebuild a table, with the words a refusal names to say how to ask
(`--allow-table-rebuild`, or `PTAH_ALLOW_TABLE_REBUILD=1` on `ptah-compat`),
and the plan's online-alteration, concurrent-index and NOT NULL backfill
settings. A provider that lists the feature plans a change its engine makes
only by rebuilding the table when the operator asks, and refuses it in those
words when the operator does not. A provider without the feature receives no
policy and plans as if none were set. The feature also lets a comparison read a
request the operator wrote: Ptah sends each `--rotate-secret` value as written
to every comparison of the engine whose actions include `rotate`, and refuses a
value that no comparison resolves, or that two resolve, naming the value.

`schema.common-objects` carries views, materialized views, sequences, roles and
groups with their memberships, grants and comments between Ptah and the
provider. Each engine that lists it names the kinds it carries, and Ptah refuses
a declaration of any other kind before it sends anything, naming the kind and
the engine. The work is split this way:

- The provider reads every object of a kind it carries, or says that it did not
  read the kind, and Ptah then plans no removal of it. A role the server holds
  outside the read's scope, such as a database's user in a YDB dev realm, is
  named without its attributes: Ptah plans no creation of it and no change to
  its attributes. Ptah counts such roles in a note on the diagnostics stream.
  With `schema.role-scope-reasons` the read says why it left them out, and the
  note gives that reason; without it the note says that nothing in the
  inspected schemas refers to them.
- Before a comparison, the provider spells the declared objects as its engine
  reports them, such as a view's query as the engine stores it.
- Ptah compares and orders the changes, and keeps each object as declared in
  the plan. The provider renders each change as one unit.
- A view whose query changed is replaced in place when the session reports
  `create_or_replace_view`. Otherwise Ptah drops the view, creates it again and
  grants again each permission the view held, which the drop took with it. A
  materialized view that changed is always dropped and created again, with its
  permissions granted again the same way.
- A table, a view or a materialized view the plan drops takes its permissions
  with it, so Ptah revokes none of them first, including one the declaration
  leaves out of a view it drops to create again. A view replaced in place keeps
  its permissions, and Ptah revokes one the declaration leaves out.
- A permission the declaration grants on a view or a materialized view the plan
  drops could only run after the drop, so Ptah refuses the plan before it runs
  anything, naming the permission and the view.
- On an engine with `schema.common-table-identity.v1`, the comment of a table
  Ptah creates is a step of its own after the table. A declaration is validated
  by rendering it, so the provider refuses there an object it cannot hold, and
  the error names the render.

A planning request does not carry the declared and observed objects as
context.

`models.report-counts` lets an engine name the count metrics each value of one
of its models contributes to, such as a topic and the number of its consumers.
`ptah schema stats` then writes one gauge per metric, and the provider counts
each value, because only the provider reads the value's content. Ptah counts
the common objects, such as tables and columns, from the read. An engine that
names no metric adds no gauge, and a metric named like a common one, such as
`tables`, makes the command fail. The exception is `indexes` with the help
`Indexes`: an engine counts there the indexes it holds outside the common
model, such as a YDB vector index, and the count adds to the indexes Ptah read.

An engine can declare its release lines in the description. Each line names a
support level and, for a line that claims testing, the image that runs it. A line
the provider keeps without running it is best effort and carries the reason. The
host refuses a description that claims testing nothing runs.
`ptah providers check` prints the lines a provider declares, and
`ptah db capabilities` certifies a server against them: the line its version
names gives the preset its name, which is the line's label, and the support
level, and the line's facts are the baseline the server's own answers are
compared with. The release line prints as the line alone.

[Issue #4204](https://github.com/stokaro/ptah/issues/4204)
owns official provider extraction, installation, and the eventual stable contract.

## Install an official provider

An official provider is installed separately from Ptah. No Ptah release pins a
provider version, and no release archive, container image, installer or GitHub
Action carries one. Install the provider of the engine you need:

```console
ptah providers install ydb
```

A provider is compatible with the provider contract, not with particular Ptah
versions. The contract version has two parts, MAJOR.MINOR. The major version
changes only on an incompatible change, and Ptah and the provider must have the
same one. The minor version grows with each compatible addition. Each provider
release declares the minimum contract it needs, and Ptah runs it when the major
versions are equal and Ptah's minor version is at least the release's. What a
provider can do beyond its minimum is agreed at startup, feature by feature. A
Ptah release does not change the contract version by itself, so a provider
release keeps working across Ptah releases that speak a contract it accepts.
Provider authors find the rules in the
[provider SDK README (latest development source)](https://github.com/stokaro/ptah/blob/master/provider-sdk/README.md).

Without `--version`, the command lists the releases of the provider's GitHub
repository and installs the newest one whose minimum contract this Ptah meets.
It considers published releases only, not drafts or prereleases, and skips a
release that does not publish `ptah-provider.json`, the small document that
names a release's archives and its minimum contract. A release whose
`ptah-provider.json` is malformed stops the command instead of being skipped.

`--version 0.2.0` (or `v0.2.0`) installs that release and does not list
releases. A release whose minimum contract this Ptah does not meet is refused,
and the message names both versions:

```text
the ydb provider 0.3.0 needs contract 26.0 or a later 26.x; this Ptah speaks contract 25.22
```

The command takes the archive for the current platform from `ptah-provider.json`.
It downloads the release's `checksums.txt` and checks `ptah-provider.json` and
the archive against it before it reads anything from the archive. A missing
`checksums.txt`, a missing or empty entry, an entry listed twice and a different
digest all refuse the install.

The command extracts only the expected executable and stores it under the user
cache directory, in `ptah/providers/<provider-id>/<version>/<os>_<arch>/`,
readable only by you. Then it writes the install record
`ptah/providers/<provider-id>/installed-<os>_<arch>.json`, which names that
version and holds the executable's SHA-256. The command never runs the
executable and never edits a manifest. Installing the release the record already
names downloads no archive. Runs that install one provider at the same time,
in one process or in several, take turns on the lock file
`ptah/providers/<provider-id>/.install.lock`, so parallel runs of one release
download it once. An interrupted or parallel run leaves no executable that is
only partly written, and the record keeps naming a complete installation.

The output names the version, its minimum contract, the executable and its
SHA-256. Ptah selects the installation by itself when the command asks for the
engine's provider, as described in
[Which provider serves an engine](#which-provider-serves-an-engine). You do not
write a manifest entry for it.

Listing releases uses the GitHub API without credentials, which limits how often
an address can ask each hour. When the limit is reached, pass `--version`. A
release that publishes no archive for the current platform fails with
`provider_platform_unsupported`, which names the platform.

`--download-base-url` (environment variable `PTAH_DOWNLOAD_BASE_URL`) replaces
`https://github.com` in every download address. A mirror serves the GitHub
release download layout:

```text
<base>/<owner>/<repo>/releases/download/v<version>/ptah-provider.json
<base>/<owner>/<repo>/releases/download/v<version>/checksums.txt
<base>/<owner>/<repo>/releases/download/v<version>/<archive>
```

A mirror cannot list releases, so `--download-base-url` needs `--version`; without
it the command fails with `provider_version_required` before it sends a request.
The mirror changes where the bytes come from and nothing else: the release's
`checksums.txt` still decides what is accepted. Add `--json` for a
machine-readable report.

## Which provider serves an engine

When a command asks for an executable provider, Ptah validates the manifest and
then takes the first candidate that exists. Later candidates are not consulted.

1. The installation the manifest selects for the engine.
2. The installation in the user store that the install record of the engine's
   provider names for the current platform.
3. None. The command fails with `provider_missing` and the instruction
   `ptah providers install <engine>`.

A command asks for an executable provider with `--provider-engine`, when a
manifest entry claims the engine, or when it names an official engine with no
built-in implementation, such as YDB or DuckDB. A built-in engine keeps its
implementation when the command does none of these, and the presence of an
installed provider changes nothing.

Rule 2 builds the installation in memory from the catalog and the install
record: the provider ID and engines come from the catalog, and the expected
SHA-256 is the one the record holds. Nothing is written to a manifest. The
installation goes through the same checks as a manifest entry: the minimum
contract, the digest before every start, then the provider ID, the declared
engines, the contract and the required features.

A candidate that exists and fails a check ends the search:

- A selected manifest entry that is disabled, missing or corrupt is never
  replaced by a stored provider.
- A stored executable whose digest differs from its record is refused with
  `provider_untrusted`. Run `ptah providers install <engine>` to replace it.
- A record whose minimum contract this Ptah does not meet is refused with
  `provider_incompatible` before the process starts. Run
  `ptah providers install <engine>` to install a release this Ptah can run.
- A record that cannot be read is refused with `provider_store_invalid`.

Only the version the record names is looked at. Other versions in the user store
are ignored, however many there are. Ptah does not search `PATH`, the working
directory or a project file.

Offline use needs no extra command. With a provider installed, normal work opens
no network connection. A host without access to GitHub can install from an
internal mirror with `--download-base-url` and `--version`, or receive a verified
executable by other means and select it with a manifest entry (absolute path and
digest).

A container image that needs a provider installs it in a build step of its own,
as the user that runs the image later, because the store is that user's cache
directory. A GitHub Actions workflow runs `ptah providers install <engine>` in a
step after the action.

## Select the executable

Create `providers.json`. Replace the executable path, checksum, and provider ID
with the values for the artifact you trust:

```json
{
  "format_version": 2,
  "providers": [
    {
      "executable": "/absolute/path/to/provider",
      "sha256": "<artifact-sha256>",
      "provider_id": "example.org/fixture",
      "engines": [{"id": "fixture", "aliases": []}],
      "environment": []
    }
  ]
}
```

The executable path must be absolute. A checksum detects changed bytes; it does
not verify a publisher. Providers run with your operating-system permissions.
Only environment entries explicitly listed as `NAME=value` reach the provider.
Database credentials belong in the database address, which travels in the session
request rather than process arguments.

Pass `--provider-config providers.json` on each supported command. This flag has
no environment-variable binding. Ptah does not discover providers from `PATH` or
the current directory. Conflicting engine names or URL aliases are errors, even
when an entry is disabled or unselected. Only the selected installation starts.

Select an engine explicitly with `--provider-engine fixture` when the command
does not identify it through a URL. An explicit engine requires an executable
provider. It never selects an embedded implementation. A manifest with multiple
engines requires an unambiguous URL or `--provider-engine`. The declared engine
IDs and aliases must match the executable's negotiated description.

A provider the flags select owns the command's connections: a URL scheme it
does not register is refused. When `--db-url`, `--dialect` and `--dev-url` name
no engine a provider serves, the command runs on the built-in engines and still
reaches a provider for a URL it learns later, from `ptah.yaml` or from a flag
such as the `--from` and `--to` of `schema diff`. That provider starts at the
first connection, dev database or plan that needs it, beside the built-in
engines, and ends with the command. The manifest named with `--provider-config`
or, without one, the user store decides which provider serves the URL, and a
missing provider reports `ptah providers install <engine>`.

A provider's connection takes no `database/sql` call: outside a plan, a
migration and the services the provider declares, the host can neither query
it nor run a statement on it. `schema test`, which applies the desired schema
that way, refuses a provider's database with "this connection does not expose
database/sql operations".

`sessions.statements` runs a statement the operator wrote and returns its rows
in pages. `db verify` and the `-- +ptah check` assertions of a migration run
through it: the host sends each check read-only, and the provider runs it only
when it proves the check is one query that changes nothing and reads only the
session's database, or refuses it with the reason. A check the database refuses
reports the server's message. The `exec`, `assert` and `catch` steps of
`migrations test`, and the scripts `ptah-compat script exec` and `script query`
run, take the same route read-write. A provider reports a row count only when
its engine measured one, so a script's `expect_rows` is refused on an engine
that does not. An engine that does not list the feature refuses each of these,
and a `catch` step fails there rather than passing on the refusal.

A Go caller can bind arguments to such a statement through
`dbschema.DatabaseConnection.OperatorStatements`. Each one arrives as the
`DataValue` case its Go type names: a signed integer as `int64_value`, an
unsigned one as `uint64_value`, a `float64` as `double_value`, a duration as
`interval_value`, and `nil` as `null`, which carries no type. The host
refuses an argument that no case holds exactly, such as an `int32`, a pointer
or a slice, before it sends the statement, so a provider never receives a value
widened from the type its caller chose.

A provider that lists `sessions.typed-arguments` receives each argument in
`RunStatementRequest.typed_arguments` instead, with the type its caller bound
it at: the scalar at its width, such as `INT32` for an `int32`, whether the type
admits null, as a pointer and a `sql.Null` value do, and whether it is a list,
as a slice other than a byte slice is. The host refuses what even that form
cannot carry, such as an array, a nested list or a second level of null.

`RunStatementRequest.arguments` and `typed_arguments` are positional, and the
placeholder style of the engine's `query_dialect` says which parameter each
position binds. Under
`dollar-named` the Nth parameter is `$pN`, so a provider binds the Nth argument
to `$pN`, and the host sends a Go argument named `pN` at position N, wherever
the call put it. That is how a statement the query builder renders for the
engine, whose arguments are `sql.Named("p1", ...)` and so on, runs through the
provider. The host refuses a named argument it cannot place exactly before it
sends anything: one for an engine whose style is another or that declares none,
names that do not give the positions 1 to N exactly once, and named and
positional arguments in one statement.

The `--include`, `--exclude` and `--schema` selectors name a provider's objects
by the selector types its engines declare under `scope.selections`, such as
`*[type=topic]`. A grant whose object type a selection declares leaves a scope
with the object it names, and a kept object keeps the objects it requires of
the kinds its selection lists, unless an exclusion names them. A read that
reports an object of a selected kind under an identity its naming cannot read
is refused.

Such a grant also follows what the desired source says about its object. A
source that neither declares the object nor describes its kind, through a
`ptah:not-described topic` header or a format that claims nothing about
topics, says nothing about who may use it, so Ptah plans no removal of the
grant. A source that describes the kind and leaves the object out drops it,
and the grant goes with the object. A revoke the source writes out is planned
either way.

`migrations checkpoint`, and the rollback `migrations down --shadow-db`
verifies or plans, replay the directory on a database the provider provisions
or names, and Ptah runs no SQL of its own there. The provider reports its
revision storage as reserved storage, so the schema read back holds only what
the migrations built, and its dev database cleanup removes that storage with
the rest. The shadow database and the target are told apart by the databases
their sessions select.

## Use a provider from ptah-compat

`ptah-compat` has no provider flag, because the pinned Atlas CLI has none. It
reads the manifest path from `PTAH_PROVIDER_CONFIG`:

```bash
PTAH_PROVIDER_CONFIG=providers.json ptah-compat schema inspect --url fixture:///tmp/fixture.db
```

The variable names one manifest file and authorizes nothing. The manifest entry
and the checks described above authorize the executable, exactly as for
`--provider-config`. A blank value is an error, and a manifest that fails to
load ends the search. Native `ptah` never reads the variable.

The provider starts when a connection, a dev database or a planning runtime
first needs it, and it ends when the command returns, fails or is interrupted. A
command that names only built-in engines starts no process. `migrate lint`,
`schema plan lint` and the lint pass of `schema apply` lint with the same
runtime, so the provider of the engine their URLs name splits and checks the
SQL. A plan of a provider's database is saved in the native JSON plan format,
because the `.plan.hcl` format cannot record which provider planned it.

On this surface a provider adds to the built-in engines. A URL scheme the
provider serves goes to the provider, and every other scheme keeps its built-in
driver. This differs from native `--provider-engine`, which asks for exactly one
provider and refuses every other scheme. Without the variable, an official
engine without a built-in implementation resolves through rules 2 and 3 above,
and a missing provider reports `ptah providers install <engine>`.

`PTAH_ATLAS_STRICT_COMPAT=1` refuses `PTAH_PROVIDER_CONFIG` by name and starts no
provider. See [Strict CE mode](../../atlas/strict-ce-mode/).

## Check the installation

List catalog records and configured installations without starting a process:

```console
ptah providers list --provider-config providers.json --json
```

The catalog lists official identities and setup links. It does not prove an
installation exists. Each configured entry reports its integrity status and
`"protocol_status": "not_checked"`. Listing exits successfully after reporting
installation failures; use `providers check` to require a usable installation.

Check one trusted executable without opening a database:

```console
ptah providers check --provider-config providers.json \
  --provider-engine fixture --json
```

Success reports `"status": "compatible"`, the executable path, provider version,
protocol revision, semantic identity, and in `provider.features` the provider's
features as negotiated with this Ptah. The process closes after negotiation.
Compatibility with the transport does not establish support for every schema
object or database operation.

## Inspect the starting state

Set `DATABASE_URL` to the disposable database address. For the independent SDK
fixture, it has the form `fixture:///absolute/path/to/database.db`.

```bash
ptah schema inspect --provider-config providers.json \
  --db-url "$DATABASE_URL" --format json
```

A new fixture database reports its `main` schema without tables. An incomplete
inspection fails; it never represents an empty authoritative database.

A read can find a table it does not describe, such as a YDB replica table no
replication writes any more. With `schema.unread-tables` the provider names
each such table in its read, with a one-line reason in its own words. No
document made from the read names the table, and `schema inspect` and `db read`
print a note instead: `note: 1 table this read does not describe: dir.rep:
<reason>`. A plan that declares a table there, or a foreign key that references
one, is refused before any step with `table dir.rep: <reason>`. A filter, such
as `--exclude`, keeps the entry, since narrowing a read cannot make the
database lose a table. Without the feature, the provider leaves the table out
silently, and a declared table there fails when its creation runs.

Some objects belong to a database rather than to a namespace of it, such as
YDB's resource pools. An engine that lists `schema.database-kinds` names their
kinds, and a read filtered with `--schemas` reports them beside the selected
namespaces, named at the database. The comparison reads them as it reads an
unfiltered read's, so a run scoped to one schema plans the database's objects
its source declares. Everything else a filtered read reports stays inside the
selected namespaces. Without the feature, such a read leaves those kinds
uninspected.

## Plan and apply

Create `schema.yaml` for the fixture's common table grammar. The fixture accepts
unqualified tables and explicitly non-null primary keys. Explicit schema
qualifiers reach the provider unchanged:

```yaml
# schema.yaml
tables:
  items:
    columns:
      id:
        type: INTEGER
        not_null: true
```

Save a plan and review its SQL:

```bash
ptah schema plan --provider-config providers.json \
  --db-url "$DATABASE_URL" --schema-file schema.yaml \
  --output change.plan.json
```

The plan contains the table creation and binds it to the selected executable,
protocol, negotiated semantics, and target metadata, including established
server capabilities. Replacing the provider or
changing those facts requires a new plan. Addresses and credentials are excluded
from this binding.

Apply the reviewed plan to the disposable database:

```bash
ptah schema apply --provider-config providers.json \
  --db-url "$DATABASE_URL" --plan change.plan.json \
  --tx-mode none --auto-approve
```

A provider can support `--tx-mode file` and `--tx-mode all` by advertising
transaction sessions for the selected engine and establishing transactional DDL
on the opened session. Otherwise these modes are refused before execution.

A provider whose session establishes `ddl_inside_transaction` as false states
that its server refuses a schema statement inside an explicit transaction. The
host then runs the statements of `--tx-mode file`, the default, without one, as
it does for a built-in engine with that capability.

`--tx-mode none` permits committed prefixes; a later failure does not undo
earlier operations. A lost commit reply reports an unknown outcome and is never
automatically retried. A commit the provider answers with `ROLLED_BACK` applied
nothing: `migrations up` and `migrations down` record the migration as failed
with no statements applied, and `ptah-compat migrate apply` removes its
revision, as each does after a refused statement. Prepared apply and saved JSON
plans preserve owner transaction requirements and refuse a conflicting mode
before sending any statement. Dev rehearsal checks the same requirements.
Editing SQL with owner transaction requirements needs a fresh owner assessment;
Atlas HCL output refuses those requirements because it cannot preserve them.

## Verify convergence

```bash
ptah schema inspect --provider-config providers.json \
  --db-url "$DATABASE_URL" --format json
ptah schema plan --provider-config providers.json \
  --db-url "$DATABASE_URL" --schema-file schema.yaml \
  --output verify.plan.json --json
```

Inspection reports `items`. The second command reports `"outcome": "no-changes"`.
The CLI closes sessions and terminates its provider processes on success and
failure.

Native YAML objects retain the provider's database, ordered namespace path,
and overload components through schema capture and serialization. Identical
names with different paths or overloads remain distinct. YAML table facets cannot
use database, catalog, path, or overload qualification. Normalization that
changes the path or overload component count is refused. Planning also requires
the corresponding provider comparison, planning, and operation services.

`schema inspect` accepts objects and facets when the provider binds every requested
schema selector to explicit namespaces. Returned tables, subjects, and coverage
regions must stay within those bounds. Unknown parent-table regions remain
unknown after capture. Invalid or widened bindings cause refusal. A read with
no selector may claim a schema and every schema below it with one region when
the provider declares how its schemas nest (`schema.subtree-coverage`), so a
native object declared in a schema that does not exist yet is planned rather
than left undecided.
A failed or canceled read returns no partial catalog. Complete inspection of one
session does not establish global absence across databases or engines; explicit
subject facts are preserved.

Providers may advertise offline conversion between desired and observed models.
The shared conversion pipeline preserves native identities, facets, and bounded
inspection knowledge. Native JSON inspection exports the desired schema through
the selected model codecs. It retains provider objects, facets, and coverage,
including exact namespace and overload identities. Model payload bytes are
base64-encoded inside the host envelope. Failed conversion or encoding returns
no document. JSON is diagnostic output and is not accepted as a schema source.
Conversion and inspection do not qualify native object planning or source-format
export.

## Run versioned migrations

Use a provider that implements revision history, session locks, and migration
metadata. The default transaction mode also requires transactional DDL. A
provider without that capability can use `--tx-mode none`; a missing required
service is refused.

Given a reviewed SQL migration directory, apply it and read its recorded state:

```bash
ptah migrations up --provider-config providers.json \
  --db-url "$DATABASE_URL" --migrations-dir "$MIGRATIONS_DIR" --json
ptah migrations status --provider-config providers.json \
  --db-url "$DATABASE_URL" --migrations-dir "$MIGRATIONS_DIR" --json
ptah migrations log --provider-config providers.json \
  --db-url "$DATABASE_URL" --json
```

`up` returns a run result, including when the database is already up to date.
`status` reports applied and pending revisions. `log` reports recorded attempts.
`status`, `log`, and tag listing create no missing revision, journal, or tag
tables.

The data-safety gate analyzes SQL through the selected provider. Destructive
statements still require review and `--allow-destructive`. An explicit migration
lock timeout is checked against the opened provider session's lock service.

### Migration bodies the provider splits

Some engines cannot run a migration body statement by statement: a definition
exists only in the query that holds it, or a schema statement runs only outside
a transaction. A provider that lists `units.migration-split` divides each body
into the units its engine runs, and Ptah asks it once per body, before the run
writes anything. A dry run asks too and runs nothing. A body the provider
refuses is refused before the run starts, with the text the provider names:

```text illustration
migration 3 cannot run up on ydb: the provider refuses "COMMIT;": <the provider's reason>
```

The units are what the revision counts, what partial hashes digest and what a
resume skips, in the native and the Atlas revision formats. Each unit carries
the definitions it needs from the statements before it, so a resumed run
replays nothing. Each unit has a placement:

- A unit that changes data only runs in a transaction of its own together with
  the checkpoint that records it, so the two commit together or not at all. When
  the engine aborts that transaction for a conflict and applies nothing, Ptah
  runs the unit again, up to five attempts. When the reply to the commit is
  lost, Ptah reads the revision back: a recorded checkpoint proves the commit.
- Any other unit runs outside a transaction. Ptah marks it in flight, runs it
  and records it. When the provider answers that the engine applied nothing,
  the mark is cleared; an outcome nobody knows keeps the mark, and a resume
  refuses to guess.

A failure never records fewer units than the revision row holds. A statement
interceptor is offered only the units that run outside a transaction, and the
statement timeout reaches the provider with each unit.

The transaction mode depends on the session's transactional DDL. Without it,
the default `file` mode runs as `none` and `--tx-mode all` is refused. With it,
a `file` or `all` transaction holds every unit, and a body with a unit that runs
outside a transaction is refused under either mode, because one transaction
cannot hold it.

The provider also judges each `-- +ptah check` assertion of the migration. An
assertion it refuses fails as `check assertion must be one read-only SELECT
statement: <reason>`, in a dry run too. The assertion still runs through the
provider's read-only statement service.

A provider without the feature runs a body by the dialect's statement rules,
whatever its engine is named. Outside a `file` or `all` transaction on a
session with transactional DDL, a body of more than one statement is refused
before anything runs:

```text illustration
migration 3 cannot run up on ydb: the provider does not split a migration
body into the units its engine runs (units.migration-split), so a body of
4 statements cannot run outside a transaction; upgrade the provider
```

`ptah-compat migrate apply` reaches the same rules through a provider selected
with `PTAH_PROVIDER_CONFIG`.

Record a tag after a successful apply:

```bash
ptah migrations tag release --provider-config providers.json \
  --db-url "$DATABASE_URL"
```

Use the same provider manifest when [rolling back migrations](../../versioned/rollback/)
or [maintaining revision metadata](../../versioned/maintain-history/).

## Apply native operations

A provider can return native operations alongside SQL when its declared models
and planning service support the requested change. `ptah schema apply` keeps
these operations intact from planning through confirmation and execution.
Where the provider lists `render.text-units` and writes every operation of the
plan as text, the confirmation output shows each operation as that statement,
the one a migration file holds; the apply still runs the native operation.
Otherwise it shows the operation's kind, subject, and captured impact and
access assessments. A conflicting transaction requirement refuses the whole
plan before any operation runs.

Ptah sends every planning service it calls each table of the plan with its
captured state. A service that plans no table's settings can set `tables` to
`PLANNING_TABLES_OUTLINED` under `planning.table-outlines`, and then receives
each table by its subject, its action and the names of its declared and held
states, which is what such a service reads to learn which tables the plan
keeps, creates, changes and drops.

Apply JSON reports use contract version `4` and an ordered `units` list.
Each entry contains `sql` or a `native` summary. Native summaries omit opaque
provider payloads and are reports, not execution plans. A summary's `text`
holds the provider's statement for the operation where the confirmation output
shows one.

Planner notes stay in the host as ordered annotations. They do not enter the
provider's rendering or execution batch and do not count as operations.
SQL previews prefix every line as a comment. JSON plans retain each note's
position, including trailing notes and plans with notes alone.

A saved plan records each native operation in the provider's own encoding,
with the codec owner, version and definition digest, beside the provider
binding. `ptah schema apply --plan` decodes the operations through the selected
provider, preflights the whole plan against the transaction mode, and refuses
another provider or release, a changed definition or a changed target before
any operation runs. Another build of the same release, such as one for another
platform, decodes the same definitions and applies the plan. Only the
native JSON plan format holds them: the Atlas `.plan.hcl` format and
`ptah-compat` refuse such a plan.

A migration file, `migrations plan`, `schema diff`, the compatibility `migrate
diff` and `schema diff`, and a rendered schema are text, so Ptah asks the
provider for SQL text units there. A provider that lists `render.text-units`
writes each change as one statement its executor runs read back from a file,
and refuses a change it performs only through a native call; Ptah then names
the operation and writes nothing. A provider without the feature keeps its
native units, and those commands refuse them. `schema plan`, `schema apply` and
saved plans keep native operations. `--edit` refuses native plans. In a file
the compatibility `migrate diff` writes, the comment above each statement names
its change from the operation's subject and the effects of its planning step;
[the statements of a generated file](../../atlas/migrate-commands/#the-statements-of-a-generated-file)
gives the wording.

A native operation is bound to the database it was planned for, so
`schema apply --dev-url` does not send the target's operations to the dev
database. It recreates the target's current schema there, plans the same
desired schema again for the dev database, and runs that plan through the
provider, as the apply runs the target's. A plan whose operations are not the
target's, one by one, refuses the apply: the rehearsal would pass on changes
the apply does not make. `ptah-compat schema apply` rehearses the same way.

`ptah migrations generate` writes the reverse migration of a provider's change
from the provider's own reversal of it. A provider that lists `models.reverse`
undoes each change its comparison returned, predicts what the forward change
leaves, and says how the reverse restores the prior definition and what it
cannot recover; the down file carries both as comments. Without the feature, a
plan that holds such a change writes no migration, and the error names the
change kind that has no reversal.

With `units.sensitive-inputs`, the provider declares environment variable names
for exact execution units before approval. Native and compatibility schema
plans retain these references, and JSON plan digests cover them. Input values
are resolved only at dispatch and never enter plans or provider process
environments. Dry runs need no values. A missing value or changed reference
set refuses execution before the batch starts. SQL editing and report
templates refuse input-bearing plans. A dev-database rehearsal of one binds
each value when its unit is dispatched on the dev database, and so does a
migration replayed on a dev database, which approves the inputs each statement
reads before the first statement runs, as `migrations up` does.

`--dry-run` lists the planned operations, decodes and preflights a saved plan,
and writes nothing. The protocol does not establish a lock acquisition that
writes nothing, so a preview on a provider-served target takes no lock and does
not wait for a running apply. Native execution support alone does not qualify
an official provider's schema models or live database behavior.

Uninspected common access kinds remain unknown coverage. Their presence in a
diff does not block unrelated table changes. A provider without common-object
planning still refuses role and grant mutations.

Native grants and revocations follow the declared lifecycle of their target.
They retain their order relative to principal changes and other access changes
on the same object. Each one still runs after the common statements ahead of
it, so the grants of a plan keep their order unless a target's lifecycle moves
one later. The common statements after it do not wait for it: a provider can
replace an input table, restart its dependent object, and then grant access to
that object in one plan. The complete graph still refuses missing footprints,
conflicting dependencies and unordered writes.

## Resolve failures

A provider call that fails names the call and its gRPC code:
`provider inspect failed (FailedPrecondition)`. When the provider lists
`errors.reasons`, as `providers check --json` shows in `provider.features`, the
line ends with the reason the provider wrote:

```text
provider compare models failed (InvalidArgument): streaming query capability was not established
```

A lock outcome reads `provider lock outcome is BUSY`, followed by the
provider's reason in the same way. So does a statement the engine refused,
and a commit the provider rolled back, when the provider speaks contract 25.18
or later:

```text
provider execution: NOT_APPLIED (0 units confirmed applied): <reason>
provider transaction outcome is ROLLED_BACK: <reason>
```

A request or a reply larger than one message holds fails with
`ResourceExhausted`. A message holds 4 MiB, or 64 MiB with a provider that lists
`transport.large-messages`. With `models.references` the host and the provider
send a model the provider describes by its reference rather than its whole
definition, which a request otherwise repeats for every table; a schema of a
few thousand tables then fits one message. With `transport.shared-coverage`
the list of coverage kinds that every table of a request repeats travels once
per message.

Ptah never prints the rest of a provider's error, or the message of an
execution or transaction result: a driver's error can quote a connection
address, a credential or a statement with its values, so only text the
provider wrote as a reason reaches the output. The JSON documents carry the
same sentence in their error message, and the revision of a migration whose
statement failed records it.

Missing official providers include an engine ID, provider ID, repository URL,
and setup URL in the error. JSON schema reports and migration setup failures
preserve these fields in `provider_diagnostic`. Installation checks use `diagnostic`.

| Code | Action |
| --- | --- |
| `provider_missing` | Run the `ptah providers install <engine>` command the message names, or select a trusted artifact through the manifest. |
| `provider_no_release` | The provider's repository has no published release with `ptah-provider.json`. Select a provider you obtained and verified through the manifest. |
| `provider_no_compatible_release` | No release has a minimum contract this Ptah meets. The message names this Ptah's contract and the newest releases with their minimums. Use a Ptah release that speaks one of them, or select a verified artifact through the manifest. |
| `provider_release_incompatible` | The release `--version` names needs a contract this Ptah does not meet. Name another release, or omit `--version` to install the newest one this Ptah can run. |
| `provider_release_asset_missing` | The release has no `ptah-provider.json`, `checksums.txt` or archive where the layout says. Check the version and the mirror. |
| `provider_release_invalid` | The release's `ptah-provider.json` is malformed or names another provider, version or engine. Report it to the provider's maintainers, or name another release with `--version`. |
| `provider_platform_unsupported` | The release publishes no archive for this platform. Install another release, or select a verified executable through the manifest. |
| `provider_checksum_missing` | The release's `checksums.txt` has no entry for `ptah-provider.json` or the archive. Nothing was installed. |
| `provider_checksum_invalid` | The entry is empty, is not a SHA-256 or is listed twice. Nothing was installed. |
| `provider_checksum_mismatch` | A downloaded file does not match `checksums.txt`. Nothing was extracted. Check the mirror or the network path. |
| `provider_version_required` | A mirror cannot list releases. Add `--version`. |
| `provider_version_invalid` | Pass a semantic version such as `0.2.0`. |
| `provider_download_failed` | Check the network, the proxy and `--download-base-url`, then run `providers install` again. When the GitHub API limit is reached, pass `--version`. |
| `provider_archive_invalid` | The archive is not a valid provider archive or lacks the executable. Nothing was installed. |
| `provider_store_failed` | Check the user cache directory and its permissions. |
| `provider_store_invalid` | The install record cannot be used. Run `ptah providers install <engine>` to replace it. |
| `provider_unknown_engine` | Name an engine from `ptah providers list`. |
| `provider_disabled` | Enable the intended installation explicitly. |
| `provider_executable_missing` | Restore the executable or correct its absolute path in the manifest. For a stored provider, run `ptah providers install <engine>`. |
| `provider_unreadable` | Check file permissions and ensure the path names a regular file. |
| `provider_untrusted` | For a manifest entry, verify provenance before accepting a different checksum. For a stored provider, run `ptah providers install <engine>`. |
| `provider_start_failed` | Check the executable platform and startup environment. |
| `provider_incompatible` | Select an artifact with the required identity and contract. For a stored provider whose minimum contract this Ptah does not meet, run `ptah providers install <engine>`. |
| `provider_identity_mismatch` | Correct the declared engines and aliases after reviewing the artifact. |
| `provider_target_mismatch` | Make the database target agree with the selected engine. |

A checksum or identity mismatch refuses startup. Fix the explicit manifest only
after verifying the intended artifact. A missing service or unsupported schema
property requires provider or protocol support; Ptah does not select an embedded
implementation after a provider failure.

A provider crash, lost response, or cancellation after dispatch can report
`"outcome": "unknown"`. Inspect the actual database before planning again. Ptah
does not automatically retry execution, and terminating a provider does not prove
rollback. A stale plan refuses application after the target changes.

YAML loading accepts document and table keys explicitly declared by the selected
provider. It preserves exact numbers and source locations and requires complete
interpretation and coverage from the owner. Unknown keys and unsupported values
are refused.

`source.entry-keys` lets a provider claim keys of column and index entries too:
`COLUMN` and `INDEX` keys, of a table's `columns`, `fields` and `indexes` and of
the top-level `indexes`, whose `table` has to be one the document declares. A
column or index key can list `inherited` keys of its entry, and a Go claim on
`ptah:schema:table`, `ptah:schema:field` or `ptah:schema:index` can list
attributes of that directive the same way. The provider receives them beside its
own, such as an index's `type`, and they stay Ptah's. A value a column or index
key declares belongs to that column or index. One a table key declares belongs
to the table or to a column of it, and a document key's to a table.

`source.hcl` lets an engine claim HCL: top-level blocks, blocks in a `table`,
and attributes of the `table`, `index`, routine and `materialized` blocks. Ptah
checks and evaluates each block as it does its own, then sends the provider the
values, never HCL text, with the index's `type` or `ops` where an index claim
inherits them. A value belongs to what its block is about: the index, the
table, the routine or the view. A top-level block's value is a setting of the
table it names.

For a document whose header holds a `ptah:not-described` entry
of a kind the engine lists in `header_kinds`, the provider also states what the
document holds of its models, from the claims and from those entries. A
document without such an entry is an ordinary Atlas file, which names none of
the objects it leaves out, so it claims nothing of the provider's models and a
plan from it drops none of their objects. Ptah reads a captured
`ptah:feature-coverage` header with the provider's model definitions; the
provider takes no part in it.

`models.predecessors` lets an engine name, in `model_predecessors`, the
identities its models had before it owned them: an owner, a kind, a schema
version and a definition digest. Ptah reads a claim a captured header makes
under one of them as the same claim of the engine's model, so an HCL file
another owner of the model exported keeps the meaning it was written with. The
YDB provider names the coordination-node model of the YDB engine built into
earlier Ptah releases this way. A file Ptah cuts the document into names the
model's current identity.

`source.hcl-platform` lets an engine claim a table's `platform` block whose
label is the engine's ID or one of its aliases, such as `platform "ydb"`. Each
`override` is a string attribute of the claim, and an `override` the claim
does not list is refused at its block. Ptah sends the overrides of a table
that writes the block, and keeps every other `platform` block for its own
readers. The claim's models need not list HCL among their source formats; a
table that writes no block states nothing about them, so its settings stay as
the database holds them. When Ptah writes HCL, the provider writes the part of
a value the block holds, and may report the rest as a loss of the same value,
which Ptah reports as a loss at the table.

The `ptah:feature-coverage` header of an HCL export leaves such a model out.
The blocks the document writes are its whole account of the model, so a table
whose block an edit removes keeps its settings too.

`source.index-kinds` lets an engine claim index types it models outside the
common index, such as YDB's `vector_kmeans_tree`. For a Go `ptah:schema:index`
directive, a YAML index entry or an HCL `index` block whose `type` is one of
them, compared in ASCII lower case with single spaces, Ptah builds no index of
its own. It sends the provider the whole declaration as one fragment, bound to
the index's table: what Ptah read of it (the type, the columns, the covered
columns, uniqueness, the comment and the operator class) and the attributes the
engine claims on the format's index declaration. The values the provider
decodes are settings of the table.

A declaration of such a type that writes
something the fragment cannot carry, such as a `where` condition, is refused
by name. When Ptah writes Go annotations or HCL, the provider writes each such
index back, and Ptah writes it as the same directive or block.

With negotiated
`source.limits`, claimed headers can narrow the model's or a subject's
knowledge. The provider interprets the header and returns the exact enrolled
model inventory with that knowledge. Ptah preserves incomplete knowledge;
namespace-scoped source coverage is refused because the adapter cannot retain
its bounds. Without this feature, a header cannot narrow a decode reply's
coverage, so the provider must refuse a source whose limit it cannot preserve.

`source.encode` lets the provider write its values back when Ptah writes HCL,
as `ptah schema export --to hcl` does. Ptah sends the values, and the provider
answers with the blocks and attributes its claims read, which Ptah writes. A
value the provider cannot write is reported as a loss where Ptah writes its
block. A facet of a column, a key, a routine or a materialized view has no place
in an encoding request and is reported as a loss too. Ptah writes what the
document knows of the provider's models itself, as a captured
`ptah:feature-coverage` header, so the document read back makes the claim the
export had. It sends the provider no coverage for HCL and refuses a reply that
carries `ptah:not-described` entries. Splitting the output repeats the header
in every file.

A schema read through the provider is written as complete for what the read
listed, such as the directories a `--schemas` read named, as the built-in
engines write it: applied with a selection, the document describes that
selection completely. Where the read knows a model less inside what it listed,
Ptah sends the namespace regions the provider's `Inspect` reported, with the
bounds they were read with, and refuses the export, naming the model, when it
cannot. A table's binding names the database the table's read facets name. The
provider decides whether the format can keep that knowledge. An HCL export
writes it in the header, regions included, without asking the provider.

With negotiated `source.encode-groups`, one model value can require several
ordered declarations. Each group accounts for its values once; each part must
match a value's model, binding and subject. Ptah keeps each HCL native object's
identity and target scope when placing its parts. A single HCL block cannot
represent several distinct native object identities.

Ptah writes Go annotations, as `ptah introspect` does, through the same call.
The provider writes a value as a directive of its own at the table or among
the standalone objects, or as attributes of `ptah:schema:table`,
`ptah:schema:field` or `ptah:schema:index`, and it may give a field the Go type
of its engine type. A Go export takes no loss: a value the provider cannot
write refuses the export. Negotiated `source.limits` lets an engine declare
`go_header_kinds` that it reads and writes as `ptah:schema:notdescribed`
annotations. Ptah preserves their reason and provenance, checks the closed
evidence vocabulary, and sends each header to its owner when reading the
source back. Unclaimed limits and limits returned without negotiation refuse
the export.

A directive of the provider's own among the standalone objects stands after
the views. With `source.go-placements`, its part may name another group of the
writer's: after the schemas, after the views, or after the default privileges.
Ptah keeps the order of the provider's parts within a group.

A native Go object reads back in the engine that owns its directive. An export
preserves that scope implicitly and refuses another target scope. An unscoped
object is representable only when its model belongs to that engine alone.

An explicit export target must name an encoding engine or one of its aliases.
Without a target, Ptah selects the provider's sole encoding engine and refuses
an ambiguous choice.

A provider can also claim the group of its own engine in a table's `platform`
key, where the common syntax holds platform properties. The claim is a
`TABLE` key `platform.<engine>`, and the engine is one the provider declares, by
its ID or an alias. For a table that writes

```yaml
tables:
  events:
    platform:
      ydb: {row_deletion_column: created_at, row_deletion_interval: PT1H}
```

the provider receives the mapping of the `ydb` group with its keys unprefixed,
and Ptah keeps no copy of the group in the table's platform overrides. Groups of
other engines stay in the overrides. A group written under `overrides` is not
the provider's and stays there too. Ptah refuses at startup a dotted key that is
not `platform.<engine>`, a platform group claimed at document scope, and a group
that names no engine of the provider. Source-loading support alone does not establish planning or execution support
for those objects and facets. Those operations require their own provider
services and declarations.

Ptah keeps a copy of what each official provider publishes, its lint rule
catalog and its Go annotation claims, recorded from a provider build under
`internal/providerpublished`. The lint rule reference, `ptah schema annotations`
and `ptah-ls` read the copy for an engine the binary does not serve, so they
list the engine's rules and directives without the provider installed. A
command still needs the provider to lint the engine or to read its directives.

A provider also declares the Go annotation syntax it owns. Each claim in
`go_source_directives` names a directive, its scope (`DOCUMENT`, `TABLE`, `COLUMN`
or `INDEX`), the attribute names it accepts and the desired models it can declare.
Ptah splits the comment into a directive name and key/value attributes, so the
provider receives parsed attributes and never comment text. A directive name
belongs to one provider. An attribute the claim does not list is refused before
the provider runs.

A claim is one of two kinds. A directive of the provider's own, such as
`ptah:fixture:retention`, is written in a doc comment and describes one part of
a table:

- `DOCUMENT` and `TABLE` directives sit on a struct. A `TABLE` directive declares
  a facet of the table of that struct, and a `DOCUMENT` directive declares an
  object that belongs to no table. A `TABLE` directive that lists `table` among
  its attributes can name another table of the file with `table=`, written with
  its schema where two schemas hold the same name. That lets a struct that maps
  to no table hold the directive. Ptah refuses a name the file does not declare
  and a name declared in more than one schema, as it does for the `table=` of an
  index directive.
- A `COLUMN` directive sits on a field, in the same comment as that field's
  `ptah:schema:field` directive, and describes the column that directive names
  with `name=`.
- An `INDEX` directive sits in the same comment as exactly one
  `ptah:schema:index` directive, on a struct or a field, and describes the index
  that directive names with `name=`. An index the directive describes in another
  struct's table is the one named by that directive's `table=`.

A claim that names `ptah:schema:table`, `ptah:schema:field` or
`ptah:schema:index` instead gives the provider attributes of Ptah's own
directive. Its scope is the part the directive declares: `TABLE` for the table
directive, `COLUMN` for the field directive and `INDEX` for the index directive.
The provider receives only the attributes it claims, with the same subject as a
directive of its own. A claim on any other Ptah directive, a scope that does not
fit the directive, a claim without attributes and an attribute the directive
already has are refused when the provider starts.

An attribute can be a platform property, such as `platform.ydb.row_deletion_column`.
The provider claims it by its full name. The value is then the provider's: Ptah
sends it to the provider and does not keep a copy in the table's, column's or
index's platform overrides. A platform property that no provider claims stays in
those overrides, and the adapter refuses a table that carries one.

Both kinds may sit before or after the `ptah:schema:table` comment. Ptah
collects the directives of one Go file and sends them to the provider as one
document, so a provider sees every table of the file when it decodes. A column
or index fragment names its table in the binding and the column or index as the
fragment subject: kind `column` or `index`, the table's schema, the table as the
only path component and the written name. The provider answers with a value
owned by that subject. Ptah refuses a value owned by another column or index
instead of placing it, and refuses a directive whose field or index directive is
missing, appears more than once in the comment, or has no `name`.

A `TABLE` directive may also declare a value for a column of its own table, such
as the columns of a column family: the provider names the column as kind `column`, the
table's schema, the table as the only path component and the column's name. The
column must be a field of the directive's struct. Ptah refuses a column of
another table, and a column the struct does not declare.

Attribute values arrive as text, exactly as written, and the provider decides what a value
means. The source position of an attribute is the position of its comment.

An engine declares in `sql_schema` how its SQL schema documents are read. In the
`FRAGMENTS` mode, the default, Ptah parses a document with the shared grammar
and sends the statements it does not model to `DecodeSource`. An engine whose
grammar the shared one cannot read, such as YQL, declares the `DOCUMENT` mode
and the feature `schema.read-sql`, and Ptah does not parse the text.

Ptah reads the files of a load, a schema file with its imports or a schema
directory, and sends them in load order to `ReadSQLSchema` with the engine's
identifier rules and the `ptah:not-described` entries of the kinds listed in
`header_kinds`. The call has no session, address or credential. Ptah maps the
reply as follows:

- Tables, namespaces, enums, views, sequences, roles and grants enter the
  common model.
- The provider's own objects are normalized by the provider.
- Under `schema.sql-facets`, the facets of a table, a column or an index, such
  as a YDB table's changefeeds, are normalized by the provider and placed
  where a YAML source places the same facet.
- Each file's declarations feed the schema-directory check.
- The reading's claim is the load's coverage.

Ptah refuses a reply that is not complete. It prints the first error with its
position, as the shared parser does:
`read SQL schema file schema.sql: unsupported statement GRANT at position 42 (line 2, column 1)`.
The JSON documents of `schema plan` and `schema apply` list every diagnostic in
`schema_diagnostics`, with its code (`syntax`, `unsupported`, `invalid` or
`incomplete`), severity, path and span.

Ptah also refuses a reply it could use only by dropping part of it:

- a reply the SDK's checks refuse;
- a complete reply with a warning, which it does not report yet;
- a declared table that carries captured comparison features, since a reading
  carries a table's facets in its own collection;
- a view or materialized view that carries settings in its `features`. A
  view's settings need `schema.common-view-facets`, which Ptah does not
  negotiate, so a provider reports them as `UNSUPPORTED` and names them.

A command that selects a provider for the engine fails with `provider_missing`
before it reads the file when none is installed.

The execution adapter does not support managed data or isolated execution of
untrusted SQL. It does not add
provider selection to `ptah-compat` or native commands outside the list above. Project-file
URLs and saved plans do not select a provider: pass `--provider-engine` with
`--provider-config` for those workflows. The selected connection scope refuses
every unregistered URL scheme, including schemes with embedded drivers.

The official catalog recognizes [YDB](../providers/ydb/),
[DuckDB](../providers/duckdb/), and [Spanner](../providers/spanner/). These entries
state provider ownership and the GitHub repository whose releases
`providers install` reads. Official extraction remains part of
[issue #4204](https://github.com/stokaro/ptah/issues/4204).

Continue with [plan approval and target checks](../../direct/plan-and-approve/) or the
[public embedding API](../public-api/).
