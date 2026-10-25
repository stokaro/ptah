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
  - "integration/providercli/provider_native_verbs_e2e_test.go"
  - "integration/providercli/provider_migrations_e2e_test.go"
  - "integration/providercli/provider_lint_cli_e2e_test.go"
  - "integration/providercli/provider_devclean_e2e_test.go"
  - "integration/providercli/provider_sql_lint_e2e_test.go"
  - "integration/providercli/provider_data_e2e_test.go"
  - "internal/embedrpc"
  - "integration/providercli/provider_inference_e2e_test.go"
  - "integration/providercli/provider_inference_mcp_e2e_test.go"
  - "integration/providercli/provider_planning_policy_e2e_test.go"
  - "integration/providercli/provider_text_units_e2e_test.go"
  - "integration/providercli/provider_common_objects_e2e_test.go"
  - "integration/providercli/provider_entry_keys_e2e_test.go"
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

A provider that owns the table lifecycle without
`schema.common-table-identity.v1` also receives altered tables. The planning
request carries the captured desired and current state of each altered table,
and every added or dropped column is a step the provider must claim. An added
column reaches the provider with its full definition, including defaults,
comments, collation, and facets. A column step reads its table and creates or
drops one column of it. The host refuses a plan that leaves such a step
unclaimed, and the error names the table and the column. A removed table
reaches the provider with its captured state, and the drop is a step the
provider must claim in the same way. Changes to existing columns, indexes, and
table comments are refused. These services do not grant common SQL rendering.

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

`schema.validate` separately checks captured declarations offline and returns
typed diagnostics. It requires neither SQL rendering nor a database session.
The host rejects declaration families it cannot transport completely; a valid
declaration does not establish that its migration can be planned or applied.

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
starts the provider.

`lint.migrations` lets a provider own the lint rules of its engine. The provider
declares the rule catalog in its description, splits migration files with its own
lexing, and returns findings for the rule codes the host enables. A result that
is not complete is an error, never a clean run. The host keeps output formats,
severity thresholds, and suppression. It also resolves a `--server-version` to
the engine's capabilities and sends them with each check.

A run that names no dialect is split by the host without one. The host answers
the check for the provider, with no finding of its rules, because the request
cannot tell the provider that the SQL may be another engine's. The provider
still annotates that split. A lint run takes an engine the selected provider
serves as its `--dialect`, and reads it from a dev URL the provider serves. It
replays the migrations on that dev database when the engine declares the dev
database services below, and refuses the dev database at the clean check when
it declares no `database.dev-cleanup`.

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
the provider, for a database the operator cannot create.

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
  read the kind, and Ptah then plans no removal of it.
- Before a comparison, the provider spells the declared objects as its engine
  reports them, such as a view's query as the engine stores it.
- Ptah compares and orders the changes, and keeps each object as declared in
  the plan. The provider renders each change as one unit.
- On an engine with `schema.common-table-identity.v1`, the comment of a table
  Ptah creates is a step of its own after the table. A declaration is validated
  by rendering it, so the provider refuses there an object it cannot hold, and
  the error names the render.

A planning request does not carry the declared and observed objects as
context.

An engine can declare its release lines in the description. Each line names a
support level and, for a line that claims testing, the image that runs it. A line
the provider keeps without running it is best effort and carries the reason. The
host refuses a description that claims testing nothing runs.
`ptah providers check` prints the lines a provider declares.

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
the ydb provider 0.3.0 needs contract 26.0 or a later 26.x; this Ptah speaks contract 25.0
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
built-in implementation, such as DuckDB. An official engine that still has a
built-in implementation, such as YDB, keeps it when the command does none of
these, and the presence of an installed provider changes nothing.

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
it nor run a statement on it. `db verify`, which queries the database for each
check, and `schema test`, which applies the desired schema that way, refuse a
provider's database with "this connection does not expose database/sql
operations".

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
protocol revision, and semantic identity. The process closes after negotiation.
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
`--tx-mode none` permits committed prefixes; a later failure does not undo
earlier operations. A lost commit reply reports an unknown outcome and is never
automatically retried. Prepared apply and saved JSON
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
unknown after capture. Invalid or widened bindings cause refusal.
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
The confirmation output shows the operation's kind, subject, and captured
impact and access assessments. A conflicting transaction requirement refuses
the whole plan before any operation runs.

Apply JSON reports use contract version `3` and an ordered `units` list.
Each entry contains `sql` or a `native` summary. Native summaries omit opaque
provider payloads and are reports, not execution plans.

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
saved plans keep native operations. `--edit` and dev-database rehearsal refuse
native plans.

`--dry-run` lists the planned operations, decodes and preflights a saved plan,
and writes nothing. The protocol does not establish a lock acquisition that
writes nothing, so a preview on a provider-served target takes no lock and does
not wait for a running apply. Native execution support alone does not qualify
an official provider's schema models or live database behavior.

## Resolve failures

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
- Each file's declarations feed the schema-directory check.
- The reading's claim is the load's coverage.

Ptah refuses a reply that is not complete. It prints the first error with its
position, as the shared parser does:
`read SQL schema file schema.sql: unsupported statement GRANT at position 42 (line 2, column 1)`.
The JSON documents of `schema plan` and `schema apply` list every diagnostic in
`schema_diagnostics`, with its code (`syntax`, `unsupported`, `invalid` or
`incomplete`), severity, path and span. Ptah also refuses a reply the SDK's
checks refuse, a complete reply with a warning, which it does not report yet,
and a table that carries facets or owned objects, which a reading cannot place
yet. A command that selects a provider for the engine fails with
`provider_missing` before it reads the file when none is installed.

The execution adapter does not support managed data, database cleanup, or
isolated execution of untrusted SQL. It does not add
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
