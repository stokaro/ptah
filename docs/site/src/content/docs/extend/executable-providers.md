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
  - "internal/providerhost"
  - "internal/providerinstall"
  - "internal/providercatalog"
  - "provider-sdk"
  - "integration/providercli/provider_cli_integration_test.go"
  - "integration/providercli/provider_migrations_e2e_test.go"
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

Provider selection is available on these native commands:

- Schema changes: `schema inspect`, `schema plan`, and `schema apply`.
- Migration execution and inspection: `migrations up`, `migrations down`,
  `migrations status`, and `migrations log`.
- Migration metadata: `migrations tag`, `migrations baseline`, `migrations set`,
  and `migrations repair`.

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

`schema.validate` separately checks captured declarations offline and returns
typed diagnostics. It requires neither SQL rendering nor a database session.
The host rejects declaration families it cannot transport completely; a valid
declaration does not establish that its migration can be planned or applied.

`lint.migrations` lets a provider own the lint rules of its engine. The provider
declares the rule catalog in its description, splits migration files with its own
lexing, and returns findings for the rule codes the host enables. A result that
is not complete is an error, never a clean run. The host keeps output formats,
severity thresholds, and suppression.

`database.dev` lets a provider create throwaway databases for its engine. The host
sends the operator's request as typed and never names an image. The provider
answers with a lease and an address, or with a typed refusal that says whether the
request is unsupported or its runtime is unavailable. The host releases the lease
when it is done, and the provider removes the leases it still holds when its
process ends.

An engine can declare its release lines in the description. Each line names a
support level and, for a line that claims testing, the image that runs it. A line
the provider keeps without running it is best effort and carries the reason. The
host refuses a description that claims testing nothing runs.
`ptah providers check` prints the lines a provider declares.

[Issue #4204](https://github.com/stokaro/ptah/issues/4204)
owns official provider extraction, installation, and the eventual stable contract.

## Install the pinned official provider

Each Ptah release pins one tested version of an official provider and, for each
platform, the SHA-256 of its archive and of the executable inside it. Install the
pinned provider for the engine you need:

```console
ptah providers install ydb
```

The command takes an engine and no version. It downloads the archive for the
current platform from the provider's GitHub release, compares the archive with
the pinned digest before it reads anything from it, extracts only the expected
executable, and compares that with its pinned digest. It then stores the
executable under the user cache directory, in
`ptah/providers/<provider-id>/<version>/<os>_<arch>/`, readable only by you. The
command never runs the executable, never edits a manifest, and never looks for a
newer release. Running it again for a verified installation downloads nothing.
An interrupted or parallel run leaves no executable that is only partly written.

The output names the executable and its SHA-256. To use the installation, write
a manifest entry with that absolute path and digest, as described in the next
section. Ptah does not select a provider from the user cache by itself; only a
manifest entry selects one.

Only engines whose provider has a release are pinned. For an engine without a
pinned release, and for a platform the pin does not cover, the command fails with
a diagnostic that names the engine or the platform.

`--download-base-url` (environment variable `PTAH_DOWNLOAD_BASE_URL`) replaces
the directory the archive is fetched from, for example an internal mirror that
serves the same file names. It changes where the bytes come from and nothing
else: the pinned digests still decide what is accepted. Add `--json` for a
machine-readable report.

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
Provider lock acquisition during a read-only preview is refused because the
experimental protocol does not establish acquisition without writes.

## Apply native operations

A provider can return native operations alongside SQL when its declared models
and planning service support the requested change. `ptah schema apply` keeps
these operations intact from planning through confirmation and execution.
The confirmation output shows the operation's kind, subject, and captured
impact and access assessments. A conflicting transaction requirement refuses
the whole plan before any operation runs.

Apply JSON reports use contract version `3` and an ordered `units` list.
Each entry contains `sql` or a `native` summary. Native summaries omit opaque
provider payloads and are reports, not saved execution plans.

Saved schema plans and migration files still require a SQL projection.
`--edit` and dev-database rehearsal refuse native plans. Provider lock
acquisition during `--dry-run` remains subject to the read-only preview refusal
above. Native execution support alone does not qualify an official provider's
schema models or live database behavior.

## Resolve failures

Missing official providers include an engine ID, provider ID, repository URL,
and setup URL in the error. JSON schema reports and migration setup failures
preserve these fields in `provider_diagnostic`. Installation checks use `diagnostic`.

| Code | Action |
| --- | --- |
| `provider_missing` | Follow the engine-specific setup page and configure a trusted artifact. |
| `provider_no_release_pinned` | This Ptah release pins no release of that provider. Select a provider you obtained and verified through the manifest. |
| `provider_platform_unsupported` | The pin has no archive for this platform. Install from another platform or select a verified executable through the manifest. |
| `provider_download_failed` | Check the network, the proxy and `--download-base-url`, then run `providers install` again. |
| `provider_archive_untrusted` | The downloaded archive does not match the pinned digest. Nothing was extracted. Check the mirror or the network path. |
| `provider_archive_invalid` | The archive is not a valid provider archive or lacks the executable. Nothing was installed. |
| `provider_executable_untrusted` | The executable in the archive does not match the pinned digest. Nothing was installed. |
| `provider_store_failed` | Check the user cache directory and its permissions. |
| `provider_unknown_engine` | Name an engine from `ptah providers list`. |
| `provider_disabled` | Enable the intended installation explicitly. |
| `provider_executable_missing` | Restore the executable or correct its absolute path. |
| `provider_unreadable` | Check file permissions and ensure the path names a regular file. |
| `provider_untrusted` | Verify provenance before accepting a different checksum. |
| `provider_start_failed` | Check the executable platform and startup environment. |
| `provider_incompatible` | Select an artifact with the required identity and protocol. |
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
are refused. Source-loading support alone does not establish planning or execution support
for those objects and facets. Those operations require their own provider
services and declarations.

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

Both kinds may sit before or after the `ptah:schema:table` comment. Ptah
collects the directives of one Go file and sends them to the provider as one
document, so a provider sees every table of the file when it decodes. A column
or index fragment names its table in the binding and the column or index as the
fragment subject: kind `column` or `index`, the table's schema, the table as the
only path component and the written name. The provider answers with a value
owned by that subject. Ptah refuses a value owned by another column or index
instead of placing it, and refuses a directive whose field or index directive is
missing, appears more than once in the comment, or has no `name`. Attribute
values arrive as text, exactly as written, and the provider decides what a value
means. The source position of an attribute is the position of its comment.

The execution adapter does not support managed data, database cleanup, or
isolated execution of untrusted SQL. It does not add
provider selection to `ptah-compat` or native commands outside the list above. Project-file
URLs and saved plans do not select a provider: pass `--provider-engine` with
`--provider-config` for those workflows. The selected connection scope refuses
every unregistered URL scheme, including schemes with embedded drivers.

The official catalog recognizes [YDB](../providers/ydb/),
[DuckDB](../providers/duckdb/), and [Spanner](../providers/spanner/). These entries
state provider ownership and distribution policy. Official extraction and
release artifacts remain part of [issue #4204](https://github.com/stokaro/ptah/issues/4204).

Continue with [plan approval and target checks](../../direct/plan-and-approve/) or the
[public embedding API](../public-api/).
