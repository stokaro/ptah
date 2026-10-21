---
title: Test an executable provider
description: Select a trusted experimental provider for native schema inspection, planning, and application.
type: how-to
sourceMode: source-neutral
audience:
  - "go-developer"
readerQuestion: "How do I test an independently built database provider through the native CLI?"
goal: "Inspect a provider database, apply a saved schema plan, and verify convergence."
sourceOfTruth:
  - "internal/cli/internal/providercommand"
  - "internal/providerhost"
  - "provider-sdk"
  - "integration/providercli/provider_cli_integration_test.go"
owns:
  - cli-ptah-providers-list
  - cli-ptah-providers-check
generated: false
overlaps: []
disposition: keep
---

Use an explicitly trusted provider executable with `ptah schema inspect`,
`ptah schema plan`, and `ptah schema apply`. You need a provider built against the
experimental SDK, its expected identity and SHA-256 checksum, and a disposable
database address accepted by that provider.

The protocol is experimental. The adapter accepts common tables and columns
whose provider declares `schema.common-table-identity.v1`; other semantics need
additional protocol support. [Issue #4204](https://github.com/stokaro/ptah/issues/4204)
owns official provider extraction, installation, and the eventual stable contract.

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
changes the path or overload component count is refused. Native planning for these objects remains part of
[issue #4204](https://github.com/stokaro/ptah/issues/4204).

`schema inspect` accepts objects and facets when the provider binds every requested
schema selector to explicit namespaces. Returned tables, subjects, and coverage
regions must stay within those bounds. Unknown parent-table regions remain
unknown after capture. Invalid or widened bindings cause refusal.
A failed or canceled read returns no partial catalog. Complete inspection of one
session does not establish global absence across databases or engines; explicit
subject facts are preserved.

Providers may advertise offline conversion between desired and observed models.
The shared conversion pipeline preserves native identities, facets, and bounded
inspection knowledge. JSON inspection exports common schema data and warns about
omitted objects and facets using model names declared by the provider. It does
not preserve those payloads; use the shared model codecs to retain captured data.
Providers without omission labels cannot produce this report. Conversion and
inspection do not qualify native object planning or faithful source export.

## Resolve failures

Missing official providers include an engine ID, provider ID, repository URL,
and setup URL in the error. JSON plan and apply reports preserve these fields
in `provider_diagnostic`. Installation checks use `diagnostic`.

| Code | Action |
| --- | --- |
| `provider_missing` | Follow the engine-specific setup page and configure a trusted artifact. |
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
are refused. This source-loading support does not make native objects or
engine-specific facets available to planning and execution yet.

The execution adapter does not support managed data, database cleanup, or
isolated execution of untrusted SQL. It does not add
provider selection to `ptah-compat` or other native command groups. Project-file
URLs and saved plans do not select a provider: pass `--provider-engine` with
`--provider-config` for those workflows. The selected connection scope refuses
every unregistered URL scheme, including schemes with embedded drivers.

The official catalog recognizes [YDB](../providers/ydb/),
[DuckDB](../providers/duckdb/), and [Spanner](../providers/spanner/). These entries
state provider ownership and distribution policy. Official extraction and
release artifacts remain part of [issue #4204](https://github.com/stokaro/ptah/issues/4204).

Continue with [plan approval and target checks](../../direct/plan-and-approve/) or the
[public embedding API](../public-api/).
