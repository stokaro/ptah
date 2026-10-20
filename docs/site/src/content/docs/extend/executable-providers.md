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
  "format_version": 1,
  "providers": [
    {
      "executable": "/absolute/path/to/provider",
      "sha256": "<artifact-sha256>",
      "provider_id": "example.org/fixture",
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
the current directory. Conflicting engine names or URL aliases are errors.

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
protocol, negotiated semantics, and target metadata. Replacing the provider or
changing those facts requires a new plan. Addresses and credentials are excluded
from this binding.

Apply the reviewed plan to the disposable database:

```bash
ptah schema apply --provider-config providers.json \
  --db-url "$DATABASE_URL" --plan change.plan.json \
  --tx-mode none --auto-approve
```

The experimental adapter does not provide transactions. `--tx-mode none` permits
committed prefixes; a later failure does not undo earlier operations. Other
transaction modes are refused before execution.

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

## Resolve failures

A checksum or identity mismatch refuses startup. Fix the explicit manifest only
after verifying the intended artifact. A missing service or unsupported schema
property requires provider or protocol support; Ptah does not select an embedded
implementation after a provider failure.

A provider crash, lost response, or cancellation after dispatch can report
`"outcome": "unknown"`. Inspect the actual database before planning again. Ptah
does not automatically retry execution, and terminating a provider does not prove
rollback. A stale plan refuses application after the target changes.

This adapter does not support native objects, engine-specific facets, managed
data, database cleanup, or isolated execution of untrusted SQL. It does not add
provider selection to `ptah-compat` or other native command groups.

Continue with [plan approval and target checks](../../direct/plan-and-approve/) or the
[public embedding API](../public-api/).
