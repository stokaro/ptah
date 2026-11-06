---
title: Provider SDK reference
description: Where each package, service and feature of the provider SDK is specified, and which decision record decided it.
type: reference
audience:
  - "go-developer"
  - "integration-developer"
readerQuestion: "Where is a provider SDK service or feature specified, and which decision record decided it?"
goal: "Find the package, the runtime.Server field, the README section and the decision record of an SDK feature."
sourceOfTruth:
  - "provider-sdk/README.md"
  - "provider-sdk/runtime/server.go"
  - "provider-sdk/provider"
  - "examples/minimal-provider/go.mod"
generated: false
searchAliases:
  - "provider feature list"
  - "provider SDK packages"
  - "runtime.Server"
overlaps: []
disposition: keep
---

The tables below map each package and feature of the provider SDK to the
section of the SDK README that specifies it and to the decision record behind
it. The rules themselves live in the README. The tables describe the SDK
release that the [minimal provider example](../tutorial/) requires:

```text title="go.mod"
	github.com/stokaro/ptah/provider-sdk v0.24.0
```

The README links below open that release. The
[README in the latest development source](https://github.com/stokaro/ptah/blob/master/provider-sdk/README.md)
can describe later additions. An ADR number names a decision record in the
`docs/adr` directory of the Ptah repository, and the README section links it.

## Packages

The SDK is one Go module, `github.com/stokaro/ptah/provider-sdk`, with four
packages:

| Package | What it holds |
| --- | --- |
| [`protocol/experimental`](https://pkg.go.dev/github.com/stokaro/ptah/provider-sdk@v0.24.0/protocol/experimental) | The generated Protobuf messages and the gRPC `Provider` service. `provider.proto` in this directory is the wire contract. |
| [`provider`](https://pkg.go.dev/github.com/stokaro/ptah/provider-sdk@v0.24.0/provider) | The service interfaces, the feature names, the contract version, the request and reply validators, and the error types. |
| [`runtime`](https://pkg.go.dev/github.com/stokaro/ptah/provider-sdk@v0.24.0/runtime) | `Serve` and `Server` for a provider; `Start`, `Options` and `Client` for a host or a test; `ErrorWithReason`; the call timeouts and message bounds. |
| [`modeljson`](https://pkg.go.dev/github.com/stokaro/ptah/provider-sdk@v0.24.0/modeljson) | `Canonical`, the JSON normalization that model definitions and their digests use. |

The README sections that apply to every service:
[Contract versions](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#contract-versions),
[Trust and process lifetime](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#trust-and-process-lifetime)
(ADR 0080),
[Bootstrap specification](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#bootstrap-specification),
[Reply validation](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#reply-validation)
(ADR 0082) and
[Versions](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#versions).

## Contract, negotiation and errors

| Name | Package | Use |
| --- | --- | --- |
| `Revision`, `ContractMinor`, `Contract` | `provider` | The contract major, the minor this SDK speaks, and both as a version. |
| `MinimumContract`, `CheckContract`, `ContractError` | `provider` | A provider's minimum contract, the host's check of it, and the refusal that names both versions. |
| `TwoWayFeatures` | `provider` | The features a host lists for a provider to use them. |
| `HostFeatures`, `ContextWithHostFeatures` | `provider` | What the host listed, as a service reads it, and a context that carries a list for a test. |
| `NegotiatedDescription`, `ValidateDescription` | `provider` | The description a host with given features receives, and its check. |
| `ErrorWithReason`, `ValidateDiagnostic` | `runtime`, `provider` | A failed call with a provider-written reason, and the check of a reason's shape. |
| `ErrStartup` | `runtime` | A failed launch or bootstrap. |
| `ErrProtocol`, `ErrIncomplete` | `provider` | A malformed exchange, and a read that is not complete. |
| `ErrOutcomeUnknown`, `ErrExecution`, `ErrStatementTimeout` | `provider` | An unresolved outcome, a confirmed failure, and a stop at the statement timeout. |
| `ExecutionError`, `TransactionError`, `LockError`, `HistoryError` | `provider` | The outcome and the provider's reason of a call that did not succeed. |
| `ErrLockBusy`, `ErrLockLost` | `provider` | A lock another session holds, and a lock that no longer establishes ownership. |
| `CallTimeout`, `ProvisionTimeout`, `MaxMessageBytes`, `LargeMessageBytes` | `runtime` | The bound of a fixed-size call, of a dev database call, and of one message without and with `transport.large-messages`. |

## Services and features

Each table lists a feature, the `runtime.Server` field of the service it
selects, the README section that specifies it, and its decision record. A
feature with no service changes messages that already exist.
`runtime.Server.Validate` refuses a service without its feature and a feature
without its service, and `provider.TwoWayFeatures()` returns the features a
host must also list.

### Services Ptah requires

Ptah starts a provider only when it offers these services, and every provider has the session service.

| Feature | Service | README section | ADR |
| --- | --- | --- | --- |
| — | `Sessions` | [Service contract](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#service-contract) | — |
| `schema.inspect` | `Inspector` | [Observed model coverage](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#observed-model-coverage) | — |
| `schema.render` | `Renderer` | [Service contract](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#service-contract) | — |
| `units.execute` | `Executor` | [Execution uncertainty](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#execution-uncertainty) | — |
| `sql.analyze` | `SQLAnalyzer` | [Validation](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#validation) | — |

### Sessions, execution and locks

Transactions, revision storage, the migration log and locks back the migration commands and `ptah schema apply`.

| Feature | Service | README section | ADR |
| --- | --- | --- | --- |
| `sessions.transactions` | `Transactions` | [Session transactions](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#session-transactions) | — |
| `sessions.history` | `History` | [Revision storage](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#revision-storage) | — |
| `sessions.migration-metadata` | `Metadata` | [Migration logs and tags](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#migration-logs-and-tags) | — |
| `sessions.locks` | `Locks` | [Session-owned locks](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#session-owned-locks) | — |
| `sessions.statements` | `Statements` | [Operator statements](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#operator-statements) | 0038 |
| `sessions.typed-arguments` | — | [Typed arguments](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#typed-arguments) | 0058 |
| `units.statement-timeout` | — | [Execution uncertainty](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#execution-uncertainty) | — |
| `units.migration-split` | `MigrationSplits` | [Migration splits](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#migration-splits) | 0055 |
| `units.sensitive-inputs` | `ExecutionInputs` | [Sensitive execution inputs](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#sensitive-execution-inputs) | 0043 |
| `errors.reasons` | — | [Error reasons](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#error-reasons) | 0059, 0061 |

### Tables and the common model

These features decide how an engine's tables, columns, indexes and common objects reach Ptah's comparison.

| Feature | Service | README section | ADR |
| --- | --- | --- | --- |
| `schema.common-table-identity.v1` | — | [Validation](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#validation) | — |
| `schema.prepare-tables` | `TablePreparer` | [Validation](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#validation) | — |
| `schema.project-table-creations` | `TableCreations` | [Validation](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#validation) | — |
| `schema.columns` | `Columns` | [Common column semantics](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#common-column-semantics) | 0045 |
| `schema.observed-columns` | — | [Complete column observations](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#complete-column-observations) | 0048 |
| `schema.observed-primary-keys` | — | [Ordered primary-key observations](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#ordered-primary-key-observations) | 0051 |
| `planning.primary-key-changes` | — | [Primary key changes](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#primary-key-changes) | 0078 |
| `schema.observed-indexes` | — | [Common index observations](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#common-index-observations) | 0052 |
| `schema.observed-index-facets` | — | [Index facets on inspected indexes](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#index-facets-on-inspected-indexes) | 0068 |
| `schema.index-methods` | — | [Prepared index kinds](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#prepared-index-kinds) | 0057 |
| `planning.index-renames` | — | [Index renames](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#index-renames) | 0060 |
| `schema.unread-tables` | — | [Unread tables](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#unread-tables) | 0063 |
| `schema.subtree-coverage` | — | [Subtree coverage](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#subtree-coverage) | 0064 |
| `schema.database-kinds` | — | [Database kinds](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#database-kinds) | 0065 |
| `schema.common-objects` | `CommonObjects` | [Common objects](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#common-objects) | 0033 |
| `schema.common-view-facets` | — | [View facets and replacement observations](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#view-facets-and-replacement-observations) | 0047 |
| `schema.common-access` | — | [Independent access observations](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#independent-access-observations) | 0042 |
| `schema.role-scope-reasons` | — | [Role scope reasons](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#role-scope-reasons) | 0076 |
| `schema.password-inputs` | — | [Password inputs](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#password-inputs) | 0071 |

### Values, models and planning

These features carry the provider's own models and its part of a plan.

| Feature | Service | README section | ADR |
| --- | --- | --- | --- |
| `values.normalize` | `ValueNormalizer` | [Owner-defined value normalization](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#owner-defined-value-normalization) | 0022 |
| `values.convert` | `ValueConverter` | [Desired and observed conversion](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#desired-and-observed-conversion) | — |
| `models.compare` | `ModelComparer` | [Contextual model comparison](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#contextual-model-comparison) | — |
| `models.compare-counts` | — | [Comparison counts](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#comparison-counts) | 0056 |
| `models.plan` | `FeaturePlanner` | [Contextual feature planning](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#contextual-feature-planning) | 0024 |
| `models.reverse` | `ChangeReverser` | [Change reversal](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#change-reversal) | 0062 |
| `models.report-counts` | `ValueReporter` | [Report counts](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#report-counts) | 0067 |
| `models.predecessors` | — | [Model predecessors](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#model-predecessors) | 0075, 0084 |
| `models.own-predecessors` | — | [Own model predecessors](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#own-model-predecessors) | 0084 |
| `planning.policy`, `planning.statement-style` | — | [Planning policy](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#planning-policy) | 0035, 0050 |
| `planning.lifecycle` | — | [Lifecycle placement](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#lifecycle-placement) | 0044 |
| `planning.object-context` | — | [Native object assessment](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#native-object-assessment) | 0049 |
| `planning.context-inventory` | — | [Planning context of a filtered read](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#planning-context-of-a-filtered-read) | 0054 |
| `planning.table-outlines` | — | [Table outlines](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#table-outlines) | 0081 |
| `render.text-units` | — | [Text units](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#text-units) | 0034 |
| `schema.declarations` | `Declarations` | [Authored declaration planning](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#authored-declaration-planning) | 0041 |
| `schema.validate` | `SchemaValidator` | [Offline schema validation](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#offline-schema-validation) | — |

### Source syntax

These features let a provider read and write its part of a YAML, HCL, Go or SQL source.

| Feature | Service | README section | ADR |
| --- | --- | --- | --- |
| `source.decode` | `SourceDecoder` | [Captured extension sources](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#captured-extension-sources) | 0025 |
| `source.encode` | `SourceEncoder` | [Source encoding](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#source-encoding) | 0037, 0040 |
| `source.encode-groups`, `source.limits` | — | [Source encoding](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#source-encoding) | 0040 |
| `source.entry-keys`, `source.hcl` | — | [YAML entry keys and HCL claims](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#yaml-entry-keys-and-hcl-claims) | 0036 |
| `source.hcl-platform` | — | [HCL platform groups](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#hcl-platform-groups) | 0072 |
| `source.index-kinds` | — | [Index kinds](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#index-kinds) | 0074 |
| `source.go-placements` | — | [Go placements](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#go-placements) | 0077 |
| `schema.read-sql` | `SQLSchemaReader` | [SQL schema documents](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#sql-schema-documents) | 0027 |
| `schema.sql-facets` | — | [Facets of a SQL schema reading](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#facets-of-a-sql-schema-reading) | 0066 |
| `sql.scan` | `SQLScanner` | [SQL scanning](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#sql-scanning) | 0069 |
| `scope.selections` | — | [Scope selections](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#scope-selections) | 0039 |

### Database context and dev databases

These features cover addresses, the databases Ptah rehearses and replays on, and the facts an offline command plans with.

| Feature | Service | README section | ADR |
| --- | --- | --- | --- |
| `database.context` | `DatabaseContexts` | [Database interpretation context](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#database-interpretation-context) | 0046 |
| `values.context` | — | [Database interpretation context](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#database-interpretation-context) | 0046 |
| `database.dev` | `DevProvisioner` | [Dev database cleanup and the replay guard](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#dev-database-cleanup-and-the-replay-guard) | — |
| `database.dev-realms` | — | [Dev database cleanup and the replay guard](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#dev-database-cleanup-and-the-replay-guard) | — |
| `database.dev-cleanup` | `DevCleaner` | [Dev database cleanup and the replay guard](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#dev-database-cleanup-and-the-replay-guard) | — |
| `database.dev-replay-guard` | `ReplayGuard` | [Dev database cleanup and the replay guard](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#dev-database-cleanup-and-the-replay-guard) | — |
| `engine.default-capabilities` | — | [Offline capabilities](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#offline-capabilities) | 0070 |
| `query.dialect` | — | [Query-builder spelling](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#query-builder-spelling) | — |

### Data, lint and embeddings

These features serve declared rows and seeds, migration lint, and `ptah inference`.

| Feature | Service | README section | ADR |
| --- | --- | --- | --- |
| `data.type-rows` | `RowTyper` | [Rows, row writes and seeds](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#rows-row-writes-and-seeds) | 0028 |
| `data.read-rows` | `RowReader` | [Rows, row writes and seeds](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#rows-row-writes-and-seeds) | 0028 |
| `data.render-writes` | `RowWriter` | [Rows, row writes and seeds](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#rows-row-writes-and-seeds) | 0028 |
| `data.seeds` | `Seeds` | [Rows, row writes and seeds](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#rows-row-writes-and-seeds) | 0028 |
| `lint.migrations` | `Linter` | [Service contract](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#service-contract) | — |
| `lint.statement-facts` | — | [Statement facts for SQL lint](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#statement-facts-for-sql-lint) | — |
| `lint.transaction-owner` | — | [Transaction owner of a lint check](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#transaction-owner-of-a-lint-check) | 0073 |
| `embeddings.store` | `EmbeddingStore` | [Embedding store](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#embedding-store) | 0032 |

### Transport

These features keep a large schema within one request. The SDK applies them on both sides, and no service sees the difference.

| Feature | Service | README section | ADR |
| --- | --- | --- | --- |
| `models.references` | — | [Model references and large messages](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#model-references-and-large-messages) | 0079 |
| `transport.large-messages` | — | [Model references and large messages](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#model-references-and-large-messages) | 0079 |
| `transport.shared-coverage` | — | [Shared coverage](https://github.com/stokaro/ptah/blob/provider-sdk/v0.24.0/provider-sdk/README.md#shared-coverage) | 0083 |

## Related pages

- [How a provider works](../architecture/) and
  [Engines, models and planning](../models-and-planning/) explain how the
  services fit together.
- [Test a provider](../testing/#check-each-service-with-the-sdks-validators)
  lists the validator of each core service.
