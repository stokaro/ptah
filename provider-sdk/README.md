# Experimental provider SDK

This module is for developers implementing Ptah subprocess providers. It defines
explicit protocol messages, service interfaces, and authenticated local transport.
Its Go module path is `github.com/stokaro/ptah/provider-sdk`.

The contract is experimental. It has a major and a minor version: a host and a
provider of different majors are refused before a session opens, and so is a
host older than the minimum a provider declares. Within a major, additions are
negotiated; see [Contract versions](#contract-versions).
[Issue #4204](https://github.com/stokaro/ptah/issues/4204) owns integration
with the application and extraction of the official providers.

## Module boundary

The SDK imports no Ptah application package or database driver. Providers own
their drivers and may build with a different dependency graph from the host.
The independent fixture module under `testdata/fixture` uses SQLite through its
own driver and a pinned published SDK version. It has no local `replace`. The
host's process acceptance builds it with `GOWORK=off`, from the pinned version.
The SDK's own process tests build it against the in-tree SDK through a workspace
file in a temporary directory, because the client under test is the in-tree one
and the tests exercise what the in-tree SDK adds. The mixed-version tests build
its contract-25.0 programs (`provider`, `lint` and `contract-host`) against
`provider-sdk/v0.2.0` instead, so that a host and a provider of different
contract minors meet. Those programs and the `backend` package use nothing a
later minor added.

SDK-only proof programs under `integration/testdata` build in
an isolated temporary module using the same workspace. They can exercise an
unreleased contract without changing the independent fixture's published pin.

`protocol/experimental` contains generated Protobuf messages and the gRPC service.
`provider` defines service contracts and validates complete results.
`runtime` starts and serves authenticated subprocesses. The runtime does not
compare schemas, order migration dependencies, or maintain migration history.

## Versions

The SDK is its own Go module, so it has its own tags: `provider-sdk/vX.Y.Z`.
The host, the fixture and every provider repository require a tag in `go.mod`,
not a pseudo-version. The tag name has the module path as its prefix, which is
why release automation that matches `v*` does not see it.

A maintainer tags the SDK when a change to the SDK has to reach a module that
consumes it, and the requirement moves in the pull request that uses the change.
A release that changes the protocol or the Go API raises the tag's minor number,
and a Go API change may break consumers. The tag says nothing about
compatibility between a host and a provider: the contract version below does.
Local work across the host and the SDK does not need a release: use an
uncommitted `go.work` file, and build with `GOWORK=off` before you push.

## Contract versions

The contract between a host and a provider has a version, major.minor.
`provider.Revision` is the major, sent as `revision` in both directions of
`Describe`. `provider.ContractMinor` is the minor this SDK speaks. This SDK
speaks contract 25.22. `provider-sdk/v0.19.0` speaks 25.21.
`provider-sdk/v0.18.0` speaks 25.19.
`provider-sdk/v0.17.0` speaks 25.18.
`provider-sdk/v0.16.0` speaks 25.17.
`provider-sdk/v0.15.0` speaks 25.16.
`provider-sdk/v0.14.0` speaks 25.15.
`provider-sdk/v0.13.0` speaks 25.14.
`provider-sdk/v0.12.0` speaks 25.12.
`provider-sdk/v0.11.0` speaks 25.9.
`provider-sdk/v0.10.0` speaks 25.8.
`provider-sdk/v0.9.0` speaks 25.7.
`provider-sdk/v0.8.0` speaks 25.6.
`provider-sdk/v0.7.0` speaks 25.5.
`provider-sdk/v0.6.0` speaks 25.4. `provider-sdk/v0.5.0`
speaks contract 25.3, `provider-sdk/v0.4.0` speaks 25.2, `provider-sdk/v0.3.0`
speaks 25.1, and `provider-sdk/v0.2.0` speaks 25.0.

**What changes which number.** The minor counts compatible additions within a
major. The major changes only with an incompatible change, and the minor then
returns to 0.

- A compatible change adds something an older peer can ignore: a new RPC,
  message, field, enum value or feature. It raises the minor by one. Contract
  25.1 adds, for example, the RPC `ReadRows`, the field
  `ExecuteRequest.statement_timeout_nanoseconds`, the enum value
  `LINT_FACT_STATEMENT_KIND` and the feature `database.dev-cleanup`. Contract
  25.2 adds the [embedding store](#embedding-store). Contract 25.3 adds the
  [common-object wire](#common-objects), [text units](#text-units), the
  [planning policy](#planning-policy), [YAML entry keys and HCL
  claims](#yaml-entry-keys-and-hcl-claims) and
  [source encoding](#source-encoding). Contract 25.4 adds
  [operator statements](#operator-statements) and
  [scope selections](#scope-selections). Contract 25.5 adds
  [grouped source encoding and Go coverage headers](#source-encoding),
  [authored declaration planning](#authored-declaration-planning),
  [independent access observations](#independent-access-observations),
  [lifecycle placement](#lifecycle-placement) and
  [sensitive execution inputs](#sensitive-execution-inputs). Contract 25.6 adds
  [common column semantics](#common-column-semantics). Contract 25.7 adds
  [database interpretation context](#database-interpretation-context),
  [view facets and replacement observations](#view-facets-and-replacement-observations),
  [complete column observations](#complete-column-observations), and
  [native object assessment](#native-object-assessment). Contract 25.8 adds
  [statement style for feature owners](#planning-policy). Contract 25.9 adds
  [ordered primary-key observations](#ordered-primary-key-observations).
  Contract 25.10 adds [common index observations](#common-index-observations).
  Contract 25.11 adds [the planning context of a filtered read](#planning-context-of-a-filtered-read).
  Contract 25.12 adds [migration splits](#migration-splits).
  Contract 25.13 adds [comparison counts](#comparison-counts).
  Contract 25.14 adds [prepared index kinds](#prepared-index-kinds).
  Contract 25.15 adds [typed statement arguments](#typed-arguments).
  Contract 25.16 adds [error reasons](#error-reasons).
  Contract 25.17 adds [index renames](#index-renames).
  Contract 25.18 adds [the reasons of execution and transaction results](#error-reasons).
  Contract 25.19 adds [change reversal](#change-reversal).
  Contract 25.20 adds [unread tables](#unread-tables).
  Contract 25.21 adds [database kinds](#database-kinds),
  [the facets of a SQL schema reading](#facets-of-a-sql-schema-reading),
  [report counts](#report-counts) and
  [index facets on inspected indexes](#index-facets-on-inspected-indexes).
  Contract 25.22 adds [subtree coverage](#subtree-coverage),
  [SQL scanning](#sql-scanning), [offline capabilities](#offline-capabilities),
  [password inputs](#password-inputs) and
  [HCL platform groups](#hcl-platform-groups).
- An incompatible change alters what an existing peer relies on: a field
  removed, renumbered, retyped or given another meaning, a field a request
  must now carry, a validator that refuses what an older peer sends, or
  another outcome for the same reply. It raises the major. Making
  `Execution.applied_count` count something else would be one; so would
  refusing a reply that contract 25.0 accepts.
- A release that changes only Go API keeps the contract version.

**Additions are negotiated in both directions.** A minor adds vocabulary; it
does not oblige a peer to use it.

- A provider lists each service and behavior it offers in
  `Description.features`, and each engine lists its own. A host calls a new
  RPC only when the provider offers its feature.
- A host lists the features it acts on in `DescribeRequest.host_features`. An
  addition to an existing message, such as a new enum value, a new reply field,
  or a request field the provider would have to honor, is used only when both
  sides list its feature. `provider.TwoWayFeatures()` returns those features:
  `units.statement-timeout`, `lint.statement-facts`, `database.dev-realms`,
  `query.dialect`, `embeddings.store`, `schema.common-objects`,
  `render.text-units`, `planning.policy`, `planning.statement-style`,
  `source.entry-keys`, `source.hcl`,
  `scope.selections`, `source.encode-groups`, `source.limits`,
  `schema.declarations`, `schema.common-access`, `planning.lifecycle`,
  `units.sensitive-inputs`, `schema.columns`, `database.context`,
  `values.context`, `schema.common-view-facets`, `schema.observed-columns`,
  `schema.observed-primary-keys`, `schema.observed-indexes`,
  `planning.object-context`, `planning.context-inventory`,
  `units.migration-split`, `models.compare-counts`, `schema.index-methods`,
  `sessions.typed-arguments`, `errors.reasons`, `planning.index-renames`,
  `models.reverse`, `schema.unread-tables`, `schema.database-kinds`,
  `schema.sql-facets`, `models.report-counts`, `schema.observed-index-facets`,
  `schema.subtree-coverage`, `schema.password-inputs`, `source.hcl-platform`
  and `engine.default-capabilities`. The embedding store's calls,
  `PrepareCommonObjects`, `ReverseChanges` and `ReportValues` are new, and each
  comes with an addition to `Engine`, so a host lists the feature to receive
  any of them. `SplitMigration` is new too, and comes with the abort flags of
  `Execution` and `TransactionResult`, so a host lists `units.migration-split`
  to call it.
- Both transport boundaries validate with the description negotiated this way,
  `provider.NegotiatedDescription`, so neither side sends an addition the other
  did not ask for. A service reads what the host listed with
  `provider.HostFeatures(ctx)`; the SDK server refuses a reply that uses an
  addition the host did not list.
- A peer that sends nothing new, such as one built from an SDK of minor 0,
  exchanges exactly the messages of contract 25.0.

**How a provider chooses its minimum.** `Description.min_contract` is the oldest
host contract the provider works with. Leave it unset, which means 25.0, when
the provider works with any host of its major: what it uses beyond that sits
behind a feature, and a host that does not list the feature does not get it.
Raise it only when the provider cannot work at all with an older host, such as
one that serves no useful operation unless the host calls an RPC of a later
minor. The SDK refuses a minimum of another major, and one above the minor of
the SDK the provider is built with.

**How the host decides.** A host announces its version in
`DescribeRequest.host_contract`. It starts a provider when the majors are equal
and its minor is at least the provider's minimum. Otherwise it refuses with a
`provider.ContractError` that names both versions, such as `provider requires
contract 25.2 or a later minor of it; this host speaks 25.1`. A host that
announces no version speaks 25.0 and cannot compare versions, so the SDK server
refuses it in `Describe` when the provider's minimum is higher.

**Release manifests.** A plugin release states the same minimum in its
`ptah-provider.json` manifest as `min_contract: {major, minor}`
([stokaro/ptah-ydb-plugin#37](https://github.com/stokaro/ptah-ydb-plugin/issues/37)),
so a host can choose a compatible release before it downloads or starts one.

**Features and minors.** A minor introduces messages and the feature names that
switch them on; a feature decides whether a given host and provider use them.
A provider built from a newer SDK keeps working with an older host of the same
major, and an older provider with a newer host, for everything both offer. The
SDK's process tests check both: a host of contract 25.0 against a provider of
this SDK, and a host of this SDK against a provider of contract 25.0.

## Service contract

| Operation | Required result |
| --- | --- |
| `Describe` | The contract major and the provider's minimum minor, provider identity, semantic identity, engines, identifier comparison rules, and advertised features, negotiated with the host's version and features. |
| `ResolveDatabaseContext` | Pure interpretation of an address into selected and authored database roots, without connecting, provisioning, or resolving credentials. |
| `Open` | A session handle for the requested engine, identifier rules, and established server capabilities. Credentials belong in this request, never in process arguments. |
| `Close` | Release one session. Repeated closes succeed. |
| `Inspect` | A complete snapshot with explicit dispositions for observed fields. Incomplete state is an error, never an empty database. |
| `Render` | One executable unit for every input change, in input order, or, in the `TEXT` form, a refusal of a change without a text form. A refused batch exposes no executable prefix. |
| `AnalyzeSQL` | Complete owner-defined statement boundaries, tokens, and explicit safety dispositions. Unsupported syntax refuses the whole batch. |
| `DecodeSource` | Complete offline interpretation of captured extension fragments, frozen model scope, and original source traces. |
| `EncodeSource` | Offline: every value written once by claimed syntax parts, with no position, or reported as one loss. |
| `ReadSQLSchema` | A complete offline reading of every SQL schema document of an engine that declares the `DOCUMENT` mode: declared objects, per-document declarations, coverage and positioned diagnostics, or an error and no schema. |
| `ConvertValues` | An offline conversion between desired and observed models with preserved identity and a complete normalized result. |
| `CompareModels` | A complete offline comparison of captured objects or facets, independent coverage sources, common context, and typed changes. |
| `PlanFeatures` | Complete offline graph contributions and ordered change and parent receipts, or diagnostics with no successful output. |
| `PlanDeclarations` | Complete offline creation contributions and ordered authored-object receipts, without observed state. |
| `DescribeExecutionInputs` | Complete offline discovery of the exact unit batch's sensitive input references, without reading values. |
| `PrepareTables` | Complete prepared key membership, declared facet adjustments and, under `schema.index-methods`, declared index kinds from captured table state. |
| `PrepareCommonObjects` | Offline: the declared views, sequences, roles and grants in the engine's spelling, each entry keeping its identity. |
| `ProjectTableCreations` | Complete predicted CREATE effects within the host's eligible facet owner inventory. |
| `ValidateSchema` | A completed offline schema check with typed invalid, unsupported, or omitted-declaration diagnostics. |
| `SplitLintStatements` | Complete statement boundaries for migration sources under the engine's own lexing, with the facts the split judges. |
| `AnnotateLintStatements` | One annotation per statement a dialect-blind scanner split. The owner declines a statement its lexing would split differently. |
| `CheckLintMigrations` | A complete set of findings for the enabled rule codes over every file of a run, or an error and no finding. |
| `ProvisionDevDatabase` | A lease and an address for a throwaway database or for a realm in a database the operator names, or a typed refusal (unsupported spec, unavailable runtime). The owner removes what it created. |
| `ReleaseDevDatabase` | Removal of what a lease names. A lease that is already gone succeeds. |
| `ListDevObjects` | Every object a dev database cleanup would remove or stop at, or an error and no list. |
| `CleanDevDatabase` | An emptied dev database scope, a refusal that removed nothing and names what stopped it, or an explicit unknown outcome. |
| `GuardReplay` | One verdict per replay statement: accepted, or refused with what cannot be confined to the scope. |
| `TypeRows` | The engine type of each column and the declared and live rows in one canonical form, offline. |
| `ReadRows` | One bounded page of a table in key order, with the existence of the table, or an error and no rows. |
| `RenderRowWrites` | One executable unit for every typed row write, in input order, or a typed refusal and no unit. |
| `ListSeedTables` | The tables of one schema, for the host's protected-table check. |
| `ApplySeed` | A seed file and its ledger entry committed together, a key conflict, a refusal that ran nothing, or an explicit unknown outcome. |
| `RecordSeed` | A ledger entry written without running the file. |
| `ReadSeedLedger` | The complete ledger, distinguishing a missing ledger from an empty one. |
| `PrepareEmbeddingStore` | A created or verified run-state store, or a refusal of a layout the owner does not recognize. |
| `ReadEmbeddingState` | One consistent snapshot of a scope of run-state records, in key order, with a token over them, or an error and no record. |
| `MutateEmbeddingState` | The records a host decision changed, written whole when the scope still has the token it was read with, or a confirmed conflict that wrote nothing. |
| `AppendEmbeddingEvent` | One audit event of an existing run, with the next sequence of that run. |
| `ReadEmbeddingEvents` | One bounded page of a run's events in sequence order. |
| `DescribeEmbeddingTarget` | Offline: the column type, table ownership, index and extensions a generation needs on an engine, or a refusal. |
| `ResolveEmbeddingRelations` | The relation each schema and table spelling names on the session's server. |
| `MeasureEmbedding` | What a plan reports: existence, a source count, vector capabilities, and refusals. |
| `PrepareEmbeddingTarget` | A generation's storage created or verified, idempotently. |
| `BuildEmbeddingIndex` | The declared index built, rebuilt or found valid, or `NOT_DECLARED`. |
| `ReadEmbeddingStructure` | A generation's column and index as the engine reports them, the search settings, and on request its row count. |
| `DropEmbeddingGeneration` | A retired generation's index and columns or table removed, or a refusal that removed nothing. |
| `ScanEmbeddingSource` | One keyset page of the source in key order, with the cursor to resume after. |
| `ReadEmbeddingSourceRows` | The rows still present among a set of keys. |
| `ReadEmbeddingBatchState` | A run and what the target holds at a batch's keys, with a token over both. |
| `CommitEmbeddingBatch` | A batch's sets and its checkpointed run committed together when the token holds, or a confirmed conflict that wrote nothing. |
| `ReadEmbeddingVerification` | One bounded page of the verification walk, never splitting a key's rows. |
| `SearchEmbeddings` | The keys nearest to one vector through the index and by exhaustive scan, or a failed search with a reason. |
| `NormalizeValues` | A complete offline batch of owner-validated values, structured normalized subjects, and versioned canonical projections. |
| `BeginTransaction` | A caller-selected handle bound to its live session. Failed opens reserve the handle against another begin. |
| `FinishTransaction` | A confirmed commit, rollback, fenced absence, or an explicit unknown outcome. |
| `ReadHistory` | A complete bounded snapshot of exact revision records in the selected session and transaction. |
| `PrepareHistory` | Verified or newly created history storage, without upgrading an earlier layout. |
| `MutateHistory` | An atomic comparison and write of one exact revision, with a confirmed or unknown outcome. |
| `TryLock` | One nonblocking acquisition attempt bound to its session, name, and single-use handle. |
| `CheckLock` | Current ownership evidence, confirmed loss, or uncertainty for that exact handle. |
| `ReleaseLock` | Confirmed release or fenced absence, without retrying an uncertain operation. |
| `Execute` | A terminal outcome and the number of consecutive units confirmed applied. Request IDs do not enable automatic retries. |
| `RunStatement` | What one statement the operator wrote did: its columns and first page of rows, a row count only when the engine measured one, the server's message for a statement it refused, a refusal that ran nothing, or an explicit unknown outcome. |
| `ReadStatementRows` | The next bounded page of an open result, or an error and no rows. |
| `CloseStatement` | Release of an open result. A result already released succeeds. |
| `SplitMigration` | Offline: the units one migration body runs as, in run order, each with the text to execute, its own source and a placement, or a refusal of the whole body; and a read-only verdict for each check assertion. |
| `ReportValues` | Offline: one count for each metric of a value's model report, for every value of the request, in order. |
| `ScanSQL` | Offline: the tokens of one text of the engine's SQL, in order, covering the text with no gap and no overlap. |

Session capabilities preserve presence: an explicit `false` is an established
answer, while an absent key remains unknown. The SDK validates names and the host
validates its capability vocabulary and dependencies. Invalid session replies
return no usable handle; both transport boundaries attempt to close a returned
handle before refusing the reply.

Common records represent tables and columns explicitly, and, under
`schema.common-objects`, views, sequences, roles, grants and comments. Native
records use a versioned provider-owned envelope. Structured references keep
database, schema, path, kind, name, and overload components separate. The
transport does not fold names or concatenate components into identity strings.

The fixture accepts a narrow table grammar. It refuses views, triggers, indexes,
and table definitions it cannot reconstruct exactly. Its coverage does not
establish support for any official engine.

## Execution uncertainty

`APPLIED` confirms the whole batch. `NOT_STARTED` and `NOT_APPLIED` confirm no
applied units. `PARTIAL` confirms a proper prefix. `UNKNOWN` leaves the final
effect unresolved and may carry a confirmed prefix.

A lost response, cancellation after dispatch, malformed execution reply, or
provider crash reports `ErrOutcomeUnknown`. The SDK does not reconnect or replay
the request. The caller must inspect actual state before deciding how to proceed.
Cancellation before dispatch sends nothing. Killing a provider does not prove
that a database transaction rolled back.

`ExecuteRequest.statement_timeout_nanoseconds` sets a limit for each unit. The
provider stops a unit that runs longer and sets `statement_timed_out` on the
reply, with `NOT_APPLIED`, `PARTIAL` or `UNKNOWN`: `UNKNOWN` when the server may
still run the unit after the provider stopped waiting. The client widens the
call's bound by the timeout of each unit, so the stop arrives as a confirmed
outcome rather than a lost reply, and returns an error that matches both
`ErrStatementTimeout` and the outcome's own sentinel. A caller can therefore
tell its own limit from a canceled call. The flag on a request without a
timeout, or on an outcome that confirms completion, is a malformed reply. A
timeout travels only when both sides list `units.statement-timeout`: otherwise
the client sends none and the server refuses one.

## Session transactions

`sessions.transactions` advertises the optional transaction service and requires
`units.execute`. Each supporting engine also declares `sessions.transactions`.
A service declaration does not establish transactional DDL; the opened session
must report that capability separately.

The caller chooses a transaction ID before `BeginTransaction`. The provider
reserves it before opening a database transaction and binds it to that session.
The request explicitly selects `DEFAULT` or `SERIALIZABLE` isolation. The reply
must echo that level. `DEFAULT` selects the engine default; `SERIALIZABLE`
protects range reads as well as individual rows. The provider must refuse a
level it cannot establish. Missing, unknown, and substituted levels are refused;
a malformed reply triggers cleanup of the requested handle.
`Execute.transaction_id` selects the handle; an empty ID selects unwrapped
execution. `APPLIED` inside a transaction confirms execution, not commit.

`FinishTransaction` accepts commit or rollback and reports `COMMITTED`,
`ROLLED_BACK`, `NOT_STARTED`, or `UNKNOWN`. A rollback of an unused ID fences a
late begin. The provider serializes begin, execution, and finish, and retains
terminal results until session close. Repeated finish requests return the stored
state without repeating a database operation. Closing the session rolls back
unfinished transactions and prevents later begin or execution.

A malformed or lost begin reply triggers bounded rollback of the exact requested
handle. Cleanup failures remain visible. A lost finish reply remains unknown;
the SDK never retries the write. Confirmed opposite states return both the result
and `ErrExecution`. Invalid replies expose no result and wrap `ErrOutcomeUnknown`.
Providers must keep transaction lifetime independent of the begin RPC context.

## Revision storage

`sessions.history` advertises the optional `History` service globally and for
its supporting engines. It provides storage primitives; migration ordering,
checksums, dirty-state recovery, and repair decisions belong to the host.
History support alone promises no distributed lock.

Every request names a session, schema, table, and native or Atlas representation.
Identifiers are separate components, never SQL fragments. Reads and mutations
can name an existing transaction. The owner must use the same transaction as
`Execute`, including visibility of uncommitted revision changes. Closing the
session or restarting the process invalidates those handles.

A host can use the transaction service only for revision data without requiring
transactional DDL. The host's history handle exposes no SQL executor. Preparing
storage precedes the transaction; nested transactions and dry-run writes are
refused. Migration locking remains the caller's responsibility. Serializable isolation requires an explicit `SERIALIZABLE` request;
`DEFAULT` makes no such promise.

`ReadHistory` returns a consistent complete snapshot, distinguishing a missing
store from an empty one. The caller sets a positive limit of at most 10,000
revisions. Exceeding that limit or the transport message bound is an error;
no partial history is returned. Exact identities, direction, progress, source
hashes, diagnostics, timestamps, and Atlas fields survive transport. Native
records carry explicit state and direction. Atlas records leave those fields
unspecified: the host derives them from progress, errors, and operator identity.
The SDK refuses provider-supplied derived state in the Atlas representation.

`PrepareHistory` runs outside transactions. It validates storage ownership and
layout and creates a missing store. It refuses incompatible existing layouts
and does not upgrade them. `MutateHistory` atomically compares the entire
expected record before replacing or deleting it. An absent expected record
means insert only if absent. A conflict changes nothing. Changing a record's
identity is refused.

`APPLIED` within a transaction confirms execution, not commit. A lost or
malformed acknowledgment leaves the outcome unknown. Neither the transport
nor a request ID permits automatic replay. The host must stop and reconcile
history and live state before another attempt.

The independent SQLite fixture exercises these storage primitives. Native
migration commands connect history to the same selected runtime as SQL safety
analysis and execution. The host and fixture require a published SDK release
without local replacements. Official-engine qualification remains required under #4204.

## Session-owned locks

`sessions.locks` advertises the optional `Locks` service globally and for each
supporting engine. The provider owns the database exclusion mechanism. The host
owns the wait policy, cancellation, and the decision to run under the lock.
The host's wait timeout ends the wait between attempts and never cancels an
attempt in flight, so a `BUSY` answer that arrives after it is reported as the
timeout. An attempt ends with its answer, the call timeout or the run's
cancellation.

Every attempt names a session, exact lock name, and single-use lock ID. Every RPC
has a separate request ID. Receipts must echo the full scope and request identity.
`TryLock` makes one nonblocking attempt. A confirmed `BUSY` permits a new attempt
with a new lock ID; a lost reply does not. `ReleaseLock` of an unused ID fences a
late acquisition. Closing the session releases its locks and fences later calls.

`CheckLock` asks the provider to establish current ownership, including the
freshness of any lease. An earlier `ACQUIRED` receipt is not proof that a later
statement remains protected. Providers must check the locks guarding a session
before execution, history writes, and transaction commits. Lost or uncertain
ownership fences further work on that session; cleanup remains available.
A lock loss during execution does not prove that the database rolled back.

Both transport boundaries attempt bounded release after an uncertain acquisition.
Cleanup names the original requested handle, even if the reply substitutes one.
It runs after caller cancellation and preserves the original uncertainty. Failed
cleanup remains an error. The transport never retries acquisition or release.

SDK tests cover exact scope binding, operation-specific outcomes, cancellation,
malformed receipts, cleanup failure, and independent request and response storage.
Independent SQLite processes also verify exclusion by database and lock name,
release on session close and process death, single-use handles, and cleanup after
a lost acquisition reply. The fixture holds a private sidecar write transaction;
it requires a canonical database pathname without hard-link aliases. Ownership
checks read a row that exists only in the uncommitted transaction. A lost lock
fences schema execution, history writes, and commits, while rollback remains
available. Process-death checks retain the schema write committed before death.

The host uses these locks through its shared migration lock guard. It retries
only confirmed busy results, cancels guarded work after ownership loss, and
retains uncertainty through release. The common migrator process checks keep
locking enabled across its transaction modes. Provider lock previews are refused
because this protocol does not establish acquisition without writes.

Native migration commands use this service for default locking and explicit
migration lock timeouts. The fixture does not qualify YDB coordination leases.
Published-module acceptance, read-only lock previews, and official-engine checks
remain required under #4204.

## Error reasons

[ADR 0059](../docs/adr/0059-a-provider-says-why-a-call-failed-in-text-it-wrote.md)
decides this addition. A failed call reaches the host as its gRPC code, and
the host's error reads `provider inspect failed (FailedPrecondition)`. The
status message is never shown: it is whatever the provider's error chain
wrote, and a driver's error can quote a connection address, a credential or a
statement with its values.

`errors.reasons` lets a provider say why. It is two-way and the provider's
alone. A failed call's status then carries one `ProviderDiagnostic` in its
details, and the host prints its reason after the code:

```text
provider inspect failed (FailedPrecondition): the streaming query capability was not established
```

A lock receipt whose outcome is not the one its operation asked for carries
one in `LockResult.diagnostic`, and `provider.LockError` reads
`provider lock outcome is BUSY: <reason>`. It never accompanies `ACQUIRED`
from `TryLock` or `CheckLock`, or `RELEASED` or `NOT_ACQUIRED` from
`ReleaseLock`.

From contract 25.18
([ADR 0061](../docs/adr/0061-a-failed-execution-says-why-in-text-the-provider-wrote.md)),
an execution result other than `APPLIED` carries one in
`Execution.diagnostic`, and a transaction result whose outcome is not the one
its action asked for carries one in `TransactionResult.diagnostic`: never
`COMMITTED` in answer to a commit, or `ROLLED_BACK` or `NOT_STARTED` in answer
to a rollback. `provider.ExecutionError` and `provider.TransactionError` gain
`Reason`, and read
`provider execution: NOT_APPLIED (0 units confirmed applied): <reason>` and
`provider transaction outcome is ROLLED_BACK: <reason>`. The `message` of
either result keeps its meaning and is never shown.

Listing the feature is the provider's promise about the text. The provider
writes each reason itself. It names what failed and why, and never quotes
statement text, a value, an address or a credential, so a reason is never the
text of an error the provider received. `runtime.ErrorWithReason(code, reason)`
builds such an error. The SDK cannot tell whether a reason quotes a value; it
checks the shape, with `provider.ValidateDiagnostic`: one line of at most 512
bytes of valid UTF-8, with no control character, no line or paragraph
separator and no white space around it.

Both transport boundaries enforce the negotiation and the shape, each alone:

- The SDK server removes every diagnostic from a call's error unless both
  sides list the feature, so a provider sets one without reading
  `HostFeatures`, and sends `the provider's reason is malformed and not shown`
  in place of a reason the check refuses or of more than one.
- It removes a lock receipt's diagnostic the same way, and refuses a receipt
  whose diagnostic is malformed or accompanies a success.
- It removes an execution or transaction result's diagnostic the same way,
  and one that accompanies a success. It puts the same sentence in place of a
  malformed one and keeps the result: a refused result would turn a confirmed
  outcome into an unknown one.
- The client shows a reason only under the negotiated feature, and writes the
  same sentence for a malformed one. A result it finds malformed in another
  way is an unknown outcome and has no reason.

A cancellation and a deadline stay the context's errors. Without the feature on
both sides, the host's error is the operation and the code.

## Trust and process lifetime

`runtime.Start` requires an absolute executable path, its SHA-256 checksum, and
an expected provider identity. The caller authorizes that executable. A checksum
detects changed bytes; it does not establish publisher authenticity.

The runtime selects an explicit environment and refuses reserved bootstrap names
and duplicate names, including case variants. It does not search the working
directory or `PATH` for providers. Subprocesses have the caller's OS privileges;
they are not a security sandbox.

Mutual TLS authenticates the local gRPC connection. The bootstrap cookie only
identifies the expected launch protocol. Messages are limited to 4 MiB in each
direction. Startup defaults to ten seconds; service calls are limited to thirty
seconds or the caller's earlier deadline. Raw provider logs and RPC error text
are discarded because they may contain credentials.

The context passed to `Start` bounds startup. The returned client owns the process
until `Close`, which is safe to call repeatedly. Session handles become invalid
when the process exits. The provider also watches its parent and exits when the
parent dies, including when the parent cannot run deferred cleanup.

## Bootstrap specification

The experimental bootstrap uses the gRPC transport of
[go-plugin v1.8.0](https://github.com/hashicorp/go-plugin/tree/v1.8.0).
Its control services are part of this experimental bootstrap specification.
A non-Go implementation needs those control services and Ptah's
[`Provider` service](protocol/experimental/provider.proto).

The host supplies `PTAH_PROVIDER_BOOTSTRAP=ptah-provider-experimental-1`, protocol
version `1`, `PTAH_PROVIDER_PARENT_PID`, and `PLUGIN_CLIENT_CERT` containing the
client certificate in PEM format. The provider requires the client certificate.
The provider authenticates it, generates its server certificate, and writes one
bootstrap line to standard output:

```text
1|1|network|address|grpc|server-certificate
```

`network` is the local listener's network, such as `unix` or loopback `tcp`.
`address` is that listener's address. `server-certificate` is the DER certificate
encoded with unpadded standard Base64. The host pins this certificate. The selected
provider service name is `ptah-provider`. Subsequent standard output belongs to
the transport's standard-I/O service and must not add bootstrap records.

The bootstrap also implements go-plugin's health, shutdown, broker, and standard-I/O
services. The pinned upstream
[`grpc_*` protocol files](https://github.com/hashicorp/go-plugin/tree/v1.8.0/internal/plugin)
define those control messages. Application traffic uses only gRPC and Protobuf;
net/rpc and gob are not supported.

## Validation

From the repository root, the existing contour runner executes every SDK process
test and refuses skipped tests:

```bash
go run ./internal/cmd/testcontour --dir provider-sdk --tags integration --race
```

The tests build the fixture module against the in-tree SDK. They exercise
inspect, render, execute, and inspect again; partial execution; crashes and
cancellation after committed writes; environment isolation; message limits;
checksum and identity refusal; and parent death.

This SDK-level evidence does not establish a completed native CLI workflow.
That acceptance condition belongs to #4204's application integration.

Each engine declares its offline identifier comparison rules. Each opened session
reports the effective rules and default schema. Missing rules, unknown comparison
modes, and unknown index namespaces are refused. The experimental contract
supports exact and ASCII-insensitive comparison; it makes no claim about other
collation semantics. Hosts must not infer these rules from a built-in target.

The experimental application adapter requires `schema.common-table-identity.v1`
for common table rendering, identity preparation, identity CREATE projection, and common SQL planning. This promises
common table and column comparison semantics without implicit schema objects,
undeclared facets, additional creation
defaults, or type coercions. A provider must refuse declarations outside that
contract. A missing feature does not select a built-in implementation. Providers
without this feature can start, inspect, analyze SQL, and run reviewed SQL
migrations through their declared session, history, lock, and metadata services.
Provider-owned planning operations retain their separately negotiated contracts.

Engines can instead advertise `schema.prepare-tables` and implement `TablePreparer`
for offline preparation. The host sends complete declared and observed tables,
explicit existence knowledge, identifier rules, capabilities, and selected
features. Results may resolve declared table, index, primary-key, or constraint
facets and column key membership. They cannot add kinds, change owners or target
bindings, restore excluded properties, or alter captured observations. The host
retains authored values separately. Complete batch receipts and cancellation
checks prevent partial preparation.

An engine cannot advertise both preparation policies. Owned preparation does not
select identity CREATE projection or common SQL planning.

`schema.project-table-creations` selects `TableCreationProjector` for offline
CREATE prediction. Each input carries a complete declared table and the host's
explicit inventory of eligible facet owners, including empty table, primary-key,
index, and named-constraint slots. The reply supplies computed desired facets,
optional observed overrides, and authoritative column key membership. Computed
kinds may be new; owners and bindings cannot change, and exclusions cannot be
restored. Observed overrides require a declared conversion for the same kind.
Complete batches, immutable captures, and cancellation checks apply at both
process boundaries. Predictions do not establish execution or inspection.

The host obtains eligible owners from the same inventory used by core acceptance.
Native CREATE planning gives the owner a captured declaration and an explicit
graph step to replace. The common scheduler transfers dependencies to the accepted
replacement and refuses any unclaimed CREATE. Preparation and prediction do not
grant common SQL rendering, validation, or native ALTER and DROP planning.

The application runtime requires namespaced provider IDs and model kinds, such
as `github.com/stokaro/ptah-ydb-plugin` and
`github.com/stokaro/ptah-ydb-plugin/column-properties`. Official manifests use
the provider ID from the catalog; repository shorthand is not an alias.

`AnalyzeSQL` accounts for every source byte with a statement span or whitespace.
It preserves source tokens, canonical tokens, and line numbers. A provider refuses
comments or syntax it cannot represent. `COMMON_DDL` permits the host's common DDL
assessment; it does not mean safe. Native behavioral and destructive effects carry
explicit reasons. `UNKNOWN` remains destructive for approval policy. Unknown
numeric dispositions, incomplete batches, gaps, and overlapping spans are errors.

A failed process launch or transport bootstrap wraps `runtime.ErrStartup`.
A rejected contract version, service contract, or provider identity wraps
`provider.ErrProtocol`; a contract version a host cannot work with is a
`*provider.ContractError`. Cancellation and deadline errors preserve their context
sentinels. A caller can distinguish these failures from a missing installation.

## Owner-defined value normalization

The `values.normalize` service uses a `ValueNormalizer` that validates
and normalizes a complete batch without a session or connection address. Inputs
identify their desired, observed, operation, or change representation. A provider
refuses representations and payload fields it cannot interpret.

A normalized envelope retains its input owner, kind, schema version, encoding,
definition digest, and required features. The response supplies a separate normalized subject and
an owner-defined canonical format and projection. The client and server check
ownership, feature selection, ordered correlation IDs, and complete results.
A failed or canceled call exposes no usable prefix.

Canonical projections are representation-local. Bind them to the negotiated
provider semantic identity, engine, target features, representation, kind,
schema version, model definition digest, and canonical format. Never substitute raw payload bytes or
serialized Protobuf for that owner-defined contract. A projection does not
replace contextual desired/observed comparison or authorize a migration.

The independent fixture normalizes a synthetic retention value offline. It
preserves integers beyond the exact range of floating-point numbers, distinguishes
omission from null and zero, and refuses unknown or duplicate fields. The fixture
SQL service does not support retention. The host captures YAML declarations and
observed objects and facets, and dispatches contextual comparison to their owner.
Native planning and migration backend operations require the separate services
described below; see [ADR 0022](../docs/adr/0022-provider-values-are-resolved-in-semantic-batches.md).

## Database interpretation context

[ADR 0046](../docs/adr/0046-database-context-is-bound-before-provider-dependencies-are-used.md)
separates authored identity from the database needed to interpret references.
Both peers negotiate `database.context` before `ResolveDatabaseContext` or a
context-bearing session. The pure RPC receives an explicit engine and address.
It returns `DatabaseContext.database`, the selected database or realm, and
`source_database`, the root of authored absolute references. The provider owns
this interpretation. The host neither parses paths nor assumes a default root.
The service cannot connect, provision resources, read environment variables, or
resolve credentials. Its receipt and errors contain no address or credentials.

The client and server resolve the address before `Open`. They require the opened
session's context to equal that receipt and its database to equal the selected
root. A mismatched session is closed and never returned. Context roots must be
nonempty, exact, single-line strings; an absent context means unknown.

`values.context` independently negotiates `Envelope.context`. This durable
interpretation context does not change `Envelope.owner` or its normalized
identity. Normalization and representation conversion preserve it exactly. The
host retains it in captures, plans, and digests, and refuses a different target
context before consuming or executing a bound model.

Without context, an owner can set `NormalizedValue.requires_database_context`
when dependencies cannot be resolved. Such a result carries no partial
`required_objects`, change semantics, or operation semantics. It may be retained
for source processing, but comparison, planning, declaration rendering, and
operation rendering refuse it. A context-bearing normalization result cannot
retain this flag. The host binds context and normalizes the complete batch before
selection or dependency collection; failure discards the batch.

## SQL schema documents

A desired schema can be a SQL file. An engine declares in `Engine.sql_schema`
how a provider takes part in reading it. A target has exactly one mode.

- `FRAGMENTS` is the mode of an engine with no declaration. The host lexes and
  parses the document with the shared grammar and sends the statements it does
  not model to `DecodeSource` as `SQL` fragments.
- `DOCUMENT` is for a dialect whose grammar the shared one cannot read, such as
  YQL. The provider reads every SQL document of the engine in full through
  `ReadSQLSchema`, and the host does not lex or parse the text. The engine
  offers the feature `schema.read-sql`, and so does the provider.
  `header_kinds` lists the kinds a `ptah:not-described` header of such a
  document may name.

`FRAGMENTS` declares neither the feature nor header kinds. The SDK refuses a
description that names the feature without `DOCUMENT`, `DOCUMENT` without the
feature, a feature no engine uses, and an unknown or duplicate header kind.

`ReadSQLSchema` is a pure function of its request. It has no session, address
or credential. The host reads the files, applies its project-directory
confinement and sends `SQLDocument` entries with an ID, a path used only in
diagnostics, and the text. The provider opens nothing. Documents arrive in the
order the load runs them, and the reply is the result of running all of them,
so a later document can alter what an earlier one declared. The request also
carries the identifier rules, the database root that absolute paths in the
documents resolve against, and the header entries of the declared kinds.

A complete reply (`complete=true`) holds:

- `tables`, `namespaces`, `enums`, `views`, `sequences`, `roles` and `grants`,
  the common objects. `tables` and `enums` are the messages `ValidateSchema`
  already uses. `SQLRole` carries no password. `SQLGrant` has a `revoked` flag
  because a REVOKE asserts absence, which differs from an unlisted grant.
- `native_objects`, the DESIRED values of provider-owned models whose source
  formats include `SQL`.
- `facets`, the DESIRED facets of the declared tables and of their columns and
  indexes, under `schema.sql-facets`; see
  [Facets of a SQL schema reading](#facets-of-a-sql-schema-reading).
- `declarations`, one entry per requested document in request order, for the
  host's check that a later file of a schema directory does not declare again
  what an earlier one did: objects left declared, objects declared under a
  clause that admits an existing one, and `CREATE TABLE` and `DROP TABLE`
  statements in order. The provider decides what a statement means. The host
  owns the directory rule.
- `coverage`, the DESIRED claim the load makes about unmanaged objects.
- `traces`, exactly one `SQLObjectTrace` for every object of the collections
  above. It selects the object by collection and index and carries a
  `SourceTrace` whose `span` lies inside the requested document. The fragment
  ID, path and root stay empty, because the document is not split into
  fragments.
- `diagnostics`, warnings only.

A failure is a gRPC error when the batch is unusable, or a reply with
`complete=false`. That reply carries at least one error `SQLDiagnostic` and no
schema: every other field is empty, so a refused load exposes no prefix. A
diagnostic has a code (`SYNTAX`, `UNSUPPORTED`, `INVALID`, `INCOMPLETE`), a
severity, a document ID, a span with byte offset, line and column, and a
message of one line and at most 512 bytes. A construct the provider recognizes
and cannot represent is `UNSUPPORTED` and named, never skipped. A message names
a span and a short token and never quotes a statement, since a schema file can
carry a secret value. A canceled call returns no result.

Both transport boundaries run the same checks and return no schema when one
fails: a request that does not name a `DOCUMENT` engine, a reply with the wrong
document count or order, a span outside its text, an unowned or unnegotiated
native object, a duplicate object under the engine's identifier rules,
missing coverage, a missing or repeated trace, and an incomplete reply without
an error diagnostic.

Every field of the host's common database model has a home in the reply or is
refused by name:

| Host model field | Home in `SQLSchema` |
| --- | --- |
| Schemas | `namespaces` |
| Tables, columns, indexes, constraints, triggers | `tables` (`DeclaredTable`) |
| Enums | `enums` |
| Views, materialized views | `views` (`materialized` flag) |
| Sequences | `sequences` |
| Roles | `roles` |
| Grants, revoked grants | `grants` (`revoked` flag) |
| Feature objects owned by the provider | `native_objects` |
| Facets of a table, a column or an index, owned by the provider | `facets` (contract 25.21) |
| What the header says is not described | `coverage` |
| Functions, row-level security policies and enabled tables, managed data, embedded fields | None. The provider reports `UNSUPPORTED` and names the construct. |

### Facets of a SQL schema reading

`schema.sql-facets` lets a reading carry what a YAML or Go source of the same
engine carries as a facet: a setting or a set of children that belongs to a
table, a column or an index, such as a YDB table's changefeeds or its TTL.
[ADR 0066](../docs/adr/0066-a-sql-schema-reading-carries-the-facets-of-its-tables.md)
records the decision.

Each entry of `SQLSchema.facets` is an `Envelope` of a DESIRED model whose
source formats include `SQL`. Its owner is a table of `tables`, named by kind
`table`, its schema and its name, or a column or an index of one, named by kind
`column` or `index` with the table as the only path component, as
`provider.ColumnReference` names a column. An owner holds at most one facet of a
kind. The reply's `coverage` covers each facet's kind, so a kind the load
claims and a table it declares without the facet say that the table has none.
Each facet has one `SQLObjectTrace` of collection `FACETS`, which points at the
statement that declares it.

The engine lists the two-way feature beside `schema.read-sql`, in the
`DOCUMENT` mode, and so does the provider; the host lists it, and a request
carries it in its target features. Both transport boundaries refuse facets the
request did not negotiate, a facet of a model the engine does not offer in
SQL or the reply does not cover, an owner that is not a declared table, column
or index of the engine, two facets of one kind on one owner, and a facet
without a trace. A refused load carries no facets. The host normalizes each
facet with its owner and places it where a YAML source places it. Without the
feature a provider cannot say a facet, and reports the construct that needs one
as `UNSUPPORTED`.

The SDK conformance tests in `runtime` read a small table dialect through both
transport boundaries: load order, a later document that alters an earlier one,
per-document declarations, positioned refusals, a refused load that exposes no
prefix, and cancellation. The fixture's `sql-document` command declares the
`DOCUMENT` mode for its narrow table dialect, with a retention object of its own
and the header kind `retention`. The process tests in `integration` read through
it, and so does the host's adapter, which maps a reading into its common model.

## Contextual model comparison

The optional `models.compare` service uses a `ModelComparer`. Each engine names
its comparison services and assigns model kinds, change kinds, and either native
object placement or common facet owner kinds. Object services also declare their
supported explicit actions. A model has one comparison owner per engine.

`CompareModels` receives desired and observed states, identifier rules, capability
facts, parent lifecycles, and the context relevant to its placement. Object
context includes explicit actions, declared relations, roles, columns, required
objects, exclusions, and the database path. Facet context includes common
constraint definitions. A missing capability differs from an explicit false.

Bound identity components retain source and normalized spelling, quoting, and
connection defaults. Parent, namespace path, and signature remain separate.
Coverage retains each independent source and its outside and bounded claims.
Sources intersect; a complete region cannot supply another source's missing
knowledge. Empty values do not establish absence. An excluded facet retains its
model, subject, and target binding, and permits no change or diagnostic.

The owner validates complete payload semantics again. Captured canonical
projections do not prove desired/observed equivalence. Results include effective
desired schema, normalized `CHANGE` values, explicit undecided diagnostics and,
when negotiated, [counts of the compared objects](#comparison-counts).
Both transport boundaries refuse incomplete replies and unowned models. The
host's comparison runtime enforces common migration policy before planning.

Normalization of a change requires explicit replacement and state-removal facts,
impact and access assessments, and reasons. Unknown impact and access remain
explicit assessments. Required object identities are structured values. These
facts must be reconstructed from the complete payload when it is decoded;
persisted metadata cannot stand in for owner validation. Planning and execution
of native changes require their own services and are not supplied by comparison.

## Desired and observed conversion

The optional `values.convert` service uses a `ValueConverter` alongside
`values.normalize`. It receives one complete batch, its source and destination
representations, and the captured target features. No session is needed.

The owner interprets source payloads again. Supplied canonical projections are
context, not proof of validation. Destination schema versions, encodings,
definition digests, payloads, and canonical formats follow the destination model.
The ordered IDs, model kinds, source identities, and normalized identity tuples
must remain unchanged. Required features may change only within the selected
profile. An error, cancellation, or incomplete response discards the whole batch.

The host registers conversion only for advertised services and paired desired
and observed models. A batch mixing captured engine or feature profiles is
refused. Conversion cannot establish inspection coverage. The shared conversion
pipeline retains bounded knowledge, object identities, and facet placement;
unknown namespaces remain unknown. Clone, encoding, and equality remain local.

This service does not resolve schema selectors or implement contextual
comparison and planning. Inspection binds selectors separately through the
owner's namespace declarations.

## Captured extension sources

Contract major 25 carries `source.decode`, complete `ValueModel` definitions,
and explicit YAML key ownership.
A provider of an earlier major is refused. The SDK has no compatibility decoder or built-in
fallback. A `SourceDecoder` receives complete captured documents without a
session, connection address, host AST, or callback. The host evaluates frontend
expressions and enforces file access policy before capture.

Each document explicitly enrolls desired models from the selected engine's
negotiated description. A model identifies its kind, representation, schema
version, encoding, SHA-256 definition digest, canonical projection format, and
accepted source formats. The full definition is a nonempty JSON object
normalized by `modeljson.Canonical`. The SDK verifies its exact SHA-256 digest
and refuses duplicate keys, lossy strings, or a changed definition. Number text
remains exact. Protobuf bytes and value payloads are not model definitions.
These checks bind the declared format; they do not prove engine semantics.

Each YAML-capable engine declares `yaml_source_keys`. A claim names the key,
its `DOCUMENT` or `TABLE` scope, a decoder selector, and the desired model kinds
it can declare. The SDK refuses duplicate keys within a scope, missing or
unsupported models, and a YAML-capable model with no key claim. The host must
also refuse collisions with common frontend keys or another selected owner.
A model's JSON definition and kind name do not imply grammar ownership.

Every YAML fragment carries the exact negotiated `yaml_key` claim. Its selector
must match, and its document must enroll every model the claim names. A table
key needs a common table binding with an entry key; a document key has no
implicit table attachment. The client and server validate these facts before
owner code runs. Other source formats cannot carry a YAML claim. Grammar claims
are metadata, not a promise that the host has connected every frontend adapter.

Each engine that decodes Go annotations declares `go_source_directives`. A claim
names the directive, its `DOCUMENT`, `TABLE`, `COLUMN` or `INDEX` scope, a decoder
selector, the attribute names it accepts, and the desired model kinds it can
declare. The host lexes the comment; the owner receives the parsed attributes as a
mapping in a `GO_ANNOTATIONS` fragment and never the comment text. The SDK refuses
a directive name claimed twice within the engine, an attribute that is empty,
duplicated or contains whitespace, `=`, `,` or a quote, and models that are
unknown or do not accept Go annotations. The host must also refuse a name claimed
by another selected owner.

A Go fragment of an engine with claims carries the exact negotiated
`go_directive`. Its selector must match, its document must enroll every model the
claim names, and every attribute key must appear in the claim. A table directive
needs a common table binding. A column or index directive needs that binding and a
`subject` of kind `column` or `index`; the host sends the table's schema, the
table as the only path component and the name the field or index directive
writes, and refuses a value the owner returns for another subject. A claim whose
directive is `ptah:schema:table`, `ptah:schema:field` or `ptah:schema:index` adds
the listed attributes to that directive and takes the scope `TABLE`, `COLUMN` or
`INDEX` respectively. A document directive has no attachment.
Other source formats cannot carry a Go claim. An engine that declares no
directives keeps accepting captured Go fragments without one.

Captured values preserve missing entries, explicit null, false, empty values,
exact number text, expressions, and structured references. Mapping keys,
block attributes, child blocks, and labels retain their separate namespaces.
Bindings carry common object identity and frontend naming. The owner receives
all bindings before interpreting fragments, so declaration order need not limit
reference resolution. It must interpret every supplied field or refuse the
whole batch, including inherited attributes it requested from the host.

A successful response contains a result for every document and fragment in
input order. Every emitted object or facet names an enrolled model, provides
its normalized value and canonical projection, and has matching subject
coverage. Duplicate normalized identities are refused. Coverage retains the
original model scope; empty scope never means all kinds in the provider's
current registry. Complete, absent, defaulted, uninspected, and unrepresentable
knowledge are distinct. Absence and defaults need a specific subject; observed
coverage cannot request defaults. An incomplete source result is refused.

Source traces name an original document and fragment and follow typed keys,
indices, attributes, child blocks, or labels. Coordinates use zero-based offsets into the original encoded bytes and
one-based lines and byte columns in that encoding. A BOM contributes bytes to
the first column; UTF-16 source is not silently re-encoded. Authored values require
positions; inherited defaults can lack an authored position. A missing path is
an error, distinct from an inherited default with no position. Validation reads
no files, checks bounds and cycles, and limits depth and batch size.

The independent fixture exercises evaluated YAML, HCL, and Go annotation
fragments. It accepts a synthetic retention mapping or HCL block, resolves its
`on` binding, and preserves exact integers. It refuses SQL and does not apply
retention to SQLite.

The host YAML frontend captures negotiated document, table, column, and index
keys for this RPC. The host Go annotation adapter captures the claimed
directives of a Go file at every scope, with attribute values as text, and the
attributes a provider claims on Ptah's table, field and index directives. It
refuses a claim on any other Ptah directive.

The host HCL adapter captures
declared blocks and attributes with their bindings, inherited values, and
header limits. HCL and Go export send the provider's values through
`EncodeSource`; HCL reports unrepresentable values as losses, while Go refuses
losses and coverage limits. Official-engine source parity remains part of
#4204; the fixtures do not establish YDB source support.

## Observed model coverage

`Inspect` returns its engine, selected target features, and explicit observed
model coverage even for an empty database. The host matches the engine and
features to the inspected session. A missing coverage record is not evidence
that a model has no objects.

`provider.InspectionValues` validates native objects, table facets, column
facets, and common view facets against their negotiated model definitions and complete subject records.
It returns an independent batch for `NormalizeValues`. Native objects use their
model kind as their identity kind. Table facets name their table, and column
facets use `provider.ColumnReference` to keep the table and column names as
separate components.

After normalization, `provider.ValidateNormalizedInspection` requires every
normalized owner to match its coverage subject. A failure discards the entire
snapshot. Hosts retain the engine, target features, model contract, and
normalized subject when capturing the owner-defined canonical projection.

Each model declares knowledge outside its namespace regions and within each
region. Database, catalog, and schema qualifiers preserve source and normalized
spelling. An absent qualifier is a wildcard; a present empty qualifier matches
only an empty component. Path prefixes retain whole components. Regions belong
to the operation's selected engine. An uninspected or unrepresentable region
requires a reason and overrides overlapping completeness. Explicit subject
records take precedence. Duplicate normalized regions, invalid components, and
namespace-wide absence or defaults are refused. A model can carry at most
4,096 regions. Under [subtree coverage](#subtree-coverage), a region may also
cover every schema nested below its schema.

The host requires the exact session database on bindings and coverage regions.
It retains native coordinates and maps common table and column regions to shared
identities. Subject-kind limits and exact empty paths keep these coordinate
spaces separate. A column path becomes its parent table; unknown parent regions
remain unknown after this mapping. Unbound, foreign, or renamed databases and
inconsistent common identifier normalization cause refusal.

A filtered inspection returns one `scope_bindings` entry per requested selector,
in request order. Each entry maps that selector to structured owner namespaces.
An empty namespace list means an empty selection, not global authority. Bindings
never grant coverage. Every returned table has a `normalized_identity`, even
without facets. Tables, coverage subjects, and whole coverage regions must fit
inside the bound namespaces, except the objects of a negotiated
[database kind](#database-kinds), which are named at the database. Filtered
replies cannot claim global completeness or include dispositions without
normalized identities. Requests and bindings are limited to 4,096 entries, with
at most 4,096 namespaces across all bindings.

`Schema.reserved_objects` records positively identified provider-owned history,
lock, log, or tag storage. Providers validate native ownership and layout before
omitting it from managed observations. A familiar name alone is insufficient.
Each record supplies source and normalized identities and every object kind
that shares the name. Reservations must include their own kind. Native conflict
kinds require negotiated observed models and explicit `UNREPRESENTABLE` subject
coverage with a reason, even inside a completely inspected namespace.

At most 4,096 conflict identities may be reserved. Missing identities, unknown
purposes, overloaded storage, duplicate source or normalized names, and overlap
with managed observations are refused. Common table conflicts cannot carry a
catalog or path. Filtered reservations must fit the requested namespaces.

The host binds reservations to the live database and common identifier rules.
Its catalog retains the occupied names through filtering and JSON snapshots.
Comparison refuses desired collisions, foreign keys and discovered feature
references into reserved storage, and removal of a containing schema. Unknown
feature references refuse comparison when reservations exist. These protections
apply to schema comparison; direct SQL remains an explicit execution operation.
The official YDB provider still needs native reservation classification under
#4204. The synthetic fixture validates the host contract only.

Desired and observed models may declare a plural `display_name` for omission
reports. It must be trimmed single-line text and agree across engines that share
a model representation. The host freezes this label in the contract and reports
captured values locally, without interpreting payloads. An engine declares a
model's count metrics only under [report counts](#report-counts). An absent
label does not get a host-invented substitute.

Native `schema inspect --format json` returns common schema data and warns about
omitted objects and facets using these labels. It is not a lossless model export.
Shared conversion and codecs retain the captured payloads and bounded coverage.
Full-document source decoding still requires unrestricted complete model
knowledge and refuses regions. Native migration planning and faithful source
export remain work in #4204.

## Captured native operations

An `OPERATION` model is distinct from desired values and comparison changes.
Normalization reconstructs its impact and access assessments from the complete
payload. Hosts can clone and persist captured operations without reaching the
provider; decoding requires owner normalization again.

A native `Render` input carries the captured operation and its grammatical role.
A table alteration also carries its actual parent. The owner renders a complete
execution unit, including the table statement. Capability facts preserve a known
false value separately from a missing answer. Providers must revalidate payload
semantics and refuse any role or context they cannot implement.

Rendering validates the whole batch at both transport boundaries. Cancellation,
foreign models, incomplete replies, and changed provenance discard all output.
The independent operation fixture executes table creation, renaming, and removal
through this contract. Contextual feature planning uses the service below.
The host binds history writes to execution handles. Official-engine transaction
qualification remains required under #4204.

## Contextual feature planning

`models.plan` advertises `PlanFeatures`. Each engine assigns a service its change,
parent, and operation model kinds. Parent assignment is independent of changed
facets: a table operation must account for attached state even when no setting
changed.

The request captures declaration and inspection records separately, including
common children, normalized feature values, exclusions, and independent coverage
sources. Missing observations do not establish absence. Identifier components,
capability values, and common graph step identities retain their original
boundaries. An established false capability is distinct from a missing fact.

A successful reply accounts for every input change and every assigned parent
model. Each contributed operation must appear in a receipt. Dependencies may
refer to common steps; explicit rewrites name the common operations they replace.
The host must validate ownership, rewrite effects, and the complete graph before
using a contribution. The SDK never orders a partial graph or treats a receipt
as evidence of execution.

Transaction requirements, operation placement, phases, and conservative impact
assessments travel as explicit metadata. Unknown values remain unknown. A
completed refusal contains diagnostics and no operations, rewrites, or successful
receipts. Cancellation and malformed replies discard the complete batch.

The host registers these services and binds their ownership into captured model
identities. Its adapter preserves common definitions, normalized feature values,
coverage sources, graph metadata, and receipts. Unmapped common properties and
conflicting primary-key facet slots are refused before the RPC.

The common migration planner dispatches native changes and parent assessments
through the selected runtime, then schedules the complete shared graph. It
retains unchanged captured tables and supplies exact common column operands.
Common column and table removals follow the feature windows. Unsupported inline
common definitions remain errors; the host never strips native settings to make
a common statement render. A replacement in the default phase inherits its
source steps' boundaries instead of joining the ordinary feature window. An
explicit phase adds its window constraints; conflicting boundaries refuse the
complete graph.

An independent SDK-only fixture exercises the production planner, shared graph,
rendering, statement execution, and inspection. It checks captured ALTER parents,
parent receipts without changed native values, common column replacement, graph
conflicts, completed refusals, and cancellation. The replacement preserves the
actual common operand even when it differs from the final desired column. The
test caller executes without a transaction.

Owner transaction requirements survive AST lowering. A forbidden transaction
reaches migration classification. A mixed plan that the generator cannot split
safely is refused. The external planner accepts a required transaction only when
the selected engine advertises the service and transactional DDL is established.

Prepared schema apply, saved JSON plans, and dev rehearsal check owner
requirements before executing any statement. The JSON plan preserves each
statement's requirements. Editing assessed SQL needs a fresh owner assessment;
Atlas HCL output refuses metadata it cannot preserve. Session capability facts
retain the difference between an established false value and an unknown value.

Raw SQL assessment, history services, and official provider extraction remain
part of #4204.
These tests do not qualify YDB extraction or the migration history backend.

The host schema writer binds each execution to its negotiated transaction and
refuses commit after a failed or uncertain statement. Commit and rollback keep
confirmed terminal results. A lost commit reply remains unknown until an explicit
cleanup request or inspection establishes state. Rollback can run after caller
cancellation. The independent transaction fixture checks real commit, rollback,
session cleanup, handle isolation, late-begin fencing, and native schema apply
with `file` and `all` transaction modes.


### Migration logs and tags

The optional `sessions.migration-metadata` service stores typed event and tag
records. Supporting engines advertise it as well. The host owns migration
ordering, event pairing, tag normalization, and recovery. Providers own storage,
layout validation, atomic mutations, and current lock ownership checks.

A log scope uses the session's durable store and cannot name an execution
transaction. A tag scope selects the active handle. Preparation inside a
transaction validates an existing store; it cannot create one. A missing tag
differs from a tag selecting version zero. Delete reports actual atomic removal.
Unknown mutation outcomes do not permit automatic replay.

Snapshot readers use caller-selected, single-use handles. Pages carry an exact
request receipt, a page number, existence, and explicit completion. The record
and byte bounds apply per page. Missing stores produce an empty complete page
without creating storage. Duplicate or inconsistent page records are refused.
Closing a reader fences late opens; session close releases remaining readers.
Transaction finish invalidates readers bound to that transaction.

The SDK cleans up the same reader after uncertain opening or page advancement.
Cleanup has a separate bounded context and does not erase the original failure.
The unit tests cover malformed receipts, cancellation, page bounds, and no
mutation replay. The SQLite fixture uses ordinary event and tag columns. It
materializes each snapshot with a 64 MiB limit and refuses larger snapshots.
The limit applies to the fixture, not the protocol. Its durable log uses a
separate connection pool owned by the session. SQLite can refuse a concurrent
write; the fixture never reroutes it through the execution transaction.

Process tests cover commit and rollback, journal durability, immutable pages,
atomic deletion, invalid layouts, triggers, lost replies, and handle cleanup.
Host adapters preserve these scopes and translate uncertainty into the common
migration error contract. Failed transaction-bound tag operations prevent commit;
rollback and reader cleanup remain available. The common migrator retains its
best-effort journal policy. Published-module adoption and official-engine
qualification remain required under #4204.

## Offline schema validation

`schema.validate` selects `ValidateSchema` independently of rendering, table
preparation, and CREATE prediction. Each supporting engine declares the service.
It requires value normalization and cannot be combined with the common table
identity policy.

The request retains captured table declarations and identities, namespace settings,
enums, native objects and facets, coverage limits, dependencies, identifier rules,
capabilities, and `no_skipped`. Providers validate those facts without inspecting a
server or reopening source files. The host refuses populated declarations its
adapter cannot represent before dispatch; it also refuses orphaned or ambiguously
attached fields, indexes, constraints, and triggers.

The result distinguishes completed semantic refusals from failure to perform the
check. Diagnostics use `INVALID_SCHEMA`, `UNSUPPORTED_FEATURE`, or
`OMITTED_DECLARATION`, with a kind and message. Malformed replies, cancellation,
and RPC errors discard all diagnostics. Empty schemas still require a completed
reply. Validation does not authorize execution or establish object existence.

## Rows, row writes and seeds

[ADR 0028](../docs/adr/0028-a-provider-owns-how-rows-are-read-written-and-seeded.md)
gives the provider what depends on the engine when rows are compared, written
and seeded. The host keeps the generic algorithm: which rows are declared, the
row diff, ordering by foreign keys, protected tables, saved plans and the
idempotent-seed policy. Each service has a feature of its own, and each
supporting engine also declares it. A host refuses a declared row set for an
engine whose provider lacks `data.type-rows` and `data.read-rows`, and a write
for one that lacks `data.render-writes`; it does not substitute common
`INSERT` text.

A row is a list of `DataValue` aligned with a column list, never SQL text. A
value is null, a boolean, a signed or unsigned 64-bit integer, a double, text,
bytes, a decimal (unscaled digits, precision and scale), a UTC timestamp, a
date as days since 1970-01-01, an interval of months, days and nanoseconds,
JSON text, or a `native` value with a provider-defined type name. A value with
no case set is a column the row does not carry, which differs from null. The
SDK refuses text that is not UTF-8, JSON that does not parse, a decimal that is
not digits or exceeds its precision, and a timestamp whose nanoseconds leave
their range.

`TypeRows` is offline. It receives the capabilities, the live table or its
absence, the declared type of each column, and the declared and live rows. It
returns the engine type and the value kind of each column, in request order,
and both row sets in one canonical form, so that a decimal read back as 1.5
pairs with a declared 1.50. Rows keep their order and the columns each carries.
Both transport boundaries refuse a reply with a different row count, a value
whose kind disagrees with its column type, and a row that gains or loses a
column.

`ReadRows` acts on a live session and can name a transaction. It reads the
requested columns of a table named by `Reference`, in the order of the key
columns, at most 10,000 rows a page and within the message limit. A page token
resumes the read; the caller never repeats a request whose reply it did not
get. A missing table is a complete page with `exists=false`; an empty table is a
complete page with `exists=true` and no rows. A read that cannot finish is an
error and no page. The SDK refuses a page of another table, more rows than the
bound, an unset value, a value of the wrong kind, and a completion that
disagrees with the next page token.

`RenderRowWrites` is offline. The host sends `RowWrite` operations with a mode
(`INSERT`, `UPSERT`, `UPDATE_BY_KEY`, `DELETE_BY_KEY`), the table, the columns
written, the key columns, one value per column and the column types from
`TypeRows`. The reply is a `RenderResult` with one unit per write, in input
order, or a `RenderRefusal` that names the write and its reason
(`UNSUPPORTED_MODE`, `UNSUPPORTED_VALUE`) and carries no unit. `UPSERT` is a
mode, not a word of one dialect: an engine without one refuses it. Rendering is
deterministic, so a saved plan and a migration file cannot disagree about a
row. A native unit must name the write's table as its owner; text units are
the owner's promise. Execution uses `Execute` and the transaction service as
for any other unit. A migration file accepts text units only, which a host asks
for with the `TEXT` form (see [Text units](#text-units)).

The seed service acts on live sessions. The host discovers files, computes the
checksum, selects them by environment and applies the protected-table check
against `ListSeedTables`. `ApplySeed` runs a file and records it in the
provider's ledger atomically, and answers with one outcome:

- `APPLIED`: the file and its entry committed together.
- `KEY_CONFLICT`: a statement hit an existing key and nothing of the file
  stayed. An idempotent run then calls `RecordSeed`, which writes the entry
  without sending the text.
- `REFUSED`: nothing ran, such as for a statement the engine runs only outside
  a transaction. The reply carries the position of that statement and a reason.
- `NOT_APPLIED`: the provider stopped and proved that nothing committed, as
  after running the file again a bounded number of times following aborts.
- `UNKNOWN`: the provider cannot prove that nothing committed. The host does not
  retry it.

`ReadSeedLedger` returns path, checksum, environment and time for each entry,
so the host can detect a changed file. The ledger layout is the provider's: it
creates a missing ledger, refuses one it does not recognize, never adopts
another layout, and replaces the entry of a path that runs again. Diagnostics
carry the path, a position and one line of reason, never the statement or a
value.

### Query-builder spelling

`Engine.query_dialect` declares, as data, what the host's query builder reads
from a dialect descriptor: the placeholder style, the quoting section, the
query section and the capability facts the builder assumes when its caller
names the engine without a target's capabilities. The field names are the
descriptor's JSON tags, with the same meaning and values. The builder renders
statements offline and never calls the provider. The SDK checks the shape:
a placeholder style, words without spaces where the descriptor holds a single
value, one line of text in every spelling and refusal, no repeated keyword or
join, and capability names a host can read. The host owns the vocabulary and
refuses a value it does not know. A host lists `query.dialect` to receive the
declaration; otherwise the SDK server leaves it out of the description.

The SDK conformance tests in `runtime` read pages and resume them, separate an
empty table from a missing one, type two spellings into one form, render each
write mode, refuse a batch with no prefix, keep a ledger entry only with the
file it ran, detect a changed checksum, refuse a file before it runs, and
report a lost seed reply as unknown without sending it again. The process
tests in `integration` run the same list against the fixture's SQLite through
its `data` program, with a transaction that rolls the declared rows back.

## Dev database cleanup and the replay guard

`database.dev-cleanup` lets a host clean a dev database a provider serves before
a replay and between runs, as it does with its own engines. `ListDevObjects`
returns every object in scope; empty schemas select the whole realm the session
treats as its database. The server's own objects and storage the provider keeps
for the session are left out. `CleanDevDatabase` reads the same set: it removes
everything, or removes nothing and names each object it has no statement for,
with the reason. A cleanup that stopped part way is `UNKNOWN`, and the host
treats the database as not clean. The host decides that the database is a dev
database; the provider cannot. An incomplete list is an error, never an empty
database.

`database.dev-replay-guard` is the offline check the host runs before it
replays migrations into a dev database. The request carries the statements and
the scope the run may change: the dev database alone, the whole server the run
owns, a server the run provisioned, or every user database of a server named as
a whole. The owner refuses each statement whose effects it cannot confine to
that scope, such as a change to another database, to something the whole
server shares, or to the replay session's own settings, and names the operation
in one line. A cleanup removes what a replay created inside the dev database
and nothing outside it, so a host that cannot read the dialect needs this
answer; without it, the host refuses the replay. A reply that does not answer
every statement exposes no verdict.

`database.dev-realms` is an engine feature beside `database.dev`. A
`ProvisionDevRequest` then carries the operator's `address` instead of a `spec`:
the owner creates a realm of its own for the run in that database, such as a
directory in a database only an administrator creates, and the release removes
it. The address can contain credentials, as an `Open` address can. The realm's
address keeps the scheme and host of the operator's address, and either the
same credentials or none; a host treats it as it treats that address. A
provisioned database's address never carries credentials. A `spec` names an
image in either form the operator types, such as `docker://ydb/24.3` or
`docker+ydb://_/ydbplatform/local-ydb:24.3/local`, and the owner chooses the
image; the host takes the engine from either form. An address travels only when
the engine and the host both list `database.dev-realms`.

A scratch database for a test case is another realm: the host provisions one per
case. The protocol therefore has no separate scratch-database operation.

## Statement facts for SQL lint

`LintFact` carries what a host's statement rules need from an owner whose
dialect the host cannot read:

- `LINT_FACT_STATEMENT_KIND` says what each statement is. Where a split judges
  it, every statement carries exactly one, and an annotation carries one on
  every statement it does not decline. `statement_class` is `CREATE_TABLE`,
  `ALTER_TABLE`, another schema statement (`SCHEMA`) or a statement that
  changes no schema object (`OTHER`); `kind` names it in the dialect's words for
  a message, and `object` names what it acts on.
- `LINT_FACT_MISSING_PRIMARY_KEY` names, in `table`, a table the statement
  creates without a primary key. It belongs to a `CREATE_TABLE` statement.
- `LINT_FACT_REQUIRED_CAPABILITY` names, in `capability`, a capability the
  statement needs from its target, and the part of the statement that needs it
  in `object`, `kind`, `table` and `clause`. The host compares the key with the
  target's capabilities.

Rule identifiers stay with the host. `statement_class` and `capability` belong
to their fact alone, and a statement with two kinds is refused. An engine offers
these facts with `lint.statement-facts`, beside `lint.migrations`, and a split
or an annotation uses them only when the host lists the same feature.

## Embedding store

[ADR 0032](../docs/adr/0032-the-embedding-store-is-a-provider-service-and-the-host-keeps-the-lifecycle.md)
puts the storage half of `ptah inference` behind the provider. The host keeps
the lifecycle: the specification, the embedding endpoint, the backfill and
catch-up loops, the batch bounds, leases and fencing, every lifecycle rule,
verification, and the names of a generation's objects. The provider keeps the
run state the host decides, in a layout of its own, and owns a generation's
storage, rows, vectors, index and searches. It stores records whole, compares
them whole, and refuses nothing for a lifecycle reason.

`embeddings.store` switches the service on, and it is a two-way feature. The
provider lists it, and each supporting engine lists it beside
`Engine.embeddings`, which names the consistency modes a generation on that
engine can be built with. The host lists it in its host features. Otherwise
the SDK drops the declaration and refuses every call, at the client before it
sends one and at the server. A host that does not list the feature keeps
everything else the provider serves.

**Run state.** The records are `EmbeddingGeneration`, `EmbeddingRun`,
`EmbeddingPointer` and `EmbeddingEvent`. Their fields are the host's run state,
so an owner can keep each field in a column of its own. Phase, status, kind and
failure class are text the owner stores without reading it.

- `ReadEmbeddingState` reads one scope as one snapshot: every generation, run
  and pointer; one run; one generation; the runs of one generation; or one
  pointer. Records come in key order with a token, a digest of exactly those
  records, which `provider.EmbeddingStateToken` computes. A scope larger than
  the bound, at most 10,000 records, or than the message limit is an error.
- `MutateEmbeddingState` carries the token and the records a decision changed.
  The owner reads the scope again and writes the records only when its token is
  the same. It writes only records the scope covers and deletes none. `APPLIED`
  returns each written record as a later read returns it.
- `AppendEmbeddingEvent` gives an event of an existing run that run's next
  sequence, from 1. `PrepareEmbeddingStore` creates a missing store, verifies an
  existing one, and refuses a layout it does not recognize.

**Batches.** `ReadEmbeddingBatchState` returns the run and the first stored row
at each of a batch's keys, with a token over both. The host decides the fencing
and which sets win. `CommitEmbeddingBatch` then makes each written key's rows
equal to its set and writes the checkpointed run, in one atomic operation, when
the token holds. The SDK refuses a batch whose sets break these rules:

- a key's writes are adjacent, in ordinal order from 0, and share one input
  hash, version and kind;
- a skip or a tombstone is a set of one, and so is every set of a generation
  that does not chunk;
- every write carries the request's generation and names a key the state was
  read over;
- an upsert carries a vector of the target's dimension, and no other write
  carries one. A NaN or an infinity is refused, here and in a search.

**Outcomes.** A write answers one of these:

- `APPLIED`: committed.
- `CONFLICT`: the token differed and nothing was written. Only the two
  compare-and-writes can answer it.
- `REFUSED`, with a reason (`UNSUPPORTED`, `INVALID`, `ABSENT`, `NOT_OWNED`) and
  one line of text: nothing was written.
- `NOT_APPLIED`: the engine refused in a way that proves nothing committed. It
  carries one line of reason.
- `UNKNOWN`: the owner cannot prove whether it committed.

The client returns a confirmed outcome other than `APPLIED` with a
`provider.EmbeddingWriteError`, and a `CONFLICT` also matches
`provider.ErrEmbeddingConflict`: the host reads again and decides again. A lost
or invalid reply is `UNKNOWN` and is never sent again. No call joins a host
transaction. An owner may run an operation again after its engine aborted it in
a way that proves nothing committed, a bounded number of times, rereading what
it compares each time.

**Storage.** A request about a generation carries an `EmbeddingSpec`, or an
`EmbeddingLocation` for a registered generation:

- the source relation, its key and input fields, its version field and filter;
- the target relation, column, layout, representation, metric, dimension and
  index;
- whether the specification chunks, the generation identity, and the names the
  host derives for the index, the ownership mark and the column comment.

The filter is the specification's own text in the engine's dialect, and only
the owner reads it. Every other name travels as a separate component.
`DescribeEmbeddingTarget` is offline. `PrepareEmbeddingTarget` is idempotent,
and adopts a table without the ownership mark only when the table is empty and
has the shape a generation creates. `DropEmbeddingGeneration` checks ownership
before it drops anything, so a refusal drops nothing.

**Reads.** `ScanEmbeddingSource` and `ReadEmbeddingVerification` page at most
10,000 rows. A verification page keeps a key's rows together and never splits
them across pages. Its stored side carries the width of a vector, never the
vector, and the SDK refuses no state of stored data, because verification
exists to report it. `SearchEmbeddings` answers the nearest keys through the
index and by exhaustive scan. A failed search is a reply with a reason, not a
failed call.

**Bounds.** A call with no earlier deadline is bounded by
`runtime.EmbeddingWriteTimeout` (2 minutes) for a mutation or a batch commit,
`runtime.EmbeddingStorageTimeout` (10 minutes) for a call that can read or
change a whole table, `runtime.EmbeddingIndexTimeout` (12 hours) for an index
build, and `runtime.CallTimeout` otherwise. A batch is one message, so a host
bounds it by the 4 MiB limit.

The SDK conformance tests in `runtime` negotiate the store in both directions,
write state on the token it was read with and refuse a second writer with an
old one, commit a batch with its checkpoint, refuse a malformed batch before
sending it, report a lost write as unknown without sending it again, assign
event sequences, page the source and the verification walk, and answer the
storage calls. The fixture's `embeddings` program serves the store on SQLite,
and the process tests in `integration` run against it: prepare, scan and
resume, a batch commit and a conflicting one, verification paging, an
exhaustive search, structure and counts, retirement with an ownership refusal,
and a crash after a commit whose checkpoint the next read shows.

## Common objects

[ADR 0033](../docs/adr/0033-common-objects-reach-a-provider-through-the-common-wire.md)
decides this wire. `schema.common-objects` carries the objects the host models
for every engine beyond tables: views, materialized views, sequences, roles,
grants and comments. They travel in common messages, as tables do, and never as
models a provider defines for itself. It is a two-way feature. The provider
lists it, and each supporting engine lists it beside `Engine.common_objects`,
the kinds it carries (`VIEW`, `MATERIALIZED_VIEW`, `SEQUENCE`, `ROLE`, `GRANT`,
`COMMENT`), each once. A host that does not list it gets no declaration and no
field below.

The host keeps the comparison, policy and ordering; the provider inspects,
validates, spells and renders:

- **Declared** objects are the shapes `ReadSQLSchema` returns, in
  `DeclaredCommonObjects`: `SQLView` (with `materialized`), `SQLSequence`,
  `SQLRole` and `SQLGrant` (with `revoked`). `SQLRole.password_declared` says a
  password is declared without carrying it, and `SQLGrant.columns` limits a
  grant to columns of its table. A comment stays in the `comment` field of its
  object. `ValidateSchemaRequest.common_objects` and
  `PlanFeaturesRequest.desired_common` carry them.
- **Observed** objects are `ObservedCommonObjects`, in
  `Schema.common_objects` and `PlanFeaturesRequest.current_common`: views and
  sequences by `Reference`, roles with their password state, one privilege per
  `ObservedGrant`, and one `ObjectComment` per object that has a comment, a
  column or an index with its table as the only path component. Each kind the
  engine carries has exactly one `CommonObjectCoverage`, `COMPLETE` or
  `UNINSPECTED` with a reason, and an object appears only for a kind the read
  covered completely. `roles_out_of_scope` names the roles the server holds
  that the read does not describe.
- **`PrepareCommonObjects`** is offline. It returns the declaration in the
  engine's spelling, such as a view body as the server stores it or a
  privilege by its permission name, with every collection of the request's
  length and order. A view keeps its schema, name and kind, a sequence its
  schema and name, a role its name and kind, and a grant its role, target and
  whether it revokes. The host compares the prepared declaration and renders
  the authored one.
- **Rendering**: `Change.common_object` is a `CommonObjectChange`: a view
  created or dropped, a sequence created, altered or dropped, a role created,
  altered or dropped, a grant or a revocation, a membership added or removed,
  and a comment set. `Change.object` names its subject. A role's password
  travels only in `RoleCreation` and `RoleAlteration`, and a diagnostic never
  repeats it. `CommonPlanningStep.common_object` carries the same operand to a
  planner.

Both transport boundaries refuse a kind the engine does not carry, an empty
name, a grant without a role, privileges or a target, a role that is a member
of itself in a declaration, an identity repeated under the engine's identifier rules, and a
prepared entry that changed its identity or its collection's length.

### View facets and replacement observations

[ADR 0047](../docs/adr/0047-common-view-facets-retain-authored-and-observed-state.md)
defines `schema.common-view-facets`. An inspected `ObservedView` carries typed
observed envelopes in `facets`, each owned by that exact view identity. These
values enter the inspection normalization inventory after table and column
facets and require their own complete model coverage. A common view has no
standalone native twin.

A desired `SQLView.features` carries captured desired facets and coverage. Its
object collection is empty. Each facet's source and normalized owner matches
the authored view. Preparation preserves the complete feature collection while
preparing common spelling. SQL `WITH` attributes remain common SQL attributes;
they cannot hold native payloads.

A replacement can carry `ViewCreation.current`, the actual full `ObservedView`.
It requires `replace`, matches the desired kind, schema, and name, and binds the
render change to every coordinate of the current identity. A planning step's
current operand must equal a captured current view. A missing operand means
unknown current state, including an authored `CREATE OR REPLACE`; it establishes
no absence. Providers refuse replacements whose preservation requirements need
unavailable current facts.

### Independent access observations

[ADR 0042](../docs/adr/0042-access-observations-have-independent-coverage.md)
defines `schema.common-access`, negotiated beside `schema.common-objects`.
`OBJECT_OWNER` and `ROLE_MEMBERSHIP` are independently covered common kinds.
Complete role inspection implies neither. Uninspected coverage retains its
reason and is never an empty collection.

`ObservedCommonObjects.object_owners` names each object by `Reference` and its
owner principal. Optional `owner_can_login` distinguishes unknown, known false
and known true. Database-root references may have an empty name. The SDK refuses
duplicate or conflicting owners after applying the engine's identifier rules.

`ObservedCommonObjects.memberships` carries unique group/member pairs, including
out-of-scope principals and self-memberships the server reports. When the engine
advertises `ROLE_MEMBERSHIP`, observed roles must leave `member_of` empty. This
keeps one membership representation and preserves facts that authored schema
policy may refuse to create.

## Text units

[ADR 0034](../docs/adr/0034-a-provider-renders-text-units-where-the-host-writes-text.md)
decides this form. A host writes text where a migration file, `schema render`, a
SQL diff or a SQL inspection needs it, and asks for it with `RenderRequest.form`
or `RenderRowWritesRequest.form` set to `TEXT`. Every unit of the reply is then
`Unit.sql`: a portable statement the same provider's `Execute` accepts read back
from a file, with no envelope and no session state. A change the engine performs
only through a native call refuses the whole batch with `RenderResult.refusal`,
the reason `NO_TEXT_FORM`, a change of the request and one line of text, and no
unit. `Render` sets a refusal only in the `TEXT` form.

`render.text-units` is a two-way feature. The provider lists it, and so does
each engine whose changes it can write as text. Without it the host sends the
`ANY` form, which is the behavior of earlier minors.

## Planning policy

[ADR 0035](../docs/adr/0035-the-operators-planning-policy-reaches-the-provider.md)
decides this feature. `planning.policy` carries the operator's planning policy,
a two-way feature an engine lists beside planning or comparison:

- `PlanFeaturesRequest.policy` is a `PlanningPolicy`: whether a change the
  engine cannot make in place may be planned as a rebuild of its table, and the
  words a refusal names to ask for it (`table_rebuild_request`, always set),
  online alteration, concurrent index builds and drops for every index or the
  listed ones, and making a column `NOT NULL` without filling its rows. A policy
  is a request: an engine without the behavior plans as it would without it.
- Contract 25.8 adds `PlanningPolicy.statement_style`. The two-way feature
  `planning.statement-style` requires `planning.policy` on the provider and
  engine. `ANNOTATED` is the zero value; `EXACT` asks a feature owner to omit
  guards and cascading drops where its operation does not rely on them. An
  exact request is refused unless both sides advertise the feature, so an
  older owner cannot silently ignore the field. Unknown enum values are
  refused. [ADR 0050](../docs/adr/0050-statement-style-reaches-feature-owners.md)
  defines the owner behavior and safety boundary.
- `RequestedChange.written` carries an object as the operator wrote it, such as
  a secret's path for a rotation, set instead of `subject`. A comparison service
  that recognizes the spelling answers it once in
  `ComparedModels.resolved_requests` with a desired object it compares, and acts
  on it as on a request with a subject. The host refuses a written request no
  service resolved.

## YAML entry keys and HCL claims

[ADR 0036](../docs/adr/0036-a-provider-claims-yaml-entry-keys-and-hcl-blocks.md)
decides these claims. `source.entry-keys` adds the `COLUMN` and `INDEX` scopes
to `YAMLSourceKey`: a key of a column entry or of an index entry. Such a
fragment carries a common table binding with an entry key and a `subject` of
kind `column` or `index` with its table as the only path component. A YAML claim
of either scope and a Go claim can list `inherited`, the common keys of the
entry the owner reads too, such as an index's `type` and `ops`; the host sends
them in `inherited_attributes`, and a fragment of such a claim carries no other.
A host that does not list the feature gets no claim that needs it, and a model
left with no YAML claim drops YAML from its source formats.

`source.hcl` adds `Engine.hcl_source`: the HCL blocks an engine reads at the top
level and inside a table, and the attributes it adds to the frontend's table,
index, routine and materialized view blocks, with their attribute kinds,
children, inherited attributes and the header kinds it reads as limits. The
fields have the meaning of the host's HCL extension contract. A fragment carries
its frozen claim in `hcl_block`: a `SourceBlock` of the claim's type at the top
level and in a table, and a mapping of the owner's attributes at an attribute
site, with the binding and subject the site needs. `SourceDocument.limits`
carries an HCL document's header entries of the declared kinds. An engine that
declares no HCL claim keeps accepting HCL fragments without one, as earlier
minors do. Both features are two-way and need `source.decode`.

## HCL platform groups

[ADR 0072](../docs/adr/0072-a-provider-claims-the-platform-group-of-a-table.md)
decides this claim. `source.hcl-platform` adds the `TABLE_PLATFORM` site to
`HCLSourceBlock`: the `platform` block of a `table` whose label is the claim's
`type`, which is the engine's ID. Each `override` label of the group is an
attribute of the claim, and every attribute is a `STRING`. The claim takes no
naming, children, declared type, grant kind or inherited attribute. Its
fragment is bound to the table, has no subject, and carries the overrides as a
mapping.

A platform claim's models need not list HCL among their source formats, and it
brings them into an HCL document's scope anyway. A model that lists HCL needs a
claim at another site too, because a table that writes no group states nothing
about what the group carries. A host reads the kind-wide knowledge of a model
that lists no HCL as uninspected and keeps the subject knowledge a decoding
states for each table whose group it read.

A group spells part of a facet. In an HCL reply under the feature, a loss may
name a value a `TABLE_PLATFORM` part writes, once: the value is written in part,
and the line says what the document does not hold of it. Every other written
value takes no loss. Under `source.limits`, an encoding request that knows a
model less needs a limit only when the model lists the request's format, so a
read of selected directories writes a platform claim's model without one.

The provider and each engine that declares such a claim list the feature,
beside `source.hcl`. Without it negotiated, the description the host receives
holds no `TABLE_PLATFORM` claim and the same models.

## Source encoding

[ADR 0037](../docs/adr/0037-a-provider-encodes-the-source-it-decodes.md)
and [ADR 0040](../docs/adr/0040-a-source-value-can-require-several-claimed-declarations.md)
decide this service. `EncodeSource` is the mirror of `DecodeSource`. The host
sends DESIRED values, native objects and facets with their table binding, in a
format it writes (YAML, HCL or Go annotations). Every value is written once or
reported once as an `EncodeLoss` with one line of text. Every syntax part must
match a negotiated claim, its scope and owner, and have no source position.
HCL blocks also carry placement among the writer's blocks. A Go export carries the bound tables'
columns, and the reply may give a field the Go type of its engine type. The
provider never writes text of a format; the host writes the syntax.

A binding names its table as the table's facets name it, so the binding of a
table the host read names the database the read was made in, and the binding
of a table a source declared names none. A coverage subject the host holds
without that database, a read's table or column or an object of a
scope-selected kind, names it too, as the read's values do.

The request's coverage is what the document knows of the provider's models.
A document the host read describes what the read listed. A kind the read knows
completely in every namespace region it reports is sent as complete, with no
regions: the document is complete for the selection a later plan applies it
to, as an embedded engine's writer writes a read that `--schemas` narrowed.

A kind the read knows less in some region carries the regions the provider's
`Inspect` reported, with the same bounds: the host reads each region it spells
back and refuses the request, naming the model, when the reading differs from
the knowledge it holds. The region of a scope-selected kind names the one
database the document's values and other regions name, or no database when the
document names no single one. The provider decides whether its format can
keep that knowledge, through limits or by refusing the request.

With the two-way feature `source.encode-groups`, the reply uses
`EncodedSource.groups`: each group accounts for its `value_ids` once and writes
them through ordered `EncodedSourcePart` entries. Each part must match a value
in the group by model claim, binding and any explicit normalized subject; each
value must match a part. This supports one facet that needs separate directives
or keys without confusing distinct column owners on one table. Without the
feature, the reply uses `EncodedFragment` entries, each with its own exact-once
value accounting. A reply cannot mix the shapes. `EncodedSourceParts` exposes
their ordered parts after validation. A peer without groups receives a loss
when one fragment cannot preserve a value.

The two-way feature `source.limits` declares `Engine.go_header_kinds` for Go
not-described headers. Go input and negotiated encoded output accept only those
kinds and unique kind/name pairs. The host writes and captures the header's
kind, name, reason and provenance, then preserves its coverage through import.
An unrepresentable access constraint or coverage state remains a loss or refusal.

HCL and Go decoding may narrow coverage only when `source.limits` is negotiated
and the document has claimed headers. Every header must be valid; the returned
model inventory stays exact. The provider interprets which of its models and
subjects a header limits. The SDK validates these ownership and knowledge
boundaries, not opaque header semantics. A missing feature or a document without
headers retains complete authored scope. Round-trip tests must prove that the
host preserves the owner's narrowed knowledge instead of inventing absence.

`source.encode` is a provider service, listed by the provider and each
supporting engine beside `source.decode`. It adds no field to an existing
message, so it is not two-way; its fragments use only the claims the host
negotiated.

The SDK conformance tests in `runtime` negotiate each 25.3 addition in both
directions, prepare a declaration, render text and refuse a change without a
text form, refuse a native unit in the `TEXT` form from a peer that is not the
SDK server, and decode what an owner encoded back to the same canonical
projection.

## Authored declaration planning

[ADR 0041](../docs/adr/0041-a-provider-plans-authored-declarations-without-inventing-observed-absence.md)
defines `schema.declarations`. Each `Engine.declarations` assignment names a
service, its desired model kinds and its operation kinds. `PlanDeclarations`
takes an explicit engine and service, ordered desired objects, declared tables
and types, common creation steps, target features, identifier rules and
capability facts. It carries no current schema, coverage or session. An authored
object is not evidence of observed absence.

Each object's subject names its value's owner. A value converted from a read,
as when a read schema is rendered, keeps the owner the read gave it, bound to
the database the read was of, while the host names the subject relative to
that database. The SDK accepts the pair when the value's database context names
the owner's database, and refuses any other owner database.

A successful complete result contains contributions and one declaration receipt
per object in input order. Each receipt preserves the subject, names a strategy
and accounts for creation steps. Every contributed step has a receipt.
Dependencies name only contributed or supplied common steps. A completed refusal
contains only diagnostics with an optional object index. Invalid or canceled
calls expose no prefix. The host schedules the whole graph before rendering;
the provider never substitutes a private graph or rewrites a common step.

## Lifecycle placement

[ADR 0044](../docs/adr/0044-lifecycle-operations-use-the-complete-common-sequence.md)
defines the two-way feature `planning.lifecycle` and the `LIFECYCLE` operation
phase. Its placement window spans the complete common sequence, narrowed by
resource effects, explicit dependencies and rewrite constraints. The host
negotiates the feature only when its scheduler implements that window. The SDK
refuses the phase without negotiation. `DEPENDENT` keeps its point placement.

## Sensitive execution inputs

[ADR 0043](../docs/adr/0043-sensitive-input-references-are-approved-before-values-are-bound.md)
defines `units.sensitive-inputs`. `DescribeExecutionInputs` inspects the exact
ordered unit batch offline and returns complete, sorted, unique input
references. It reads no values and performs no I/O. SQL and native units use the
same boundary. The initial source is `ENVIRONMENT`, with an ASCII variable name.

The host includes references in reviewable plan metadata and its approval digest.
Its operator policy authorizes the references it may resolve. Immediately before
dispatch it re-describes cloned units, requires the approved references to match
and resolves their values. `ExecuteRequest` carries the selected engine,
`expected_inputs` and exactly one ephemeral binding per approved reference.
An explicitly empty value is valid; an absent binding is not. An engine without
input discovery accepts no references or bindings. The SDK server repeats
discovery and equality checks before invoking the executor.

Values never enter units, plans, history, rendered SQL, diagnostics or logs.
The provider checks the session's engine and binds values through its database
interface. Failed discovery or cancellation refuses before execution; dispatched
work retains the ordinary confirmation and uncertainty contract. A host lists
the feature only when the approval and dispatch paths are implemented. Providers
never fall back to inherited process environment values.

`RunStatement` has no sensitive-input bindings. An authorized write-only
statement that needs inputs uses `Execute` units. A query or result-bearing
statement that needs such inputs refuses; it cannot borrow process environment
values or silently discard its result contract.

## Password inputs

`schema.password-inputs` lets a declared role password travel as an
execution input instead of as a value. The provider and each engine that
takes such inputs list it, beside `schema.common-objects` and
`units.sensitive-inputs`, and the host lists it. With it negotiated, a
`RoleCreation` or a `RoleAlteration` that sets a password carries
`password_input`, an `ExecutionInput` the host names, and `password` stays
empty. That holds for a password and for a password hash alike, because the
host cannot tell an engine's hash form from a password. Without the feature,
`password` carries the value, as contract 25.3 sends it, and no input is
named.

The provider renders a unit that names the input and never a value, and
`DescribeExecutionInputs` reports it for that unit, so a plan, a migration file
and a saved plan hold the reference. The value reaches the provider only as an
execution binding, under the rules of
[sensitive execution inputs](#sensitive-execution-inputs). Where the engine
takes a password hash in another clause, the provider chooses the clause from
the bound value.

Both transport boundaries refuse, in a render request and in a planning step,
an input the session did not negotiate, a value once it did, a change that
carries both, an input with an unknown source or a name the environment
grammar refuses, an input for a role that declares no password, and an input
for an alteration that does not change the password. Their messages never
repeat a value.
[ADR 0071](../docs/adr/0071-a-declared-password-travels-as-an-execution-input.md)
records why a value on the wire is not kept.

## Operator statements

[ADR 0038](../docs/adr/0038-a-provider-runs-the-operators-statements-and-returns-what-they-read.md)
decides this service. `sessions.statements` runs a statement the operator
wrote, such as a `db verify` check, a test assertion or a script step, on a live
session, and returns what it read. The host composes none of these statements;
a plan's units still go through `Execute`. The provider lists the feature, and
each engine that supports it lists it too. It adds RPCs and no field to an
existing message, so it is not two-way.

- `RunStatement` takes one statement, its arguments as `DataValue` in the
  order the statement's placeholders take them or with their types under
  [typed arguments](#typed-arguments), an access, an optional
  transaction and a page bound from 1 to 10,000. `READ_WRITE` runs the
  statement as written. `READ_ONLY` runs it only when the owner proves, before
  running it, that it is one query that changes nothing and reads only objects
  of the session's database, and runs it where a write is refused; otherwise
  the reply is `REFUSED` and nothing ran. A read-only statement never names a
  transaction.
- A `SUCCEEDED` reply carries the result's columns as `DataColumn`, the first
  page of rows, and `rows_affected` only when the engine measured how many rows
  the statement changed: its absence means the count is not reported, which a
  host never reads as zero. `FAILED` carries the server's message for the
  statement, at most 4 KiB, with what the transport adds removed and never a
  credential, and the server's code. `UNKNOWN` says the provider cannot tell
  whether the statement ran; the host never sends it again.
- A result larger than one page stays open. `ReadStatementRows` takes its next
  page token and the result's column count, and `CloseStatement` releases it
  before the last page. The last page, the end of the transaction and the end
  of the session release it too.

Both transport boundaries refuse a statement without a session, a request ID or
UTF-8 text, an unset argument, a read-only statement in a transaction, and a
reply that changed its request ID, carries rows or a token on an outcome other
than `SUCCEEDED`, a message on one other than `FAILED`, a refusal on one other
than `REFUSED`, or a row that does not fit its columns or its bound. A reply to
a statement that may write, when it does not arrive or cannot be read, is an
unknown outcome; to a read-only one, it is an error.

### Typed arguments

[ADR 0058](../docs/adr/0058-a-statement-argument-carries-the-type-its-caller-bound-it-at.md)
decides this addition. A `DataValue` has one signed and one unsigned integer
of 64 bits, one double and a null of no type, so an engine that binds a
parameter at a type, such as YDB, cannot tell an `Int32` from an `Int64` or a
null `Optional<Uint64>` from a null of anything else. `sessions.typed-arguments`
adds `RunStatementRequest.typed_arguments`: each argument with the type its
caller bound it at. It is two-way. The provider lists it beside
`sessions.statements`, and a request carries typed arguments only when the
negotiated description lists it, then instead of `arguments` and never beside
them. A request names no engine, so a provider that lists the feature reads
typed arguments on every session.

A `StatementArgument` carries an `ArgumentType` and its value. The type is a
scalar at its width (`INT8` to `INT64`, `UINT8` to `UINT64`, `FLOAT32`,
`FLOAT64`, `BOOL`, `STRING`, `BYTES`, `TIMESTAMP` and `INTERVAL`), `optional`
when the type admits null, such as a Go pointer or a `sql.Null` value, and
`list` when the argument is a list of the scalar, such as a Go slice. `UNTYPED`
is a null of no type, such as Go's `nil`. A value keeps the `DataValue` case
it has under `arguments`: the signed scalars are `int64_value`, the unsigned
ones `uint64_value`, both floats `double_value`, and `INTERVAL` is
`interval_value`. A provider binds the value at the type: an engine without
parameter types can bind the value and ignore the type. A list carries its
elements, in order, and no value; an empty list still has its type.

Both transport boundaries refuse typed arguments the negotiated description
does not list the feature for, both forms in one request, an unknown or
unspecified scalar, an `UNTYPED` argument that is optional, a list or not
null, a value on a list or elements on anything else, an unset value, a null
the type does not admit, a value in another case than its scalar's, an integer
outside its scalar's range, and a `FLOAT32` value that a 32-bit float does not
hold exactly. `provider.ValidateRunStatementRequest` checks the arguments and
`provider.ValidateTypedArguments` the negotiation.

A host that does not list the feature sends `arguments`, so a provider that
lists it keeps reading them. A provider that does not list it receives
`arguments` alone, and the host refuses before sending an argument whose type
`DataValue` cannot carry exactly.

## Scope selections

[ADR 0039](../docs/adr/0039-a-provider-declares-how-a-scope-names-its-objects-and-which-grants-name-them.md)
decides these declarations. `scope.selections` adds `Engine.scope_selections`:
how the `--include`, `--exclude` and `--schema` selectors name the engine's
objects. It is a two-way feature; the provider lists it and so does each engine
that declares selections. A host that does not list it gets none and reads the
provider's objects as an earlier minor does.

Each `ScopeSelection` names a model kind with a DESIRED and an OBSERVED model,
the selector type that names it, such as `topic` in `*[type=topic]`, and its
naming. The host reads a selector's name with the naming; the owner never
parses a selector.

- `IN_SCHEMA` names an object by its schema and name, as a table is named, and
  its identity carries no database, catalog or path. `DATABASE_WIDE` names it
  by its name alone, in which a dot is a character. `SCHEMA_OR_DATABASE` names
  one in a schema as `IN_SCHEMA` does and one on the database by its name.
- `relation` says the object's name is a relation's, so excluding it excludes a
  table of its name.
- `grant_object_type`, such as `TOPIC`, is the object type a grant spells for
  an object of the kind, so a grant rides that object through a scope. Its
  YAML and annotation attribute is `on_` and the type in lower case with
  underscores, such as `on_topic`.
- `keeps_required` lists the kinds of the engine's other selections whose
  objects a kept object keeps when its normalized value requires them, unless
  an exclusion names them.

Both transport boundaries refuse a selection of a kind the engine does not
model as desired and observed, a kind selected twice, an unknown naming, a
selector type that is not lower-case letters, digits and underscores starting
with a letter or that two selections define with different naming or relation,
a relation that is not in a schema, a grant object type that is not upper-case
words, is a common target such as `TABLE`, or names two selections, and a kept
requirement that names no other selection.

The host reads a kind one way whichever engine holds it, so it refuses a
provider whose engines select one kind differently or compare its names by
other identifier rules, and a grant object type that two engines give to two
kinds. It refuses an object of a selected kind whose identity the naming cannot
read: an identity with a database, a catalog, a path, a parent or an overload,
a database-wide one with a schema, and one in a schema that leaves the schema
out when the engine has a default schema. A grant names an object in the
default schema without a schema, and the host reads the name with the engine's
identifier rules.

The SDK conformance tests in `runtime` page a statement's result, release one
before its last page, refuse a malformed reply from a peer that is not the SDK
server, leave a write whose reply cannot be read unknown, and negotiate the
scope selections in both directions.


## Common column semantics

[ADR 0045](../docs/adr/0045-a-provider-states-common-column-comparison-and-readback-semantics.md)
defines `schema.columns`. The provider and engine advertise this two-way
feature together with `schema.project-table-creations`. It does not grant
common-table planning authority. `runtime.Server.Columns` supplies the
`provider.Columns` implementation.

`CompareColumns` receives full declared and observed parent tables and the
host's explicit column pairs. Separate desired and current identities preserve
renames. Requested aspects are closed: `COLUMN_TYPE`, `COLUMN_DEFAULT`,
`COLUMN_NULLABILITY` and `COLUMN_SEQUENCE`. A complete ordered reply repeats
both identities and provides exactly one receipt for each requested aspect.
Type/default receipts state `SAME` or `CHANGED`; effective nullability requires
an explicit boolean, including false. Sequence deltas can change only identity
start and increment. Restart operations remain with the column-operation owner.
Domains and generated expressions retain their existing comparison owners.

`ReadColumnDefaults` interprets every supplied observed column, including
observed-only rollback columns. A typed receipt distinguishes absent defaults,
literals and expressions. Empty literals are valid. Absence requires absent
input and empty output; an expression requires nonempty input and output.
Neither service reads a database or substitutes defaults for unavailable facts.
Cancellation and incomplete replies expose no successful prefix.

With this feature negotiated, every `ProjectedTableCreation.columns` list
predicts the complete declared column inventory in order. Names remain exact;
effective type and nullability are required. Authored declarations remain
unchanged, and predictions establish no catalog inspection. An unnegotiated
peer refuses these new fields. The host consumes effective receipts separately
from the source fields used for export and operation operands.

## Complete column observations

[ADR 0048](../docs/adr/0048-common-column-observations-have-one-complete-wire-representation.md)
defines `schema.observed-columns`. When both peers negotiate it, every inspected
`Column` contains one `observed` value using the full `ObservedColumn` shape.
The wrapper retains the column name and native facets; its common type,
nullability, primary-key flag, and optional default remain empty. The complete
observation is the exclusive common fact source, including auto-increment,
identity start and increment, sequence restart, and owned-sequence identity.

The observed name must match its wrapper, ordinals must follow column order,
types must be nonempty, and nullability must be `YES` or `NO`. Optional default
presence distinguishes no default from an explicit empty value. Missing,
reordered, conflicting, or unnegotiated observations are refused. Native facets
retain independent evidence and cannot supply a second writable copy of a
common default or sequence setting. Providers refuse unrepresentable extras
rather than dropping them during conversion.

## Ordered primary-key observations

`schema.observed-primary-keys` carries each inspected table's complete primary
key in `Table.primary_key`, using `ObservedConstraint`. The provider and engine
both declare this two-way feature, which requires `schema.observed-columns`.
A peer refuses the record without negotiation.

The record binds the exact table name and schema and has type `PRIMARY KEY`.
Its `column_names` retains key order, which can differ from table-column order.
Every key member is a known, unique column whose observed primary-key flag is
true. A missing record means the table has no primary key, so all membership
flags must be false. Included columns are known, unique and separate from key
members. The singular `column_name` alias remains empty.

The record preserves its name, method, comment, included columns, deferral,
enforcement and hash-shard settings. Foreign-key, check, unique-null and
dependency attributes refuse. An initial mode requires deferral and is
`IMMEDIATE` or `DEFERRED`. The record is inspection evidence, never a table
creation operand. The host maps it into the common catalog without sorting
its members or inventing a name. The common source declaration owns the
writable key; native observations can retain independent evidence.
[ADR 0051](../docs/adr/0051-inspected-primary-keys-retain-their-common-order.md)
defines the ownership and export controls.

## Common index observations

`schema.observed-indexes` carries each inspected table's complete common
secondary indexes in `Table.indexes`, using `ObservedIndex`. The provider and
engine both declare this two-way feature, which requires
`schema.observed-columns`. A peer refuses the list without negotiation.

The list is the whole set: with the feature negotiated, an index the list does
not name is absent, so a host compares declared indexes with the held ones
instead of finding every declared index missing. Each index has a name unique
within its table and binds the table's exact name and schema. It names a key,
as columns or parts, and every key and covered column is a known column named
once. A part with neither a column nor an expression refuses. A primary key is
not an index here: `Table.primary_key` records it. An index the common model
cannot describe stays in the provider's native facets and is left out of the
list rather than approximated. The list is inspection evidence, never a table
creation operand.
[ADR 0052](../docs/adr/0052-inspected-indexes-reach-the-host-and-index-changes-reach-the-table-owner.md)
defines how the host compares the list and how an index change reaches the
provider that owns the table.

The host compares an index's uniqueness, key columns and covered columns with
the held index. Without [prepared index kinds](#prepared-index-kinds) it
compares no kind, so a change of kind alone plans nothing.

## Index facets on inspected indexes

`schema.observed-index-facets` lets inspection report the settings of a common
index a table holds as facets of that index, in `ObservedIndex.facets`. A
source states the same settings as a facet of the index its index directive
or entry declares, so the host compares one owner's facet on both sides, as it
compares a table's or a column's. Each facet is owned by its index:
`provider.IndexReference` builds the owner from the table's identity, with
kind `index`, the table as the only path component and the index's name. The
SDK places the facets of a table's indexes after its columns' facets in the
values the host normalizes, in the order the indexes are listed.

The provider and the engine both declare this two-way feature, which requires
`schema.observed-indexes` on the engine. A read carries index facets only when
its `target_features` list the feature, and a peer refuses one owned by
another index, by the table or by a column. With the feature negotiated, the
host treats every facet such an index carries as one an
[index rename](#index-renames) keeps: the rename is the owner's step, and the
host compares a renamed index's facets under the name the declaration gives
it.
[ADR 0068](../docs/adr/0068-an-inspected-index-carries-its-own-facets.md)
records the decision.

## Prepared index kinds

`schema.index-methods` lets table preparation state the kind of each common
index a table declares, in `PreparedTable.indexes`: one `PreparedIndex` per
`DeclaredTable.indexes` entry, in the same order. Its `method` spells the kind
exactly as `ObservedIndex.method` spells the kind of a held index, so the host
compares the two spellings exactly and plans a change of kind alone as a drop
and a create. An empty method is a kind like any other. The provider reads the
kind from the declaration alone, never from the held index. It refuses the
preparation of a kind it cannot read, and its schema validation gives the
reason.

The provider and the engine both declare this two-way feature, which requires
`schema.observed-indexes` and `schema.prepare-tables` on the engine. A reply
states kinds only for a request whose `target_features` list it. Both
transport boundaries refuse kinds without negotiation and, with it, a list
that misses a declared index, names one the table does not declare, names one
twice or leaves the declared order. The host keeps the source's type in the
declaration: the prepared kind serves the comparison alone.
[ADR 0057](../docs/adr/0057-a-provider-spells-a-declared-index-kind-as-it-reports-a-held-one.md)
records why the provider spells the declared kind.

## Index renames

`planning.index-renames` lets the owner of a table rename one of its indexes
in place. The host's comparison pairs an index the table holds with one the
declaration states under another name and with one definition, on a target
whose session reports the capability `index_rename`. For an altered table the
provider owns, the host then sends one common step per pair, with
`CommonPlanningStep.renamed_index` naming the index as the table holds it
(`from`) and as the declaration names it (`to`). The step reads the table,
drops the index subject `from` and creates the index subject `to`, and the
owner claims it with an operation that keeps the index, as it claims an index
addition or removal. An unclaimed step refuses the plan.

The provider and the engine both declare this two-way feature, which requires
`schema.observed-indexes` on the engine. A request carries a rename only when
its `target_features` list the feature. Both transport boundaries refuse a
rename that is not negotiated, that sits beside another operand or without a
parent, that names one index twice, whose parent the request does not alter,
whose parent does not hold `from` and declare `to`, or whose effects are not
the read, the drop and the create in that order.

A provider reports `index_rename` only to a host that lists the feature: a
host that does not list it has no step to send a pair with, and refuses the
plan. Without the capability the comparison pairs nothing, and a rename is a
drop and a create. A comment on a renamed index moves with it through the
[common-object wire](#common-objects): the host clears the comment under the
held name and sets it under the new one.
[ADR 0060](../docs/adr/0060-a-table-owner-renames-an-index-in-place.md) records
the decision.

## Change reversal

`models.reverse` lets the owner of a change kind undo an accepted change of
that kind. `ptah migrations generate` writes a reverse plan beside the
forward one, and a change a provider's comparison returned is an owner's
payload the host cannot invert. An engine declares one `ModelReversal` per
service in `Engine.reversals`, each naming the CHANGE models it reverses; a
change kind belongs to at most one service.

`ReverseChanges` is offline: no session, no database, no source document. The
host sends the accepted changes of one service in plan order, each as the
comparison returned it, and the provider returns one `ReversedChange` per
change, in order:

- `value` is a CHANGE value of the same kind whose operands describe the
  reverse direction. It is absent when the reverse has nothing to run, such
  as a value the server never returns, and then at least one limitation says
  what the rollback leaves as the forward change left it.
- `forward_state` is what the forward change leaves at the subject, as the
  owner predicts it: the subject itself (`OBJECT`, of the subject's kind) or a
  facet attached to it (`FACET`), each a complete OBSERVED value or, without
  one, the absence of one. The host plans the reverse against these
  predictions, which are never inspection evidence.
- `strategy` says how the reverse restores the prior definition, and
  `limitations` what it cannot recover, such as deleted rows or consumed
  messages. Each is one line, and the host writes them into the reverse
  migration as comments.

The provider and the engine list this two-way feature beside `models.plan` and
`models.compare`; a host lists it to receive the declarations and to call the
service. Both transport boundaries refuse a request whose changes are not
valid CHANGE values of the service's kinds, a result that changes its
subject or the change kind, a reverse without a change or a limitation, and a
forward state without a placement, of a kind with no OBSERVED model, of an
object of another kind than its subject, or named twice.
[ADR 0062](../docs/adr/0062-a-provider-reverses-the-changes-its-comparison-returned.md)
records why the owner reverses its changes rather than the host comparing
again.

## Unread tables

`schema.unread-tables` lets a read name each table it found and does not
describe, in `Schema.unread_tables`. An entry is the table's reference and a
`ProviderDiagnostic` that says why the read leaves it out, such as a YDB
replica table that no replication writes any more. A table named there exists,
so its absence from `tables` says nothing about it. Which tables a read leaves
out is the provider's decision.

The reason is one line under the rule of [error reasons](#error-reasons): the
provider writes it, names the table and why, and quotes no value, address or
credential. It is required, and the host shows it whether or not the session
negotiated `errors.reasons`.

The provider and each engine that reports such tables list the feature, and
the host lists it. Both transport boundaries refuse a snapshot with entries
when the feature was not negotiated, and an entry that is not a common table
of the read's engine (kind `table`, no catalog, path or overload), whose reason
`ValidateDiagnostic` refuses, that names a table twice, that names a table
`tables` or a table reservation also names, or that lies outside a filtered
read's scope. A read carries at most 4,096 entries. The host also requires the
session's database.

The host keeps the entries with the read and writes them into no document. Its
comparison refuses a declared table, and a foreign key, at an unread table
before preparation, in the provider's words, and its read commands print a
note naming each unread table and its reason.
[ADR 0063](../docs/adr/0063-a-read-names-the-tables-it-leaves-out.md)
records why none of the existing channels of a read carries this.

## Subtree coverage

`schema.subtree-coverage` lets a coverage region cover a schema and every
schema nested below it: `ModelNamespace.schema_subtree` in a read's coverage
and `ComparisonNamespace.schema_subtree` in a comparison's. An engine whose
schemas nest declares the character that separates them in
`IdentifierRules.schema_separator`, such as `/` for a YDB directory path,
which is one schema. A subtree region covers a subject whose normalized schema
is the region's, or begins with the region's followed by the separator; the
empty root schema covers every schema. `provider.SchemaWithin` is that rule,
and the host's coverage lookup and a provider's comparison both follow it.

A region names one exact schema without the flag, so an unfiltered read that
walks every directory can claim only the directories it listed, and a native
object declared in a directory that does not exist yet has an unknown current
state. With the feature, the read claims the root subtree complete and sends
an unknown subtree region for each schema it leaves out; unknown knowledge in a
matching region still overrides complete knowledge.

The provider and each engine that sends such regions list the feature, and
the host lists it. An engine that lists it declares a separator, and one that
declares a separator lists it; a separator is one character that is not a
letter, a digit or a space. A host that does not list the feature receives
neither the feature nor the separator. Both transport boundaries refuse a
subtree region in a read's coverage or a comparison's when the feature was not
negotiated, or, for a comparison, when its identifier rules carry no
separator, and a subtree region that names no schema. A filtered read sends
none: its regions fit inside the namespaces its selectors bind, which each
name one exact schema. A schema's subtree and the schema alone are different
regions.
[ADR 0064](../docs/adr/0064-a-read-covers-a-directory-and-everything-below-it.md)
records why a region with no schema does not serve.

## Report counts

`models.report-counts` lets an engine declare the object-count metrics each
value of a model contributes to, in `Engine.reports`, and lets the owner count
captured values with `ReportValues`. It is the host's reporting service carried
across the boundary: `ptah schema stats` writes one gauge per metric, and the
host cannot count a provider's values itself, because only the owner reads a
payload. A YDB topic value, for example, counts one topic and the number of its
consumers.

A `ModelReport` names one desired or observed model of the engine that has a
`display_name`, and one to 64 metrics. A metric has a `name`, lowercase ASCII
letters, digits and underscores that start with a letter, and a one-line
`help`. A report names each metric once, reports of one representation give a
name one help text, and engines that share a model report the same metrics for
it. A name the host counts itself, such as `tables`, makes `schema stats`
refuse. The reports sit on the engine rather than on the model, so a model's
copy in coverage or a request is the same whether or not the host counts.

`ReportValues` is offline: no session, no database. The host sends the values
of one engine and one representation, each of a reported model, and the
provider answers each value under its ID, in request order, with one count for
each metric of its model's report in the declared order. A count is at most
the largest signed 64-bit integer.

The provider and each engine that declares reports list the feature, and the
host lists it to receive the reports and to call the service; without it the
negotiated description carries no report. Both transport boundaries refuse an
unnegotiated call, a value of a model the engine does not report, values of two
representations, and a reply that is incomplete, answers in another order, or
counts a metric the model does not declare, twice, or not at all.
[ADR 0067](../docs/adr/0067-a-provider-counts-what-its-values-hold.md)
records why the owner counts, and why the counts are not part of a read.

## Comparison counts

`models.compare-counts` lets a comparison service say how many objects it
compared, in `ComparedModels.counts`: one `ComparisonCount` per change kind it
counts. A change record carries no direction a reader could count, so the
owner states its additions and removals itself. The provider lists this
two-way feature beside `models.compare`, and so does each engine whose
services count. A service counts only for a request whose `target_features`
list the feature. The SDK refuses counts in any other reply.

`matched` counts the objects both sides hold that the service takes to be one
object, whether or not they differ. An object the plan drops and creates again
is matched, because the service reports one change for it. `added` counts the
changes of the kind that create an object the observed schema lacks, and
`removed` the changes that drop an object the desired schema does not declare.
The desired schema holds `matched + added` objects of the kind, and the observed
schema `matched + removed`. An object the service leaves out counts nowhere:
one the run excludes, one it keeps without comparing, one it reports as
undecided, and one below a parent only one side holds, whose state the
parent's own change owns. A facet service counts the objects its settings
stand for, such as the tables with row-level security enabled, by the same
rules.

An entry claims every object of its kind in the batch. A kind the service
does not count has no entry, and a reader then knows nothing about the kind
rather than that it holds none. A kind the batch holds nothing of may have no
entry or an entry of zeros.

Both transport boundaries refuse counts without negotiation, a kind that is not
one of the service's change kinds, and a kind counted twice. They refuse
`added + removed` above the number of the reply's changes of the kind, since
one change cannot both add and drop an object, and more changes of the kind
than `matched + added + removed`. The changes of a kind are those whose value
has the kind, `ComparedChange.value.value.kind`; `ComparedChange.kind` names
the compared model.
[ADR 0056](../docs/adr/0056-a-comparison-service-counts-the-objects-it-compares.md)
records why the owner counts and what an older peer does.

## Native object assessment

[ADR 0049](../docs/adr/0049-native-object-context-is-assessed-without-inventing-schema-differences.md)
defines `planning.object-context`. An unchanged native object can need lifecycle
operations when another object changes. `ModelPlanning.context_kinds` assigns
assessment to one planner per native kind, with both desired and observed
contracts. The host invokes that service even without changes, table actions,
or named objects; unknown kind coverage can hide a dependent.

`PlanFeaturesRequest.desired_objects` and `current_objects` carry complete
owner-local `ComparisonState` captures and coverage. They remain visible beyond
the assigned kinds. The host derives the sorted, unique `changed_subjects` from
its complete accepted native-change batch before service assignment. This is
read-only evidence, never permission to operate on another kind. SDK validation
checks captured membership and ordering; it does not repeat sibling comparison
semantics. A missing capture or unresolved dependency context refuses. Services without
context assignments retain their ordinary request shape and carry no object
context fields.

`PlannedFeatures.objects` accounts for every assigned-kind subject in records
and native-subject coverage. Parent namespace claims remain captured knowledge
and create no object receipt. `provider.PlanningObjectSubjects` returns this ordered
union as independent copies, pairing normalized identities and preferring
desired spelling. Each receipt preserves exact identity and kind, states a
strategy, and names contributed steps once. Empty steps explicitly mean no work.
Change, parent, and object receipts can share steps; every operation must have a
receipt. Diagnostics can name a `context_object` index and carry no successful
output. The host validates and orders the complete graph.

An observed model can retain its database while its selected subject is local.
That localization requires both captured owner database spellings to equal the
request's nonempty `database_path`. Other identity components remain exact.
Foreign roots and invented replacement observations are refused.

## Planning context of a filtered read

[ADR 0054](../docs/adr/0054-a-filtered-read-carries-the-planning-context-it-does-not-compare.md)
defines `planning.context-inventory`, which requires `planning.object-context`.
A filtered read covers only its selected namespaces, and its snapshot may claim
nothing outside them. A planning service that must prove what lies outside,
such as one that asks whether any streaming query in the database reads a
changed input, gets that proof from `Schema.planning_context`.

The inventory holds the observed values of the engine's context kinds that the
read leaves out of its scope, and their coverage, which may span the session's
database. Each object lies outside the scope and has complete subject
coverage. An unfiltered read carries none. `provider.PlanningContextValues`
returns the values for the host to normalize.

The host never compares the inventory. It passes it to the planning service
that assesses those kinds as `PlanFeaturesRequest.context_inventory`, an
observed `ComparisonState` disjoint from `desired_objects` and
`current_objects`. Its subjects are outside the assessment inventory, so the
service returns no receipt for them. A plan saved by `ptah schema plan
--output` records a source fingerprint that covers the inventory, so the plan
is stale against a read whose planning context differs.

## Database kinds

[ADR 0065](../docs/adr/0065-a-filtered-read-reports-the-objects-of-its-database.md)
defines `schema.database-kinds`. Some objects belong to a database rather than
to a namespace of it, such as YDB's resource pools, and a read filtered to
namespaces would otherwise have to leave them uninspected. An engine lists the
kinds of such objects in `Engine.database_kinds`: each the kind of one of its
OBSERVED models, named once, and none a kind a planning service assesses as
context. The list is non-empty exactly when the engine lists the feature,
which the provider lists too, and a host that does not list it receives none.

An object of a database kind is named at the database: its reference has the
database and no catalog, schema, path or overload. On a filtered read the scope
rule gives way for a coverage subject of a database kind whose subject and
normalized subject are such references of the subject's own kind, and for a
coverage region of a database kind whose scope is the database namespace (the
database, an empty catalog and an empty schema, no path prefix). Everything
else a filtered read names stays inside its bound namespaces, and a database
kind cannot claim global completeness either. The host binds these subjects
and regions to the session's database and reads them as it reads an
unfiltered read's.

## Migration splits

[ADR 0055](../docs/adr/0055-a-provider-splits-a-migration-body-into-the-units-its-engine-runs.md)
defines `units.migration-split`. The provider lists it beside
`sessions.transactions`, which requires `units.execute`, and
`sessions.history`. Each engine that declares it also declares its own
transactions and history. It adds an RPC and fields to existing messages, so
it is two-way.

`SplitMigration` splits one migration body, in one direction and with its
directives, into the units the engine runs. It reads the text alone: the
request names the engine and carries the body and the migration's check
assertions, and no session, server version or database. Each unit carries the
text `Execute` receives, with every definition and setting it needs carried in
from earlier statements; `source`, the unit's own statements as written, which
partial hashes and failure records read; and a placement.

- `WITH_CHECKPOINT` changes data only. The host runs it in a serializable
  transaction of its own and commits its checkpoint in that transaction.
- `OUTSIDE` runs outside any transaction. The host marks it in flight, runs it
  and checkpoints it.

A body the engine cannot run as units is refused whole: no unit, the refused
byte range in the body, and a reason on one line that quotes nothing from the
body. A body with no statement has no unit. The reply carries one verdict per
assertion, in request order, beside a refusal too: `read_only`, or a reason on
one line saying why the assertion is not one statement that changes nothing.

`Execution.transaction_aborted` and `TransactionResult.aborted` say that the
engine aborted a transaction for a conflict and applied nothing, so a new
attempt may succeed. The first answers `NOT_APPLIED` in a caller transaction
only, and never beside `statement_timed_out`; the second answers `ROLLED_BACK`
to `COMMIT` only. The host decides whether to try again; the SDK never does.

Both transport boundaries refuse a request without an engine or a body, or for
an engine that does not declare the feature; a unit with empty `sql` or
`source` or no placement; units beside a refusal; a refusal range outside the
body, or a reason that is not one line; a verdict count unlike the request's;
a reason beside a `read_only` verdict, or none without one; and either abort
flag on another outcome or without the feature. A client that receives a
misused abort flag returns no reply and reports an unknown outcome, so no
caller reads the flag and tries again.

## SQL scanning

[ADR 0069](../docs/adr/0069-a-provider-scans-the-sql-text-of-its-engine.md)
defines `sql.scan`. The provider and each engine that scans its SQL list it. It
adds an RPC and nothing to an existing message, so it is one-way: a host that
does not know `ScanSQL` never calls it.

`ScanSQL` returns the tokens of one text of the engine's SQL. It reads the text
alone: the request names the engine and carries the text, and no session,
server version or database. `head` is set when the text begins where the server
reads settings ahead of a query, and cleared for a range that starts inside a
text. The tokens are the token types of the host's lexer: `UNKNOWN` for text
the server reads that is no other kind, `STRING`, `COMMENT`, `SEMICOLON`,
`WHITESPACE`, `IDENTIFIER` for a name, a keyword or a number, and `OPERATOR`.
They cover the text with no gap and no overlap, so each token carries only the
byte offset at which it ends. A `STRING` carries the text the literal stands
for, and an `IDENTIFIER` the name with its quoting undone, where the scanner
reads one.

The host uses the tokens wherever it reads the engine's text without parsing
it: view lineage, the indentation of the `sql` template function, placeholder
rewriting, statement splitting and the other scans of migration bodies.

Both transport boundaries refuse a request for an engine that does not list
the feature, or whose text is not UTF-8; and a reply whose tokens do not reach
the end of the text, with an empty token, a token that ends past the text or
inside a UTF-8 sequence, a token of no known kind, or a value on a token that
is neither a string nor an identifier. `Server.Validate` refuses the feature
without `Server.SQLScanner`, and the scanner without the feature.

## Offline capabilities

[ADR 0070](../docs/adr/0070-a-provider-states-the-capabilities-its-engine-is-planned-with-offline.md)
defines `engine.default-capabilities`. The provider and each engine that
states its facts list it, and so does the host. It adds a field to an existing
message, so it is two-way: negotiation removes `Engine.default_capabilities`
for a host that does not list the feature.

`Engine.default_capabilities` holds the capability facts the host plans the
engine with when no session states the server's: an offline render, a
generation without a database, an export. They are the facts of the release
line a command assumes without a server. `ReleaseLine.capabilities` holds each
line's, which the host plans with for a `--server-version` that names the
line: one equal to `line`, or the longest line it starts with followed by a
dot. A present false is an established answer, and an absent key is unknown,
as in `Session.capabilities`. A command that connects plans with the session's
facts.

`ValidateDescription` refuses the fields on an engine that does not list the
feature, the feature on an engine with no default facts, on one with a
release line that has none, or without the provider's feature, and a name a
session could not state.
