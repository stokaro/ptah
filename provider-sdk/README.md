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
speaks contract 25.2. `provider-sdk/v0.3.0` speaks contract 25.1, and
`provider-sdk/v0.2.0` speaks 25.0.

**What changes which number.** The minor counts compatible additions within a
major. The major changes only with an incompatible change, and the minor then
returns to 0.

- A compatible change adds something an older peer can ignore: a new RPC,
  message, field, enum value or feature. It raises the minor by one. Contract
  25.1 adds, for example, the RPC `ReadRows`, the field
  `ExecuteRequest.statement_timeout_nanoseconds`, the enum value
  `LINT_FACT_STATEMENT_KIND` and the feature `database.dev-cleanup`. Contract
  25.2 adds the [embedding store](#embedding-store).
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
  `query.dialect` and `embeddings.store`. The embedding store's calls are new,
  and its engine declaration is an addition to `Engine`, so a host lists the
  feature to receive either.
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
| `Open` | A session handle for the requested engine, identifier rules, and established server capabilities. Credentials belong in this request, never in process arguments. |
| `Close` | Release one session. Repeated closes succeed. |
| `Inspect` | A complete snapshot with explicit dispositions for observed fields. Incomplete state is an error, never an empty database. |
| `Render` | One executable unit for every input change, in input order. A refused batch exposes no executable prefix. |
| `AnalyzeSQL` | Complete owner-defined statement boundaries, tokens, and explicit safety dispositions. Unsupported syntax refuses the whole batch. |
| `DecodeSource` | Complete offline interpretation of captured extension fragments, frozen model scope, and original source traces. |
| `ReadSQLSchema` | A complete offline reading of every SQL schema document of an engine that declares the `DOCUMENT` mode: declared objects, per-document declarations, coverage and positioned diagnostics, or an error and no schema. |
| `ConvertValues` | An offline conversion between desired and observed models with preserved identity and a complete normalized result. |
| `CompareModels` | A complete offline comparison of captured objects or facets, independent coverage sources, common context, and typed changes. |
| `PlanFeatures` | Complete offline graph contributions and ordered change and parent receipts, or diagnostics with no successful output. |
| `PrepareTables` | Complete prepared key membership and declared facet adjustments from captured table state. |
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

Session capabilities preserve presence: an explicit `false` is an established
answer, while an absent key remains unknown. The SDK validates names and the host
validates its capability vocabulary and dependencies. Invalid session replies
return no usable handle; both transport boundaries attempt to close a returned
handle before refusing the reply.

Common records represent tables and columns explicitly. Native records use a
versioned provider-owned envelope. Structured references keep database, schema,
path, kind, name, and overload components separate. The transport does not fold
names or concatenate components into identity strings.

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
| Facets and feature objects owned by the provider | `native_objects` |
| What the header says is not described | `coverage` |
| Functions, row-level security policies and enabled tables, managed data, embedded fields | None. The provider reports `UNSUPPORTED` and names the construct. |

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
desired schema, normalized `CHANGE` values, and explicit undecided diagnostics.
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
retention to SQLite. The host YAML frontend captures negotiated document and
table keys for this RPC. The host Go annotation adapter captures the claimed
directives of a Go file at every scope, with attribute values as text, and the
attributes a provider claims on Ptah's table, field and index directives. It
refuses a claim on any other Ptah directive. The host HCL
adapter, official-engine adapters, complete native planning, and history
remain part of #4204; this transport is not a claim of YDB source support.

## Observed model coverage

`Inspect` returns its engine, selected target features, and explicit observed
model coverage even for an empty database. The host matches the engine and
features to the inspected session. A missing coverage record is not evidence
that a model has no objects.

`provider.InspectionValues` validates native objects, table facets, and column
facets against their negotiated model definitions and complete subject records.
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
4,096 regions.

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
inside the bound namespaces. Filtered replies cannot claim global completeness
or include dispositions without normalized identities. Requests and bindings
are limited to 4,096 entries, with at most 4,096 namespaces across all bindings.

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
captured values locally, without interpreting payloads or declaring count metrics.
An absent label does not get a host-invented substitute.

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
for any other unit. A migration file accepts text units only.

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
storage calls. The fixture provider does not serve the store;
[issue #4204](https://github.com/stokaro/ptah/issues/4204) owns its SQLite
implementation and the process tests.
