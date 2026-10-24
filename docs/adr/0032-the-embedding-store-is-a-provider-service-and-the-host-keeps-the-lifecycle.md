# ADR 0032: The embedding store is a provider service, and the host keeps the lifecycle

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Answers: the follow-up record that
[ADR 0028](0028-a-provider-owns-how-rows-are-read-written-and-seeded.md) requires
in its consequences, "YDB embeddings leave the host before the embedded engine is
deleted".

Relates to: [ADR 0013](0013-inference-state-transitions-answer-with-their-provenance.md),
[ADR 0014](0014-the-outbox-boundary-is-a-transaction-and-the-order-is-a-sequence.md),
[ADR 0016](0016-an-outbox-event-lives-until-every-live-generation-has-passed-it.md),
[ADR 0017](0017-a-chunked-corpus-is-a-set-per-source-row.md),
[ADR 0021](0021-provider-recognition-does-not-authorize-execution.md),
[ADR 0023](0023-engine-knowledge-moves-with-the-provider.md),
[ADR 0031](0031-official-providers-are-installed-separately-and-ptah-pins-no-provider-release.md)

Scope: the run state, the generation storage, the vector writes, the
verification reads and the nearest-neighbor searches of `ptah inference` and of
the inference tools of `ptah mcp`, on an engine that a provider serves. The
PostgreSQL backend stays in the host and does not change. The `outbox`
consistency mode on a provider engine is decided here only as far as the shape
of its later addition.

## Context

The owner decided that `ptah inference` on YDB keeps working when `ydb://` moves
to the external provider `stokaro/ptah-ydb-plugin`. ADR 0031 deletes the embedded
YDB code in the release that makes the provider serve `ydb://`, and ADR 0028 makes
this record a precondition of that release.

What the code does today:

**The lifecycle is engine-neutral, and one interface reaches the engine.**
`embedbackend.Backend` has 21 methods, and the `Store` it returns has 17. The
native CLI (`internal/cli/inference`), the inference tools of `ptah mcp`
(`internal/agentapi`) and the readiness gate (`internal/embedreport`) reach a
database only through them. `internal/embedconnect.Open` picks the implementation
by URL scheme: `internal/embedpg` for PostgreSQL, and `internal/embedydb` for
`ydb://` and `ydbs://`. `ptah inference describe` has no database, and picks YDB
when the specification's representation is `float`.

**The YDB half is YQL written in the host.** `internal/embedydb` is 3,424 lines of
non-test Go in 14 files. It imports `ydb-go-sdk/v3` to classify an abort it may
retry, `internal/dbschema/ydb` to describe a row table, and Ptah's YDB index and
comment helpers. It spells keyset scans over the primary key, `UPDATE` and
`UPSERT ... SELECT` writes, `ALTER TABLE ... ADD INDEX ... GLOBAL USING
vector_kmeans_tree`, and `Knn::CosineDistance` with its siblings. It stores a
vector as the bytes `Knn::ToBinaryStringFloat` writes.

**The YDB store already decides with the host's rules.** Each lifecycle operation
reads the rows it decides on inside one serializable transaction, builds an
`embedstore.Memory` from them, applies the in-memory store's rule, and writes back
only the rows that changed. YDB aborts a transaction whose reads another one
changed, and the operation runs again on what is there now, up to 12 attempts.
`embedstore.Snapshot` exists for this pattern. A batch commit reads the run and
what the target holds at the batch's keys, decides the fencing check and
`embedrun.ResolveWrite`, and writes the winning sets and the checkpoint in the
same transaction. The decisions are the host's. The transaction and the
statements are YDB's.

**What YDB cannot do is refused by name.** The `outbox` consistency mode needs
row triggers, which YDB lacks. An index needs the `vector_indexes` capability, a
write into an indexed generation needs `vector_index_maintained_on_write`, and the
key fields must be the source table's primary key.

**The tests that cover it run YDB in Ptah's pipelines.**
`integration/embedydb_store_live_test.go`, `integration/inference_ydb_cli_e2e_test.go`
and `integration/agentapi/inference_ydb_live_test.go`, 891 lines together, run the
store, the CLI lifecycle and the MCP tools against a live YDB server.
`internal/embedydb` has one unit test file.

**A provider session cannot carry any of it.** `internal/embedydb` works over a
`*dbschema.DatabaseConnection` on `database/sql`. The provider SDK has no
embedding operation, and a provider session is a handle on the other side of gRPC.

**PostgreSQL holds the same contract another way.** `internal/embedpg` (6,917
lines) takes advisory locks instead of retrying aborts. It holds a source-scoped
lock from target creation through run creation (ADR 0016), and it captures source
changes with two triggers and a companion table (ADR 0014). It stays in the host.
The service below has to be one that a pgvector provider could implement as
well, so each part of it was checked against what `internal/embedpg` does.

## Decision

The host keeps the lifecycle, and the provider owns the storage. A new provider
service, `embeddings.store`, carries every operation `embedbackend.Backend` sends
to a database. An adapter in the host implements `embedbackend.Backend` over it.
The engine-neutral packages do not change.

### Who owns what

The host keeps what is the same on every engine:

- the specification, its identity, preprocessing, chunking and input hashes;
- the embedding endpoint (`internal/embedprovider`) and every request to it.
  Source text crosses the provider boundary only as the rows below, and vectors
  only as the writes and searches below;
- the backfill and catch-up loops, the batch bounds, and their order: embed, then
  commit the vectors with their checkpoint, then record the event;
- every lifecycle rule: the decisions of `embedstore.Memory` (register, claim,
  save, abandon, pointer moves, maintenance, retirement), the phase transitions,
  the fencing token and `embedrun.ResolveWrite`;
- verification, evaluation scoring, cutover and rollback decisions, plans,
  approvals and reports;
- the names of a generation's objects: `embedgen.IndexName`,
  `embedgen.TableComment` and the vector column's comment. The host sends them in
  each request, so the name that is created and the name that is dropped come
  from one derivation on every engine;
- the retry policy after a conflict.

The provider owns what depends on the engine:

- the layout of the run-state store: its tables, columns, keys and types, their
  creation and verification, and the atomic compare-and-write that serializes
  lifecycle decisions;
- how a schema and table spelling resolves to a relation;
- whether the engine can hold a specification, and the column type, index method
  and operator a generation needs;
- creating a generation's storage, and proving that storage is the generation's
  before it drops any;
- reading the source by keyset and by key, reading and writing the target,
  encoding vectors, and refusing a value the engine cannot store;
- building the index and reporting whether it is ready;
- reading back the structure, the row counts and the verification walk;
- nearest-neighbor search through the index and without it, and the session
  settings that decide what a search returns;
- the measurement a plan reports.

A provider stores what the host decided and does not decide it again. It reads
records, compares them and writes them whole, and refuses nothing for a lifecycle
reason. Every lifecycle refusal is the host's, from the same rules and in the
same words on every engine.

### The service

One feature, `embeddings.store`, switches the service on. The provider lists it in
`Description.features` and in each supporting engine's features. The host lists it
in `DescribeRequest.host_features`, because the service adds a declaration to an
existing message, `Engine.embeddings`. The SDK drops the declaration and refuses
every call when either side does not list the feature.

```proto
// Engine gains: EmbeddingEngine embeddings = 15;
message EmbeddingEngine {
  // The consistency modes a generation on this engine can be built with, by the
  // host's names, such as "immutable".
  repeated string consistency_modes = 1;
}

// The run-state store. Session-bound.
rpc PrepareEmbeddingStore(PrepareEmbeddingStoreRequest) returns (EmbeddingWriteResult);
rpc ReadEmbeddingState(ReadEmbeddingStateRequest) returns (EmbeddingState);
rpc MutateEmbeddingState(MutateEmbeddingStateRequest) returns (EmbeddingWriteResult);
rpc AppendEmbeddingEvent(AppendEmbeddingEventRequest) returns (EmbeddingWriteResult);
rpc ReadEmbeddingEvents(ReadEmbeddingEventsRequest) returns (EmbeddingEventPage);

// A generation's storage. DescribeEmbeddingTarget is offline; the rest are
// session-bound.
rpc DescribeEmbeddingTarget(DescribeEmbeddingTargetRequest) returns (EmbeddingTargetObjects);
rpc ResolveEmbeddingRelations(ResolveEmbeddingRelationsRequest) returns (EmbeddingRelations);
rpc MeasureEmbedding(MeasureEmbeddingRequest) returns (EmbeddingMeasurement);
rpc PrepareEmbeddingTarget(PrepareEmbeddingTargetRequest) returns (EmbeddingWriteResult);
rpc BuildEmbeddingIndex(BuildEmbeddingIndexRequest) returns (EmbeddingWriteResult);
rpc ReadEmbeddingStructure(ReadEmbeddingStructureRequest) returns (EmbeddingStructure);
rpc DropEmbeddingGeneration(DropEmbeddingGenerationRequest) returns (EmbeddingWriteResult);

// Rows, vectors and searches. Session-bound.
rpc ScanEmbeddingSource(ScanEmbeddingSourceRequest) returns (EmbeddingSourcePage);
rpc ReadEmbeddingSourceRows(ReadEmbeddingSourceRowsRequest) returns (EmbeddingSourceRows);
rpc ReadEmbeddingBatchState(ReadEmbeddingBatchStateRequest) returns (EmbeddingBatchState);
rpc CommitEmbeddingBatch(CommitEmbeddingBatchRequest) returns (EmbeddingWriteResult);
rpc ReadEmbeddingVerification(ReadEmbeddingVerificationRequest) returns (EmbeddingVerificationPage);
rpc SearchEmbeddings(SearchEmbeddingsRequest) returns (EmbeddingSearchResult);
```

Each method of `embedbackend.Backend` maps onto the service:

| Host method | Service |
| --- | --- |
| `EnsureSchema` | `PrepareEmbeddingStore` |
| The reads of `Store()` | `ReadEmbeddingState`, `ReadEmbeddingEvents` |
| The decisions of `Store()`, and `ReachPhase` | `ReadEmbeddingState`, the host's rule, then `MutateEmbeddingState` |
| `AppendEvent` | `AppendEmbeddingEvent` |
| `ResolveRelation`, `WithResolvedRelations` | `ResolveEmbeddingRelations` |
| `TargetObjects` | `DescribeEmbeddingTarget` |
| `CheckMode`, `Outbox` | `Engine.embeddings.consistency_modes` |
| `PrepareRun` | `PrepareEmbeddingTarget`, then `MutateEmbeddingState` |
| `NewSource` | `ScanEmbeddingSource`, `ReadEmbeddingSourceRows` |
| `NewTarget` | `ReadEmbeddingBatchState`, the host's rules, then `CommitEmbeddingBatch` |
| `EnsureRunIndex` | `BuildEmbeddingIndex`, then `MutateEmbeddingState` |
| `NewSearcher` | `SearchEmbeddings`, with the settings from `ReadEmbeddingStructure` |
| `TargetColumnExists`, `ReadStructure`, `CountGenerationRows`, `GenerationIndexExists` | `ReadEmbeddingStructure` |
| `VerificationCorpus` | `ReadEmbeddingVerification` |
| `RetireGeneration` | `ReadEmbeddingStructure`, `MutateEmbeddingState`, then `DropEmbeddingGeneration` |
| `Measure` | `MeasureEmbedding` |

### The run-state store

The store holds four kinds of record: generations, runs, pointers and events.
Each is a typed message whose fields are the fields of `embedstore.Generation`,
`embedrun.Run`, `embedstore.Pointer` and `embedrun.Event`, so a provider can keep
each field in a column of its own. Phase, status, event kind and failure class are
text the host defines. The provider stores that text and does not read it. A time
is UTC seconds and nanoseconds, and an absent time is the zero time.

`ReadEmbeddingState` reads one scope as one consistent snapshot. A scope is every
generation, run and pointer; one run; one generation; the runs of one generation;
or one pointer. The reply lists the records in key order, with a token. The token
is a digest of exactly those records. Equal records give an equal token within one
provider build, and any change to a record in the scope gives another, including
a record that appears. A store that does not exist is `exists=false`, which
differs from an empty store. A scope larger than the caller's bound, at most
10,000 records, or than the message limit is an error, never a partial snapshot.

`MutateEmbeddingState` carries a scope, the token the host read, and the records
the decision changed. The provider reads the scope again. When the token it
computes equals the expected one, it writes every record whole and commits, in
one atomic operation. It answers one of these outcomes:

- `APPLIED`, with each written record as a later read returns it, so a time the
  engine keeps to the microsecond comes back as stored;
- `CONFLICT`, when the token differs. Nothing was written;
- `REFUSED` with a reason, when the write cannot be made at all. Nothing was
  written;
- `NOT_APPLIED`, when the engine refused in a way that proves nothing committed
  and the provider stopped retrying;
- `UNKNOWN`, when the provider cannot prove whether it committed.

A mutation writes only records its scope covers, and never deletes one, because
no lifecycle rule removes a record. The host decides a lifecycle operation the way
the YDB store decides it today: it reads, decides on an `embedstore.Memory` built
from the snapshot, and mutates with the token. On `CONFLICT` it reads and decides
again, up to the host's bound.

`AppendEmbeddingEvent` checks that the run exists, gives the event the run's next
sequence number, and writes it, in one atomic operation. It answers the sequence.
`ReadEmbeddingEvents` pages a run's events in sequence order.
`PrepareEmbeddingStore` creates a missing store and verifies an existing one. It
refuses a layout it does not recognize as its own, and does not upgrade one.

### A generation's storage

Every request about a generation carries the parts of the specification the
engine needs, never the specification document:

- the source relation, its key fields, input fields, version field and filter;
- the target relation, column, layout, representation, metric, dimension, index
  method and index options;
- whether the specification chunks, the generation identity, and the object names
  the host derives.

The filter is the specification's own boolean expression in the engine's dialect,
which is what that field is for, and the provider is the only side that reads it.
Every other name travels as a separate component, never as SQL text.

- `DescribeEmbeddingTarget` is offline. It answers the column type, whether the
  generation owns a table and whether that table references its source, the index
  method and operator, and the extensions the engine needs. Otherwise it answers a
  refusal that names what the engine cannot hold.
- `PrepareEmbeddingTarget` creates the generation's storage, or verifies that a
  prepared one is still complete. It is idempotent. It refuses a column that holds
  another generation, a table it cannot prove is the generation's, and key fields
  the engine cannot address a row by. It adopts a table without the ownership mark
  only when the table is empty and has exactly the shape a generation creates.
- `BuildEmbeddingIndex` builds the declared index, or rebuilds one whose build did
  not finish. It answers `NOT_DECLARED`, `ALREADY_VALID`, `BUILT` or `REBUILT`.
- `ReadEmbeddingStructure` answers the column, its type and dimension, the index,
  whether the index is ready, its method and operator, whether the engine's vector
  support is present, the settings a search runs under, and on request the number
  of rows the generation holds. It takes a location from a registry record rather
  than a specification, because a retirement has only the record.
- `DropEmbeddingGeneration` drops the index, and the columns or the table the
  approval names. It checks ownership before it drops anything, so a refusal drops
  nothing. A drop that stopped part way is `UNKNOWN`, and running it again
  finishes it.
- `MeasureEmbedding` answers what a plan reports: whether the source, the target
  table and the column exist; the number of rows in scope, or that they were not
  counted; the vector capabilities by the names the plan holds a database to
  (`vector_type`, `vector_index`); whether the declared index can be built; and
  refusals, one line each.

Preparing storage and recording the run are two calls on every engine. The
storage step is idempotent and the record is written once, so a prepare stopped
between them is finished by running it again. This is YDB's order today. The
source-scoped lock PostgreSQL holds across both exists for change capture (ADR
0016), and it arrives with change capture.

### Rows, vectors and searches

`ScanEmbeddingSource` reads one keyset page after a cursor, in key order. Each
row carries its key components and version as text, and each input field as null
or text. `ReadEmbeddingSourceRows` rereads a set of keys under the filter, and a
key missing from the answer is a row that is gone. A key travels as text in the
host's key order, because that is the one form every key type has. The provider
converts it back to the column's type.

A batch commit is a compare-and-write over the run and the batch's keys:

1. `ReadEmbeddingBatchState` returns the run, what the target holds at each key
   (the generation, input hash, version and kind of the set's first row), and a
   token over both.
2. The host decides: the fencing check of `embedstore.Memory.SaveRun`, then
   `embedrun.ResolveWrite` for each key.
3. `CommitEmbeddingBatch` carries the token, the checkpointed run and the winning
   sets. The provider rereads the run and those keys and compares the token. It
   then makes each key's stored rows equal to its set, writes the run, and
   commits, in one atomic operation. A set is ordered by ordinal from 0. A skip or
   a tombstone is a set of one at ordinal 0, which is how ADR 0017 stores an empty
   set.

A vector is a list of float32 values of the generation's dimension. The SDK
refuses a NaN or an infinity in a write and in a search, because verification
relies on the writer to keep them out. A write into an indexed generation, on a
server line whose index does not take later writes, is `REFUSED` with the reason
`UNSUPPORTED`. A batch travels in one request, so the host bounds a batch by the
message limit. At 1,536 dimensions a vector is 6 KiB, and 4 MiB holds about 600
rows with their keys.

`ReadEmbeddingVerification` pages the walk that `embedverify.Verify` folds over.
Each row carries the key, the source side when the row is in scope, and the stored
side when a row stands at the key. The source side is the input fields as null or
text, and the version. The stored side is the generation, input hash, version,
kind, ordinal and the width of the stored vector. A row never carries a vector.
Rows of one key are adjacent, and a page never splits them. Each page reads both
sides of its keys in one read, so a row written between two reads cannot appear on
one side only. The host computes the input hash, as it does today.

`SearchEmbeddings` returns the keys nearest to one vector, through the index as an
application's query would and by exhaustive scan, and says whether an index served
the first list. A failed search is a reply with a reason, which the evaluation
records against its case, not a failed call.

### Atomicity and outcomes

- `MutateEmbeddingState`, `CommitEmbeddingBatch` and `AppendEmbeddingEvent` are
  each one atomic operation in the provider, and `APPLIED` means committed. No
  call joins a transaction of the host's. The service needs neither
  `sessions.transactions` nor `units.execute`.
- Before it answers, the provider may run its operation again after the engine
  aborted it in a way that proves nothing committed, a bounded number of times.
  Each attempt rereads what it compares. This is the retry an optimistic engine
  such as YDB needs, and it is not a lifecycle retry: a `CONFLICT` goes back to
  the host, which decides again.
- A lost reply, a cancellation after dispatch and an invalid reply to a write are
  `UNKNOWN`, and the SDK never resends. The host stops, reports, and reads the
  state again before anything else decides. A batch commit is atomic, so the run
  read back says whether its checkpoint landed.
- A read is complete or it is an error. A page that cannot finish returns no rows,
  and a snapshot that does not fit returns no records.
- Every write request carries a request ID, and its result echoes it.

### Trust

The session calls act on a session the host opened under ADR 0021, and
credentials travel only in `Open`. The provider writes only to the run-state store
and to the storage of the generation a request names. It creates, alters and
drops only objects whose names the request carries, and it drops only storage it
can prove belongs to that generation.

Source text, vectors and the filter can be sensitive. A diagnostic is one line
that names objects and a reason. It carries no row content, no vector, no filter
and no credential. The event record has no field for row content, as
`embedstore.EventFields` has none today.

### Versioning

The service is a compatible addition, so it is contract 25.2 (ADR 0031).
`provider.ContractMinor` becomes 2, and `provider.Revision` stays 25.
`Engine.embeddings` is an addition to an existing message, so `embeddings.store`
is a two-way feature. A host of contract 25.0 or 25.1 never sees the service, and
a provider of an older minor never receives a call. A provider that serves the
service does not raise its minimum contract for it. A host that does not list the
feature still gets everything else the provider serves.

The SDK release that carries contract 25.2 is `provider-sdk/v0.4.0`. The host and
the fixture keep requiring v0.3.0 until a second pull request moves them, and each
pull request passes on its own.

### Change capture is a later minor

No engine a provider serves can capture source changes in the writer's own
transaction today. Contract 25.2 therefore has no change-capture call, and a
provider engine declares only `immutable`. The host refuses another mode and names
the modes the engine declares.

When an engine can capture changes, a later minor adds the feature
`embeddings.change-capture` with the calls `embedbackend.Outbox` makes:

- install the capture and the boundary at prepare;
- read the settled events after a cursor, up to a horizon;
- count the unprocessed events, and report the horizon;
- prune below the floor that every usable live feeder has passed.

Its rules are those of ADR 0014 and ADR 0016. The transaction identity says
whether an event is settled, the sequence orders events, and a prune never passes
a live feeder. The cursor stays the host's pair of numbers, and the provider
defines what they mean on its engine.

### How the host adapts

A new host package beside `internal/embedpg` implements `embedbackend.Backend`
over a provider client and a session. `embedconnect.Open` selects it for a URL
whose engine a provider serves, through the selection ADR 0031 defines.
`ptah inference` gains the shared `--provider-config` and `--provider-engine`
flags, and `inference describe` uses `--provider-engine` instead of the
representation to choose the engine. The host lists `embeddings.store` in its host
features. The engine-neutral packages and `internal/embedpg` do not change.

### What moves

- `internal/embedydb` moves into the plugin and is deleted from the host, with
  the YDB branch of `embedconnect` and its `float` rule.
- The three YDB test files under `integration/` move into the plugin's pipeline.
  The owner decided that the YDB parity tests of this service run there, which
  settles for embeddings the question ADR 0028's amendments leave open. Ptah's
  pipelines test the service on the fixture provider.
- The plugin keeps the table names, columns, keys and vector encoding the host
  writes today: `ptah_embedding_generation`, `ptah_embedding_run`,
  `ptah_embedding_event` and `ptah_embedding_pointer` with their keys, the five
  columns beside a source row, and the `Knn::ToBinaryStringFloat` bytes. A
  database whose generations and runs the embedded engine wrote is then resumed,
  verified, cut over and retired by the plugin. That state is the operator's
  data, so its layout does not change at the flip.

## Alternatives considered

**Keep YDB embeddings in the host after the flip.** `internal/embedydb` would stay
linked with the YDB driver. ADR 0031 deletes the embedded YDB code in the release
that makes the provider serve `ydb://`, and ADR 0023 moves engine knowledge to its
provider. Rejected.

**A general SQL proxy.** The host would get `Query` and `Exec` calls with text and
keep the YQL of `internal/embedydb`. The host would go on composing YQL, which ADR
0028 rejected for rows for the same reason. Rejected.

**Let the provider run the lifecycle.** The provider would run the backfill loop,
the leases and the verification. Every provider would reimplement ADRs 0013 to
0017, and the same refusal would read differently on each engine. Rejected: only
the storage depends on the engine.

**Host-driven transactions over `sessions.transactions`.** The host would begin a
serializable transaction, read and write through calls that name it, and commit
with `FinishTransaction`. That keeps an optimistic transaction open across round
trips and host work, which widens every conflict window on YDB. The transaction
service has no typed answer for "aborted by a conflict, nothing committed": such a
commit reads as `ROLLED_BACK` with free text. The service would also depend on
`units.execute`. Rejected: a compare-and-write keeps each atomic step inside one
provider call.

**Send the expected records instead of a token.** The provider would compare each
record the host read, which is as correct and keeps the comparison visible in the
request. A decision over every generation, run and pointer would then send the
whole store twice in one exchange. Rejected for size, since the token is a digest
of the same records.

**Generic records described by the host.** The host would send column descriptors,
and the provider would store maps of values. The host would then own the layout,
against the owner's split, and the YDB provider would still have to keep the
layout today's tables have. Rejected.

**Keep run state outside the target database.** A file or a control plane could
hold it. A checkpoint must not advance past vectors that are not committed, and
only one transaction in the target database can promise that (`embedstore`).
Rejected.

**Add change capture now.** It would make the service complete for PostgreSQL. No
provider engine can implement it, so its vocabulary would ship with no
implementation and no test. Deferred to a later minor.

## Consequences

- The SDK grows 18 calls, an engine declaration, and the record and specification
  messages. It speaks contract 25.2.
- The plugin takes over everything about embeddings that is YDB's: the store
  layout and its serialization, the scans, the writes, the vector encoding, the
  index, the searches and the refusals.
- A lifecycle decision costs two round trips, a read and a mutation, and so does a
  batch commit. The host today spends one database transaction on each. The extra
  round trips are local gRPC.
- A decision over the whole store conflicts with every concurrent batch commit, as
  it does in the YDB store today. The host retries it within a bound.
- The PostgreSQL backend keeps its lock-held prepare and its outbox. A provider
  engine has neither until a later minor adds change capture, and until then it
  builds `immutable` generations only.
- `inference describe` needs `--provider-engine` to describe the target objects of
  a provider engine.
- Ptah's pipelines stop running inference against YDB. The plugin's pipeline
  carries those tests, and the fixture carries the service's conformance in
  Ptah's pipelines.
- The embedded YDB engine is not deleted until a plugin release that serves
  `embeddings.store` is published and the host adapter is merged. That is the
  condition ADR 0028 sets on the deletion.

## Acceptance criteria

These are for the implementation. None holds today.

- `provider.proto`, the SDK interface, both transport boundaries and the
  validators carry the service as contract 25.2, with `provider.ContractMinor` at
  2. The validators refuse:
  - a reply bound to another request, scope, run or table;
  - records out of key order, repeated, or outside their scope, and an empty
    token;
  - a page that holds more than its bound, or whose completion disagrees with its
    next page token;
  - a write whose generation is not the request's, a set whose ordinals are not
    0, 1, 2 and so on in order, a skip or a tombstone that is not a set of one, and
    a vector of the wrong width or with a value that is not finite;
  - a refusal without a reason, and an outcome with a payload it cannot carry.
- In-process SDK tests cover the negotiation in both directions: a host that does
  not list `embeddings.store` gets no declaration and no call, and a provider that
  sends the declaration anyway is refused. They also cover each validator, a
  `CONFLICT`, and a lost write that is reported as unknown and not sent again.
  Each validator is mutation-checked.
- The fixture provider serves the service on SQLite, and SDK process tests run
  against it: prepare, scan and resume, a batch commit and a conflicting one,
  verification paging, an exhaustive search, structure and counts, retirement with
  an ownership refusal, and a crash after a commit whose checkpoint the next read
  shows.
- The host adapter runs the native CLI lifecycle against the fixture in Ptah's
  pipelines: prepare, backfill, resume, index (`NOT_DECLARED` on the fixture),
  verify, evaluate, cutover, rollback and retire. The inference tools of `ptah
  mcp` run against it too.
- The plugin implements the service. Its pipeline runs the scenarios of the three
  YDB test files on the YDB lines it supports, through the native CLI and the MCP
  tools, including a database whose state the embedded engine wrote.
- `docs/site/src/content/docs/inference/reference/support-and-limitations.md` and
  `docs/site/src/content/docs/extend/executable-providers.md` document the feature
  and the installation `ydb://` needs.

## Decided questions

- One feature covers the whole service. A provider that keeps run state but cannot
  write vectors serves nothing a generation can use.
- The host derives the names of a generation's objects and sends them.
- The plugin keeps today's YDB layout.
- Change capture waits for an engine that can implement it.
