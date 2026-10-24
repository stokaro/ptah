# ADR 0028: A provider owns how rows are read, written and seeded

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0019](0019-a-declared-row-set-is-artifact-content.md),
[ADR 0021](0021-provider-recognition-does-not-authorize-execution.md),
[ADR 0023](0023-engine-knowledge-moves-with-the-provider.md),
[ADR 0026](0026-official-providers-are-pinned-in-the-release-installed-explicitly-and-bundled-by-the-distribution.md)

Scope: declared rows (managed data), seed files and the query builder's write
statements, including YDB's `UPSERT INTO`. Embeddings (`ptah inference`) are
out of scope. They need a provider process, a model endpoint and an outbox, and
this decision does not move or change them. Their YDB path stays as ADR 0013 to
0017 describe until a separate decision covers it.

## Context

The host writes rows in three places, and each reaches YDB code that a provider
cannot call.

**Declared rows.** `internal/atlasschema/managed_data.go` plans them,
`internal/cli/migratedata` and `migration/generator/checkpoint_data.go` write
them into migration files, and `internal/cli/internal/schemaops` applies them.
All of them go through `internal/managedrows.Compare`. That function works with a live
database in these ways:

- It reads the live rows with `dbschema.ReadTableRows(ctx, conn, ...)`, which
  needs a `*dbschema.DatabaseConnection` over `database/sql`.
- It types both row sets with the engine's `dataext.Rows.TypeRows` when the
  descriptor says `ManagedData.TypedRows`. For YDB that is `internal/ydbdata`
  (98 lines) over `internal/ydbtype`, which holds the canonical form of every YDB
  value (a Decimal read back as 12.5 pairs with a declared `12.50`; a moment in
  UTC pairs with the local moment the driver returns).
- It renders each value with `dataext.Literals.Literal(ydbType, value)`. YQL
  converts almost no literal, so a value is written only in its column's own type.

A provider-selected run has none of these. `dbschema.WithNativeConnections`
registers a connector for the URL scheme, but a provider session is not a
`*sql.DB`. The SDK has no operation that reads rows, so `ReadTableRows` has
nothing to call. `engine.Target.Data` is the seam where the services plug in,
and `providerhost` fills none of it.

**Seeds.** `migration/seeder/seeder.go` applies SQL files and records them in the
`schema_seeds` tracker. For an engine whose descriptor says `Seeds.ByOwner` the
file runs through `dataext.Seeds`, whose `Database` parameter exposes
`BeginTx(ctx, *sql.TxOptions) (*sql.Tx, error)` and `Conn`. YDB's implementation is
`internal/dbschema/ydb/seeds.go`. It runs the file and the tracker row in one
serializable transaction, runs it again when YDB aborts the transaction for a
conflict, and refuses before running a file that holds a statement YDB runs only
outside a transaction (a scheme statement, `BATCH UPDATE`, `BATCH DELETE`). The
shared seeder imports `internal/dbschema/ydb` to recognize a key conflict
(`ydbschema.KeyConflict`, seeder.go:647), so the host names a YDB error code in
generic code. An interface that returns `*sql.Tx` cannot be implemented by a
process on the other side of gRPC.

**Query builder.** `core/query` is in the stable embedder API. `RenderInsert`,
`RenderUpdate` and `RenderDelete` look the dialect up with `platform.Lookup` and
read `descriptor.Query`. `InsertStatement.Upsert` renders `UPSERT INTO ... VALUES
(...)` when the descriptor says `UpsertInto`, and arguments are `$p1`, `$p2` as
`sql.NamedArg` for YDB. This is data, not code: `descriptor.Query` is a struct with
JSON tags (`upsert`, `upsert_refusal`, `update_refusal`, `offset_fetch`,
`unsupported_joins` and so on), and placeholder and quoting rules are descriptor
fields. The renderer never runs a provider. An embedder that imports
`core/query` has no provider process, and must not need one to build a
statement.

YDB's descriptor lives in `internal/builtindialects/ydb.go`. ADR 0026 deletes the
embedded YDB engine at the flip, and `platform.Lookup("ydb")` then fails. The
query builder would lose YDB, and so would every library user.

The plugin already has the pieces for transactions: `BeginTransaction`,
`FinishTransaction`, `Execute` over `Unit`s (SQL text or a native envelope),
outcome classes (`APPLIED`, `PARTIAL`, `UNKNOWN`), and provider-owned history
storage. It has no row operation.

## Decision

The host keeps the generic algorithm: reading declared rows, the row diff
(`datadiff`), ordering by foreign keys (`dataorder`), protected tables, saved plan
records and the idempotent-seed policy. The provider owns everything that depends
on the engine: how a value is typed, how a row is read back, how a write is
spelled, how a seed file runs and how its ledger is stored.

### Four row services, each behind its own feature

A provider declares the features it implements. The host refuses a declared row
set for an engine whose provider lacks `data.type-rows` and `data.read-rows`, and
refuses a write for one that lacks `data.render-writes`. It does not substitute
common INSERT text.

```proto
// Offline. Replaces dataext.Rows.
rpc TypeRows(TypeRowsRequest) returns (TypedRows);
// Session. Replaces dbschema.ReadTableRows for provider engines.
rpc ReadRows(ReadRowsRequest) returns (RowPage);
// Offline. Replaces dataext.Literals and the host's INSERT/UPDATE/DELETE text.
rpc RenderRowWrites(RenderRowWritesRequest) returns (RenderResult);
// Session. Replaces dataext.Seeds.
rpc ListSeedTables(...) returns (...);
rpc ApplySeed(ApplySeedRequest) returns (SeedResult);
rpc RecordSeed(RecordSeedRequest) returns (SeedResult);
rpc ReadSeedLedger(...) returns (SeedLedger);
```

**Values.** A row is a list of `DataValue` aligned with a column list, never SQL
text. `DataValue` is a oneof: null, bool, int64, uint64, double, string, bytes,
decimal (digits, precision, scale), timestamp (UTC seconds and nanoseconds),
date, interval, json text, and `native` (a provider-defined type name and bytes
for a value the common cases cannot hold). The host does not interpret
`native`; it compares by the canonical form the provider returns.

**`TypeRows`** takes the capabilities of the session, the live table (columns and
their engine types, or absent), the declared type of each column, and the declared
and live rows. It returns the engine type of each column and both row sets in one
canonical form. This is `dataext.Rows.TypeRows` with a wire shape. It is offline
because the host reads the rows first.

**`ReadRows`** takes a session, a table reference (the structured `Reference`,
never an SQL fragment), the column list, the key columns and a page token. It
returns rows in canonical `DataValue` form, in key order, in pages bounded by the
4 MiB message limit and a row limit the caller sets (at most 10,000 rows, like
history). A read that cannot finish returns an error and no rows; an empty table
is a page with `complete=true` and no rows. The call can name a transaction, so
a comparison and the writes that follow see the same snapshot where the engine
allows it.

**`RenderRowWrites`** is the write path. The host sends typed row operations, not
SQL:

```proto
message RowWrite {
  string change_id = 1;
  enum Mode { MODE_UNSPECIFIED = 0; INSERT = 1; UPSERT = 2; UPDATE_BY_KEY = 3; DELETE_BY_KEY = 4; }
  Mode mode = 2;
  Reference table = 3;
  repeated string columns = 4;          // columns written, key columns included
  repeated string key_columns = 5;
  repeated DataValue values = 6;        // one row, aligned with columns
  map<string, string> column_types = 7; // from TypeRows
}
```

The reply is the existing `RenderResult`: one `Unit` per input write, in input
order, as SQL text or a native envelope. A refused batch exposes no prefix. The
units are what a saved plan records for review, what a data migration file
contains (`.up.sql` and `.down.sql`), and what `Execute` runs. Rendering is
deterministic: the same request gives the same bytes, so the `schema apply` and
migration paths cannot disagree about a row. A provider that writes rows with a
native call instead of text returns a `native` unit for plan application. The
migration file writer accepts text units only and refuses a `native` unit with a
message that names the engine.

`UPSERT` is a mode of the operation, not a YDB word. A provider that has no upsert
refuses the mode with a typed reason. The YDB plugin renders `UPSERT INTO` and
decides its types and literals. The generic `INSERT`/`UPDATE`/`DELETE` text for
engines inside the host is untouched.

Ordering across tables (parents first to insert, children first to delete) stays
in the host. The provider orders rows only inside one write when its engine needs
it.

**Execution** reuses the existing services. The host runs the rendered units with
`Execute`. With `sessions.transactions` it opens one transaction for the data stage
and finishes it, so declared rows commit or roll back together. Without it the
host runs the units unwrapped and reports `PARTIAL` or `UNKNOWN` as the SDK
already defines. `APPLIED` inside a transaction confirms execution, not commit.
A lost reply is `UNKNOWN`, and the host stops and reports; it never replays.

**Seeds.** A seed file is text the operator wrote. The host discovers files,
computes the checksum, applies environment selection and protected-table
refusal, and calls the provider:

- `ApplySeed(session, seed{path, filename, checksum, text, env})` runs the file and
  records it in the provider's ledger atomically ("commit together or not at
  all"). The result is one of `APPLIED`, `KEY_CONFLICT`, `REFUSED`, `NOT_APPLIED`,
  `UNKNOWN`. `KEY_CONFLICT` replaces the host's check `ydbschema.KeyConflict`; the
  host's idempotent policy calls `RecordSeed` on it. `REFUSED` is a preflight
  refusal that ran nothing, such as the YDB statements that cannot run in a
  transaction; it carries the statement position and reason.
- `RecordSeed` writes the ledger row without running the file.
- `ReadSeedLedger` returns path, checksum, environment and time for each applied
  seed, so the host can detect a changed file (`ChecksumMismatchError`) and skip
  an applied one.
- `ListSeedTables` returns the table names the protected-table check compares
  against. This replaces `descriptor.Seeds.TablesQuery`.

The ledger layout belongs to the provider, as the history layout does. A
provider marks it with a version marker, refuses an unmarked table, and does not
adopt another layout. The word `schema_seeds` is the provider's choice.

Retry is the provider's, within a limit. YDB aborts a conflicting transaction and
the abort proves that nothing committed, so the provider may run the file again
before it answers. It stops after a bounded number of attempts and answers
`NOT_APPLIED` with the reason. Any case where the provider cannot prove that
nothing committed is `UNKNOWN`, and the host does not retry it.

### Query-builder statements: the provider declares the spelling as data

The provider does not render library statements by RPC. `Engine` gains a
`query_dialect` message that carries what `core/query` reads from the descriptor:
the `Query` section field by field, the placeholder style (YDB's named
`$p1` arguments), the identifier quoting rule, and the capability keys that gate
`RETURNING` and similar clauses. The fields are the descriptor's existing JSON
tags, with the same meaning, so there is one vocabulary.

`core/query` gains a public entry that takes this description. The host loads it
from `Describe` when it starts a provider and registers it for the engine in
the platform registry the renderer reads, so `query.RenderInsert(stmt, "ydb")`
works wherever a provider-selected runtime is active. An embedder with no
provider process calls the new entry with a description it builds in code or
reads from a provider it started itself. Statements the data cannot express
are refused with the existing unsupported-dialect error naming the engine and
the clause.

This keeps the library offline and keeps the knowledge that YDB spells upsert as
`UPSERT INTO`, and that its arguments are named, in the plugin's declaration.
The renderer still has the code that writes each enumerated form, which is
generic: another engine can choose `UpsertInto`. If a provider needs a form the
enumeration lacks, the owner adds a form to the enumeration, which is a core
change and not a YDB one.

### Trust and authorization

`TypeRows` and `RenderRowWrites` are offline and carry no credential. `ReadRows`,
`ApplySeed`, `RecordSeed`, `ReadSeedLedger` and `ListSeedTables` act on one
session the host opened under ADR 0021. A row operation names a table by
`Reference`, never by SQL text, and the provider validates that the table and
columns exist in the session's database before it writes. The host applies the
protected-table fence and `--allow-prod` rules before the call. A provider cannot
override them, and the SDK says so: a result that wrote to a table the request did
not name is a protocol violation.

Seed file text and row values can be sensitive. Diagnostics carry the file name,
the statement position and a reason, not the statement or a value.

### Versioning

Each of the two groups (rows and seeds, and `query_dialect`) is a protocol change
that bumps `provider.Revision` and the SDK minor tag. `ReadRows` and the seed
service ship with the fixture provider on SQLite, so the SDK conformance suite
has a subject that is not YDB. The plugin pins the tag and `go.mod` bumps follow
the order in `provider-sdk/README.md`.

### What moves

`internal/ydbdata`, `internal/ydbtype`'s value and literal parts, YDB's seeds in
`internal/dbschema/ydb/seeds.go`, and the YDB row of `descriptor` data in
`internal/builtindialects/ydb.go` move into the plugin or are deleted.
`seeder.go` loses its import of `internal/dbschema/ydb`. `descriptor.Seeds.ByOwner`
and `ManagedData.TypedRows` become properties that any provider declares, not
flags of a built-in descriptor.

## Alternatives considered

**Keep SQL text in the host and ask the provider only for literals.** The host
would keep `INSERT` assembly and call a `Literal(type, value)` RPC per value. It is
the smallest change and keeps `managedrows` as it is. It sends one call per value
unless batched, leaves statement shape in the host, and would not cover `UPSERT`,
which is a different statement, not a different literal. Rejected for the rows
and kept for the query builder only in the sense that `query_dialect` is data.

**Let the provider own the whole row diff.** The provider reads the rows, diffs
and returns writes. Every provider would reimplement the diff and the foreign-key
order. Rejected: the algorithm is generic, and only the typing and the spelling
are engine facts.

**Give the provider a general `database/sql` proxy.** Expose a `Query` and `Exec`
RPC with SQL text and let the host keep its code. The host would keep composing
YQL, and the rule of ADR 0023 (the host calls a service and renders what it
returns) would be gone. Rejected.

**Render library statements by RPC.** One RPC per `RenderInsert`. A library user
would need a provider process to build a string, and the stable `core/query` API
would gain a runtime dependency. Rejected. The declared-data route was chosen
because `descriptor.Query` already is data.

**Move seeds to plain `Execute` plus the history service.** A seed file is
statements, and the history store holds records. It would drop the atomic "file and
ledger together" guarantee, or force the host to know the ledger's table. Rejected.

## Consequences

- The SDK grows seven RPCs and one engine declaration, and a value type.
- The plugin takes over typing, literals, `UPSERT` rendering, seed execution and the
  ledger. That is the YDB-specific writing logic.
- The host's `dataext` interfaces change shape. `Seeds` loses its `*sql.Tx`
  parameters. Callers in `atlasschema`, `schemaops`, `migratedata` and
  `checkpoint_data` read the services through a context-borne runtime instead of
  `builtin.Bundled()`.
- Reading rows through gRPC pages is slower than a local `database/sql` read. The
  comparison reads only the declared key and value columns, as it does today.
- Library users of `core/query` get a new registration entry and lose the implicit
  `ydb` dialect when the embedded descriptor goes. That is a deliberate break
  (pre-v1, so no compatibility is owed) and the Stable Embedder API ledger
  records it through the `apidiff` approval list.
- Embeddings (`ptah inference`) keep working on YDB until the flip. Their YDB code
  moves into the plugin as part of the flip program. It is neither dropped nor left
  in the host after the embedded engine is deleted. This ADR does not design it.
  It is a flip precondition (ADR 0026): the embedded YDB engine is not deleted
  until a follow-up record, "YDB embeddings leave the host before the embedded
  engine is deleted", is accepted and implemented. That record is required work.
- Migration files accept text units only. A `native` unit is not portable in a
  versioned migration, so the writer refuses it with a message that names the
  engine. A provider that writes rows through a native call must also render text
  for migration use, or the engine's declared rows are refused in migrations.

## Acceptance criteria

These are for the future implementation. None holds today.

- `provider.proto`, the SDK interfaces, both transport boundaries and validators
  carry the rows and seeds services. Validators refuse a reply with a row count
  that disagrees with its page, a value whose kind disagrees with its column type,
  a unit count different from the request, and a write to a table the request did
  not name.
- SDK conformance in `provider-sdk/integration` runs against the fixture provider
  on SQLite: paging and resume, an empty table versus a missing table, the exact
  rendering of each mode, a refused batch with no prefix, a transaction that rolls
  back declared rows, a lost reply reported as unknown and not replayed, seed
  apply with a ledger row that exists only with the file, `KEY_CONFLICT`, and a
  changed checksum.
- A differential corpus feeds the declared-row fixtures under
  `integration/internal/fixtures/entities/` that include a YDB table, and the
  query-builder cases in `core/query`, through the bundled code and the plugin.
  Rendered units equal the goldens byte for byte; typed rows equal after
  canonicalization. Rows covered include Decimal, Timestamp, Date, Json, Bytes,
  nullable and optional columns, and keys that are not the first column.
- Live tests under `integration/` (`*_live_test.go`, engine asked through
  `dbtarget`) run against YDB through the plugin: `schema apply` with declared
  rows on a table with a foreign-key-like order, a re-run that plans nothing,
  `ptah seed` with an idempotent re-run, and a seed that holds `BATCH UPDATE`
  refused before it runs. Expected values come from reading the table back.
- A mutation check: removing the plugin's typing of Decimal makes the re-run plan
  a change, and the test reddens.
- `core/query` has a test that renders `UpsertInto` for an engine description built
  from a provider's `Describe`, and a refusal test for a clause the description
  cannot express.
- A guard fails if `migration/seeder` or any other generic package imports an
  engine-named package, using `go list`.
- `docs/site/src/content/docs/extend/executable-providers.md` documents the
  features, and `docs/capabilities.md` shows the provider-declared rows.

## Decided questions

- Migration files accept text units only.
- The ledger report names the seed file that conflicted. The provider need not say
  which key conflicted.
- Embeddings ordering is stated in Consequences as a flip precondition.
