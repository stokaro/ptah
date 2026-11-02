# ADR 0055: A provider splits a migration body into the units its engine runs

Status: Proposed

Contract: 25.12

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0023](0023-engine-knowledge-moves-with-the-provider.md),
[ADR 0030](0030-ptah-compat-resolves-providers-through-the-native-pipeline.md),
[ADR 0038](0038-a-provider-runs-the-operators-statements-and-returns-what-they-read.md).

## Context

The migrator takes what an engine adds from `migratorext.Services`, found by
dialect name through `internal/migratorext/builtin.Lookup`. A dialect no
built-in package serves gets the zero value: a statement-by-statement split
(`sqlutil.SplitStatementsForDialect`), timeouts as session settings, and the
shared check rules (`internal/migratorext/migratorext.go`). YDB alone sets
`Units`, `QueryDeadlines` and `ReadProof` (`internal/migratorext/ydb/ydb.go`).
The other packages there give catalog queries, timeout spellings, owner
checks, block grammar, a body's own transactions and check rules. The YDB
package goes with the engine. It gives `migration/migrator`:

- **Units.** A body runs as the queries `internal/yqlquery.Split` returns:
  each scheme or `BATCH` statement alone, each run of data statements as one
  query, and every `PRAGMA`, `DECLARE`, named expression and action carried
  into each later query. Counts, checkpoints, partial hashes, failure records
  and resume points are taken over them (`migrations.go`, `ydb_execution.go`,
  `partial_hashes.go`, `atlas_failure_shape.go`, `revisions.go`).
- **Refusal** of a client delimiter, an unsupported translation setting, a
  transaction-control statement and a block that mixes scheme and data
  statements, before anything runs, on up, down and repair
  (`refuseUnsplittableSQL`).
- **Placement.** `file` runs as `none`; `all` is refused by the
  transactional-DDL capability (`effectiveTxMode`, `validateTxModeAllDialect`).
  A scheme or `BATCH` query is marked in flight, run and checkpointed. A data
  query commits with its checkpoint in one serializable transaction, is tried
  up to five times when YDB aborts it for a conflict, and is read back when
  the commit reply is lost (`dataQueryCommit`). A failure never records less
  than the row holds (`revisionFailureRecord`). An interceptor sees scheme
  queries only (`runMigrationFileStatement`).
- **Deadlines.** The statement timeout bounds each query, and a timed-out
  index build is canceled (`ydb_timeouts.go`).
- **Checks.** An assertion passes `yqlreach.ReadOnly`, reads only local tables
  (`ydbschema.ProveLocalReads`) and runs in a read-only snapshot (`checks.go`,
  `verify.go`).

The contract carries part of this. `ExecuteSQL` sends one SQL unit per
`Execute`, with the statement timeout under `units.statement-timeout`
(`internal/providerhost/session.go`, `execution_units.go`). A history write
can join a provider transaction (`HistoryScope.transaction_id`), but
`BeginHistoryTransaction` exposes no executor, and `BeginTransaction` refuses a
session without `transactional_ddl` (`history.go`, `transactions.go`). A check
runs through `RunStatement` with `READ_ONLY`, where the YDB provider proves it
read-only and local (ADR 0038). Nothing tells the host how a body splits:
`AnalyzeSQL` and `SplitLintStatements` serve lint
(`internal/providerhost/sql.go`).

The YDB provider's session names its engine `ydb`, so it gets the built-in
services while they exist, and its data committer and check proof call
`DatabaseConnection.BeginTx` and `Conn`, which refuse a provider's connection
(`dbschema/connection.go`). Measured on local-ydb 26.2.1.14 with the plugin
selected through `--provider-config`: a migration holding one `CREATE TABLE`
applies, and the next one, `$v = "a"u;` followed by two `UPSERT` statements
that read `$v`, fails in `file` mode with `failed to execute migration SQL:
unsupported feature: this connection does not expose database/sql
operations` and is left dirty at `applied=0/1`; `--tx-mode none` then refuses
the dirty version. With the package gone, a generic grammar splits YQL, a
`$name` no longer reaches the next statement, and `file` mode reaches
`BeginTransaction` (`internal/sqliterebuild`), which refuses the YDB
provider's session.

The plugin's splitter (its `internal/yqlquery`) matches the host's corpus and
serves seeds, streaming queries, comment statements and rule YD102. It has no
`Source`. Its `Execute` answers every dispatched failure `UNKNOWN`, as does a
failed commit (`internal/backend/execution.go`, `transactions.go`), so the
host can tell neither a rejected unit nor an abort it may retry.

## Decision

### The provider splits, the host runs

The two-way feature `units.migration-split` adds the offline RPC
`SplitMigration`. It requires `units.execute`, `sessions.transactions` and
`sessions.history`, and an engine that declares it also declares its own
transactions and history, which the host runs its units and checkpoints with.
The host sends one body in one direction and the check assertions of its
migration. The provider returns the units in run order or a refusal, and a
verdict for each assertion.

The split reads the text alone. The request carries no session, no server
version and no database, so a dry run and a unit count need no connection,
and one body has one split on every server. Where the grammar differs between
server releases, the provider splits by the grammar it supports, and a server
refuses a unit its release does not accept when the unit runs.

A unit is the text `Execute` receives, with every definition and setting it
needs carried in; the source of its own statements; and a placement.
`WITH_CHECKPOINT` changes data only: the host runs it in a serializable
transaction of its own, writes its checkpoint with `MutateHistory` in that
transaction, and commits. `OUTSIDE` runs outside any transaction: the host
marks it in flight, runs it and checkpoints it, as under `none`.

The provider owns statement boundaries, grouping, carried text, refusals,
the classification of a failure, an abort or a deadline, and the verdicts.
The YDB provider maps `yqlquery.Split`: a data query is `WITH_CHECKPOINT`, a
scheme or `BATCH` query is `OUTSIDE`, and what the built-in engine refuses is
refused.

The host owns the order of migrations and units, counts, checksums, partial
hashes over `source`, checkpoints, history, locks, resume, the transaction
mode, the retry policy, observers, interceptors, dry runs and the wording of
every refusal. It splits each body once, before a run touches anything.
Units pass `DescribeExecutionInputs` as any SQL unit does. Shared code reads
the negotiated feature, never the engine name.

On a session without `transactional_ddl`, `file` runs as `none` and `all`
stays refused. On one with it, a `file` or `all` transaction holds every
unit, and a split that places any unit `OUTSIDE` refuses a `file` run before
it runs. The engine does not run an `OUTSIDE` unit in a transaction, so a
`file` run cannot hold the body in one; the host refuses it rather than move
the unit out and run the file as several transactions. An `all` run is
refused for the same reason. An interceptor is offered `OUTSIDE` units only.

### Failure semantics

- **Partial sequence.** Each `Execute` carries one unit. A run stops at the
  first failed unit, the units before it stay checkpointed, and a resume
  starts at it.
- **`OUTSIDE`.** `NOT_APPLIED` means the engine rejected the unit and applied
  nothing, and the mark is cleared. `UNKNOWN` keeps the mark, and a resume
  refuses to guess.
- **`WITH_CHECKPOINT`.** A failed unit is rolled back, and `ROLLED_BACK` or
  `NOT_STARTED` proves nothing applied. A lost commit reply is settled by
  `ReadHistory`: a recorded checkpoint proves the commit. A failure record
  keeps the larger of its count and the row's.
- **Abort.** `Execution.transaction_aborted` or `TransactionResult.aborted`
  says the engine aborted the transaction for a conflict and applied nothing.
  The host waits, rebuilds the checkpoint and runs the unit again, under its
  own bound. It never retries `UNKNOWN`, a deadline or an `OUTSIDE` unit, and
  it checks the migration lock before each commit. The flags belong to
  `units.migration-split` rather than to a feature of their own: the host
  retries only a `WITH_CHECKPOINT` unit, which only a split produces, so a
  host that acts on an abort is one that splits, and a separate feature
  would let a peer list aborts that nothing could retry.
- **Deadline.** Each `Execute` carries the statement timeout; bookkeeping
  calls carry none. A stopped `WITH_CHECKPOINT` unit is `NOT_APPLIED` with
  `statement_timed_out`, and its transaction never commits. A stopped
  `OUTSIDE` unit is `NOT_APPLIED` when the provider undid it, such as a
  canceled index build, `APPLIED` when it finished first, and `UNKNOWN`
  otherwise.

### Checks, providers without the feature, and ptah-compat

The verdict replaces the host's text rules for the engine, which read YQL
with another grammar. A refused assertion fails as "check assertion must be
one read-only SELECT statement: <reason>", in a dry run that defers it too.
`RunStatement` with `READ_ONLY` still proves the reads at run time. The host
keeps one column, one row, the value's truth, and `all` and `oneof` groups.

Without the feature, the host splits with its generic splitter and keeps its
generic check rules, as today. A `file` or `all` transaction on a session
with `transactional_ddl` is unchanged. Outside one, a body of one statement
runs as one unit, and a longer body is refused before anything runs:

    migration 3 cannot run up on ydb: the provider does not split a migration
    body into the units its engine runs (units.migration-split), so a body of
    4 statements cannot run outside a transaction; upgrade the provider

No Atlas edition has a YDB driver (`docs/conformance.md`), so the built-in
engine is the reference for `ptah-compat migrate apply` on YDB. Atlas
revisions count and hash units as they count queries today, and the plugin's
split equals the host's corpus, `Source` included, so a revision the built-in
engine wrote resumes at the same unit. `-- atlas:delimiter` stays refused,
`-- atlas:txmode` follows the rules above, and strict mode starts no provider
(ADR 0030).

## Wire

```proto
rpc SplitMigration(SplitMigrationRequest) returns (MigrationSplit);

// The split reads the text alone: no session, no server version and no
// database.
message SplitMigrationRequest {
  string engine = 1;
  // One body in one direction, directives included.
  string body = 2;
  // The migration's check assertions, in file order. None runs.
  repeated string assertions = 3;
}
message MigrationSplit {
  // In run order. Empty with a refusal, or for a body with no statement.
  repeated MigrationUnit units = 1;
  SplitRefusal refusal = 2;
  // One per request assertion, in request order, with a refusal too.
  repeated AssertionVerdict assertions = 3;
}
message MigrationUnit {
  // Execute's Unit.sql, with every definition and setting it needs carried
  // in from earlier statements.
  string sql = 1;
  // The unit's own statements as written. Partial hashes and failure
  // records read it.
  string source = 2;
  enum Placement { PLACEMENT_UNSPECIFIED = 0; WITH_CHECKPOINT = 1; OUTSIDE = 2; }
  Placement placement = 3;
}
message SplitRefusal {
  // The refused text's byte range in body, which the host quotes.
  uint32 start = 1;
  uint32 end = 2;
  // One line. It quotes nothing from the body.
  string reason = 3;
}
message AssertionVerdict {
  // The text is one statement that changes nothing.
  bool read_only = 1;
  // Set exactly when read_only is false. One line.
  string reason = 2;
}
// Additions, sent only when both sides list units.migration-split.
message Execution {
  // NOT_APPLIED in a caller transaction only, and never beside
  // statement_timed_out: the engine aborted it for a conflict, applied
  // nothing, and a new attempt may succeed.
  bool transaction_aborted = 6;
}
message TransactionResult {
  // ROLLED_BACK in answer to COMMIT only, for the same reason.
  bool aborted = 4;
}
```

Each transport boundary refuses a request with no engine or body, or for an
engine that does not declare the feature; a unit with empty `sql` or `source`
or no placement; units beside a refusal; a refusal range outside the body, or
a reason that is not one line; a verdict count unlike the request's; a reason
beside a `read_only` verdict, or none without one; and either flag on another
outcome or without the feature: `transaction_aborted` outside a caller
transaction, on an outcome other than `NOT_APPLIED` or beside
`statement_timed_out`, and `aborted` on anything but a `ROLLED_BACK` answer to
`COMMIT`. An abort is not a stop at the statement timeout: the host retries
the one and never the other, so a reply that sets both is malformed. The
client returns no reply with a misused flag, so no caller reads it and
retries.

## Consequences

- The host deletes the YDB parts of `ydb_execution.go` and `ydb_timeouts.go`
  and the `Units`, `QueryDeadlines` and `ReadProof` fields of
  `migratorext.Services`. `internal/providerhost` gains a unit transaction: a
  serializable provider transaction that runs one `WITH_CHECKPOINT` unit and
  its history write, without the transactional-DDL rule of `BeginTransaction`.
- The plugin serves `SplitMigration` from `internal/yqlquery`, adds `Source`,
  and records it in its oracle corpus (`internal/rowdata/testdata/oracle`). It
  answers `NOT_APPLIED` for a rejection that applied nothing, as
  `statementFailure` already classifies one, and sets the abort flags on
  YDB's `ABORTED` status.
- A run asks the provider once per body. A command that counts units starts
  the provider, even where it opens no session.
- A provider without the feature loses multi-statement bodies outside a
  transaction. That refusal is the fail-closed state.
- The SDK release comes first. The host and the plugin adopt it in their own
  changes, against the published tag.

## Alternatives considered

**Keep the splitter in the host**, picked by a grammar name the provider
declares. The host would keep YQL for an engine it no longer ships, against
ADR 0023, and the host and plugin copies would drift. Rejected.

**Extend the lint split.** `SplitLintStatements` and `AnalyzeSQL` return
statements and facts, with no execution text, carried definitions or
placement. Lint and execution would then share one message. Rejected.

**Let the provider run the body and write its checkpoints.** The history
contract leaves ordering, checksums, retry and recovery to the host
(`HistoryRevision` in `provider.proto`), and each provider would rebuild
resume and Atlas revisions. Rejected.

**Split inside `Execute`.** `applied_count` reports a prefix, but no
checkpoint commits with each data query. Rejected.

**Split on a session; signal an abort with a diagnostic code.** The built-in
split reads text alone, and a dry run must not need a connection. A code
would make the host parse provider strings. Rejected for an offline call and
two flags.

## Tests

The SDK tests the validators in `provider-sdk/provider`, one failure row per
refusal above, each mutation-checked: with the check it covers disabled, the
row fails. `provider-sdk/runtime` tests the negotiation, which refuses the
call when either side did not list the feature, and that both transport ends
refuse an invalid request, an invalid reply and a misused abort flag.

The fixture gains a mode `migration-units` under
`provider-sdk/testdata/fixture/cmd/`. Its toy grammar carries
`DEFINE name = literal;` into later units, so `sql` differs from
`source`; groups a run of `INSERT`, `UPDATE` and `DELETE` as
`WITH_CHECKPOINT`; places other statements `OUTSIDE`; and refuses `COMMIT`.
Its session reports `transactional_ddl` false, or true to prove an `OUTSIDE`
unit refuses a `file` run, and environment switches abort commits or lose a
commit reply after it lands. `TestProviderMigrationUnits` in
`integration/providercli/provider_migration_units_e2e_test.go` drives it.

The plugin tests `SplitMigration` over its corpus and adds `TestHostMigrations`
in `integration/host_migrations_test.go` on each YDB line it supports. Each
host test below gets a subtest in the fixture and one in the plugin:

| Host test (`integration/dbschema/ydb`) | Fixture subtest | Plugin live subtest |
| --- | --- | --- |
| `TestYDBBinary_RunsVersionedMigrations` | CLI lifecycle, carried definition | `$name` across `CREATE TABLE`, `--statement-timeout` |
| `TestYDBMigrator_RunsQueriesAndRollsBack` | counts 4/4 and 2/2, native and Atlas | same in YQL |
| `TestYDBMigrator_ResumesWhereAFailedMigrationStopped` | `failed 2/3`, resume at unit 3 | same |
| `TestYDBMigrator_RunsADataQueryAsOneTransaction` | key conflict leaves neither row, `failed 1/2` | same |
| `TestYDBMigrator_RefusesWhatItCannotSplit` | refusal, no revision row | `DELIMITER`, `atlas:delimiter`, `--!ansi_lexer`, `COMMIT`, mixed `DO` |
| `TestYDBMigrator_RecordsProgressAfterEachQuery` | `applied` 1, 2, 3 by `ReadHistory` | same |
| `TestYDBMigrator_RefusesToResumeOverAnEditedPrefix` | edit inside a committed `source` | same |
| `TestYDBMigrator_RunsADataQueryWithItsCheckpoint` | interceptor sees `OUTSIDE` only | same |
| `TestYDBMigrator_FailureKeepsTheProgressTheRowRecords` | lost commit reply; row keeps 2/3 | proxy cut after the commit |
| `TestYDBMigrator_RepairRunsADataQueryWithItsCheckpoint` | `ResumeFrom` 2, no in-flight mark | same |
| `TestYDBMigrator_RepairRefusesWhatItCannotSplit` | refusal on repair; row stays `failed 1/2` | `DELIMITER` |
| `TestYDBMigrator_RunsBlocksAndBatchStatementsOutsideATransaction` | `OUTSIDE` placement | `DO`, `EVALUATE FOR`, `BATCH`: `applied 6/6` |
| `TestYDBMigrator_DryRunRunsNoDataQuery` | no `Execute`, no `BeginTransaction` | same |

The same harness lifts the statement-timeout rows of `timeout_live_test.go`,
`TestYDBBinary_DBVerify` and `TestYDBChecks_RefuseAReadOfAnExternalTable`.
