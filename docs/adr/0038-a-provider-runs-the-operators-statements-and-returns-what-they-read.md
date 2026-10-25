# ADR 0038: A provider runs the operator's statements and returns what they read

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0021](0021-provider-recognition-does-not-authorize-execution.md),
[ADR 0023](0023-engine-knowledge-moves-with-the-provider.md),
[ADR 0028](0028-a-provider-owns-how-rows-are-read-written-and-seeded.md)

Contract: 25.4, with ADR 0039.

## Context

Some commands run SQL the operator wrote and read what it returns. On a
built-in engine they do it through `database/sql`. A provider's connection
takes no `database/sql` call: `dbschema.(*DatabaseConnection).QueryContext`,
`ExecContext`, `BeginTx` and `WithIsolatedQuerySession` return "this connection
does not expose database/sql operations" for it (`dbschema/connection.go`,
`rawSQLUnavailable`). These commands stop there. `db verify` and `schema test`
were measured on master with the fixture provider, and the others reach the
same calls in the code:

- **`ptah db verify`** runs each check's assertion through
  `migration/migrator.VerifyChecks` (`verify.go`), one isolated read-only
  session per check, and reads one row of one column.
- **Migration checks.** A `-- +ptah check` in a migration runs through
  `runCheckGroups` (`migration/migrator/checks.go`) in the same isolated
  session, so a migration that holds one cannot be applied on a provider
  engine.
- **`ptah schema test` and `ptah migrations test`** run `migration/dbtest`.
  Its `exec` step calls `ExecContext`, and its `assert` steps (`row_count`,
  `scalar`, `result_set`, `match`, `true`, `error_matches`, `error_contains`,
  `expect_any_error`) call `QueryContext` and compare the rows or the error
  text (`engine.go`).
- **`ptah-compat script`** (`internal/atlasscript`) runs `query`, `exec` and
  `loop` scripts. `exec` and `loop` open a transaction with `BeginTx`, bind the
  script's `args` to the statement, and read `sql.Result.RowsAffected` for
  `expect_rows`.

The embedded YDB engine runs all of these through the `database/sql` driver of
ydb-go-sdk, and adds what YQL needs around a check:

- A check is one read-only YQL `SELECT`, which YDB's own checks prove from the
  text (`internal/migratorext/ydb`, `checks.Validate`).
- A check reads only tables of the database: `checks.ReadProof` reads the
  catalog and refuses an assertion that reaches an external data source or
  table (`ydbschema.ProveLocalReads`).
- A check runs in a snapshot read-only transaction (`ReadOnlyChecks`).

And YDB's driver answers a write with a row count it did not measure, which the
descriptor states as `Scripts.RowCountsUnmeasured` (`internal/builtindialects/ydb.go`);
`ptah-compat script` reads it and refuses `expect_rows` there rather than judge
it against a zero (`internal/cli/atlas/script.go`).

The contract has no service for this. `Execute` runs units and returns an
outcome and a count of confirmed units, no rows. `ReadRows` reads one table by
a structured reference. ADR 0028 rejected a general `database/sql` proxy,
because the host would keep composing YQL and calling a service would no
longer be the only way engine text reaches the engine. Here the host composes
nothing: every statement is text the operator wrote, as a seed file and a
migration body are.

## Decision

### One feature for operator statements

The feature `sessions.statements` adds three RPCs. They are new, so the host
calls them only when the provider and the engine list the feature, and it is
not a two-way feature.

```proto
rpc RunStatement(RunStatementRequest) returns (StatementResult);
rpc ReadStatementRows(ReadStatementRowsRequest) returns (StatementRows);
rpc CloseStatement(CloseStatementRequest) returns (Empty);

message RunStatementRequest {
  string session_id = 1;
  // Correlation only, as for Execute.
  string request_id = 2;
  // Empty runs outside an explicit transaction. Otherwise the handle must
  // belong to session_id and remain active. Never set with READ_ONLY.
  string transaction_id = 3;
  // One statement, as the operator wrote it.
  string sql = 4;
  // Bound in the order the statement's placeholders take them, in the
  // engine's own placeholder syntax.
  repeated DataValue arguments = 5;
  enum Access { ACCESS_UNSPECIFIED = 0; READ_WRITE = 1; READ_ONLY = 2; }
  Access access = 6;
  // From 1 to 10,000: the rows of the first page. A page also stays within
  // the message limit.
  uint32 max_rows = 7;
}
message StatementResult {
  string request_id = 1;
  enum Outcome {
    OUTCOME_UNSPECIFIED = 0;
    // The statement ran. columns, rows and rows_affected say what it did.
    SUCCEEDED = 1;
    // The engine refused the statement and it changed nothing. error says why.
    FAILED = 2;
    // The provider ran nothing: a READ_ONLY statement it cannot prove, or a
    // statement its engine cannot run here. refusal says why.
    REFUSED = 3;
    // The provider cannot say whether the statement ran.
    UNKNOWN = 4;
  }
  Outcome outcome = 2;
  // The result's columns, in order, with their engine types. Empty for a
  // statement that returns no rows.
  repeated DataColumn columns = 3;
  // Every value is set; null is a value.
  repeated Row rows = 4;
  // Present only when the engine measured how many rows the statement changed.
  optional uint64 rows_affected = 5;
  // Empty exactly when every row is in this reply.
  string next_page_token = 6;
  // FAILED only: the server's message for the statement and its code, such
  // as an SQLSTATE.
  string error = 7;
  string error_code = 8;
  // REFUSED only: one line naming what stopped the statement.
  string refusal = 9;
}
message ReadStatementRowsRequest {
  string session_id = 1;
  string page_token = 2;
  uint32 max_rows = 3;
}
message StatementRows {
  repeated Row rows = 1;
  // Empty exactly when this page is the last one.
  string next_page_token = 2;
}
message CloseStatementRequest {
  string session_id = 1;
  string page_token = 2;
}
```

A value travels as the `DataValue` of ADR 0028, and a column as its
`DataColumn`, so a row read by `ReadRows` and a row a statement returns have one
shape. The host turns a value into the Go value `database/sql` gives for the
same kind, and formats it as it formats one today.

A result larger than one page stays open on the provider's side. The token
names it, `ReadStatementRows` returns the next page, and `CloseStatement`
releases it before the last page. Finishing the transaction or closing the
session releases every result opened in it, so a result never outlives what it
was read in.

### Read-only statements are the provider's to prove

`READ_ONLY` is what a check asks for. The provider runs the statement only when
it can prove, before running it, that the statement is one query that changes
nothing and reads only objects of the session's database, and runs it where a
write is refused, such as a snapshot read-only transaction. Otherwise it answers
`REFUSED` and names what it could not prove. This is YDB's `checks.Validate`,
`ReadProof` and `ReadOnlyChecks`, held by the owner of the grammar. The host's
text checks for its own dialects do not apply to a provider's engine: they read
the statement with another engine's grammar. The host keeps what holds on every
engine: one column, one row, and the truth of the value.

`READ_WRITE` runs the statement as written. It is what `exec` steps, test
assertions and scripts ask for, on a database the operator named.

### Row counts are reported only when measured

`rows_affected` is present only when the engine measured the count. The host
reads its absence as "not reported": `sql.Result.RowsAffected` returns an error,
and `expect_rows` is refused rather than judged, which is what YDB's descriptor
flag does today. The flag stays where it is for the built-in engines; a
provider's engine states the same fact on each result.

### Errors keep the server's words

A test assertion matches the error a statement raises, so `error` carries the
server's message for that statement and `error_code` its code. The provider
removes what the transport adds, such as a server address, and never includes
credentials. The message is at most 4 KiB and may span lines; it is text the
operator's statement provoked, which is why it may hold one of its values, as
the `database/sql` error does today. `refusal` is the provider's own one line,
as every other diagnostic of the contract is.

### How the host uses it

The host gives the commands above a `database/sql` driver of its own over these
RPCs and `BeginTransaction` and `FinishTransaction`, so `*sql.Rows`, `*sql.Tx`
and `sql.Result` keep the meaning the callers read. Only those callers take the
handle. The connection's general `QueryContext` and `ExecContext` keep refusing
a provider's engine, so host code that writes engine SQL of its own, such as a
catalog query or a revision table `DROP`, still cannot reach one by accident.

### Validation

Both transport boundaries refuse:

- a request with no session, no statement, a `max_rows` outside 1 to 10,000, an
  unspecified access, or a transaction with `READ_ONLY`;
- a reply whose request ID changed, or whose outcome is unspecified;
- columns, rows, `rows_affected` or a page token on any outcome but `SUCCEEDED`;
  a row whose length differs from the columns; more rows than `max_rows`;
- an `error` on any outcome but `FAILED`, a `FAILED` without one, or one over
  4 KiB or holding a NUL; an `error_code` over 64 bytes or holding a space;
- a `refusal` on any outcome but `REFUSED`, a `REFUSED` without one, or one that
  is not one line;
- a page of more rows than its request asked for.

A reply the host cannot read leaves the outcome unknown. The host never sends a
statement again after `UNKNOWN`.

### Trust

`RunStatement` acts on one session the host opened under ADR 0021, and the host
decides which statements to send: every one is text the operator wrote into a
check, a test or a script. `READ_ONLY` is the host's request and the provider's
proof; a provider that runs a statement it did not prove breaks the contract,
as a replay guard that waves a statement through does.

### Versioning

`sessions.statements` is a compatible addition, part of contract 25.4. A
provider that serves it does not raise its minimum contract: an older host never
calls the RPCs, and refuses those commands for the engine as it does today.

## Alternatives considered

**Extend `Execute` with results.** A unit would return rows, a row count and the
server's error. `Execute` runs units a plan rendered, under the statement
timeout and the outcome rules of a migration; a result would be one more field
for every one of them to ignore, and a check needs a read-only proof that a
plan unit does not. Rejected.

**Prove checks offline and run them with `Execute`.** The provider would judge a
check's text, as `GuardReplay` judges a replayed statement, and the host would
run it. `Execute` returns no rows, and the proof that a check reads only local
tables reads the catalog of the session's database, which an offline call does
not have. Rejected.

**Keep the commands off provider engines.** `db verify`, `schema test`,
`migrations test`, migration checks and scripts would refuse a provider's engine
by name. YDB runs all of them today, so the flip would drop them. Rejected.

**A session capability for row counts.** ADR 0030 planned a capability the
session reports, `execute.row-counts`, read by the script runner. A capability
says what a session does for every statement; an engine can measure the count
of one statement and not of another, such as a scheme statement and a data
statement on YDB. The result says it for the statement it ran, and needs no new
capability key. Rejected; see the amendment to ADR 0030.

**One reply with every row.** A statement would return all its rows at once or
fail. A script's query report and a test's `result_set` have no bound the host
can set, and the 4 MiB limit would turn a large result into a refusal the
embedded engine does not have. Rejected; the pages follow `ReadRows`.

## Consequences

- The SDK grows three RPCs, their messages and one feature. The rows reuse the
  value types of ADR 0028.
- The host gains a `database/sql` driver over a provider session, used by
  `db verify`, migration checks, `migration/dbtest` and `ptah-compat script`, and
  reads row counts from the result rather than from a YDB descriptor flag.
- The YDB provider proves checks read-only and local, which moves
  `internal/migratorext/ydb`'s checks and `ProveLocalReads` into the plugin.
- An assertion that matches the server's message keeps working. One that matches
  what the YDB driver adds around it, such as the server address, does not: the
  provider removes it.
- A provider that serves `sessions.statements` runs SQL the operator wrote on
  the operator's database. That is what these commands do today on every
  built-in engine.

## Acceptance criteria

These are for the implementation. None holds today.

- The SDK carries the three RPCs as contract 25.4, and both boundaries refuse
  each case under "Validation". In-process tests cover paging, a result closed
  before its last page, a result released by its transaction, and a lost reply
  reported as unknown and not sent again; each validator is mutation-checked.
- The fixture provider serves `sessions.statements` on SQLite: a query over two
  pages, a write with a measured count, a write with no count, a failed
  statement with the server's message, and a `READ_ONLY` statement it refuses
  because it writes.
- The host runs `db verify`, a migration check, a `schema test` case with every
  assertion kind, and a `ptah-compat script` of each kind through the fixture
  provider, and refuses `expect_rows` on a result with no count.
- The YDB provider's CI runs the same commands on each YDB line it supports,
  refuses a check that reads an external table, and refuses `expect_rows`.
