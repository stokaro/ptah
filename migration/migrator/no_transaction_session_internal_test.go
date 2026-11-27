package migrator

// White-box testing required: the session-prefix classifier is a recovery
// safety boundary whose replay/reject decisions are not observable separately
// through the exported migration API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/filetx"
)

func TestNoTransactionResumeAction(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      noTransactionPrefixAction
	}{
		{name: "empty", statement: "-- comment only", want: noTransactionPrefixDurable},
		{name: "durable create", statement: "CREATE TABLE users (id BIGINT)", want: noTransactionPrefixDurable},
		{name: "durable insert", statement: "INSERT INTO users VALUES (1)", want: noTransactionPrefixDurable},
		{name: "replay set", statement: "SET search_path = app, public", want: noTransactionPrefixReplay},
		{name: "replay reset", statement: "RESET ROLE", want: noTransactionPrefixReplay},
		{name: "replay pragma", statement: "PRAGMA foreign_keys = ON", want: noTransactionPrefixReplay},
		{name: "reject temporary table", statement: "CREATE TEMP TABLE work (id BIGINT)", want: noTransactionPrefixReject},
		{name: "reject temporary view", statement: "CREATE TEMPORARY VIEW work AS SELECT 1", want: noTransactionPrefixReject},
		{name: "reject select side effect", statement: "SELECT set_config('search_path', 'app', false)", want: noTransactionPrefixReject},
		{name: "reject procedural body", statement: "DO $$ BEGIN NULL; END $$", want: noTransactionPrefixReject},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := noTransactionResumeAction(test.statement, grammarForDialect("postgres"))
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A resumed body that runs its own transactions replays the session state of
// its committed prefix by the transactions the statements ran in. Inside one,
// the body's BEGIN and COMMIT and the state that ended with the transaction
// are durable, and a plain SET outlived it. A statement no transaction holds
// keeps the no-transaction rule, under which a MySQL LOCK TABLES is session
// state to replay.
func TestOwnTransactionResumeAction(t *testing.T) {
	statements := []string{
		"SET search_path = app", "BEGIN", "LOCK TABLE a IN ACCESS EXCLUSIVE MODE", "SET LOCAL lock_timeout = '1s'",
		"SET application_name = 'owntx'", "COMMIT", "CREATE TABLE b (id int)",
	}
	blocks, err := filetx.ReadUnwrapped(statements)
	qt.New(t).Assert(err, qt.IsNil)
	oneQuery := ownTransactionRun{blocks: blocks, statements: statements, transactions: blocks.AsOneQuery(len(statements)), oneQuery: true}
	perStatement := ownTransactionRun{blocks: blocks, statements: statements, transactions: blocks}
	// A second file: control the unwrapped reading accepts, and statements
	// whose text sets no session state, inside committed transactions.
	more := []string{
		"DO $$ BEGIN CREATE TABLE c (id int); END $$", "SELECT setval('s', 10)", "COMMIT", "BEGIN", "BEGIN",
		"SAVEPOINT s", "ROLLBACK TO SAVEPOINT s", "WITH v AS (SELECT 1) INSERT INTO c SELECT * FROM v", "CALL p()",
		"DO $$ BEGIN PERFORM set_config('search_path', 'x', false); END $$", "SELECT pg_advisory_lock(1)", "COMMIT",
	}
	moreBlocks, err := filetx.ReadUnwrapped(more)
	qt.New(t).Assert(err, qt.IsNil)
	moreOneQuery := ownTransactionRun{blocks: moreBlocks, statements: more, transactions: moreBlocks.AsOneQuery(len(more)), oneQuery: true}
	morePerStatement := ownTransactionRun{blocks: moreBlocks, statements: more, transactions: moreBlocks}
	tests := []struct {
		name      string
		run       ownTransactionRun
		index     int
		statement string
		dialect   string
		want      noTransactionPrefixAction
	}{
		{name: "a setting before BEGIN in its transaction", run: oneQuery, index: 0, statement: statements[0], dialect: "postgres", want: noTransactionPrefixReplay},
		{name: "the file's BEGIN", run: oneQuery, index: 1, statement: statements[1], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a lock inside a transaction", run: oneQuery, index: 2, statement: statements[2], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "SET LOCAL inside a transaction", run: oneQuery, index: 3, statement: statements[3], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a plain SET inside a transaction", run: oneQuery, index: 4, statement: statements[4], dialect: "postgres", want: noTransactionPrefixReplay},
		{name: "the file's COMMIT", run: oneQuery, index: 5, statement: statements[5], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a lock inside a block run per statement", run: perStatement, index: 2, statement: statements[2], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a setting outside the blocks run per statement", run: perStatement, index: 0, statement: statements[0], dialect: "postgres", want: noTransactionPrefixReplay},
		{
			name: "a MySQL lock no transaction holds", run: ownTransactionRun{}, index: 0, statement: "LOCK TABLES a WRITE",
			dialect: "mysql", want: noTransactionPrefixReplay,
		},
		{name: "a DO in a committed transaction", run: moreOneQuery, index: 0, statement: more[0], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a SELECT in a committed transaction", run: moreOneQuery, index: 1, statement: more[1], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a COMMIT outside a block", run: moreOneQuery, index: 2, statement: more[2], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a COMMIT outside a block run per statement", run: morePerStatement, index: 2, statement: more[2], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a BEGIN inside a block", run: moreOneQuery, index: 4, statement: more[4], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a savepoint", run: moreOneQuery, index: 5, statement: more[5], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a rollback to a savepoint", run: moreOneQuery, index: 6, statement: more[6], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a WITH in a committed transaction", run: moreOneQuery, index: 7, statement: more[7], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a CALL in a committed transaction", run: moreOneQuery, index: 8, statement: more[8], dialect: "postgres", want: noTransactionPrefixDurable},
		{name: "a DO that sets a session setting", run: moreOneQuery, index: 9, statement: more[9], dialect: "postgres", want: noTransactionPrefixReject},
		{name: "a session advisory lock", run: moreOneQuery, index: 10, statement: more[10], dialect: "postgres", want: noTransactionPrefixReject},
		{name: "a DO no transaction holds", run: morePerStatement, index: 0, statement: more[0], dialect: "postgres", want: noTransactionPrefixReject},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ownTransactionResumeAction(test.index, test.statement, grammarForDialect(test.dialect), test.run), qt.Equals, test.want)
		})
	}
}

func TestNoTransactionResumeAction_RejectsSQLServerTemporaryTable(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "local bare", statement: "CREATE TABLE #work (id BIGINT)"},
		{name: "global bare", statement: "CREATE TABLE ##work (id BIGINT)"},
		{name: "local bracketed", statement: "CREATE TABLE [#work] (id BIGINT)"},
		{name: "global bracketed", statement: "CREATE TABLE [##work] (id BIGINT)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := noTransactionResumeAction(test.statement, grammarForDialect("sqlserver"))
			c.Assert(got, qt.Equals, noTransactionPrefixReject)
		})
	}
}

// A T-SQL block, IF or WHILE is one statement of the body, and its first word
// says nothing about the statements it runs: it is durable when each of them
// is, and refused when one of them set state a replay would have to restore,
// since a replay would run the others a second time.
func TestNoTransactionResumeAction_ReadsWhatATransactSQLStatementRuns(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      noTransactionPrefixAction
	}{
		{name: "a block of inserts", statement: "BEGIN\n  INSERT INTO dbo.t VALUES (1);\n  INSERT INTO dbo.t VALUES (2);\nEND", want: noTransactionPrefixDurable},
		{name: "a TRY block and its CATCH block", statement: "BEGIN TRY INSERT INTO dbo.t VALUES (1); END TRY BEGIN CATCH UPDATE dbo.t SET id = 0; END CATCH", want: noTransactionPrefixDurable},
		{name: "an IF that creates a table", statement: "IF OBJECT_ID(N'dbo.t') IS NULL CREATE TABLE dbo.t (id int)", want: noTransactionPrefixDurable},
		{name: "a WHILE that deletes", statement: "WHILE 1 = 0 DELETE FROM dbo.t WHERE id = 1", want: noTransactionPrefixDurable},
		{name: "a block that declares a variable", statement: "BEGIN DECLARE @n int = 1; SET @n = 2; INSERT INTO dbo.t VALUES (@n); END", want: noTransactionPrefixDurable},
		{name: "statements with no semicolon between them", statement: "CREATE TABLE dbo.a (id int) INSERT INTO dbo.a VALUES (1)", want: noTransactionPrefixDurable},
		{name: "a block that sets a session option", statement: "BEGIN SET NOCOUNT ON; INSERT INTO dbo.t VALUES (1); END", want: noTransactionPrefixReject},
		{name: "an IF that sets a session option", statement: "IF 1 = 1 SET XACT_ABORT ON", want: noTransactionPrefixReject},
		{name: "a session option after a statement", statement: "CREATE TABLE dbo.a (id int) SET NOCOUNT ON", want: noTransactionPrefixReject},
		{name: "a block that fills a temporary table", statement: "BEGIN INSERT INTO #work VALUES (1); END", want: noTransactionPrefixReject},
		{name: "a block that runs a procedure", statement: "BEGIN EXEC dbo.p; END", want: noTransactionPrefixReject},
		{name: "a session option alone", statement: "SET NOCOUNT ON", want: noTransactionPrefixReplay},
		{name: "a procedure body", statement: "CREATE PROCEDURE dbo.p AS BEGIN SET NOCOUNT ON; SELECT 1; END", want: noTransactionPrefixDurable},
		{name: "an unclosed block", statement: "BEGIN INSERT INTO dbo.t VALUES (1);", want: noTransactionPrefixReject},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(noTransactionResumeAction(test.statement, grammarForDialect("sqlserver")), qt.Equals, test.want)
		})
	}
}
