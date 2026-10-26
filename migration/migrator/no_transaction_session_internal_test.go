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
			got := noTransactionResumeAction(test.statement, "postgres")
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
			c.Assert(ownTransactionResumeAction(test.index, test.statement, test.dialect, test.run), qt.Equals, test.want)
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
			got := noTransactionResumeAction(test.statement, "sqlserver")
			c.Assert(got, qt.Equals, noTransactionPrefixReject)
		})
	}
}

func TestIsTransactionControlStatement(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		dialect   string
		want      bool
	}{
		{name: "PostgreSQL begin", statement: "BEGIN", dialect: "postgres", want: true},
		{name: "PostgreSQL set transaction", statement: "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", dialect: "postgres", want: true},
		// A chained commit or rollback ends the migration transaction and opens
		// another one on the same session, so the connection still reports a
		// transaction while the body before it has already been made durable or
		// discarded. Reading "is this bare COMMIT or ROLLBACK?" instead of "does
		// this end the transaction?" lets exactly these two through.
		{name: "MySQL commit", statement: "COMMIT", dialect: "mysql", want: true},
		{name: "MySQL rollback", statement: "ROLLBACK", dialect: "mysql", want: true},
		{name: "MySQL commit and chain", statement: "COMMIT AND CHAIN", dialect: "mysql", want: true},
		{name: "MariaDB rollback and chain", statement: "ROLLBACK AND CHAIN", dialect: "mariadb", want: true},
		{name: "MySQL commit and no chain release", statement: "COMMIT AND NO CHAIN RELEASE", dialect: "mysql", want: true},
		{name: "MySQL start transaction", statement: "START TRANSACTION", dialect: "mysql", want: true},
		{name: "MySQL savepoint", statement: "SAVEPOINT ptah_save", dialect: "mysql", want: true},
		{name: "MySQL release savepoint", statement: "RELEASE SAVEPOINT ptah_save", dialect: "mysql", want: true},
		{name: "MySQL rollback to savepoint", statement: "ROLLBACK TO SAVEPOINT ptah_save", dialect: "mysql", want: true},
		{name: "PostgreSQL abort", statement: "ABORT", dialect: "postgres", want: true},
		{name: "MySQL autocommit zero", statement: "SET autocommit = 0", dialect: "mysql", want: true},
		{name: "MariaDB session autocommit", statement: "SET @@session.autocommit = 1", dialect: "mariadb", want: true},
		{name: "SQL Server implicit transactions", statement: "SET IMPLICIT_TRANSACTIONS ON", dialect: "sqlserver", want: true},
		{name: "SQL Server begin transaction", statement: "BEGIN TRANSACTION", dialect: "sqlserver", want: true},
		{name: "SQL Server save transaction", statement: "SAVE TRANSACTION ptah_save", dialect: "sqlserver", want: true},
		{name: "SQL Server begin try", statement: "BEGIN TRY", dialect: "sqlserver", want: false},
		{name: "SQL Server end try", statement: "END TRY", dialect: "sqlserver", want: false},
		{name: "PostgreSQL end", statement: "END", dialect: "postgres", want: true},
		{name: "PostgreSQL prepare transaction", statement: "PREPARE TRANSACTION 'ptah_tx'", dialect: "postgres", want: true},
		{name: "MySQL XA start", statement: "XA START 'ptah_tx'", dialect: "mysql", want: true},
		{name: "PostgreSQL search path", statement: "SET search_path = app, public", dialect: "postgres", want: false},
		{name: "durable create", statement: "CREATE TABLE users (id BIGINT)", dialect: "postgres", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := isTransactionControlStatement(test.statement, test.dialect)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestIsTransactionControlStatement_MySQLFamily(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		dialect   string
	}{
		{name: "MySQL start transaction", statement: "START TRANSACTION", dialect: "mysql"},
		{name: "MySQL begin", statement: "BEGIN", dialect: "mysql"},
		{name: "MySQL commit and chain", statement: "COMMIT AND CHAIN", dialect: "mysql"},
		{name: "MySQL rollback and chain", statement: "ROLLBACK AND CHAIN", dialect: "mysql"},
		{name: "MySQL savepoint", statement: "SAVEPOINT ptah", dialect: "mysql"},
		{name: "MySQL release savepoint", statement: "RELEASE SAVEPOINT ptah", dialect: "mysql"},
		{name: "MariaDB start transaction", statement: "START TRANSACTION", dialect: "mariadb"},
		{name: "MariaDB begin", statement: "BEGIN", dialect: "mariadb"},
		{name: "MariaDB commit and chain", statement: "COMMIT AND CHAIN", dialect: "mariadb"},
		{name: "MariaDB rollback and chain", statement: "ROLLBACK AND CHAIN", dialect: "mariadb"},
		{name: "MariaDB savepoint", statement: "SAVEPOINT ptah", dialect: "mariadb"},
		{name: "MariaDB release savepoint", statement: "RELEASE SAVEPOINT ptah", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := isTransactionControlStatement(test.statement, test.dialect)
			c.Assert(got, qt.IsTrue)
		})
	}
}
