package migrator

// White-box testing required: the session-prefix classifier is a recovery
// safety boundary whose replay/reject decisions are not observable separately
// through the exported migration API.

import (
	"testing"

	qt "github.com/frankban/quicktest"
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

func TestPostgresSearchPathReplayState(t *testing.T) {
	tests := []struct {
		name        string
		statement   string
		wantChanged bool
		wantKnown   bool
	}{
		{name: "explicit assignment", statement: "SET search_path = app, public", wantChanged: true, wantKnown: true},
		{name: "explicit session assignment", statement: "SET SESSION search_path TO app", wantChanged: true, wantKnown: true},
		{name: "local assignment", statement: "SET LOCAL search_path TO app", wantChanged: true, wantKnown: false},
		{name: "default", statement: "SET search_path TO DEFAULT", wantChanged: true, wantKnown: false},
		{name: "current", statement: "SET search_path FROM CURRENT", wantChanged: true, wantKnown: false},
		{name: "role placeholder", statement: `SET search_path = "$user", public`, wantChanged: true, wantKnown: false},
		{name: "reset search path", statement: "RESET search_path", wantChanged: true, wantKnown: false},
		{name: "reset all", statement: "RESET ALL", wantChanged: true, wantKnown: false},
		{name: "unrelated setting", statement: "SET statement_timeout = '1s'", wantChanged: false, wantKnown: false},
		{name: "durable statement", statement: "CREATE TABLE users (id BIGINT)", wantChanged: false, wantKnown: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed, known := postgresSearchPathReplayState(test.statement)
			c.Assert(changed, qt.Equals, test.wantChanged)
			c.Assert(known, qt.Equals, test.wantKnown)
		})
	}
}
