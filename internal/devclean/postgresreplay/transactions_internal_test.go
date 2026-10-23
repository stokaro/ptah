package postgresreplay

// White-box testing required: this file verifies the unexported transaction
// reading of the PostgreSQL replay guard.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
)

func TestTransactionBoundaries_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		want       []int
	}{
		{
			name:       "no transaction control",
			statements: []string{"CREATE TABLE a (id int)", "INSERT INTO a VALUES (1)"},
			want:       nil,
		},
		{
			name:       "file wrapped in one transaction",
			statements: []string{"BEGIN", "CREATE TABLE a (id int)", "COMMIT"},
			want:       []int{0, 2},
		},
		{
			name:       "start transaction and end",
			statements: []string{"START TRANSACTION", "CREATE TABLE a (id int)", "END"},
			want:       []int{0, 2},
		},
		{
			name:       "work and transaction noise words",
			statements: []string{"BEGIN WORK", "SELECT 1", "COMMIT TRANSACTION"},
			want:       []int{0, 2},
		},
		{
			name:       "modes",
			statements: []string{"BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY, NOT DEFERRABLE", "SELECT 1", "COMMIT WORK"},
			want:       []int{0, 2},
		},
		{
			name:       "serializable deferrable",
			statements: []string{"START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE", "SELECT 1", "COMMIT"},
			want:       []int{0, 2},
		},
		{
			name:       "two transactions in one file with work between",
			statements: []string{"BEGIN", "SELECT 1", "COMMIT", "CREATE TABLE a (id int)", "BEGIN", "SELECT 2", "COMMIT"},
			want:       []int{0, 2, 4, 6},
		},
		{
			name:       "lower case",
			statements: []string{"begin", "select 1", "commit"},
			want:       []int{0, 2},
		},
		{
			name:       "begin inside a routine body is not a boundary",
			statements: []string{"CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$"},
			want:       nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := guard{dialect: platform.Postgres}.TransactionBoundaries(test.statements)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestTransactionBoundaries_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		wantIndex  int
		wantReason string
	}{
		{
			name:       "transaction never closed",
			statements: []string{"BEGIN", "CREATE TABLE a (id int)"},
			wantIndex:  0,
			wantReason: "never closed",
		},
		{
			name:       "commit without begin",
			statements: []string{"CREATE TABLE a (id int)", "COMMIT"},
			wantIndex:  1,
			wantReason: "no transaction is open",
		},
		{
			name:       "end without begin",
			statements: []string{"END"},
			wantIndex:  0,
			wantReason: "no transaction is open",
		},
		{
			name:       "nested begin",
			statements: []string{"BEGIN", "BEGIN", "COMMIT", "COMMIT"},
			wantIndex:  1,
			wantReason: "already open since statement 1",
		},
		{
			name:       "second commit",
			statements: []string{"BEGIN", "COMMIT", "COMMIT"},
			wantIndex:  2,
			wantReason: "no transaction is open",
		},
		{
			name:       "rollback",
			statements: []string{"BEGIN", "CREATE TABLE a (id int)", "ROLLBACK"},
			wantIndex:  2,
			wantReason: "ROLLBACK takes the file's own work back",
		},
		{
			name:       "abort",
			statements: []string{"BEGIN", "ABORT"},
			wantIndex:  1,
			wantReason: "ABORT takes the file's own work back",
		},
		{
			name:       "rollback outside a transaction",
			statements: []string{"ROLLBACK"},
			wantIndex:  0,
			wantReason: "ROLLBACK takes the file's own work back",
		},
		{
			name:       "savepoint",
			statements: []string{"BEGIN", "SAVEPOINT s", "COMMIT"},
			wantIndex:  1,
			wantReason: "SAVEPOINT is partial transaction state",
		},
		{
			name:       "release savepoint",
			statements: []string{"BEGIN", "RELEASE SAVEPOINT s", "COMMIT"},
			wantIndex:  1,
			wantReason: "RELEASE is partial transaction state",
		},
		{
			name:       "rollback to savepoint",
			statements: []string{"BEGIN", "SAVEPOINT s", "ROLLBACK TO SAVEPOINT s", "COMMIT"},
			wantIndex:  1,
			wantReason: "SAVEPOINT is partial transaction state",
		},
		{
			name:       "commit and chain",
			statements: []string{"BEGIN", "COMMIT AND CHAIN"},
			wantIndex:  1,
			wantReason: "AND [NO] CHAIN",
		},
		{
			name:       "commit and no chain",
			statements: []string{"BEGIN", "COMMIT AND NO CHAIN"},
			wantIndex:  1,
			wantReason: "AND [NO] CHAIN",
		},
		{
			name:       "prepare transaction",
			statements: []string{"BEGIN", "PREPARE TRANSACTION 'x'"},
			wantIndex:  1,
			wantReason: "PREPARE TRANSACTION leaves a prepared transaction",
		},
		{
			name:       "commit prepared",
			statements: []string{"COMMIT PREPARED 'x'"},
			wantIndex:  0,
			wantReason: "COMMIT PREPARED leaves a prepared transaction",
		},
		{
			name:       "unknown begin mode",
			statements: []string{"BEGIN SNAPSHOT 'abc'", "COMMIT"},
			wantIndex:  0,
			wantReason: "not a transaction mode",
		},
		{
			name:       "trailing words after commit",
			statements: []string{"BEGIN", "COMMIT garbage"},
			wantIndex:  1,
			wantReason: "which the replay does not read",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := guard{dialect: platform.Postgres}.TransactionBoundaries(test.statements)
			c.Assert(got, qt.IsNil)
			var structure *replay.TransactionStructureError
			c.Assert(err, qt.ErrorAs, &structure)
			c.Assert(structure.Index, qt.Equals, test.wantIndex)
			c.Assert(structure.Reason, qt.Contains, test.wantReason)
		})
	}
}

// A dialect of the family other than PostgreSQL reads none: the words carry
// other semantics there, and each statement stays refused by Validate.
func TestTransactionBoundaries_OtherDialectsReadNone(t *testing.T) {
	for _, dialect := range []string{platform.CockroachDB, platform.YugabyteDB, platform.Spanner} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			got, err := guard{dialect: dialect}.TransactionBoundaries([]string{"BEGIN", "SELECT 1", "COMMIT"})
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.IsNil)
		})
	}
}

// The boundary statements are accepted only through TransactionBoundaries.
// Validate, which reads one statement, still refuses each of them.
func TestValidate_TransactionControlStaysRefusedStatementByStatement(t *testing.T) {
	for _, statement := range []string{"BEGIN", "COMMIT", "END", "START TRANSACTION", "ROLLBACK", "SAVEPOINT s"} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			err := guard{dialect: platform.Postgres}.Validate(statement, replay.Scope{Session: true, Server: true})
			c.Assert(err, qt.ErrorMatches, `postgres migration replay rejects .* session or transaction state .*`)
		})
	}
}

func TestValidatePostgresReplayStatement_TransactionScopedLock_HappyPath(t *testing.T) {
	statements := []string{
		`LOCK TABLE tax_codes IN SHARE ROW EXCLUSIVE MODE`,
		`LOCK app.users IN ACCESS EXCLUSIVE MODE NOWAIT`,
		`LOCK TABLE ONLY "a", "b" IN ROW SHARE MODE`,
		`LOCK TABLE users`,
	}

	for _, statement := range statements {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)

			err := validatePostgresReplayStatement(platform.Postgres, replay.Tokens(statement, lexerOptions()), replay.Scope{})

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidatePostgresReplayStatement_TransactionScopedLock_FailurePath(t *testing.T) {
	const generic = `postgres migration replay rejects LOCK session or transaction state because .*`
	const catalog = `postgres migration replay rejects LOCK of protected namespace "pg_catalog" because .*`
	tests := []struct {
		name      string
		dialect   string
		statement string
		wantErr   string
	}{
		{name: "catalog table", dialect: platform.Postgres, statement: `LOCK TABLE pg_catalog.pg_class IN ACCESS EXCLUSIVE MODE`, wantErr: catalog},
		{name: "second table in a catalog", dialect: platform.Postgres, statement: `LOCK TABLE users, pg_catalog.pg_class`, wantErr: catalog},
		{name: "unknown mode", dialect: platform.Postgres, statement: `LOCK TABLE users IN WHATEVER MODE`, wantErr: generic},
		{name: "trailing words", dialect: platform.Postgres, statement: `LOCK TABLE users IN SHARE MODE EXTRA`, wantErr: generic},
		{name: "no table", dialect: platform.Postgres, statement: `LOCK TABLE`, wantErr: generic},
		{
			name:      "other dialect of the family",
			dialect:   platform.CockroachDB,
			statement: `LOCK TABLE users IN SHARE MODE`,
			wantErr:   `cockroachdb migration replay rejects LOCK session or transaction state because .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := validatePostgresReplayStatement(test.dialect, replay.Tokens(test.statement, lexerOptions()), replay.Scope{})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
