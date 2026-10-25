package postgresreplay

// White-box testing required: this file verifies the unexported transaction
// reading of the PostgreSQL replay guard.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
)

// The guard reads a file through filetx, whose tests hold the full grammar,
// and answers the boundaries as indexes. A file with no transaction control
// answers none.
func TestTransactionBoundaries_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		want       []int
	}{
		{name: "no transaction control", statements: []string{"CREATE TABLE a (id int)"}, want: nil},
		{name: "two transactions", statements: []string{"BEGIN", "SELECT 1", "COMMIT", "SELECT 2", "BEGIN", "COMMIT"}, want: []int{0, 2, 4, 5}},
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

// A file filetx refuses is refused as the replay's own structure error, which
// names the replay and the statement.
func TestTransactionBoundaries_FailurePath(t *testing.T) {
	c := qt.New(t)
	got, err := guard{dialect: platform.Postgres}.TransactionBoundaries([]string{"BEGIN", "BEGIN", "COMMIT", "COMMIT"})
	c.Assert(got, qt.IsNil)
	var structure *replay.TransactionStructureError
	c.Assert(err, qt.ErrorAs, &structure)
	c.Assert(structure.Index, qt.Equals, 1)
	c.Assert(err, qt.ErrorMatches, `postgres migration replay rejects transaction control at statement 2: a transaction is already open since statement 1`)
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
