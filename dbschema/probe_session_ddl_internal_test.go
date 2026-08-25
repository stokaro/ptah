package dbschema

// White-box testing required: what is under test is a statement
// WithRolledBackTransaction issues INSIDE the transaction it owns, observed by
// wiring a recording fake driver into a DatabaseConnection's unexported db
// field. Nothing in the public surface can observe the order of statements on
// one connection, and reaching it from outside would mean a live CockroachDB.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// TestWithRolledBackTransaction_AsksOnlyTheServerThatWouldCommitTheDDL pins who is asked to
// keep DDL inside the transaction, and who is not.
//
// CockroachDB defaults autocommit_before_ddl to on, so the DDL a probe creates
// makes the server commit the transaction the probe is holding. Measured on
// cockroachdb/cockroach:v25.4.0, `ptah schema apply --dry-run` against any
// schema with a CHECK constraint then failed on the recovery rather than on the
// probe:
//
//	ERROR: savepoint "ptah_check_probe" does not exist (SQLSTATE 3B001)
//
// The PostgreSQL row is not decoration. That server has no such variable and
// answers `unrecognized configuration parameter`, and a failed statement inside
// a PostgreSQL transaction poisons every later one -- so asking everybody would
// break the servers that were working (stokaro/ptah#2140).
func TestWithRolledBackTransaction_AsksOnlyTheServerThatWouldCommitTheDDL(t *testing.T) {
	tests := []struct {
		name    string
		version string
		wants   bool
	}{
		{
			name:    "a server that would commit the DDL",
			version: "CockroachDB CCL v25.4.0 (x86_64-pc-linux-gnu, built 2025/10/28)",
			wants:   true,
		},
		{
			// The control, and the one a too-wide rule breaks.
			name:    "PostgreSQL, which has no such variable",
			version: "PostgreSQL 17.6 on x86_64-pc-linux-gnu, compiled by gcc",
			wants:   false,
		},
		{
			// A server nobody recognized is left alone for the same reason as
			// PostgreSQL: an unknown variable is an error, and an error inside
			// the transaction is worse than the setting being wrong.
			name:    "a version this does not recognize",
			version: "",
			wants:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var executed []string
			db := dbtest.OpenWithExec(
				t,
				func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
					return dbtest.QueryResult{Columns: []string{"value"}, Rows: [][]driver.Value{{int64(1)}}}, nil
				},
				func(statement string, _ []driver.NamedValue) (driver.Result, error) {
					executed = append(executed, statement)
					return driver.RowsAffected(0), nil
				},
			)
			conn := &DatabaseConnection{db: db.SQL, info: types.DBInfo{Version: test.version}}

			ran, err := conn.WithRolledBackTransaction(context.Background(), "probe", func(ctx context.Context, tx *sql.Tx) error {
				_, execErr := tx.ExecContext(ctx, "CREATE TEMPORARY TABLE probe (id int)")
				return execErr
			})

			c.Assert(err, qt.IsNil)
			c.Assert(ran, qt.IsTrue)
			// The body's own statement is always there, so a run that executed
			// nothing cannot pass for one that asked correctly.
			c.Assert(executed, qt.Contains, "CREATE TEMPORARY TABLE probe (id int)")
			c.Assert(askedToKeepDDL(executed), qt.Equals, test.wants)
		})
	}
}

// TestWithRolledBackTransaction_AsksBeforeTheBodyRuns pins the order, which is the whole
// point: the setting has to be in force before the first DDL, because it is the
// first DDL that triggers the commit it prevents.
func TestWithRolledBackTransaction_AsksBeforeTheBodyRuns(t *testing.T) {
	c := qt.New(t)
	var executed []string
	db := dbtest.OpenWithExec(
		t,
		func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
			return dbtest.QueryResult{Columns: []string{"value"}, Rows: [][]driver.Value{{int64(1)}}}, nil
		},
		func(statement string, _ []driver.NamedValue) (driver.Result, error) {
			executed = append(executed, statement)
			return driver.RowsAffected(0), nil
		},
	)
	conn := &DatabaseConnection{
		db:   db.SQL,
		info: types.DBInfo{Version: "CockroachDB CCL v25.4.0"},
	}

	_, err := conn.WithRolledBackTransaction(context.Background(), "probe", func(ctx context.Context, tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx, "CREATE TEMPORARY TABLE probe (id int)")
		return execErr
	})

	c.Assert(err, qt.IsNil)
	c.Assert(indexOfKeepDDL(executed) < indexOf(executed, "CREATE TEMPORARY TABLE probe (id int)"), qt.IsTrue,
		qt.Commentf("statements: %v", executed))
}

// askedToKeepDDL reports whether the session was told to keep DDL where it was
// put, so a row states one fact rather than picking an assertion.
func askedToKeepDDL(executed []string) bool {
	return indexOfKeepDDL(executed) >= 0
}

func indexOfKeepDDL(executed []string) int {
	for i, statement := range executed {
		if strings.Contains(statement, "autocommit_before_ddl") {
			return i
		}
	}
	return -1
}

func indexOf(executed []string, want string) int {
	for i, statement := range executed {
		if statement == want {
			return i
		}
	}
	return -1
}
