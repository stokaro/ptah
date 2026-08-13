//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/dbschema/mysql"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// functionRoundTripSchema is the desired state both cases below converge on.
//
// The body is spelled the way the target engine spells it. That is the same
// contract the view and trigger renderers already carry: Ptah supplies the
// engine-correct envelope around a body the operator writes for the target.
func functionRoundTripSchema(body string) *goschema.Database {
	return &goschema.Database{
		Functions: []goschema.Function{{
			Name: "ptah_rt_fn", Parameters: "a INT", Returns: "int",
			Language: "sql", Volatility: "IMMUTABLE", Security: "INVOKER", Body: body,
		}},
	}
}

// applyPlannedSQL plans the diff between desired and the live database, executes
// every statement, and returns the statements it ran.
func applyPlannedSQL(c *qt.C, db *sql.DB, dialect string, desired *goschema.Database) []string {
	c.Helper()

	reader := mysql.NewMySQLReader(db, "")
	live, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	diff := schemadiff.Compare(desired, live)
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, desired, dialect)
	c.Assert(err, qt.IsNil)

	for _, statement := range statements {
		_, execErr := db.Exec(statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return statements
}

// readBackDiff reads the live catalog and diffs it against desired.
func readBackDiff(c *qt.C, db *sql.DB, desired *goschema.Database) *difftypes.SchemaDiff {
	c.Helper()

	reader := mysql.NewMySQLReader(db, "")
	live, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	return schemadiff.Compare(desired, live)
}

// TestFunctionRoundTrip_MySQLFamily_Integration is the acceptance for
// stokaro/ptah#929's function row on MySQL and MariaDB.
//
// Rendering a CREATE FUNCTION proves nothing on its own. The three parts the
// capability.Sequences doc comment requires of any preset claiming a key --
// emit, read back, plan -- only agree if a function declared once, applied to a
// live server and read back out of information_schema diffs to nothing. This
// asserts exactly that, and then that a changed body produces exactly one
// change rather than none (a silent planner) or two (a comparator that also
// false-diffs an attribute the catalog spells differently).
//
// The catalog is the oracle here, never Ptah's own generated SQL: the read side
// goes through the same information_schema.ROUTINES and
// information_schema.PARAMETERS queries the production reader uses.
//
// MariaDB is not a copy of the MySQL case. The two engines disagree with each
// other about the type they report -- MySQL 26.7.0 says `int` where MariaDB
// 10.11.18 says `int(11)` for the same declaration -- so a reader that
// normalized only what MySQL returns converges here and reports a permanent
// `parameters, returns` diff there.
func TestFunctionRoundTrip_MySQLFamily_Integration(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		dsn     func(*testing.T) string
	}{
		{name: "mysql", dialect: platform.MySQL, dsn: skipIfNoMySQL},
		{name: "mariadb", dialect: platform.MariaDB, dsn: skipIfNoMariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dsn := test.dsn(t)
			c := qt.New(t)

			db, err := sql.Open("mysql", dsn)
			c.Assert(err, qt.IsNil)
			defer db.Close()

			_, err = db.Exec("DROP FUNCTION IF EXISTS ptah_rt_fn")
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _, _ = db.Exec("DROP FUNCTION IF EXISTS ptah_rt_fn") })

			desired := functionRoundTripSchema("RETURN a + 1")

			// 1. The plan is not silent: it carries the function.
			created := applyPlannedSQL(c, db, test.dialect, desired)
			c.Assert(created, qt.Not(qt.HasLen), 0)

			// 2. The server really hosts it. This is the catalog answering,
			//    not Ptah's rendered text.
			var value int
			c.Assert(db.QueryRow("SELECT ptah_rt_fn(41)").Scan(&value), qt.IsNil)
			c.Check(value, qt.Equals, 42)

			// 3. Round trip: reading it back and diffing yields nothing.
			c.Check(readBackDiff(c, db, desired).HasChanges(), qt.IsFalse)

			// 4. A changed body is exactly one change, named as the body.
			changed := functionRoundTripSchema("RETURN a + 2")
			bodyDiff := readBackDiff(c, db, changed)
			c.Assert(bodyDiff.FunctionsModified, qt.HasLen, 1)
			c.Check(bodyDiff.FunctionsModified[0].FunctionName, qt.Equals, "ptah_rt_fn")
			c.Check(bodyDiff.FunctionsModified[0].Changes, qt.HasLen, 1)
			c.Check(bodyDiff.FunctionsModified[0].Changes["body"], qt.Not(qt.Equals), "")
			c.Check(bodyDiff.FunctionsAdded, qt.HasLen, 0)
			c.Check(bodyDiff.FunctionsRemoved, qt.HasLen, 0)

			// 5. And planning that change converges again.
			applyPlannedSQL(c, db, test.dialect, changed)
			c.Check(readBackDiff(c, db, changed).HasChanges(), qt.IsFalse)

			c.Assert(db.QueryRow("SELECT ptah_rt_fn(40)").Scan(&value), qt.IsNil)
			c.Check(value, qt.Equals, 42)
		})
	}
}
