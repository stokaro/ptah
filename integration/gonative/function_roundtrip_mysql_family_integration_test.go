//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/dbschema/mysql"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// roundTripDatabase is the database this test owns outright. It must match the
// `ptah_rt_%` pattern the CI grant scopes ptah_user to, and it must never be
// the shared ptah_test.
const roundTripDatabase = "ptah_rt_functions"

// newOwnedDatabase creates roundTripDatabase on the server dsn names and
// returns a connection whose default database is it.
//
// The test owns a whole database rather than cleaning up after itself in the
// shared one, because cleanup was never the real problem. applyPlannedSQL below
// executes EVERY statement the planner emits, and the planner is told the
// desired schema is the whole truth about the connected database -- so a
// desired schema declaring one function and no tables plans a DROP for every
// table it finds. Measured against a shared ptah_test holding one unrelated
// table, running this test emptied SHOW TABLES on both engines. No amount of
// dropping the function afterwards fixes that; the assumption is only true in a
// database this test alone owns, and here it is true by construction.
//
// It also confines the leak that started this: whatever the body creates goes
// away with the database.
func newOwnedDatabase(c *qt.C, dsn string) *sql.DB {
	c.Helper()

	admin, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer admin.Close()

	// Heal a database an earlier crashed run may have left behind, so a leak is
	// self-correcting rather than permanent.
	_, err = admin.Exec("DROP DATABASE IF EXISTS " + roundTripDatabase)
	c.Assert(err, qt.IsNil)
	_, err = admin.Exec("CREATE DATABASE " + roundTripDatabase)
	c.Assert(err, qt.IsNil)

	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	config.DBName = roundTripDatabase
	db, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)

	// Registered first so it runs LAST: t.Cleanup is LIFO, and a cleanup
	// registered later must be able to still use this connection. The original
	// version of this test used `defer db.Close()`, which runs before EVERY
	// t.Cleanup, so its drop ran on a closed connection, returned
	// "sql: database is closed", and had its error discarded -- which is
	// precisely how the leak stayed invisible.
	c.Cleanup(func() { _ = db.Close() })
	// Registered after, so it runs first. It opens its own connection so that it
	// depends on nothing another cleanup may already have released.
	c.Cleanup(func() { dropOwnedDatabase(c, dsn) })

	return db
}

// dropOwnedDatabase removes roundTripDatabase and verifies the server agrees it
// is gone.
//
// The verification is the point. A cleanup whose error is discarded is
// indistinguishable from one that worked, and it runs on the failure path too,
// where leaks actually happen. If this cannot clean up, this test fails rather
// than a neighbour's.
func dropOwnedDatabase(c *qt.C, dsn string) {
	c.Helper()

	admin, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer admin.Close()

	_, err = admin.Exec("DROP DATABASE IF EXISTS " + roundTripDatabase)
	c.Check(err, qt.IsNil)

	var remaining int
	c.Check(admin.QueryRow(
		"SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
		roundTripDatabase).Scan(&remaining), qt.IsNil)
	c.Check(remaining, qt.Equals, 0, qt.Commentf("database %s survived cleanup", roundTripDatabase))

	var routines int
	c.Check(admin.QueryRow(
		"SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ?",
		roundTripDatabase).Scan(&routines), qt.IsNil)
	c.Check(routines, qt.Equals, 0, qt.Commentf("routines survived in %s", roundTripDatabase))
}

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

			db := newOwnedDatabase(c, dsn)

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
