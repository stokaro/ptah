//go:build integration

package gonative_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbschema/mysql"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// propertyRoundTripDatabase is the database this file owns outright. Like
// roundTripDatabase it must match the `ptah_rt_%` pattern the CI grant scopes
// ptah_user to, and it must never be the shared ptah_test: applyPropertySQL
// below executes every statement the planner emits, and the planner is told the
// desired schema is the whole truth about the connected database, so a desired
// schema declaring one function and no tables plans a DROP for every table it
// finds. It is a different name from roundTripDatabase so that the two files
// cannot delete each other's objects if they ever run concurrently.
const propertyRoundTripDatabase = "ptah_rt_fnprops"

func newPropertyDatabase(c *qt.C, dsn string) *sql.DB {
	c.Helper()

	admin, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer admin.Close()

	// Heal a database an earlier crashed run may have left behind.
	_, err = admin.Exec("DROP DATABASE IF EXISTS " + propertyRoundTripDatabase)
	c.Assert(err, qt.IsNil)
	_, err = admin.Exec("CREATE DATABASE " + propertyRoundTripDatabase)
	c.Assert(err, qt.IsNil)

	config, err := mysqldriver.ParseDSN(dsn)
	c.Assert(err, qt.IsNil)
	config.DBName = propertyRoundTripDatabase
	db, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)

	// Registered first so it runs LAST: c.Cleanup is LIFO, and the drop
	// registered below must still have a usable connection.
	c.Cleanup(func() { _ = db.Close() })
	c.Cleanup(func() { dropPropertyDatabase(c, dsn) })

	return db
}

// dropPropertyDatabase removes the owned database and verifies the server
// agrees it is gone. The verification is the point: a cleanup whose error is
// discarded is indistinguishable from one that worked, and it runs on the
// failure path too, where leaks actually happen.
func dropPropertyDatabase(c *qt.C, dsn string) {
	c.Helper()

	admin, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer admin.Close()

	_, err = admin.Exec("DROP DATABASE IF EXISTS " + propertyRoundTripDatabase)
	c.Check(err, qt.IsNil)

	var remaining int
	c.Check(admin.QueryRow(
		"SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?",
		propertyRoundTripDatabase).Scan(&remaining), qt.IsNil)
	c.Check(remaining, qt.Equals, 0,
		qt.Commentf("database %s survived cleanup", propertyRoundTripDatabase))

	var routines int
	c.Check(admin.QueryRow(
		"SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ?",
		propertyRoundTripDatabase).Scan(&routines), qt.IsNil)
	c.Check(routines, qt.Equals, 0,
		qt.Commentf("routines survived in %s", propertyRoundTripDatabase))
}

func propertyFunction(fn goschema.Function) *goschema.Database {
	return &goschema.Database{Functions: []goschema.Function{fn}}
}

func applyPropertySQL(c *qt.C, db *sql.DB, dialect string, desired *goschema.Database) []string {
	c.Helper()

	reader := mysql.NewMySQLReader(db, "")
	live, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)

	// CompareWithDialect, not Compare: the dialect is what tells the comparator
	// that `integer` and `int` are one type and that two spellings of a routine
	// name are one routine. Production reaches the same place through
	// CompareWithDatabase, which sets opts.Dialect from the live connection.
	diff := schemadiff.CompareWithDialect(desired, live, dialect)
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, desired, dialect)
	c.Assert(err, qt.IsNil)

	for _, statement := range statements {
		_, execErr := db.Exec(statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return statements
}

func propertyDiff(c *qt.C, db *sql.DB, dialect string, desired *goschema.Database) *difftypes.SchemaDiff {
	c.Helper()

	reader := mysql.NewMySQLReader(db, "")
	live, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	return schemadiff.CompareWithDialect(desired, live, dialect)
}

// mysqlFamilyTargets is the pair every test in this file runs against. MariaDB
// is not a copy of the MySQL case: the two engines report different catalog
// spellings for the same declaration, and MariaDB's image ships with binary
// logging off while MySQL's ships with it on, so the two are different
// deployments of the same rules.
var mysqlFamilyTargets = []struct {
	name    string
	dialect string
	dsn     func(*testing.T) string
}{
	{name: "mysql", dialect: platform.MySQL, dsn: skipIfNoMySQL},
	{name: "mariadb", dialect: platform.MariaDB, dsn: skipIfNoMariaDB},
}

// TestFunctionPropertyRoundTrip_MySQLFamily_Integration is the acceptance for
// the whole "a property the renderer writes cannot be read back" class.
//
// Rendering proves nothing here. Each case declares a function, applies the
// plan to a live server, reads the catalog back through the production reader
// and diffs against the SAME desired state. An empty diff is the only evidence
// that the property survived the round trip. The second apply is what separates
// a real fix from a coincidence: before this change, a declared STABLE function
// reported `volatility: VOLATILE -> STABLE` after the first apply AND after the
// second, so the schema planned the same destructive DROP-then-CREATE forever.
//
// The `changed` half is the other direction and it is equally load-bearing: a
// comparator that reported nothing would also produce an empty diff, so each
// case then changes exactly that property and requires exactly one change,
// named.
func TestFunctionPropertyRoundTrip_MySQLFamily_Integration(t *testing.T) {
	base := goschema.Function{
		Name: "ptah_prop_fn", Parameters: "a INT", Returns: "int",
		Language: "sql", Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN a + 1",
	}

	withVolatility := func(v string) goschema.Function {
		fn := base
		fn.Volatility = v
		return fn
	}
	withSecurity := func(s string) goschema.Function {
		fn := base
		fn.Security = s
		return fn
	}

	tests := []struct {
		name     string
		declared goschema.Function
		changed  goschema.Function
		property string
	}{
		{
			name:     "volatility immutable to stable",
			declared: withVolatility("IMMUTABLE"),
			changed:  withVolatility("STABLE"),
			property: "volatility",
		},
		{
			name:     "volatility stable to volatile",
			declared: withVolatility("STABLE"),
			changed:  withVolatility("VOLATILE"),
			property: "volatility",
		},
		{
			name:     "volatility volatile to immutable",
			declared: withVolatility("VOLATILE"),
			changed:  withVolatility("IMMUTABLE"),
			property: "volatility",
		},
		{
			name:     "security invoker to definer",
			declared: withSecurity("INVOKER"),
			changed:  withSecurity("DEFINER"),
			property: "security",
		},
		{
			name:     "security definer to invoker",
			declared: withSecurity("DEFINER"),
			changed:  withSecurity("INVOKER"),
			property: "security",
		},
		{
			// The desired side canonicalizes INTEGER to `integer` while both
			// catalogs report the base type `int`. Comparing those exactly
			// reported `parameters, returns` drift on a function that already
			// matched, on every inspection.
			name: "type alias INTEGER against the catalog's int",
			declared: goschema.Function{
				Name: "ptah_prop_fn", Parameters: "a INTEGER", Returns: "INTEGER",
				Language: "sql", Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN a + 1",
			},
			changed: goschema.Function{
				Name: "ptah_prop_fn", Parameters: "a INTEGER", Returns: "INTEGER",
				Language: "sql", Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN a + 2",
			},
			property: "body",
		},
	}

	for _, target := range mysqlFamilyTargets {
		for _, test := range tests {
			t.Run(target.name+"/"+test.name, func(t *testing.T) {
				dsn := target.dsn(t)
				c := qt.New(t)
				db := newPropertyDatabase(c, dsn)

				desired := propertyFunction(test.declared)

				// 1. The plan carries the function.
				created := applyPropertySQL(c, db, target.dialect, desired)
				c.Assert(created, qt.Not(qt.HasLen), 0)

				// 2. The server really hosts it: the catalog answering, not
				//    Ptah's rendered text.
				var value int
				c.Assert(db.QueryRow("SELECT ptah_prop_fn(41)").Scan(&value), qt.IsNil)
				c.Check(value, qt.Equals, 42)

				// 3. Round trip: reading it back and diffing yields nothing.
				c.Check(propertyDiff(c, db, target.dialect, desired).HasChanges(), qt.IsFalse)

				// 4. And it still yields nothing after a second apply. This is
				//    the row that fails when a property cannot survive a read:
				//    the diff is not merely non-empty, it is PERMANENT.
				applyPropertySQL(c, db, target.dialect, desired)
				c.Check(propertyDiff(c, db, target.dialect, desired).HasChanges(), qt.IsFalse)

				// 5. Changing exactly that property is exactly one change,
				//    named. A comparator that reported nothing would pass 3
				//    and 4 and fail here.
				changedDiff := propertyDiff(c, db, target.dialect, propertyFunction(test.changed))
				c.Assert(changedDiff.FunctionsModified, qt.HasLen, 1)
				c.Check(changedDiff.FunctionsModified[0].FunctionName, qt.Equals, "ptah_prop_fn")
				c.Check(changedDiff.FunctionsModified[0].Changes, qt.HasLen, 1)
				c.Check(changedDiff.FunctionsModified[0].Changes[test.property], qt.Not(qt.Equals), "")
				c.Check(changedDiff.FunctionsAdded, qt.HasLen, 0)
				c.Check(changedDiff.FunctionsRemoved, qt.HasLen, 0)

				// 6. Planning that change converges again.
				applyPropertySQL(c, db, target.dialect, propertyFunction(test.changed))
				c.Check(propertyDiff(c, db, target.dialect, propertyFunction(test.changed)).HasChanges(), qt.IsFalse)
			})
		}
	}
}

// TestFunctionOrderedCreateStatements_ExecuteOneByOne_Integration is the
// acceptance for the compatibility dev-database path.
//
// It executes every element of GetOrderedCreateStatements through the writer
// dbschema.ConnectToDatabase builds, unchanged, which is exactly what
// materializeOnDev does. convertMySQLURL does not enable go-sql-driver's
// multiStatements option, so an element holding two statements is refused:
// before this change the single element was
//
//	DROP FUNCTION IF EXISTS `p_fn`;
//	CREATE FUNCTION `p_fn`(a INT) RETURNS int DETERMINISTIC ...
//
// and both engines answered Error 1064 at line 2. The planner integration test
// could not see it because GenerateSchemaDiffSQLStatements splits its output
// before executing.
func TestFunctionOrderedCreateStatements_ExecuteOneByOne_Integration(t *testing.T) {
	for _, target := range mysqlFamilyTargets {
		t.Run(target.name, func(t *testing.T) {
			dsn := target.dsn(t)
			c := qt.New(t)
			newPropertyDatabase(c, dsn)

			desired := propertyFunction(goschema.Function{
				Name: "ptah_prop_fn", Parameters: "a INT", Returns: "int",
				Language: "sql", Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN a + 1",
			})

			statements, err := renderer.GetOrderedCreateStatements(desired, target.dialect)
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.Not(qt.HasLen), 0)

			config, err := mysqldriver.ParseDSN(dsn)
			c.Assert(err, qt.IsNil)
			config.DBName = propertyRoundTripDatabase
			url := fmt.Sprintf("%s://%s", target.dialect, config.FormatDSN())

			conn, err := dbschema.ConnectToDatabase(context.Background(), url)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = conn.Close() })

			for i, statement := range statements {
				c.Check(conn.Writer().ExecuteSQL(context.Background(), statement), qt.IsNil,
					qt.Commentf("element %d/%d: %s", i+1, len(statements), statement))
			}

			// The function is really there, so the loop above was not vacuous:
			// a renderer that emitted nothing would also pass every ExecuteSQL.
			c.Assert(queryRoutineCount(c, dsn, "ptah_prop_fn"), qt.Equals, 1)
		})
	}
}

func queryRoutineCount(c *qt.C, dsn, name string) int {
	c.Helper()

	admin, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer admin.Close()

	var count int
	c.Assert(admin.QueryRow(
		"SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = ? AND ROUTINE_NAME = ? AND ROUTINE_TYPE = 'FUNCTION'",
		propertyRoundTripDatabase, name).Scan(&count), qt.IsNil)
	return count
}

// TestFunctionCaseOnlySpelling_MySQLFamily_Integration is the acceptance for
// routine-name case folding.
//
// Stored-routine names are case-insensitive on both engines -- measured on
// MySQL 26.7.0, with `foo` in the catalog `SELECT Foo(1)` resolves to it and
// `CREATE FUNCTION BAR` is Error 1304 while `bar` exists -- but the diff keyed
// them by exact spelling. Live `ptah_case_fn` against desired `Ptah_Case_Fn`
// therefore produced BOTH an addition and a removal; the planner created the
// new spelling and then executed `DROP FUNCTION IF EXISTS` on the old one,
// which resolves to the same routine, and a SUCCESSFUL apply left the database
// with no function at all.
//
// The count after the apply is the assertion that matters: it was 0.
//
// Both directions are exercised, and that is not symmetry for its own sake.
// Folding is needed on BOTH sides of the comparison, and a fix that folded only
// one side would still converge whenever the unfolded side happened to be
// lowercase already. A first version of this test created the function as
// `ptah_case_fn` and desired `Ptah_Case_Fn`; a mutant that folded only the
// desired side survived it, because folding `Ptah_Case_Fn` reaches the exact
// key the catalog already had. The mixed-case LIVE row is what makes the
// database-side fold load-bearing.
func TestFunctionCaseOnlySpelling_MySQLFamily_Integration(t *testing.T) {
	makeFunction := func(name string) *goschema.Database {
		return propertyFunction(goschema.Function{
			Name: name, Parameters: "a INT", Returns: "int",
			Language: "sql", Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN a + 1",
		})
	}

	tests := []struct {
		name    string
		live    string
		desired string
	}{
		{
			// Exercises the DATABASE-side fold: the catalog spelling is mixed
			// case, so an unfolded database key never meets the desired one.
			name: "live mixed case against desired lower case",
			live: "Ptah_Case_Fn", desired: "ptah_case_fn",
		},
		{
			// Exercises the DESIRED-side fold, the other half of the same rule.
			name: "live lower case against desired mixed case",
			live: "ptah_case_fn", desired: "Ptah_Case_Fn",
		},
	}

	for _, target := range mysqlFamilyTargets {
		for _, test := range tests {
			t.Run(target.name+"/"+test.name, func(t *testing.T) {
				dsn := target.dsn(t)
				c := qt.New(t)
				db := newPropertyDatabase(c, dsn)

				applyPropertySQL(c, db, target.dialect, makeFunction(test.live))
				c.Assert(queryRoutineCount(c, dsn, test.live), qt.Equals, 1)

				// The two spellings are one routine, so there is nothing to do.
				caseDiff := propertyDiff(c, db, target.dialect, makeFunction(test.desired))
				c.Check(caseDiff.FunctionsAdded, qt.HasLen, 0)
				c.Check(caseDiff.FunctionsRemoved, qt.HasLen, 0)
				c.Check(caseDiff.FunctionsModified, qt.HasLen, 0)

				// And applying it plans nothing and destroys nothing.
				applied := applyPropertySQL(c, db, target.dialect, makeFunction(test.desired))
				c.Check(applied, qt.HasLen, 0)
				c.Check(queryRoutineCount(c, dsn, test.live), qt.Equals, 1,
					qt.Commentf("a successful apply left no function of that name"))

				var value int
				c.Assert(db.QueryRow("SELECT ptah_case_fn(41)").Scan(&value), qt.IsNil)
				c.Check(value, qt.Equals, 42)
			})
		}
	}
}

// TestFunctionParametersIgnoreASameNamedProcedure_Integration is the acceptance
// for restricting the parameter read to functions.
//
// Both engines let one schema hold a procedure and a function of the same name,
// and information_schema.PARAMETERS keys both by SPECIFIC_NAME. Measured on
// MySQL 26.7.0, `dual_name(a INT) RETURNS int` beside
// `dual_name(IN p_x VARCHAR(50), IN p_y DECIMAL(10,2))` returned three rows for
// that name under a query filtered only by schema and ordinal, and the
// function's reconstructed signature became
// `a int, p_x varchar(50), p_y decimal(10,2)` -- parameter drift no apply could
// ever resolve.
//
// The procedure is created directly rather than through Ptah because Ptah does
// not declare procedures; it is a fact about the database the reader must
// tolerate, not a thing under test.
func TestFunctionParametersIgnoreASameNamedProcedure_Integration(t *testing.T) {
	for _, target := range mysqlFamilyTargets {
		t.Run(target.name, func(t *testing.T) {
			dsn := target.dsn(t)
			c := qt.New(t)
			db := newPropertyDatabase(c, dsn)

			desired := propertyFunction(goschema.Function{
				Name: "ptah_prop_fn", Parameters: "a INT", Returns: "int",
				Language: "sql", Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN a + 1",
			})
			applyPropertySQL(c, db, target.dialect, desired)
			c.Check(propertyDiff(c, db, target.dialect, desired).HasChanges(), qt.IsFalse)

			_, err := db.Exec(
				"CREATE PROCEDURE ptah_prop_fn(IN p_x VARCHAR(50), IN p_y DECIMAL(10,2)) SELECT 1")
			c.Assert(err, qt.IsNil)

			// The procedure's arguments must not reach the function's signature.
			reader := mysql.NewMySQLReader(db, "")
			live, err := reader.ReadSchema()
			c.Assert(err, qt.IsNil)
			c.Assert(live.Functions, qt.HasLen, 1)
			c.Check(live.Functions[0].Parameters, qt.Equals, "a int")
			c.Check(live.Functions[0].Parameters, qt.Not(qt.Contains), "p_x")
			c.Check(live.Functions[0].Parameters, qt.Not(qt.Contains), "p_y")

			// So the schema still converges with the procedure sitting there.
			c.Check(propertyDiff(c, db, target.dialect, desired).HasChanges(), qt.IsFalse)

			// The procedure is this test's own object; take it away explicitly
			// so the owned-database drop is not the only thing standing between
			// it and a leak.
			c.Cleanup(func() {
				_, dropErr := db.Exec("DROP PROCEDURE IF EXISTS ptah_prop_fn")
				c.Check(dropErr, qt.IsNil)
			})
		})
	}
}

// TestFunctionReplacementIsTwoStatements_Integration pins that the drop a
// replacement needs is still planned, now as its own statement.
//
// Splitting the pair out of the renderer could have lost the replacement
// altogether, which would be a worse defect than the one it fixes: the CREATE
// alone is Error 1304 "FUNCTION ... already exists" on both engines. This runs
// a real modification against a live server and requires both halves.
func TestFunctionReplacementIsTwoStatements_Integration(t *testing.T) {
	for _, target := range mysqlFamilyTargets {
		t.Run(target.name, func(t *testing.T) {
			dsn := target.dsn(t)
			c := qt.New(t)
			db := newPropertyDatabase(c, dsn)

			first := propertyFunction(goschema.Function{
				Name: "ptah_prop_fn", Parameters: "a INT", Returns: "int",
				Language: "sql", Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN a + 1",
			})
			created := applyPropertySQL(c, db, target.dialect, first)
			// An ADDED function needs no drop: nothing of that name is there.
			c.Assert(created, qt.HasLen, 1)
			c.Check(created[0], qt.Contains, "CREATE FUNCTION")
			c.Check(created[0], qt.Not(qt.Contains), "DROP FUNCTION")

			second := propertyFunction(goschema.Function{
				Name: "ptah_prop_fn", Parameters: "a INT", Returns: "int",
				Language: "sql", Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN a + 2",
			})
			replaced := applyPropertySQL(c, db, target.dialect, second)
			c.Assert(replaced, qt.HasLen, 2)
			c.Check(replaced[0], qt.Contains, "DROP FUNCTION IF EXISTS")
			c.Check(replaced[1], qt.Contains, "CREATE FUNCTION")
			// Each statement stands alone: no element carries a second one.
			for i, statement := range replaced {
				c.Check(strings.Count(statement, ";"), qt.Equals, 0,
					qt.Commentf("element %d ends without a terminator: %s", i+1, statement))
			}

			c.Check(propertyDiff(c, db, target.dialect, second).HasChanges(), qt.IsFalse)
			var value int
			c.Assert(db.QueryRow("SELECT ptah_prop_fn(40)").Scan(&value), qt.IsNil)
			c.Check(value, qt.Equals, 42)
		})
	}
}
