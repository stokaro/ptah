package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// functionSchema declares a single stored function, with no table, so the
// rendered output is about that object and nothing else.
func functionSchema(fn goschema.Function) *goschema.Database {
	return &goschema.Database{Functions: []goschema.Function{fn}}
}

// TestRender_MySQLFamilyEmitsTheCharacteristicTheEngineDemands pins the clause
// that decides whether the statement is accepted at all.
//
// With binary logging on and log_bin_trust_function_creators off -- the
// defaults of the mysql:26.7 image docker-compose.yaml pins -- MySQL 26.7.0
// refuses a function carrying none of DETERMINISTIC, NO SQL or READS SQL DATA:
//
//	CREATE FUNCTION f() RETURNS integer RETURN 1
//	  -> Error 1418 (HY000)
//
// Measured against the same server, MODIFIES SQL DATA and a bare
// NOT DETERMINISTIC are refused with that same 1418, which is why the VOLATILE
// row below expects READS SQL DATA rather than the MODIFIES spelling its name
// suggests. A renderer that emitted the latter would render a statement the
// engine will not accept, and no offline test that only checks "a
// characteristic is present" would notice.
func TestRender_MySQLFamilyEmitsTheCharacteristicTheEngineDemands(t *testing.T) {
	tests := []struct {
		name       string
		volatility string
		want       string
	}{
		{name: "immutable is the only deterministic one", volatility: "IMMUTABLE", want: "DETERMINISTIC"},
		{name: "stable takes the remaining accepted cell", volatility: "STABLE", want: "NOT DETERMINISTIC NO SQL"},
		{name: "volatile reads rather than modifies", volatility: "VOLATILE", want: "READS SQL DATA"},
		{name: "unset reads", volatility: "", want: "READS SQL DATA"},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				sql, err := renderer.GetOrderedCreateStatements(functionSchema(goschema.Function{
					Name: "func_probe", Returns: "integer", Language: "sql",
					Volatility: test.volatility, Body: "RETURN 1",
				}), dialect)

				c.Assert(err, qt.IsNil)
				joined := strings.Join(sql, "\n")
				c.Check(joined, qt.Contains, "CREATE FUNCTION `func_probe`() RETURNS integer "+test.want)
				// MODIFIES SQL DATA is refused with the same Error 1418 whether
				// it stands alone or follows NOT DETERMINISTIC, so no volatility
				// may render it. Measured across all fifteen combinations on
				// MySQL 26.7.0; see mysqlroutine.Characteristic for the grid.
				c.Check(joined, qt.Not(qt.Contains), "MODIFIES SQL DATA")
			})
		}
	}
}

// TestRender_MySQLFamilyRefusesCaseCollidingFunctionNames pins that two
// declarations the target cannot tell apart are refused rather than silently
// reduced to one.
//
// The duplicate-definition check in core/goschema keys functions by their exact
// name, which is right for PostgreSQL, where `Foo` and `foo` really are two
// functions. On MySQL and MariaDB they are one routine and the comparator folds
// them, so the two keyings disagreed -- and the disagreement lost a declaration
// instead of reporting it. Measured on MySQL 26.7.0 and MariaDB 12.3.2, two
// declared functions produced `FunctionsAdded = [ptah_dup_fn]`, one planned
// statement, and one row in information_schema.ROUTINES after an apply that
// exited 0.
//
// The PostgreSQL rows are the control and they are the reason this is not in
// the dialect-blind validator: folding there would refuse a schema PostgreSQL
// hosts perfectly well.
func TestRender_MySQLFamilyRefusesCaseCollidingFunctionNames(t *testing.T) {
	colliding := &goschema.Database{Functions: []goschema.Function{
		{
			Name: "Ptah_Dup_Fn", Returns: "int", Language: "sql",
			Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN 1",
		},
		{
			Name: "ptah_dup_fn", Returns: "int", Language: "sql",
			Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN 2",
		},
	}}
	distinct := &goschema.Database{Functions: []goschema.Function{
		{
			Name: "ptah_dup_one", Returns: "int", Language: "sql",
			Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN 1",
		},
		{
			Name: "ptah_dup_two", Returns: "int", Language: "sql",
			Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN 2",
		},
	}}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect+"/colliding is refused", func(t *testing.T) {
			c := qt.New(t)
			_, err := renderer.GetOrderedCreateStatements(colliding, dialect)

			c.Assert(err, qt.IsNotNil)
			c.Check(err.Error(), qt.Contains, "Ptah_Dup_Fn")
			c.Check(err.Error(), qt.Contains, "ptah_dup_fn")
			c.Check(err.Error(), qt.Contains, "case")
		})
		t.Run(dialect+"/two distinct names are fine", func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(distinct, dialect)

			c.Assert(err, qt.IsNil)
			c.Check(statements, qt.HasLen, 2)
		})
	}

	// Control: PostgreSQL routine names are case-sensitive, so the same schema
	// is legitimate there and must still render both functions.
	t.Run("postgres hosts both spellings", func(t *testing.T) {
		c := qt.New(t)
		statements, err := renderer.GetOrderedCreateStatements(colliding, platform.Postgres)

		c.Assert(err, qt.IsNil)
		c.Check(statements, qt.HasLen, 2)
	})
}

// TestRender_MySQLFamilyRendersOneStatementPerElement pins the invariant that
// makes this list executable statement by statement.
//
// A whole-schema render targets a database that does not have these objects
// yet, so no function needs a drop here. The visitor used to emit
// `DROP FUNCTION IF EXISTS` in front of every CREATE anyway, which put two
// statements in one element -- and an element is what the compatibility
// dev-database path hands to ExecuteSQL unchanged. go-sql-driver refuses a
// two-statement string unless multiStatements is on, and convertMySQLURL does
// not turn it on, so materializing any desired schema containing a function
// failed with Error 1064 on both engines.
//
// The replace shape a MODIFIED function needs did not go away; it moved to the
// planner, which now emits the drop as its own node. `CREATE OR REPLACE
// FUNCTION` is still never rendered -- it is Error 1064 on MySQL 26.7.0 -- so
// the family still shares one shape.
func TestRender_MySQLFamilyRendersOneStatementPerElement(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			sql, err := renderer.GetOrderedCreateStatements(functionSchema(goschema.Function{
				Name: "func_probe", Returns: "integer", Language: "sql", Body: "RETURN 1",
			}), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.HasLen, 1)
			c.Check(strings.Count(sql[0], ";"), qt.Equals, 1)
			c.Check(sql[0], qt.Contains, "CREATE FUNCTION `func_probe`")
			c.Check(sql[0], qt.Not(qt.Contains), "DROP FUNCTION")
			c.Check(sql[0], qt.Not(qt.Contains), "CREATE OR REPLACE FUNCTION")
		})
	}
}

// TestRender_MySQLFamilyNoLongerBlamesTheEngine is the wording guard.
//
// `-- CREATE FUNCTION f1 not supported in MySQL` is a claim about the server,
// and the server contradicts it: the same statement this renderer now emits was
// ACCEPTED on MySQL 26.7.0 and MariaDB 10.11.18. An operator who read the old
// sentence concluded their database could not host a function.
func TestRender_MySQLFamilyNoLongerBlamesTheEngine(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			sql, err := renderer.GetOrderedCreateStatements(functionSchema(goschema.Function{
				Name: "func_probe", Returns: "integer", Language: "sql", Body: "RETURN 1",
			}), dialect)

			c.Assert(err, qt.IsNil)
			joined := strings.Join(sql, "\n")
			c.Check(joined, qt.Not(qt.Contains), "not supported in MySQL")
			c.Check(joined, qt.Not(qt.Contains), "not supported in MariaDB")
		})
	}
}

// TestRender_SQLServerNamesTheFunctionItDoesNotGenerate is the control for the
// three tests above: it is the one target of the three named in #929 whose
// preset now declares Functions false, and its diagnostic must name Ptah rather
// than the engine, because SQL Server hosts functions perfectly well.
//
// Without this row a mutant that made every dialect emit DDL would still pass
// the MySQL-family tests.
func TestRender_SQLServerNamesTheFunctionItDoesNotGenerate(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.GetOrderedCreateStatements(functionSchema(goschema.Function{
		Name: "func_probe", Returns: "integer", Language: "sql", Body: "RETURN 1",
	}), platform.SQLServer)

	c.Assert(err, qt.IsNil)
	joined := strings.Join(sql, "\n")
	c.Check(joined, qt.Contains, `-- SQLSERVER: CREATE FUNCTION "func_probe" is not generated for this target; skipped.`)
	c.Check(joined, qt.Not(qt.Contains), "not supported in SQL Server")
}
