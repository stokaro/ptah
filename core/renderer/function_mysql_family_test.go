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
	c := qt.New(t)

	tests := []struct {
		name       string
		volatility string
		want       string
	}{
		{name: "immutable is the only deterministic one", volatility: "IMMUTABLE", want: "DETERMINISTIC"},
		{name: "stable reads", volatility: "STABLE", want: "READS SQL DATA"},
		{name: "volatile reads rather than modifies", volatility: "VOLATILE", want: "READS SQL DATA"},
		{name: "unset reads", volatility: "", want: "READS SQL DATA"},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			c.Run(dialect+"/"+test.name, func(c *qt.C) {
				sql, err := renderer.GetOrderedCreateStatements(functionSchema(goschema.Function{
					Name: "func_probe", Returns: "integer", Language: "sql",
					Volatility: test.volatility, Body: "RETURN 1",
				}), dialect)

				c.Assert(err, qt.IsNil)
				joined := strings.Join(sql, "\n")
				c.Check(joined, qt.Contains, "CREATE FUNCTION `func_probe`() RETURNS integer "+test.want)
				// The refused spellings must never appear.
				c.Check(joined, qt.Not(qt.Contains), "MODIFIES SQL DATA")
				c.Check(joined, qt.Not(qt.Contains), "NOT DETERMINISTIC")
			})
		}
	}
}

// TestRender_MySQLFamilyReplacesWithDropThenCreate pins the replace shape.
//
// `CREATE OR REPLACE FUNCTION f() RETURNS integer DETERMINISTIC RETURN 2` is
// Error 1064 on MySQL 26.7.0. MariaDB 10.11.18 accepts it, but the
// drop-then-create pair is accepted by both, so the family shares one shape and
// a schema that converges on one engine converges on the other.
func TestRender_MySQLFamilyReplacesWithDropThenCreate(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		c.Run(dialect, func(c *qt.C) {
			sql, err := renderer.GetOrderedCreateStatements(functionSchema(goschema.Function{
				Name: "func_probe", Returns: "integer", Language: "sql", Body: "RETURN 1",
			}), dialect)

			c.Assert(err, qt.IsNil)
			joined := strings.Join(sql, "\n")
			c.Check(joined, qt.Contains, "DROP FUNCTION IF EXISTS `func_probe`;")
			c.Check(joined, qt.Not(qt.Contains), "CREATE OR REPLACE FUNCTION")
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
	c := qt.New(t)

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		c.Run(dialect, func(c *qt.C) {
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
