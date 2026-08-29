package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/convert/toschema"
	"go.5x5.cz/ptah/internal/parser"
)

// renderedSQL reads a schema source and renders its routines back.
//
// It drives the whole path -- parse, convert to the model, convert back to the
// AST, render -- because the defect was a property the model filled in and the
// renderer emitted, and neither end is wrong on its own.
func renderedSQL(c *qt.C, dialect, sql string) string {
	c.Helper()

	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	database, err := toschema.ToDatabase(statements, dialect)
	c.Assert(err, qt.IsNil)
	schemamodel.Finalize(&database)
	c.Assert(database.Functions, qt.Not(qt.HasLen), 0)

	nodes := make([]ast.Node, 0, len(database.Functions))
	for _, function := range database.Functions {
		nodes = append(nodes, fromschema.FromFunction(function))
	}
	out, err := renderer.RenderSQL(dialect, nodes...)
	c.Assert(err, qt.IsNil)
	return out
}

// TestRenderSQL_AProcedureCarriesNoVolatility keeps the renderer from emitting
// DDL PostgreSQL refuses.
//
// Measured on PostgreSQL 18: `CREATE PROCEDURE p() ... LANGUAGE plpgsql
// SECURITY INVOKER VOLATILE` answers `ERROR: invalid attribute in procedure
// definition`, and the identical statement without VOLATILE succeeds and
// records prokind 'p'. The volatility default is filled in for every routine,
// so a procedure reaching the renderer carried one (stokaro/ptah#2435).
//
// SECURITY is deliberately still asserted present: it is accepted on both, and
// removing it along with the volatility would be a second change nothing
// measured.
func TestRenderSQL_AProcedureCarriesNoVolatility(t *testing.T) {
	c := qt.New(t)

	out := renderedSQL(c, "postgres",
		"CREATE PROCEDURE p() LANGUAGE plpgsql AS $$ BEGIN UPDATE t SET c = 1; END; $$;")

	c.Assert(out, qt.Contains, "CREATE OR REPLACE PROCEDURE")
	c.Assert(out, qt.Contains, "SECURITY INVOKER")
	c.Assert(out, qt.Not(qt.Contains), "VOLATILE",
		qt.Commentf("a procedure may not carry a volatility attribute:\n%s", out))
}

// TestRenderSQL_AFunctionStillCarriesItsVolatility is the control.
//
// A guard that removed the attribute from every routine would pass the test
// above and silently drop a property of every function in every schema.
func TestRenderSQL_AFunctionStillCarriesItsVolatility(t *testing.T) {
	c := qt.New(t)

	out := renderedSQL(c, "postgres",
		"CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$ BEGIN UPDATE t SET c = 1; END; $$;")

	c.Assert(out, qt.Contains, "CREATE OR REPLACE FUNCTION")
	c.Assert(out, qt.Contains, "VOLATILE")
}
