package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mssql"
)

// SQL Server hosts stored procedures, and Ptah rendered them as a named skip
// because nothing read one back. It reads them now, so the renderer owes the
// statement (stokaro/ptah#1784).

// renderProcedure renders a minimal procedure node.
func renderProcedure(c *qt.C, parameters, body string) string {
	c.Helper()
	node := ast.NewCreateFunction("dbo.p_report").
		SetKind(goschema.FunctionKindProcedure).
		SetParameters(parameters).
		SetLanguage("sql").
		SetBody(body)

	sql, err := mssql.New().Render(node)

	c.Assert(err, qt.IsNil)
	return sql
}

func TestRenderer_ProcedureUsesTheProcedureStatement(t *testing.T) {
	c := qt.New(t)

	sql := renderProcedure(c, "@id int", "BEGIN\n  SELECT @id;\nEND")

	c.Assert(sql, qt.Contains, "CREATE OR ALTER PROCEDURE [dbo].[p_report] @id int")
	c.Assert(sql, qt.Contains, "BEGIN\n  SELECT @id;\nEND;")
}

// TestRenderer_ProcedureTakesNoReturnsClause is the property that separates the
// two statements: `CREATE PROCEDURE ... RETURNS` does not parse.
func TestRenderer_ProcedureTakesNoReturnsClause(t *testing.T) {
	c := qt.New(t)

	sql := renderProcedure(c, "@id int", "BEGIN\n  SELECT @id;\nEND")

	c.Assert(sql, qt.Not(qt.Contains), "RETURNS")
}

// TestRenderer_ProcedureParametersCarryNoParentheses holds the other syntactic
// difference. T-SQL answers `CREATE PROCEDURE p(@a int)` with
// `Incorrect syntax near '('`, so borrowing the function spelling would produce
// a statement the server refuses.
func TestRenderer_ProcedureParametersCarryNoParentheses(t *testing.T) {
	c := qt.New(t)

	sql := renderProcedure(c, "@id int, @label varchar(50)", "BEGIN\n  SELECT 1;\nEND")

	c.Assert(sql, qt.Contains, "PROCEDURE [dbo].[p_report] @id int, @label varchar(50)")
	c.Assert(sql, qt.Not(qt.Contains), "p_report](@id")
}

// TestRenderer_FunctionKeepsItsOwnSpelling is the control: the procedure branch
// must not change what a function renders as.
func TestRenderer_FunctionKeepsItsOwnSpelling(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateFunction("dbo.f_add").
		SetParameters("@n int").
		SetReturns("int").
		SetLanguage("sql").
		SetBody("BEGIN\n  RETURN @n + 1;\nEND")

	sql, err := mssql.New().Render(node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE OR ALTER FUNCTION [dbo].[f_add](@n int)")
	c.Assert(sql, qt.Contains, "RETURNS int")
}

// TestRenderer_DropUsesTheMatchingVerb is the half a live run found: DROP
// FUNCTION and DROP PROCEDURE are different statements, and the server answers
// the wrong one with "does not exist" about an object that is right there.
func TestRenderer_DropUsesTheMatchingVerb(t *testing.T) {
	c := qt.New(t)
	procedure := ast.NewDropFunction("dbo.p_report").SetKind(goschema.FunctionKindProcedure).SetIfExists()
	function := ast.NewDropFunction("dbo.f_add").SetIfExists()

	procedureSQL, procedureErr := mssql.New().Render(procedure)
	functionSQL, functionErr := mssql.New().Render(function)

	c.Assert(procedureErr, qt.IsNil)
	c.Assert(functionErr, qt.IsNil)
	c.Assert(procedureSQL, qt.Contains, "DROP PROCEDURE IF EXISTS [dbo].[p_report];")
	c.Assert(functionSQL, qt.Contains, "DROP FUNCTION IF EXISTS [dbo].[f_add];")
}
