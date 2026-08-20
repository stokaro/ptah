package mssql

// White-box testing required: the body extractor is the half of a procedure
// read that comes out of statement text rather than the catalog, and it has no
// exported entry point.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// sys.sql_modules keeps the whole CREATE statement, so a procedure's body has
// to be cut out of it. The function walk finds the body by scanning past
// RETURNS, which a procedure does not have, so the same walk would run off the
// end and hand back the entire statement (stokaro/ptah#1784).

func TestProcedureBody_StartsAfterTheOpeningAs(t *testing.T) {
	c := qt.New(t)

	body := procedureBody("CREATE PROCEDURE dbo.p @id int AS\nBEGIN\n  SELECT @id;\nEND;")

	c.Assert(body, qt.Equals, "BEGIN\n  SELECT @id;\nEND")
}

// TestProcedureBody_SkipsTheExecuteAsClause is the case the function walk could
// not have reached. Measured on SQL Server 2025, a procedure created
// `WITH EXECUTE AS OWNER AS BEGIN ... END` keeps both words in the definition,
// so taking the first standalone AS reports `OWNER AS BEGIN ... END` as the
// body and replans the procedure on every run.
func TestProcedureBody_SkipsTheExecuteAsClause(t *testing.T) {
	c := qt.New(t)

	body := procedureBody("CREATE PROCEDURE dbo.p @n int WITH EXECUTE AS OWNER AS\nBEGIN\n  SELECT @n;\nEND;")

	c.Assert(body, qt.Equals, "BEGIN\n  SELECT @n;\nEND")
	c.Assert(body, qt.Not(qt.Contains), "OWNER")
}

// TestProcedureBody_IgnoresAsInsideBrackets holds the depth rule: a bracketed
// identifier or a parameter list can carry the word without opening a body.
func TestProcedureBody_IgnoresAsInsideBrackets(t *testing.T) {
	c := qt.New(t)

	body := procedureBody("CREATE PROCEDURE [dbo].[as] (@x int) AS\nSELECT 1;")

	c.Assert(body, qt.Equals, "SELECT 1")
}

// TestProcedureBody_IgnoresAsInsideAStringLiteral is the other skip the walk
// owes: a default or a comment carrying the word is not a body opener.
func TestProcedureBody_IgnoresAsInsideAStringLiteral(t *testing.T) {
	c := qt.New(t)

	body := procedureBody("CREATE PROCEDURE dbo.p @s varchar(20) AS\nSELECT ' AS ' + @s;")

	c.Assert(body, qt.Equals, "SELECT ' AS ' + @s")
}

// TestProcedureBody_DiffersFromTheFunctionWalk is the point of having a second
// walk: handed a procedure, the function one returns the whole statement,
// because it never finds the RETURNS it scans for.
func TestProcedureBody_DiffersFromTheFunctionWalk(t *testing.T) {
	c := qt.New(t)
	definition := "CREATE PROCEDURE dbo.p @id int AS\nBEGIN\n  SELECT @id;\nEND;"

	fromProcedureWalk := procedureBody(definition)
	fromFunctionWalk := functionBody(definition)

	c.Assert(fromProcedureWalk, qt.Equals, "BEGIN\n  SELECT @id;\nEND")
	c.Assert(fromFunctionWalk, qt.Contains, "CREATE PROCEDURE")
}

// TestFunctionBody_StillCutsAFunction is the control: adding the second walk
// must not change what the first one answers.
func TestFunctionBody_StillCutsAFunction(t *testing.T) {
	c := qt.New(t)

	body := functionBody("CREATE FUNCTION dbo.f (@n int)\nRETURNS int\nAS\nBEGIN\n  RETURN @n;\nEND;")

	c.Assert(body, qt.Equals, "BEGIN\n  RETURN @n;\nEND")
}

func TestRoutineKind_SeparatesTheTwo(t *testing.T) {
	c := qt.New(t)

	c.Assert(isProcedureObjectType("P "), qt.IsTrue)
	c.Assert(isProcedureObjectType("FN"), qt.IsFalse)
	c.Assert(routineKind("P "), qt.Equals, "procedure")
	c.Assert(routineKind("FN"), qt.Equals, "")
}

// TestProcedureAwareReturns_AnswersNothingForAProcedure keeps the empty return
// a decision rather than an accident of a missing catalog row.
func TestProcedureAwareReturns_AnswersNothingForAProcedure(t *testing.T) {
	c := qt.New(t)

	c.Assert(procedureAwareReturns("P ", "int"), qt.Equals, "")
	c.Assert(procedureAwareReturns("FN", "int"), qt.Equals, "int")
}
