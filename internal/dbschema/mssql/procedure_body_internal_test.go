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

// TestProcedureBody_IgnoresAsInsideAComment is the skip the test above claims
// and never made: its own doc names a comment, and the case it holds is a
// string literal. The two are different scans, and only one of them was there.
//
// Measured on SQL Server 2025 through a view rather than a procedure, because a
// view header is where a comment is routinely written -- the body came back as
// `lives here */ AS SELECT ...` and the replay was refused with `Incorrect
// syntax near 'lives'`. All three walks in this file shared the blindness
// (stokaro/ptah#2115).
func TestProcedureBody_IgnoresAsInsideAComment(t *testing.T) {
	c := qt.New(t)

	body := procedureBody("CREATE PROCEDURE dbo.p -- runs AS the nightly job\nAS\nSELECT 1;")

	c.Assert(body, qt.Equals, "SELECT 1")
}

// TestFunctionBody_IgnoresAsInsideAComment is the same question of the walk
// that cuts a function, whose RETURNS is scanned for the same way.
func TestFunctionBody_IgnoresAsInsideAComment(t *testing.T) {
	c := qt.New(t)

	body := functionBody("CREATE FUNCTION dbo.f () RETURNS int /* known AS the tally */ AS\nBEGIN RETURN 1 END")

	c.Assert(body, qt.Equals, "BEGIN RETURN 1 END")
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

// TestProcedureBody_SkipsAParametersAs holds the other AS a header can carry:
// T-SQL accepts `@a AS INT` in a procedure's parameter list, and the
// definition keeps it, measured on SQL Server 2025 RTM-CU9.
func TestProcedureBody_SkipsAParametersAs(t *testing.T) {
	c := qt.New(t)

	body := procedureBody("CREATE PROCEDURE dbo.p @a AS INT AS SELECT @a AS a;")

	c.Assert(body, qt.Equals, "SELECT @a AS a")
}

// TestFunctionBody_SkipsTheExecuteAsClause is the function's half of
// TestProcedureBody_SkipsTheExecuteAsClause. A function created `RETURNS int
// WITH EXECUTE AS OWNER AS BEGIN ... END` has the body `BEGIN ... END`. Read
// from the clause's AS, it would be `OWNER AS BEGIN ... END`, and the function
// would be replanned on every run.
func TestFunctionBody_SkipsTheExecuteAsClause(t *testing.T) {
	c := qt.New(t)

	body := functionBody("CREATE FUNCTION dbo.f() RETURNS int WITH EXECUTE AS OWNER AS BEGIN RETURN 1; END;")

	c.Assert(body, qt.Equals, "BEGIN RETURN 1; END")
}

// A function's optional AS does not become part of its catalog body.
func TestFunctionBody_OptionalAS(t *testing.T) {
	for _, test := range []struct{ name, definition, body string }{
		{"scalar", "CREATE FUNCTION dbo.f() RETURNS INT WITH EXECUTE AS OWNER BEGIN RETURN 1; END;", "BEGIN RETURN 1; END"},
		{"inline table", "CREATE FUNCTION dbo.f() RETURNS TABLE RETURN SELECT 1 AS id;", "RETURN SELECT 1 AS id"},
		{"return variable", "CREATE FUNCTION dbo.f() RETURNS @begin TABLE (id INT) WITH EXECUTE AS OWNER BEGIN INSERT @begin VALUES (1); RETURN; END;", "BEGIN INSERT @begin VALUES (1); RETURN; END"},
	} {
		t.Run(test.name, func(t *testing.T) { c := qt.New(t); c.Assert(functionBody(test.definition), qt.Equals, test.body) })
	}
}
