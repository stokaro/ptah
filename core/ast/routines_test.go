package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/ast/mocks"
)

func TestNewMySQLRoutine(t *testing.T) {
	c := qt.New(t)

	routine := ast.NewMySQLRoutine(" CREATE FUNCTION fn() RETURNS int RETURN 1; ", "mysql", ast.RoutineKindFunction)
	routine.Name = "fn"
	routine.Parameters = ""
	routine.Returns = "int"
	routine.SetCharacteristics([]string{"DETERMINISTIC"})
	routine.Body = ast.MySQLRoutineBody{
		SQL: "RETURN 1",
		Statements: []ast.MySQLRoutineStatement{{
			Kind: ast.MySQLRoutineStatementReturn,
			SQL:  "RETURN 1",
		}},
	}

	c.Assert(routine.SQL, qt.Equals, "CREATE FUNCTION fn() RETURNS int RETURN 1;")
	c.Assert(routine.Dialect, qt.Equals, "mysql")
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindFunction)
	c.Assert(routine.Name, qt.Equals, "fn")
	c.Assert(routine.Returns, qt.Equals, "int")
	c.Assert(routine.Characteristics, qt.DeepEquals, []string{"DETERMINISTIC"})
	c.Assert(routine.Body.Statements, qt.DeepEquals, []ast.MySQLRoutineStatement{{
		Kind: ast.MySQLRoutineStatementReturn,
		SQL:  "RETURN 1",
	}})
}

func TestMySQLRoutineNode_AcceptDelegatesToRawSQL(t *testing.T) {
	c := qt.New(t)

	routine := ast.NewMySQLRoutine(" CREATE PROCEDURE p() SELECT 1; ", "mysql", ast.RoutineKindProcedure)
	visitor := &mocks.MockVisitor{}

	err := routine.Accept(visitor)

	c.Assert(err, qt.IsNil)
	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{"RawSQL:CREATE PROCEDURE p() SELECT 1;"})
}

func TestMySQLRoutineNode_AcceptPropagatesRawSQLError(t *testing.T) {
	c := qt.New(t)

	routine := ast.NewMySQLRoutine("CREATE PROCEDURE p() SELECT 1;", "mysql", ast.RoutineKindProcedure)
	visitor := &mocks.MockVisitor{ReturnError: true}

	err := routine.Accept(visitor)

	c.Assert(err, qt.IsNotNil)
	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{"RawSQL:CREATE PROCEDURE p() SELECT 1;"})
}

func TestNewPostgresDoBlock(t *testing.T) {
	c := qt.New(t)

	block := ast.NewPostgresDoBlock(" DO $$ BEGIN RAISE NOTICE 'x'; END $$; ")

	c.Assert(block.SQL, qt.Equals, "DO $$ BEGIN RAISE NOTICE 'x'; END $$;")
	c.Assert(block.Language, qt.Equals, "")
	c.Assert(block.Body.SQL, qt.Equals, "")
}

func TestPostgresDoBlockNode_AcceptDelegatesToRawSQL(t *testing.T) {
	c := qt.New(t)

	block := ast.NewPostgresDoBlock(" DO $$ BEGIN PERFORM 1; END $$; ")
	visitor := &mocks.MockVisitor{}

	err := block.Accept(visitor)

	c.Assert(err, qt.IsNil)
	c.Assert(block.SQL, qt.Equals, "DO $$ BEGIN PERFORM 1; END $$;")
	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{"RawSQL:DO $$ BEGIN PERFORM 1; END $$;"})
}

func TestNewPostgresRoutine(t *testing.T) {
	c := qt.New(t)

	routine := ast.NewPostgresRoutine(" CREATE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$; ", "postgres", ast.RoutineKindProcedure)
	routine.Name = "p"
	routine.Language = "sql"

	c.Assert(routine.SQL, qt.Equals, "CREATE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$;")
	c.Assert(routine.Dialect, qt.Equals, "postgres")
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindProcedure)
	c.Assert(routine.Name, qt.Equals, "p")
	c.Assert(routine.Language, qt.Equals, "sql")
}

func TestPostgresRoutineNode_AcceptDelegatesToRawSQL(t *testing.T) {
	c := qt.New(t)

	routine := ast.NewPostgresRoutine(" CREATE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$; ", "postgres", ast.RoutineKindProcedure)
	visitor := &mocks.MockVisitor{}

	err := routine.Accept(visitor)

	c.Assert(err, qt.IsNil)
	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{"RawSQL:CREATE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$;"})
}

func TestNewSQLServerRoutine(t *testing.T) {
	c := qt.New(t)

	routine := ast.NewSQLServerRoutine(" CREATE FUNCTION [dbo].[f]() RETURNS int AS BEGIN RETURN 1; END ", "sqlserver", ast.RoutineKindFunction)
	routine.Name = "[dbo].[f]"
	routine.Returns = "int"
	routine.Form = ast.SQLServerRoutineFormScalarFunction

	c.Assert(routine.SQL, qt.Equals, "CREATE FUNCTION [dbo].[f]() RETURNS int AS BEGIN RETURN 1; END")
	c.Assert(routine.Dialect, qt.Equals, "sqlserver")
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindFunction)
	c.Assert(routine.Name, qt.Equals, "[dbo].[f]")
	c.Assert(routine.Returns, qt.Equals, "int")
	c.Assert(routine.Form, qt.Equals, ast.SQLServerRoutineFormScalarFunction)
}

func TestSQLServerRoutineNode_AcceptDelegatesToRawSQL(t *testing.T) {
	c := qt.New(t)

	routine := ast.NewSQLServerRoutine(" CREATE PROCEDURE [dbo].[p] AS SELECT 1; ", "sqlserver", ast.RoutineKindProcedure)
	visitor := &mocks.MockVisitor{}

	err := routine.Accept(visitor)

	c.Assert(err, qt.IsNil)
	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{"RawSQL:CREATE PROCEDURE [dbo].[p] AS SELECT 1;"})
}
