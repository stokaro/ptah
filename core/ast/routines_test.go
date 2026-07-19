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
