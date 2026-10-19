package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

func TestNewRoutine(t *testing.T) {
	c := qt.New(t)

	routine := ast.NewRoutine(" CREATE FUNCTION fn() RETURNS int DETERMINISTIC RETURN 1; ", "mysql", ast.RoutineKindFunction)
	routine.Name = "fn"
	routine.Returns = "int"
	routine.Options = []ast.RoutineOption{{Text: "DETERMINISTIC"}}
	routine.Body = ast.RoutineBody{SQL: "RETURN 1"}

	c.Assert(routine.SQL, qt.Equals, "CREATE FUNCTION fn() RETURNS int DETERMINISTIC RETURN 1;")
	c.Assert(routine.Dialect, qt.Equals, "mysql")
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindFunction)
	c.Assert(routine.Name, qt.Equals, "fn")
	c.Assert(routine.Returns, qt.Equals, "int")
	c.Assert(routine.Options, qt.DeepEquals, []ast.RoutineOption{{Text: "DETERMINISTIC"}})
	c.Assert(routine.Body, qt.Equals, ast.RoutineBody{SQL: "RETURN 1"})
}

// TestRoutineNode_AcceptHandsTheVisitorTheRoutine pins that the visitor
// receives the routine, with its dialect and kind still on it. A renderer needs
// those to decide the delimiter it wraps the body in, and a visitor handed the
// bare statement has neither.
func TestRoutineNode_AcceptHandsTheVisitorTheRoutine(t *testing.T) {
	c := qt.New(t)

	routine := ast.NewRoutine(" CREATE PROCEDURE p() SELECT 1; ", "mysql", ast.RoutineKindProcedure)
	visitor := &MockVisitor{}

	err := routine.Accept(visitor)

	c.Assert(err, qt.IsNil)
	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{"Routine:CREATE PROCEDURE p() SELECT 1;"})
	c.Assert(routine.Dialect, qt.Equals, "mysql")
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindProcedure)
}

func TestRoutineNode_AcceptPropagatesTheVisitorError(t *testing.T) {
	c := qt.New(t)

	routine := ast.NewRoutine("CREATE PROCEDURE p() SELECT 1;", "mysql", ast.RoutineKindProcedure)
	visitor := &MockVisitor{ReturnError: true}

	err := routine.Accept(visitor)

	c.Assert(err, qt.ErrorMatches, "mock error")
	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{"Routine:CREATE PROCEDURE p() SELECT 1;"})
}

func TestNewDoBlock(t *testing.T) {
	c := qt.New(t)

	block := ast.NewDoBlock(" DO $$ BEGIN RAISE NOTICE 'x'; END $$; ")

	c.Assert(block.SQL, qt.Equals, "DO $$ BEGIN RAISE NOTICE 'x'; END $$;")
	c.Assert(block.Language, qt.Equals, "")
	c.Assert(block.Body.SQL, qt.Equals, "")
}

func TestDoBlockNode_AcceptHandsTheVisitorTheBlock(t *testing.T) {
	c := qt.New(t)

	block := ast.NewDoBlock(" DO $$ BEGIN PERFORM 1; END $$; ")
	visitor := &MockVisitor{}

	err := block.Accept(visitor)

	c.Assert(err, qt.IsNil)
	c.Assert(block.SQL, qt.Equals, "DO $$ BEGIN PERFORM 1; END $$;")
	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{"DoBlock:DO $$ BEGIN PERFORM 1; END $$;"})
}
