package exprkey_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/exprkey"
)

// One argument list declared on two routines of one kind is one key: the
// server's spelling depends on the list, not on the routine's name.
func TestRoutineArguments_OneListIsOneKey(t *testing.T) {
	c := qt.New(t)

	c.Assert(exprkey.RoutineArguments(schemamodel.Function{Name: "a", Parameters: "b text DEFAULT 'X'"}), qt.Equals,
		exprkey.RoutineArguments(schemamodel.Function{Name: "b", Parameters: "b text DEFAULT 'X'"}))
}

// A procedure's list and a function's are two keys, because the server takes
// them under different rules: a procedure refuses an OUT argument after one
// with a default, and a function does not. Two lists that differ only in a
// literal's case are two keys too.
func TestRoutineArguments_DifferentListsAreDifferentKeys(t *testing.T) {
	procedure := schemamodel.FunctionKindProcedure
	tests := []struct {
		name  string
		left  schemamodel.Function
		right schemamodel.Function
	}{
		{name: "a function and a procedure", left: schemamodel.Function{Parameters: "b text"}, right: schemamodel.Function{Kind: procedure, Parameters: "b text"}},
		{name: "the case of a literal", left: schemamodel.Function{Parameters: "b text DEFAULT 'X'"}, right: schemamodel.Function{Parameters: "b text DEFAULT 'x'"}},
		{name: "a kind word inside the list", left: schemamodel.Function{Parameters: "procedure"}, right: schemamodel.Function{Kind: procedure}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(exprkey.RoutineArguments(test.left), qt.Not(qt.Equals), exprkey.RoutineArguments(test.right))
		})
	}
}
