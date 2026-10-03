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
// literal's case are two keys too, and so are two results, since the server
// spells each result. Two bodies and two languages are two keys, because a
// server that rewrites a body when it stores it answers for the body too
// (stokaro/ptah#4058).
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
		{name: "two results", left: schemamodel.Function{Returns: "SETOF public.a"}, right: schemamodel.Function{Returns: "SETOF public.b"}},
		{name: "a result written into the list", left: schemamodel.Function{Parameters: "a integer", Returns: "integer"}, right: schemamodel.Function{Parameters: "a integerinteger"}},
		{name: "two bodies", left: schemamodel.Function{Returns: "bigint", Body: "SELECT 1"}, right: schemamodel.Function{Returns: "bigint", Body: "SELECT 2"}},
		{name: "two languages", left: schemamodel.Function{Returns: "bigint", Language: "sql"}, right: schemamodel.Function{Returns: "bigint", Language: "plpgsql"}},
		{name: "a body written into the language", left: schemamodel.Function{Language: "sql", Body: "x"}, right: schemamodel.Function{Language: "sqlx"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(exprkey.RoutineArguments(test.left), qt.Not(qt.Equals), exprkey.RoutineArguments(test.right))
		})
	}
}

// One body declared on two views is one key, and two bodies are two: the
// server's spelling of a view depends on its body alone.
func TestViewBody_OneBodyIsOneKey(t *testing.T) {
	c := qt.New(t)

	c.Assert(exprkey.ViewBody("SELECT id FROM all_items()"), qt.Equals, exprkey.ViewBody("SELECT id FROM all_items()"))
	c.Assert(exprkey.ViewBody("SELECT id FROM all_items()"), qt.Not(qt.Equals), exprkey.ViewBody("SELECT id FROM all_items() i"))
}
