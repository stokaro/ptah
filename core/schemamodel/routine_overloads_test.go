package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// routineSignatures names each routine the way a statement names it, kind
// first, so an assertion says which routines survived rather than how many.
func routineSignatures(functions []schemamodel.Function) []string {
	names := make([]string, 0, len(functions))
	for _, function := range functions {
		kind := schemamodel.FunctionKindFunction
		if function.IsProcedure() {
			kind = schemamodel.FunctionKindProcedure
		}
		names = append(names, kind+" "+function.Name+"("+function.Parameters+")")
	}
	return names
}

// Finalize keeps every overload a document declares. PostgreSQL tells
// overloads apart by their input argument types, and a key of the name alone
// kept the first and dropped the rest without a word (stokaro/ptah#3672). A
// function and a procedure of one name are two routines too.
func TestFinalize_KeepsEveryRoutineOverload(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{Functions: []schemamodel.Function{
		{Name: "app.f", Parameters: "a int", Returns: "int", Body: "SELECT 1"},
		{Name: "app.f", Parameters: "a int, b text", Returns: "int", Body: "SELECT 2"},
		{Name: "app.f", Parameters: "a text", Returns: "int", Body: "SELECT 3"},
		{Name: "app.f", Kind: schemamodel.FunctionKindProcedure, Parameters: "a int", Body: "SELECT 4"},
	}}

	schemamodel.Finalize(database)

	c.Assert(routineSignatures(database.Functions), qt.DeepEquals, []string{
		"function app.f(a int)",
		"function app.f(a int, b text)",
		"function app.f(a text)",
		"procedure app.f(a int)",
	})
}

// A repeated declaration of one overload still folds into one, however its
// arguments are spelled: parameter names, a default, the IN mode, an OUT
// argument and a type alias do not name a different routine.
func TestFinalize_FoldsARepeatedOverload(t *testing.T) {
	tests := []struct {
		name   string
		repeat string
	}{
		{name: "the same text", repeat: "a int"},
		{name: "another parameter name", repeat: "b int"},
		{name: "a type alias", repeat: "a integer"},
		{name: "a default", repeat: "a int DEFAULT 1"},
		{name: "the IN mode", repeat: "IN a int"},
		{name: "an OUT argument", repeat: "a int, OUT b text"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{Functions: []schemamodel.Function{
				{Name: "app.f", Parameters: "a int", Returns: "int", Body: "SELECT 1"},
				{Name: "app.f", Parameters: test.repeat, Returns: "int", Body: "SELECT 1"},
			}}

			schemamodel.Finalize(database)

			c.Assert(routineSignatures(database.Functions), qt.DeepEquals, []string{"function app.f(a int)"})
		})
	}
}

// Two sources may each declare an overload of one name: they are two routines,
// not a conflict.
func TestMerge_KeepsOverloadsFromTwoSources(t *testing.T) {
	c := qt.New(t)
	first := &schemamodel.Database{Functions: []schemamodel.Function{
		{Name: "app.f", Parameters: "a int", Returns: "int", Body: "SELECT 1"},
	}}
	second := &schemamodel.Database{Functions: []schemamodel.Function{
		{Name: "app.f", Parameters: "a int, b text", Returns: "int", Body: "SELECT 2"},
	}}

	merged, err := schemamodel.Merge(first, second)

	c.Assert(err, qt.IsNil)
	c.Assert(routineSignatures(merged.Functions), qt.DeepEquals, []string{
		"function app.f(a int)",
		"function app.f(a int, b text)",
	})
}

// Routines that call each other cannot be ordered, and the ones left over are
// appended as declared. An overload among them is its own routine there too,
// and is not taken for the one of its name already appended.
func TestFinalize_KeepsOverloadsInADependencyCycle(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{Functions: []schemamodel.Function{
		{Name: "a", Parameters: "x int", Returns: "int", Body: "SELECT b(x)"},
		{Name: "a", Parameters: "x int, y int", Returns: "int", Body: "SELECT b(x + y)"},
		{Name: "b", Parameters: "x int", Returns: "int", Body: "SELECT a(x)"},
	}}

	schemamodel.Finalize(database)

	c.Assert(routineSignatures(database.Functions), qt.DeepEquals, []string{
		"function a(x int)",
		"function a(x int, y int)",
		"function b(x int)",
	})
}
