package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// TestFunctionCanonicalize_KeepsTheReturnClauseKeywordsUppercase pins the
// spelling the catalog reports.
//
// Type names are lowercased so an annotation written as `returns="VOID"` does
// not differ from pg_proc on every run. The two clause keywords are not type
// names: measured on PostgreSQL 17, `pg_get_function_result` prints `SETOF
// integer` and `TABLE(a integer, b text)`. Lowercasing those would make every
// set-returning routine differ from its own catalog row, and the comparison is
// a string equality, so the plan would replace a routine nobody touched on
// every apply (stokaro/ptah#3121).
func TestFunctionCanonicalize_KeepsTheReturnClauseKeywordsUppercase(t *testing.T) {
	rows := []struct {
		name    string
		returns string
		want    string
	}{
		{name: "a set of a scalar", returns: "SETOF integer", want: "SETOF integer"},
		{name: "a set written in lowercase", returns: "setof INTEGER", want: "SETOF integer"},
		{name: "a table", returns: "TABLE(a integer, b text)", want: "TABLE(a integer, b text)"},
		{name: "a table written in lowercase", returns: "table(A INTEGER)", want: "TABLE(a integer)"},
		{name: "a plain scalar is lowered", returns: "VOID", want: "void"},
		{name: "a type whose name starts with the keyword", returns: "SETOFTHING", want: "setofthing"},
		{name: "nothing at all", returns: "", want: ""},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			function := schemamodel.Function{Name: "f", Returns: row.returns}

			function.Canonicalize()

			c.Assert(function.Returns, qt.Equals, row.want)
		})
	}
}
