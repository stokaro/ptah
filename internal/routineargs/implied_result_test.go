package routineargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/routineargs"
)

// Each row is a function declared with these arguments and no RETURNS, and the
// result type PostgreSQL 18.6 reported for it with pg_get_function_result. OUT
// and INOUT arguments return a value; one is the result, two or more make a
// record (stokaro/ptah#3690).
func TestImpliedResult_AgreesWithTheCatalog(t *testing.T) {
	tests := []struct {
		name      string
		arguments string
		want      string
	}{
		{name: "one OUT argument", arguments: "a integer DEFAULT 1, OUT b text", want: "text"},
		{name: "one INOUT argument with an alias", arguments: "INOUT a int4", want: "integer"},
		{name: "an INOUT argument with a default", arguments: "INOUT a int DEFAULT 1", want: "integer"},
		{name: "a type modifier", arguments: "a int, OUT b varchar(10)", want: "character varying"},
		{name: "an array with a modifier", arguments: "a int, OUT b numeric(10,2)[]", want: "numeric[]"},
		{name: "an unnamed argument", arguments: "OUT timestamptz", want: "timestamp with time zone"},
		{name: "a quoted name and a two-word type", arguments: `OUT "B" double precision`, want: "double precision"},
		{name: "two OUT arguments", arguments: "a int, OUT b int, OUT c text", want: "record"},
		{name: "an INOUT and an OUT argument", arguments: "a int, INOUT b int, OUT c text", want: "record"},
		{name: "the mode in lower case", arguments: "a integer, out b text", want: "text"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(routineargs.ImpliedResult(test.arguments), qt.Equals, test.want)
		})
	}
}

// Arguments that return nothing imply no result: the function then has to
// declare RETURNS, and there is nothing to fill in.
func TestImpliedResult_NoOutputImpliesNothing(t *testing.T) {
	tests := []struct {
		name      string
		arguments string
	}{
		{name: "no arguments", arguments: ""},
		{name: "input arguments only", arguments: "a integer, IN b text"},
		{name: "a variadic argument", arguments: "VARIADIC a integer[]"},
		{name: "a name starting with out", arguments: "outer integer"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(routineargs.ImpliedResult(test.arguments), qt.Equals, "")
		})
	}
}
