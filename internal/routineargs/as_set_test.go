package routineargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/routineargs"
)

// Each row is a set-returning function's result as pg_get_function_result
// printed it. The PostgreSQL rows are what PostgreSQL 18.6 prints and come back
// unchanged; the CockroachDB rows are what v26.3.2 prints for the same
// declarations, without the SETOF its proretset states (stokaro/ptah#4058).
func TestAsSet(t *testing.T) {
	tests := []struct {
		name     string
		reported string
		want     string
	}{
		{name: "PostgreSQL prints SETOF", reported: "SETOF items", want: "SETOF items"},
		{name: "PostgreSQL prints TABLE", reported: "TABLE(a integer, b text)", want: "TABLE(a integer, b text)"},
		{name: "CockroachDB leaves SETOF out of a row type", reported: "items", want: "SETOF items"},
		{name: "CockroachDB prints record for TABLE", reported: "record", want: "SETOF record"},
		{name: "SETOF in another case is kept", reported: "setof items", want: "setof items"},
		{name: "nothing reported stays nothing", reported: "", want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(routineargs.AsSet(test.reported), qt.Equals, test.want)
		})
	}
}
