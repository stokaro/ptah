package routineargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/routineargs"
)

func TestWithoutTypeModifiers_PreservesTypeIdentity(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "length", input: "varchar(20)", want: "varchar"},
		{name: "precision and scale", input: "numeric(10, 2)", want: "numeric"},
		{name: "array", input: "varchar(20)[][]", want: "varchar[][]"},
		{name: "time zone", input: "timestamp(3) with time zone", want: "timestamp with time zone"},
		{name: "quoted type", input: `"type(20)"`, want: `"type(20)"`},
		{name: "quoted parameter", input: `"code(20)" varchar(20)`, want: `"code(20)" varchar`},
		{name: "escaped quote", input: `"type""(20)"`, want: `"type""(20)"`},
		{name: "missing close", input: "varchar(20", want: "varchar(20"},
		{name: "extra close", input: "varchar(20))", want: "varchar(20))"},
		{name: "unclosed quote", input: `"type(20)`, want: `"type(20)`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(routineargs.WithoutTypeModifiers(test.input), qt.Equals, test.want)
		})
	}
}
