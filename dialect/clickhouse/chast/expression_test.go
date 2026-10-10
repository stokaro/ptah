package chast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/clickhouse/chast"
)

func TestSkippingIndexExpression(t *testing.T) {
	for _, test := range []struct {
		name  string
		parts []string
		want  string
	}{
		{"one column", []string{"value"}, "value"},
		{"several columns", []string{"a", "b"}, "(a, b)"},
		{"a list the catalog reports for a tuple key", []string{"a, b"}, "(a, b)"},
		{"a function call", []string{"lower(payload)"}, "lower(payload)"},
		{"a tuple already", []string{"(a, b)"}, "(a, b)"},
		{"a function with several arguments", []string{"tuple(a, lower(b))"}, "tuple(a, lower(b))"},
		{"a lambda", []string{"arrayMap(x -> x + 1, values)"}, "arrayMap(x -> x + 1, values)"},
		{"an array literal", []string{"has([1, 2], a)"}, "has([1, 2], a)"},
		{"a comma in a string", []string{"concat(a, ',')"}, "concat(a, ',')"},
		{"a comma in a bare string", []string{"'a,b'"}, "'a,b'"},
		{"no parts", nil, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chast.SkippingIndexExpression(test.parts), qt.Equals, test.want)
		})
	}
}
