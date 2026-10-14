package pgtypealias_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgtypealias"
)

func TestReference(t *testing.T) {
	for _, test := range []struct {
		name, desired, observed string
		equal                   bool
	}{
		{"catalog operator class", "pg_catalog.numeric_ops", "numeric_ops", true},
		{"quoted catalog", `"pg_catalog"."numrange_subdiff"`, "numrange_subdiff", true},
		{"unquoted case", "NUMERIC_OPS", "numeric_ops", true},
		{"quoted case", `"NUMERIC_OPS"`, "numeric_ops", false},
		{"qualified user name", `"app".numeric_ops`, "app.numeric_ops", true},
		{"distinct user schema", "app.numeric_ops", "numeric_ops", false},
		{"quoted whitespace", `" app ".numeric_ops`, "app.numeric_ops", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgtypealias.Reference(test.desired) == pgtypealias.Reference(test.observed), qt.Equals, test.equal)
		})
	}
}

func TestCanonicalTypeReferences(t *testing.T) {
	for _, test := range []struct{ spelling, want string }{
		{`"app".quantity`, "app.quantity"},
		{`"app"."Quantity"`, `app."Quantity"`},
		{`" app ".quantity`, `" app ".quantity`},
		{"pg_catalog.int4", "integer"},
		{`"pg_catalog"."int4"`, "integer"},
		{`"INT4"`, `"INT4"`},
		{"app.int4", "app.int4"},
		{`"App"."Range"[]`, `"App"."Range"[]`},
		{`"app"."Value"(10)[]`, `app."Value"(10)[]`},
		{`"app"."x(y)[z]"[]`, `app."x(y)[z]"[]`},
		{`"char"`, `"char"`},
		{`"integer"`, `"integer"`},
		{`"numeric"`, "numeric"},
		{`"int"`, `"int"`},
		{`pg_catalog.char`, `"char"`},
		{`pg_catalog.int4[]`, `integer[]`},
		{`INT[]`, `integer[]`},
		{`decimal(10, 2)[]`, `numeric(10,2)[]`},
		{`"decimal"`, `"decimal"`},
		{`"serial"`, `"serial"`},
	} {
		t.Run(test.spelling, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgtypealias.Canonical(test.spelling), qt.Equals, test.want)
		})
	}
}
