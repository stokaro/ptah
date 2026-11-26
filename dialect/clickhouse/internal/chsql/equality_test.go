package chsql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/clickhouse/internal/chsql"
)

// A quoted name and its bare spelling are one name, since the server prints a
// sorting key written (`id`, `a b`) back as (id, `a b`); a string literal
// with the same text is a value and stays apart from the name.
func TestSameKey_QuotedNames(t *testing.T) {
	for _, test := range []struct {
		name, a, b string
		want       bool
	}{
		{name: "a backquoted name", a: "(`id`, `a b`)", b: "(id, `a b`)", want: true},
		{name: "a double-quoted name", a: `"id"`, b: "id", want: true},
		{name: "a doubled backtick", a: "`a``b`", b: "`a\\`b`", want: true},
		{name: "a string literal", a: "concat(id, 'id')", b: "concat(id, id)", want: false},
		{name: "another name", a: "(`id`, `a b`)", b: "(id, `a c`)", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(chsql.SameKey(test.a, test.b), qt.Equals, test.want)
			c.Assert(chsql.SameExpression(test.a, test.b), qt.Equals, test.want)
		})
	}
}
