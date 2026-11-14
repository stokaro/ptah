package pgregclass_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgregclass"
)

// The expressions are column defaults as PostgreSQL 18 writes them back in
// pg_get_expr: a sequence on the search path bare, one off it qualified, a name
// that needs quotes in double quotes inside the constant.
func TestConstants(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       []string
	}{
		{name: "nextval", expression: "nextval('s'::regclass)", want: []string{"s"}},
		{name: "qualified", expression: "nextval('app.s'::regclass)", want: []string{"app.s"}},
		{name: "quoted name", expression: `nextval('"Order Seq"'::regclass)`, want: []string{`"Order Seq"`}},
		{name: "quote in the name", expression: `nextval('"it''s"'::regclass)`, want: []string{`"it's"`}},
		{name: "upper-case cast", expression: "nextval('public.s'::REGCLASS)", want: []string{"public.s"}},
		{name: "spaces around the cast", expression: "nextval('s' :: regclass)", want: []string{"s"}},
		{
			name:       "inside a larger expression",
			expression: "('ord-'::text || nextval('s1'::regclass)) || currval('s2'::regclass)",
			want:       []string{"s1", "s2"},
		},
		{name: "text argument", expression: "nextval('s'::text)", want: nil},
		{name: "no cast", expression: "'s'", want: nil},
		{name: "a literal that reads like one", expression: `'nextval(''s''::regclass)'::text`, want: nil},
		{name: "cast of a column", expression: "x::regclass", want: nil},
		{name: "empty", expression: "", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgregclass.Constants(test.expression), qt.DeepEquals, test.want)
		})
	}
}
