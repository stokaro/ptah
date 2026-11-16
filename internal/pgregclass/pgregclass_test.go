package pgregclass_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
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
		{name: "cast of a quoted column", expression: `"x"::regclass`, want: nil},
		{name: "empty", expression: "", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgregclass.Constants(test.expression), qt.DeepEquals, test.want)
		})
	}
}

// TestRelations_NamesARelation reads text a planned statement, a column default
// as the server writes it back, or a routine body can hold. Each constant the
// server converts to regclass as it reads the text names the relation, read
// as the server reads a relation's name.
func TestRelations_NamesARelation(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []pgregclass.Relation
	}{
		{name: "nextval of a constant", text: "nextval('app.ids')", want: []pgregclass.Relation{{Schema: "app", Name: "ids"}}},
		{name: "nextval of a constant cast to regclass", text: "nextval('app.ids'::regclass)", want: []pgregclass.Relation{{Schema: "app", Name: "ids"}}},
		{name: "currval", text: "currval('app.ids')", want: []pgregclass.Relation{{Schema: "app", Name: "ids"}}},
		{name: "setval names its first argument", text: "SELECT setval('app.ids', 10, 'app.other')", want: []pgregclass.Relation{{Schema: "app", Name: "ids"}}},
		{name: "nextval qualified by pg_catalog", text: "pg_catalog.nextval('app.ids')", want: []pgregclass.Relation{{Schema: "app", Name: "ids"}}},
		{name: "nextval spelled in upper case", text: "NEXTVAL ( 'App.Ids' )", want: []pgregclass.Relation{{Schema: "app", Name: "ids"}}},
		{name: "a cast outside a call", text: "SELECT 'app.t'::regclass", want: []pgregclass.Relation{{Schema: "app", Name: "t"}}},
		{name: "a cast to pg_catalog.regclass", text: "SELECT 'app.t'::pg_catalog.regclass", want: []pgregclass.Relation{{Schema: "app", Name: "t"}}},
		{name: "CAST AS regclass", text: "SELECT CAST('app.t' AS regclass)", want: []pgregclass.Relation{{Schema: "app", Name: "t"}}},
		{name: "an unqualified name", text: "nextval('ids')", want: []pgregclass.Relation{{Name: "ids"}}},
		{name: "quoted parts keep their case", text: `nextval('"App"."Ids"')`, want: []pgregclass.Relation{{Schema: "App", Name: "Ids"}}},
		{name: "a dot inside a quoted part", text: `nextval('"my.app".ids')`, want: []pgregclass.Relation{{Schema: "my.app", Name: "ids"}}},
		{name: "a doubled quote inside a quoted part", text: `nextval('app."it""s"')`, want: []pgregclass.Relation{{Schema: "app", Name: `it"s`}}},
		{name: "white space around the parts", text: "nextval(' app . ids ')", want: []pgregclass.Relation{{Schema: "app", Name: "ids"}}},
		{name: "a database part", text: "nextval('db.app.ids')", want: []pgregclass.Relation{{Schema: "app", Name: "ids"}}},
		{name: "an escape string", text: `nextval(E'app.\x69ds')`, want: []pgregclass.Relation{{Schema: "app", Name: "ids"}}},
		{name: "a dollar-quoted string", text: "nextval($$app.ids$$)", want: []pgregclass.Relation{{Schema: "app", Name: "ids"}}},
		{
			name: "several, in order",
			text: "('n-'::text || nextval('a'::regclass)) || currval('b.c')",
			want: []pgregclass.Relation{{Name: "a"}, {Schema: "b", Name: "c"}},
		},
		{
			name: "a planned column default",
			text: `ALTER TABLE "public"."tasks" ADD COLUMN "n" bigint DEFAULT nextval('rebase.ids')`,
			want: []pgregclass.Relation{{Schema: "rebase", Name: "ids"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgregclass.Relations(test.text, platform.Postgres), qt.DeepEquals, test.want)
		})
	}
}

// TestRelations_NamesNothing reads text whose strings the server does not
// convert to regclass as it reads the text: they are ordinary text, a name it
// looks up only when the expression runs, or no relation's name.
func TestRelations_NamesNothing(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{name: "a string default", text: "DEFAULT 'app.ids'"},
		{name: "an argument of another function", text: "lower('app.ids')"},
		{name: "a constant cast to text", text: "nextval('app.ids'::text)"},
		{name: "a name computed from strings", text: "nextval('app.' || 'ids')"},
		{name: "nextval of another schema", text: "app.nextval('app.ids')"},
		{name: "a quoted function name in upper case", text: `"NEXTVAL"('app.ids')`},
		{name: "a line comment", text: "-- nextval('app.ids')"},
		{name: "a block comment", text: "/* 'app.ids'::regclass */"},
		{name: "a string that holds a call", text: `SELECT 'nextval(''app.ids'')'`},
		{name: "a quoted column cast to regclass", text: `"relid"::regclass`},
		{name: "an OID", text: "'1259'::regclass"},
		{name: "the invalid OID", text: "'-'::regclass"},
		{name: "four parts", text: "'a.b.c.d'::regclass"},
		{name: "an empty part", text: "nextval('app..ids')"},
		{name: "an empty quoted part", text: `nextval('""')`},
		{name: "two words", text: "nextval('app ids')"},
		{name: "an unterminated quote", text: `nextval('"app')`},
		{name: "an empty string", text: "nextval('')"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgregclass.Relations(test.text, platform.Postgres), qt.IsNil)
		})
	}
}
