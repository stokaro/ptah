package ownqualifier_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ownqualifier"
)

// strip takes the qualifier of public.notes out of expression.
func strip(expression string) string {
	return ownqualifier.Strip(expression, ownqualifier.StoredName("public"), ownqualifier.StoredName("notes"))
}

// TestStrip_TakesTheOwnSchemaOut holds each spelling of a column the
// expression qualifies with its own table and that table's schema.
func TestStrip_TakesTheOwnSchemaOut(t *testing.T) {
	tests := []struct{ name, expression, want string }{
		{"unquoted", "public.notes.owner = 1", "notes.owner = 1"},
		{"quoted", `"public"."notes".owner = 1`, `"notes".owner = 1`},
		{"mixed quoting", `public."notes".owner = 1`, `"notes".owner = 1`},
		{"case the server folds", "PUBLIC.Notes.owner = 1", "Notes.owner = 1"},
		{"spaces between the parts", "public . notes . owner = 1", "notes . owner = 1"},
		{"a comment between the parts", "public./*c*/notes.owner = 1", "notes.owner = 1"},
		{"two references", "public.notes.a < public.notes.b", "notes.a < notes.b"},
		{"inside a call", "lower(public.notes.n) = 'x'", "lower(notes.n) = 'x'"},
		{"after a multibyte literal", "'é' = public.notes.n", "'é' = notes.n"},
		{"a composite field", "(public.notes.c).f = 1", "(notes.c).f = 1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(strip(test.expression), qt.Equals, test.want)
		})
	}
}

// TestStrip_KeepsAnyOtherSpelling is the control: each expression either has no
// such qualifier or mentions the table where taking the qualifier out could
// change which table a column reads, and it comes back as written.
func TestStrip_KeepsAnyOtherSpelling(t *testing.T) {
	tests := []struct{ name, expression string }{
		{"the table alone", "notes.owner = 1"},
		{"another schema", "other.notes.owner = 1"},
		{"another table", "public.other.owner = 1"},
		{"a schema in a case the server does not fold", `"Public".notes.owner = 1`},
		{"a qualifier that is itself qualified", "db.public.notes.owner = 1"},
		{"a relation, not a column", "EXISTS (SELECT 1 FROM public.notes)"},
		{"a call", "public.notes(1) = 1"},
		{"a string constant", "x = 'public.notes.owner'"},
		{"a dollar-quoted string", "x = $$public.notes.owner$$"},
		{"a subquery exposing the name", "EXISTS (SELECT 1 FROM other.notes WHERE public.notes.id = 1)"},
		{"an alias exposing the name", "EXISTS (SELECT 1 FROM other.t AS notes WHERE public.notes.id = notes.id)"},
		{"a column named like the table", "public.notes.notes = 1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(strip(test.expression), qt.Equals, test.expression)
		})
	}
}

// TestStoredName_MatchesTheServersSpelling pins the quoting rules: an unquoted
// name folds to lower case, a quoted one keeps its case and its doubled quotes
// stand for one.
func TestStoredName_MatchesTheServersSpelling(t *testing.T) {
	tests := []struct{ name, stored, expression, want string }{
		{"a quoted mixed-case name", "Orders", `public."Orders".n = 1`, `"Orders".n = 1`},
		{"an unquoted spelling of a mixed-case name", "Orders", `public.Orders.n = 1`, `public.Orders.n = 1`},
		{"a doubled quote", `a"b`, `public."a""b".n = 1`, `"a""b".n = 1`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ownqualifier.Strip(test.expression, ownqualifier.StoredName("public"), ownqualifier.StoredName(test.stored))
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
