package tableref_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/tableref"
)

func TestCanonical_PreservesStructuralIdentity(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		table  string
		want   string
	}{
		{name: "plain table", table: "users", want: "users"},
		{name: "schema-qualified table", schema: "tenant", table: "data", want: "tenant.data"},
		{name: "literal dot", table: "tenant.data", want: `"tenant.data"`},
		{name: "literal dot in schema", schema: "tenant.archive", table: "data", want: `"tenant.archive".data`},
		{name: "embedded quote", table: `user"events`, want: `"user""events"`},
		{name: "legacy mixed case", schema: "ExtensionStore", table: "uuid-ossp", want: "ExtensionStore.uuid-ossp"},
		{name: "leading space in schema", schema: " a", table: "t", want: `" a".t`},
		{name: "trailing space in schema", schema: "a ", table: "t", want: `"a ".t`},
		{name: "leading space in table", schema: "a", table: " t", want: `a." t"`},
		{name: "unqualified table with spaces", table: " t ", want: `" t "`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := tableref.Canonical(tt.schema, tt.table)

			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

// TestCanonical_ReadsBackThroughParse pins that Parse reads what Canonical
// writes as the same two names, so ` a` and `a` stay two schemas after a name
// is written as a key and read back.
func TestCanonical_ReadsBackThroughParse(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		table  string
	}{
		{name: "plain", schema: "a", table: "t"},
		{name: "leading space in schema", schema: " a", table: "t"},
		{name: "spaces around both", schema: " a ", table: " t "},
		{name: "dot and space", schema: " a.b", table: "t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := tableref.Parse(tableref.Canonical(tt.schema, tt.table))

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, tableref.Ref{Schema: tt.schema, Name: tt.table, Qualified: true})
		})
	}
}

func TestCanonicalExact_PreservesQuotedIdentifierWhitespace(t *testing.T) {
	c := qt.New(t)

	c.Assert(tableref.CanonicalExact(" Extension Store ", "pgcrypto"), qt.Equals, `" Extension Store ".pgcrypto`)
	c.Assert(tableref.CanonicalExact("Extension Store", " pgcrypto "), qt.Equals, `"Extension Store"." pgcrypto "`)
	c.Assert(tableref.CanonicalExact("extensions", "uuid-ossp"), qt.Equals, `extensions."uuid-ossp"`)
	c.Assert(tableref.CanonicalExact("Extensions", "pgcrypto"), qt.Equals, `"Extensions".pgcrypto`)
}

func TestParse_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  tableref.Ref
	}{
		{name: "plain", value: "users", want: tableref.Ref{Name: "users"}},
		{
			name:  "qualified",
			value: "audit.users",
			want:  tableref.Ref{Schema: "audit", Name: "users", Qualified: true},
		},
		{name: "double quoted literal dot", value: `"tenant.data"`, want: tableref.Ref{Name: "tenant.data"}},
		{name: "backtick literal dot", value: "`tenant.data`", want: tableref.Ref{Name: "tenant.data"}},
		{name: "bracket literal dot", value: "[tenant.data]", want: tableref.Ref{Name: "tenant.data"}},
		{
			name:  "escaped delimiters",
			value: `"a""b".` + "`c``d`",
			want:  tableref.Ref{Schema: `a"b`, Name: "c`d", Qualified: true},
		},
		{name: "escaped bracket", value: "[a]]b]", want: tableref.Ref{Name: "a]b"}},
		{name: "space before quote", value: `  "tenant.data"  `, want: tableref.Ref{Name: "tenant.data"}},
		{
			name:  "literal delimiter in unquoted component",
			value: "analytics.events`; DROP TABLE audit; --",
			want: tableref.Ref{
				Schema:    "analytics",
				Name:      "events`; DROP TABLE audit; --",
				Qualified: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := tableref.Parse(tt.value)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

func TestParse_FailurePath(t *testing.T) {
	tests := []string{"", `"unterminated`, "[unterminated", "a.b.c", "a..b", `"a"tail`}
	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)
			_, ok := tableref.Parse(value)

			c.Assert(ok, qt.IsFalse)
		})
	}
}

// A column reference writes a name bare where the bare name reads back as
// itself, and quotes it where it would not: a dot or a double quote inside it,
// a leading backtick or bracket, which [tableref.ParseColumn] reads as a quote,
// and white space at either end, which it trims.
func TestColumn_ReadsBackTheSameNames(t *testing.T) {
	tests := []struct {
		name                  string
		schema, table, column string
		want                  string
	}{
		{name: "plain", table: "t", column: "id", want: "t.id"},
		{name: "a schema", schema: "app", table: "t", column: "id", want: "app.t.id"},
		{name: "mixed case stays bare", table: "Order", column: "Id", want: "Order.Id"},
		{name: "a dot in the table", table: "a.b", column: "id", want: `"a.b".id`},
		{name: "a dot in the column", table: "a", column: "x.y", want: `a."x.y"`},
		{name: "a dot in the schema", schema: "s.t", table: "a", column: "id", want: `"s.t".a.id`},
		{name: "a double quote", table: `a"b`, column: "id", want: `"a""b".id`},
		{name: "space at both ends", table: " t ", column: " i ", want: `" t "." i "`},
		{name: "a tab at the end", table: "t\t", column: "id", want: "\"t\t\".id"},
		{name: "a leading backtick", table: "`t", column: "id", want: "\"`t\".id"},
		{name: "a leading bracket", table: "[t]", column: "id", want: `"[t]".id`},
		{name: "a bracket inside stays bare", table: "t[1]", column: "id", want: "t[1].id"},
		{name: "space inside stays bare", table: "a b", column: "id", want: "a b.id"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := tableref.Column(test.schema, test.table, test.column)
			ref, ok := tableref.ParseColumn(got)

			c.Assert(got, qt.Equals, test.want)
			c.Assert(ok, qt.IsTrue)
			c.Assert(ref, qt.Equals, tableref.ColumnRef{Schema: test.schema, Table: test.table, Column: test.column})
		})
	}
}

// A reference written by hand reads with the quotes [tableref.Parse] accepts.
func TestParseColumn_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  tableref.ColumnRef
	}{
		{name: "double quotes", value: `"T"."Id"`, want: tableref.ColumnRef{Table: "T", Column: "Id"}},
		{name: "backticks", value: "`a.b`.id", want: tableref.ColumnRef{Table: "a.b", Column: "id"}},
		{name: "brackets", value: "[dbo].[t].[id]", want: tableref.ColumnRef{Schema: "dbo", Table: "t", Column: "id"}},
		{name: "space around the parts", value: " t . id ", want: tableref.ColumnRef{Table: "t", Column: "id"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := tableref.ParseColumn(test.value)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A column reference names a table and a column, and at most a schema
// before them.
func TestParseColumn_FailurePath(t *testing.T) {
	tests := []string{"", "id", `"a.b"`, "d.s.t.id", "t.", ".id", `"t.id`, `"t"x.id`}
	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)

			got, ok := tableref.ParseColumn(value)

			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, tableref.ColumnRef{})
		})
	}
}

// TestPhrase pins both spellings. The quoted one is what every refusal naming
// a table wrote before Phrase existed, so a change to it changes those
// messages too.
func TestPhrase(t *testing.T) {
	tests := []struct {
		name  string
		table string
		want  string
	}{
		{name: "named", table: "orders", want: `table "orders"`},
		{name: "qualified", table: "audit.orders", want: `table "audit.orders"`},
		{name: "quote in the name", table: `o"rders`, want: `table "o\"rders"`},
		{name: "no name", table: "", want: "a table"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tableref.Phrase(tt.table), qt.Equals, tt.want)
		})
	}
}
