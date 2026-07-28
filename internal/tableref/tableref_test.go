package tableref_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/tableref"
)

func TestCanonical_PreservesStructuralIdentity(t *testing.T) {
	c := qt.New(t)

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
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got := tableref.Canonical(tt.schema, tt.table)

			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestParse_HappyPath(t *testing.T) {
	c := qt.New(t)

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
		c.Run(tt.name, func(c *qt.C) {
			got, ok := tableref.Parse(tt.value)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

func TestParse_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []string{"", `"unterminated`, "[unterminated", "a.b.c", "a..b", `"a"tail`}
	for _, value := range tests {
		c.Run(value, func(c *qt.C) {
			_, ok := tableref.Parse(value)

			c.Assert(ok, qt.IsFalse)
		})
	}
}
