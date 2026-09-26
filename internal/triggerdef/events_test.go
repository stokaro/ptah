package triggerdef_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/triggerdef"
)

// TestEvents splits an event list into its members, keeping a quoted column
// whole when it holds a space, a comma or the word OR.
func TestEvents(t *testing.T) {
	tests := []struct {
		name string
		list string
		want []triggerdef.Event
	}{
		{
			name: "one event",
			list: "insert",
			want: []triggerdef.Event{{Keyword: "INSERT"}},
		},
		{
			name: "a list with an update column list",
			list: "update of b,a or DELETE",
			want: []triggerdef.Event{{Keyword: "UPDATE", Columns: []string{"b", "a"}}, {Keyword: "DELETE"}},
		},
		{
			name: "a quoted column holding the separators",
			list: `UPDATE OF "a, b or c", "x""y" OR TRUNCATE`,
			want: []triggerdef.Event{{Keyword: "UPDATE", Columns: []string{`"a, b or c"`, `"x""y"`}}, {Keyword: "TRUNCATE"}},
		},
		{
			name: "a comma list another engine writes stays one member",
			list: "INSERT, UPDATE",
			want: []triggerdef.Event{{Keyword: "INSERT, UPDATE"}},
		},
		{
			name: "no text",
			list: "  ",
			want: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(triggerdef.Events(test.list), qt.DeepEquals, test.want)
		})
	}
}

// TestCanonical orders the members the way PostgreSQL 18.6 reports them,
// keeps an UPDATE's columns in their declared order, and folds each column
// only through the function it is given.
func TestCanonical(t *testing.T) {
	tests := []struct {
		name   string
		list   string
		column func(string) string
		want   string
	}{
		{
			name: "PostgreSQL's order",
			list: "truncate or update or delete or insert",
			want: "INSERT OR DELETE OR UPDATE OR TRUNCATE",
		},
		{
			name: "the columns keep their order and spelling",
			list: `UPDATE OF B, "Total" OR DELETE`,
			want: `DELETE OR UPDATE OF B, "Total"`,
		},
		{
			name:   "the columns go through the fold",
			list:   `UPDATE OF B, "Total"`,
			column: strings.ToLower,
			want:   `UPDATE OF b, "total"`,
		},
		{
			name: "an unknown member keeps its place after the known ones",
			list: "INSTEAD OR INSERT",
			want: "INSERT OR INSTEAD",
		},
		{
			name: "no text",
			list: "",
			want: "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(triggerdef.Canonical(test.list, test.column), qt.Equals, test.want)
		})
	}
}

// TestIncludesAndNamesColumns answers the questions a renderer asks before it
// refuses a trigger it cannot create.
func TestIncludesAndNamesColumns(t *testing.T) {
	tests := []struct {
		name             string
		list             string
		wantTruncate     bool
		wantNamesColumns bool
	}{
		{name: "truncate in a list", list: "INSERT OR TRUNCATE", wantTruncate: true},
		{name: "a column named like the keyword", list: "UPDATE OF truncate", wantNamesColumns: true},
		{name: "a bare update", list: "UPDATE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			events := triggerdef.Events(test.list)
			c.Assert(triggerdef.Includes(events, "TRUNCATE"), qt.Equals, test.wantTruncate)
			c.Assert(triggerdef.NamesColumns(events), qt.Equals, test.wantNamesColumns)
		})
	}
}
