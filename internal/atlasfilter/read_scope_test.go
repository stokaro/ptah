package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/selectext"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/readscope"
)

// TestKindsLeftOut pins which selectors relieve a read of answering for a kind.
// The rows that leave nothing out matter as much as the rest: a selector that
// names some objects of a kind cannot vouch for one the account cannot see.
func TestKindsLeftOut(t *testing.T) {
	tableChildren := []readscope.Kind{readscope.Tables, readscope.Constraints, readscope.Triggers}
	tests := []struct {
		name    string
		exclude []string
		include []string
		want    []readscope.Kind
	}{
		{name: "no selector"},
		{
			name:    "every trigger",
			exclude: []string{"*[type=trigger]"},
			want:    []readscope.Kind{readscope.Triggers},
		},
		{
			name:    "every trigger, spelled table.trigger",
			exclude: []string{"*.*[type=trigger]"},
			want:    []readscope.Kind{readscope.Triggers},
		},
		{
			name:    "every trigger, spelled schema.table.trigger",
			exclude: []string{"*.*.*[type=trigger]"},
			want:    []readscope.Kind{readscope.Triggers},
		},
		{
			name:    "every view",
			exclude: []string{"*[type=view]"},
			want:    []readscope.Kind{readscope.Views},
		},
		{
			name:    "every routine",
			exclude: []string{"*[type=function]"},
			want:    []readscope.Kind{readscope.Routines},
		},
		{
			name:    "every constraint",
			exclude: []string{"*[type=constraint]"},
			want:    []readscope.Kind{readscope.Constraints},
		},
		{
			name:    "every table takes what hangs off a table",
			exclude: []string{"*[type=table]"},
			want:    tableChildren,
		},
		{
			name:    "two kinds in one selector",
			exclude: []string{"*[type=trigger|view]"},
			want:    []readscope.Kind{readscope.Views, readscope.Triggers},
		},
		{
			name:    "two kinds in one flag",
			exclude: []string{"*[type=trigger],*[type=view]"},
			want:    []readscope.Kind{readscope.Triggers, readscope.Views},
		},
		{
			name:    "two kinds in two flags",
			exclude: []string{"*[type=view]", "*[type=trigger]"},
			want:    []readscope.Kind{readscope.Views, readscope.Triggers},
		},
		{
			name:    "a leading schema selector",
			exclude: []string{"*[type=schema].*[type=view]"},
			want:    []readscope.Kind{readscope.Views},
		},
		{
			name:    "everything",
			exclude: []string{"*"},
			want:    []readscope.Kind{readscope.Tables, readscope.Constraints, readscope.Triggers, readscope.Views, readscope.Routines, readscope.Triggers, readscope.Constraints},
		},
		{
			name:    "a named trigger names no kind",
			exclude: []string{"audit_*[type=trigger]"},
		},
		{
			name:    "a named table names no kind",
			exclude: []string{"only_here"},
		},
		{
			name:    "a glob that happens to match what a kind is called names no kind",
			exclude: []string{"t*[type=trigger]", "o*[type=view]", "object[type=function]"},
		},
		{
			name:    "a foreign key is not every constraint",
			exclude: []string{"*[type=foreign_key]"},
		},
		{
			name:    "a field selector leaves the object",
			exclude: []string{"*[type=table].comment"},
		},
		{
			name:    "a pattern too deep for a table matches none",
			exclude: []string{"*.*.*[type=table]"},
		},
		{
			name:    "a malformed selector counts for nothing",
			exclude: []string{"*[type=nosuchtype]"},
		},
		{
			name:    "including only tables leaves views and routines out",
			include: []string{"*[type=table]"},
			want:    []readscope.Kind{readscope.Views, readscope.Routines},
		},
		{
			name:    "including one named table leaves views and routines out",
			include: []string{"orders[type=table]"},
			want:    []readscope.Kind{readscope.Views, readscope.Routines},
		},
		{
			name:    "including only views leaves tables and routines out",
			include: []string{"*[type=view]"},
			want:    []readscope.Kind{readscope.Tables, readscope.Constraints, readscope.Triggers, readscope.Routines},
		},
		{
			name:    "an include with no type can name any kind",
			include: []string{"orders"},
		},
		{
			name:    "an include with no type beside a typed one",
			include: []string{"orders", "*[type=view]"},
		},
		{
			name:    "both flags add up",
			exclude: []string{"*[type=trigger]"},
			include: []string{"*[type=table]"},
			want:    []readscope.Kind{readscope.Triggers, readscope.Views, readscope.Routines},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := atlasfilter.KindsLeftOut(selectext.None(), test.exclude, test.include)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestKindsLeftOut_FailurePath leaves nothing out for a run that selected no
// owners, which the filter refuses in its own words.
func TestKindsLeftOut_FailurePath(t *testing.T) {
	c := qt.New(t)
	got := atlasfilter.KindsLeftOut(selectext.Set{}, []string{"*[type=trigger]"}, nil)
	c.Assert(got, qt.HasLen, 0)
}
