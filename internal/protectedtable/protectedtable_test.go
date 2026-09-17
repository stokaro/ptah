package protectedtable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/protectedtable"
)

func TestSetFencesTheTableAnEntryNames(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		schema  string
		table   string
		want    string
	}{
		{name: "the bare name", entries: []string{"regions"}, table: "regions", want: "regions"},
		{
			name:    "the bare name of a table that has a schema",
			entries: []string{"regions"},
			schema:  "reference",
			table:   "regions",
			want:    "regions",
		},
		{
			name:    "the qualified name",
			entries: []string{"reference.regions"},
			schema:  "reference",
			table:   "regions",
			want:    "reference.regions",
		},
		{name: "a different case", entries: []string{"REGIONS"}, table: "regions", want: "REGIONS"},
		{
			name:    "a different case in the qualified name",
			entries: []string{"Reference.Regions"},
			schema:  "reference",
			table:   "regions",
			want:    "Reference.Regions",
		},
		{name: "surrounded by space", entries: []string{"  regions \t"}, table: "regions", want: "regions"},
		{
			name:    "one entry of several",
			entries: []string{"countries", "regions", "currencies"},
			table:   "regions",
			want:    "regions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			entry, fenced := protectedtable.New(tt.entries).Entry(tt.schema, tt.table)

			c.Assert(fenced, qt.IsTrue)
			// The entry comes back in the words the caller wrote, because that
			// is what a refusal quotes back at them.
			c.Assert(entry, qt.Equals, tt.want)
		})
	}
}

func TestSetLeavesEveryOtherTableAlone(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		schema  string
		table   string
	}{
		{name: "another table", entries: []string{"countries"}, table: "regions"},
		{
			// The qualified entry is the narrower statement: it fences the
			// table in one schema, and the same name in another is untouched.
			name:    "the same name in another schema",
			entries: []string{"reference.regions"},
			schema:  "tenant_acme",
			table:   "regions",
		},
		{
			name:    "a prefix of the name",
			entries: []string{"region"},
			table:   "regions",
		},
		{name: "no entries at all", entries: nil, table: "regions"},
		{
			// A repeated flag and a list read out of a file both produce these,
			// and an empty entry that fenced the unqualified name would fence
			// every table.
			name:    "entries that are only space",
			entries: []string{"", "   ", "\t"},
			table:   "regions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			entry, fenced := protectedtable.New(tt.entries).Entry(tt.schema, tt.table)

			c.Assert(fenced, qt.IsFalse)
			c.Assert(entry, qt.Equals, "")
		})
	}
}

func TestEmptyReportsWhetherAnythingIsFenced(t *testing.T) {
	tests := []struct {
		name    string
		entries []string
		want    bool
	}{
		{name: "nothing declared", entries: nil, want: true},
		{name: "only blank entries", entries: []string{"", "  "}, want: true},
		{name: "one entry", entries: []string{"regions"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(protectedtable.New(tt.entries).Empty(), qt.Equals, tt.want)
		})
	}
}

// TestTheZeroSetFencesNothing is what lets a caller with no entries skip the
// work of deciding what its change touches, without a branch of its own.
func TestTheZeroSetFencesNothing(t *testing.T) {
	c := qt.New(t)

	var set protectedtable.Set

	c.Assert(set.Empty(), qt.IsTrue)
	entry, fenced := set.Entry("reference", "regions")
	c.Assert(fenced, qt.IsFalse)
	c.Assert(entry, qt.Equals, "")
}
