package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// clearedRangeSchemas builds one declared range and one catalog range for a
// comparison.
func clearedRangeSchemas(declared goschema.Range, current catalog.Range) (*goschema.Database, *catalog.Database) {
	declared.Name = "measurement"
	current.Name = "measurement"
	current.Subtype = "int8"
	declared.Subtype = "int8"
	return &goschema.Database{Ranges: []goschema.Range{declared}},
		&catalog.Database{Ranges: []catalog.Range{current}}
}

// rangeIsModified reports whether the comparison plans to change the range.
func rangeIsModified(c *qt.C, declared goschema.Range, current catalog.Range) bool {
	c.Helper()

	target, currentSchema := clearedRangeSchemas(declared, current)
	diff := schemadiff.Compare(target, currentSchema)
	return len(diff.RangesModified) > 0
}

// TestCompare_AnOmittedRangeAttributeIsNotARemoval is the adoption rule, and it
// is the reason the attribute cannot simply be compared.
//
// Somebody pointing Ptah at an existing database writes the range they know
// about. The catalog's SUBTYPE_DIFF is not theirs to lose, and for a range the
// plan is DROP TYPE plus CREATE TYPE: an error while the type is in use, and a
// silent rebuild without the function when it is not (stokaro/ptah#2223).
func TestCompare_AnOmittedRangeAttributeIsNotARemoval(t *testing.T) {
	c := qt.New(t)

	modified := rangeIsModified(c,
		goschema.Range{},
		catalog.Range{SubtypeDiff: "int8_subdiff", Canonical: "int8_canonical"})

	c.Assert(modified, qt.IsFalse)
}

// TestCompare_AClearedRangeAttributeIsARemoval is what the omission rule cost
// until now: with a bare string in the model there was no spelling that removed
// one of these at all, and a schema holding an attribute the declaration does
// not want reported itself synced forever.
func TestCompare_AClearedRangeAttributeIsARemoval(t *testing.T) {
	tests := []struct {
		name      string
		cleared   []string
		current   catalog.Range
		wantPlan  bool
		wantEntry string
	}{
		{
			name:      "a cleared subtype_diff removes the one the catalog holds",
			cleared:   []string{"subtype_diff"},
			current:   catalog.Range{SubtypeDiff: "int8_subdiff"},
			wantPlan:  true,
			wantEntry: "subtype_diff",
		},
		{
			name:     "a cleared canonical removes the one the catalog holds",
			cleared:  []string{"canonical"},
			current:  catalog.Range{Canonical: "int8_canonical"},
			wantPlan: true,
		},
		{
			// The control that keeps this from planning on every run: a cleared
			// attribute the catalog also has none of is agreement, not a change.
			name:     "a cleared attribute the catalog does not hold is no change",
			cleared:  []string{"subtype_diff"},
			current:  catalog.Range{},
			wantPlan: false,
		},
		{
			// The other control: clearing one attribute says nothing about the
			// others, so a catalog value beside it is still not planned away.
			name:     "clearing one attribute leaves its neighbor alone",
			cleared:  []string{"canonical"},
			current:  catalog.Range{SubtypeDiff: "int8_subdiff"},
			wantPlan: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			modified := rangeIsModified(c,
				goschema.Range{ClearedAttributes: test.cleared},
				test.current)

			c.Assert(modified, qt.Equals, test.wantPlan)
		})
	}
}
