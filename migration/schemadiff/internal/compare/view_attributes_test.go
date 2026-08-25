package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// TestViewDefinitions_PlansAChangeOfAttributes pins that a view which gains or
// loses its WITH clause is reported as changed.
//
// It is what binds a view to the tables it names -- they cannot be altered
// under a schema-bound view, and an indexed view requires it -- so a difference
// here is a different view. Before this, removing SCHEMABINDING from a document
// and applying it answered `Schema is synced, no changes to be made.` while the
// database kept the binding (stokaro/ptah#2125).
func TestViewDefinitions_PlansAChangeOfAttributes(t *testing.T) {
	tests := []struct {
		name     string
		declared []string
		actual   []string
		want     bool
	}{
		{
			name:     "the document drops the binding",
			declared: nil,
			actual:   []string{"SCHEMABINDING"},
			want:     true,
		},
		{
			name:     "the document adds the binding",
			declared: []string{"SCHEMABINDING"},
			actual:   nil,
			want:     true,
		},
		{
			name:     "one attribute replaced by another",
			declared: []string{"VIEW_METADATA"},
			actual:   []string{"SCHEMABINDING"},
			want:     true,
		},
		{
			// The controls. Each is a shape that would re-plan the same change
			// on every run if the comparison were a list comparison.
			name:     "the same clause in the other order",
			declared: []string{"VIEW_METADATA", "SCHEMABINDING"},
			actual:   []string{"SCHEMABINDING", "VIEW_METADATA"},
			want:     false,
		},
		{
			name:     "the same clause in another case",
			declared: []string{"schemabinding"},
			actual:   []string{"SCHEMABINDING"},
			want:     false,
		},
		{
			name:     "neither side has one",
			declared: nil,
			actual:   nil,
			want:     false,
		},
		{
			// nil and an empty list both mean "no attributes", and a document
			// that has been through a round trip may carry either.
			name:     "nil against an empty list",
			declared: make([]string, 0),
			actual:   nil,
			want:     false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.ViewDefinitions(
				goschema.View{Name: "dbo.v", Body: "SELECT id FROM dbo.t", Attributes: test.declared},
				types.DBView{Name: "v", Schema: "dbo", Body: "SELECT id FROM dbo.t", Attributes: test.actual},
			)

			_, changed := diff.Changes["attributes"]
			c.Assert(changed, qt.Equals, test.want)
			// The body is identical in every row, so a run that reported a body
			// change would be measuring something other than the clause.
			c.Assert(diff.Changes, qt.Not(qt.HasLen), 2)
		})
	}
}
