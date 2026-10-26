package schemadiff_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
)

// inheritFoldingSpelling spells every declared role as one that inherits
// nothing, as the reader of an engine whose roles have no inheritance reports
// each of them.
type inheritFoldingSpelling struct{}

func (inheritFoldingSpelling) LowerDesired(_ context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
	spelled := *request.Desired
	spelled.Roles = slices.Clone(request.Desired.Roles)
	for i := range spelled.Roles {
		spelled.Roles[i].Inherit = false
	}
	return &spelled, nil
}

// TestCompare_SpellsCommonObjectsForTheComparisonAlone compares a role in the
// spelling the target gives it, so the read matches the declaration, and plans
// a new role as it was declared. The control registers no spelling and finds
// the declared inheritance changed.
func TestCompare_SpellsCommonObjectsForTheComparisonAlone(t *testing.T) {
	cases := []struct {
		name         string
		spelling     schemapreparation.Lowering
		wantModified []string
	}{
		{name: "registered", spelling: inheritFoldingSpelling{}, wantModified: nil},
		{name: "absent", spelling: nil, wantModified: []string{"reader"}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(engine.New(engine.Provider{ID: "example.org/custom", Targets: []engine.Target{{
				Name: "custom", Preparation: schemapreparation.Identity{}, Spelling: tt.spelling,
			}}}))
			desired, current := loweringComparisonInputs()
			desired.Roles = []schemamodel.Role{{Name: "reader", Login: true, Inherit: true}, {Name: "writer", Login: true, Inherit: true}}
			current.Roles = []catalog.Role{{Name: "reader", Login: true, PasswordState: catalog.RolePasswordAbsent}}

			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "custom", runtime)

			c.Assert(err, qt.IsNil)
			var modified []string
			for _, change := range diff.RolesModified {
				modified = append(modified, change.RoleName)
			}
			c.Assert(modified, qt.DeepEquals, tt.wantModified)
			c.Assert(diff.RolesAdded, qt.DeepEquals, difftypes.RoleChanges{{Name: "writer", Login: true, Inherit: true}})
			c.Assert(desired.Roles[0].Inherit, qt.IsTrue)
		})
	}
}
