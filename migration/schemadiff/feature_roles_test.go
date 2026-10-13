package schemadiff_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

type roleCaptureRuntime struct {
	*engine.Runtime
	roles []objectidentity.ID
}

func (r *roleCaptureRuntime) CompareFeatures(ctx context.Context, request schemaext.ComparisonRequest) (schemaext.ComparisonResult, error) {
	r.roles = slices.Clone(request.DeclaredRoles)
	return r.Runtime.CompareFeatures(ctx, request)
}

// The public comparison path captures only declared roles. Catalog roles do
// not grant an owner authority to remove their state.
func TestFeatureComparison_CapturesDeclaredRoles(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	recording := &roleCaptureRuntime{Runtime: runtime}
	desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "app_owner"}, {Name: "Ops Team"}}}
	current := &catalog.Database{Roles: []catalog.Role{{Name: "observed_only"}}}

	_, err = schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", recording)

	c.Assert(err, qt.IsNil)
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	c.Assert(recording.roles, qt.DeepEquals, []objectidentity.ID{builder.Role("app_owner"), builder.Role("Ops Team")})
	c.Assert(recording.roles[1].Name.Normalized, qt.Equals, "Ops Team")
	c.Assert(desired.Roles, qt.DeepEquals, []schemamodel.Role{{Name: "app_owner"}, {Name: "Ops Team"}})
}
