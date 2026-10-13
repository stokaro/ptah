package engine_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

func declaredRole(name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("postgres")).Role(name)
}

// An owner must not change another owner's authority to remove state by
// changing the shared list of managed roles.
func TestFeatureComparison_DeclaredRolesAreIsolatedBetweenServices(t *testing.T) {
	c := qt.New(t)
	var first, second []objectidentity.ID
	provider := comparisonProvider(comparisonFunc(unchangedComparison))
	one, two := provider.Comparisons[0], provider.Comparisons[0]
	one.Kinds, two.Kinds = []schemaext.Kind{conversionFirst}, []schemaext.Kind{conversionSecond}
	one.Service = comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		first = slices.Clone(r.DeclaredRoles)
		r.DeclaredRoles[0] = declaredRole("invented")
		return unchangedComparison(ctx, r)
	})
	two.Service = comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		second = slices.Clone(r.DeclaredRoles)
		r.DeclaredRoles[0] = declaredRole("another")
		return unchangedComparison(ctx, r)
	})
	provider.Comparisons = []engine.ObjectComparison{one, two}
	runtime := mustRuntime(c, provider)
	objects := comparisonRequest(c)
	request := schemaext.ComparisonRequest{Target: objects.Target, Owners: objects.Parents,
		Desired:       schemaext.FeatureState{Objects: objects.Desired.Objects},
		DeclaredRoles: []objectidentity.ID{declaredRole("z_owner"), declaredRole("Ops Team")}}

	result, err := runtime.CompareFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	want := []objectidentity.ID{declaredRole("Ops Team"), declaredRole("z_owner")}
	c.Assert(first, qt.DeepEquals, want)
	c.Assert(second, qt.DeepEquals, want)
	c.Assert(request.DeclaredRoles, qt.DeepEquals, []objectidentity.ID{declaredRole("z_owner"), declaredRole("Ops Team")})
}

func TestObjectComparison_NoDeclaredRolesManagesNone(t *testing.T) {
	c := qt.New(t)
	var received []objectidentity.ID
	calls := 0
	runtime := mustRuntime(c, comparisonProvider(comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		calls++
		received = r.DeclaredRoles
		return unchangedComparison(ctx, r)
	})))
	result, err := runtime.CompareObjects(t.Context(), comparisonRequest(c))
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(received, qt.IsNil)
}

func TestObjectComparison_RefusesInvalidDeclaredRolesBeforeDispatch(t *testing.T) {
	name := objectidentity.Part{Source: "owner", Normalized: "owner"}
	for _, test := range []struct {
		name string
		role objectidentity.ID
	}{
		{name: "table", role: objectidentity.ID{Kind: objectidentity.KindTable, Name: name}},
		{name: "missing name", role: objectidentity.ID{Kind: objectidentity.KindRole}},
		{name: "missing normalized name", role: objectidentity.ID{Kind: objectidentity.KindRole, Name: objectidentity.Part{Source: "owner"}}},
		{name: "missing source name", role: objectidentity.ID{Kind: objectidentity.KindRole, Name: objectidentity.Part{Normalized: "owner"}}},
		{name: "catalog", role: objectidentity.ID{Kind: objectidentity.KindRole, Name: name, Catalog: name}},
		{name: "schema", role: objectidentity.ID{Kind: objectidentity.KindRole, Name: name, Schema: name}},
		{name: "parent", role: objectidentity.ID{Kind: objectidentity.KindRole, Name: name, Parent: name}},
		{name: "signature", role: objectidentity.ID{Kind: objectidentity.KindRole, Name: name, Signature: "integer"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			runtime := mustRuntime(c, comparisonProvider(comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
				calls++
				return unchangedComparison(ctx, r)
			})))
			request := comparisonRequest(c)
			request.DeclaredRoles = []objectidentity.ID{test.role}

			result, err := runtime.CompareObjects(t.Context(), request)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(calls, qt.Equals, 0)
		})
	}
}
