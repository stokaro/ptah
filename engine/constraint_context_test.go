package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

func constraintContextRequest() schemaext.FacetComparisonRequest {
	request := facetRequest()
	subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).Constraint("orders", "customer_fk")
	request.Owners[0].Subject = subject
	request.Desired.Records[0].Subject = subject
	request.Current.Records[0].Subject = subject
	request.DesiredConstraints = []schemaext.ConstraintContext{{Subject: subject, Type: "FOREIGN KEY", Columns: []string{"tenant", "customer"}}}
	request.CurrentConstraints = []schemaext.ConstraintContext{request.DesiredConstraints[0].Clone()}
	return request
}

func TestConstraintContextsAreIsolatedBetweenServicesAndCaller(t *testing.T) {
	c := qt.New(t)
	calls := 0
	service := facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		calls++
		c.Assert(request.DesiredConstraints, qt.HasLen, 1)
		c.Assert(request.CurrentConstraints, qt.HasLen, 1)
		c.Assert(request.DesiredConstraints[0].Columns, qt.DeepEquals, []string{"tenant", "customer"})
		c.Assert(request.CurrentConstraints[0].Columns, qt.DeepEquals, []string{"tenant", "customer"})
		request.DesiredConstraints[0].Columns[0] = "changed"
		request.CurrentConstraints[0].Columns[0] = "changed"
		return schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}, nil
	})
	provider := facetProvider(service)
	provider.FacetComparisons[0].OwnerKinds = []objectidentity.Kind{objectidentity.KindConstraint}
	second := provider.FacetComparisons[0]
	provider.FacetComparisons[0].Kinds = []schemaext.Kind{conversionFirst}
	second.Kinds = []schemaext.Kind{conversionSecond}
	provider.FacetComparisons = append(provider.FacetComparisons, second)
	runtime := mustRuntime(c, provider)
	request := constraintContextRequest()
	parent := objectidentity.NewBuilder(identifier.ForDialect("postgres")).Table("orders")
	primary := objectidentity.PrimaryKeyOf(parent)
	request.Owners = append(request.Owners, schemaext.ParentState{Subject: primary, Desired: true, Current: true})
	request.DesiredConstraints = append(request.DesiredConstraints, schemaext.ConstraintContext{Subject: primary, Type: "PRIMARY KEY", Columns: []string{"id"}})
	request.CurrentConstraints = append(request.CurrentConstraints, request.DesiredConstraints[1].Clone())
	result, err := runtime.CompareFeatures(t.Context(), schemaext.ComparisonRequest{Target: request.Target, Owners: request.Owners,
		Desired: schemaext.FeatureState{Facets: request.Desired.Records}, Current: schemaext.FeatureState{Facets: request.Current.Records},
		DesiredConstraints: request.DesiredConstraints, CurrentConstraints: request.CurrentConstraints})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(calls, qt.Equals, 2)
	c.Assert(request.DesiredConstraints[0].Columns, qt.DeepEquals, []string{"tenant", "customer"})
	c.Assert(request.CurrentConstraints[0].Columns, qt.DeepEquals, []string{"tenant", "customer"})
}

func TestConstraintContextsRefuseInvalidInputBeforeDispatch(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*schemaext.FacetComparisonRequest)
	}{
		{name: "duplicate", edit: func(r *schemaext.FacetComparisonRequest) {
			r.DesiredConstraints = append(r.DesiredConstraints, r.DesiredConstraints[0])
		}},
		{name: "missing owner", edit: func(r *schemaext.FacetComparisonRequest) {
			r.DesiredConstraints[0].Subject.Name.Source = "missing"
			r.DesiredConstraints[0].Subject.Name.Normalized = "missing"
		}},
		{name: "absent declaration", edit: func(r *schemaext.FacetComparisonRequest) {
			r.Owners[0].Desired = false
			r.Desired = schemaext.FacetState{}
		}},
		{name: "absent observation", edit: func(r *schemaext.FacetComparisonRequest) {
			r.Owners[0].Current = false
			r.Current = schemaext.FacetState{}
		}},
		{name: "empty type", edit: func(r *schemaext.FacetComparisonRequest) { r.DesiredConstraints[0].Type = "" }},
		{name: "unknown type", edit: func(r *schemaext.FacetComparisonRequest) { r.DesiredConstraints[0].Type = "UNKNOWN" }},
		{name: "empty column", edit: func(r *schemaext.FacetComparisonRequest) { r.CurrentConstraints[0].Columns[0] = "" }},
		{name: "primary key identity", edit: func(r *schemaext.FacetComparisonRequest) { r.DesiredConstraints[0].Type = "PRIMARY KEY" }},
		{name: "invalid column", edit: func(r *schemaext.FacetComparisonRequest) { r.CurrentConstraints[0].Columns[0] = "bad\x00name" }},
		{name: "wrong object kind", edit: func(r *schemaext.FacetComparisonRequest) {
			r.Owners[0].Subject.Kind = objectidentity.KindTable
			r.DesiredConstraints[0].Subject = r.Owners[0].Subject
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			provider := facetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
				calls++
				return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
			}))
			provider.FacetComparisons[0].OwnerKinds = []objectidentity.Kind{objectidentity.KindConstraint}
			runtime := mustRuntime(c, provider)
			request := constraintContextRequest()
			test.edit(&request)
			result, err := runtime.CompareFacets(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Complete, qt.IsFalse)
			c.Assert(calls, qt.Equals, 0)
		})
	}
}
