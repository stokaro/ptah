package engine_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
)

// replacingChange is a change its owner applies only by replacing the common
// object the setting is attached to.
type replacingChange struct{ Number int }

const replacingKind schemaext.Kind = "example.org/replacing"

func (*replacingChange) Kind() schemaext.Kind { return replacingKind }
func (v *replacingChange) CloneChange() schemaext.ChangeValue {
	return &replacingChange{Number: v.Number}
}
func (*replacingChange) ReplacesOwner() bool { return true }

// withReplacingChanges registers replacingChange beside the provider's own
// change model.
func withReplacingChanges(provider engine.Provider) engine.Provider {
	encode := func(value schemaext.Payload) (json.RawMessage, error) { return json.Marshal(value) }
	provider.Codecs = append(provider.Codecs, schemaext.Codec{
		Prototype: &replacingChange{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(`{"type":"object","properties":{"Number":{"type":"integer"}}}`),
		Clone:      func(v schemaext.Payload) (schemaext.Payload, error) { return v.(*replacingChange).CloneChange(), nil },
		Encode:     encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			return schemaext.DecodeJSON[*replacingChange](data)
		},
	})
	provider.FacetComparisons[0].ChangeKinds = append(provider.FacetComparisons[0].ChangeKinds, replacingKind)
	return provider
}

// viewFacetProvider compares the attached settings of materialized views.
func viewFacetProvider(service schemaext.FacetComparisonService) engine.Provider {
	provider := withReplacingChanges(facetProvider(service))
	provider.FacetComparisons[0].OwnerKinds = []objectidentity.Kind{objectidentity.KindMatView}
	return provider
}

func viewFacetSchemas() (*schemamodel.Database, *catalog.Database) {
	request := facetRequest()
	return &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{
			Name: "daily", StructName: "Daily", Body: "SELECT 1", Facets: request.Desired.Records[0].Values,
		}}},
		&catalog.Database{MatViews: []catalog.MaterializedView{{Name: "daily", Body: "SELECT 1", Facets: request.Current.Records[0].Values}}}
}

// changeEachView answers with one change per materialized view owner.
func changeEachView(value func() schemaext.ChangeValue) facetComparisonFunc {
	return func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		result := schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}
		for _, owner := range r.Owners {
			result.Changes = append(result.Changes, schemaext.FacetChange{Kind: r.Kinds[0], Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: value()}})
		}
		return result, nil
	}
}

// A change to a view's attached settings joins that view's entry, which keeps
// the view: the owner applies the change in place.
func TestViewFacetChangesJoinTheirViewWithoutReplacingIt(t *testing.T) {
	c := qt.New(t)
	var owners []schemaext.ParentState
	runtime := mustRuntime(c, viewFacetProvider(facetComparisonFunc(func(ctx context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		owners = r.Owners
		return changeEachView(func() schemaext.ChangeValue { return &comparedChange{Number: 1} })(ctx, r)
	})))
	desired, current := viewFacetSchemas()
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime))
	c.Assert(owners, qt.HasLen, 1)
	c.Assert(owners[0].Subject.Kind, qt.Equals, objectidentity.KindMatView)
	c.Assert(owners[0].Desired, qt.IsTrue)
	c.Assert(owners[0].Current, qt.IsTrue)
	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
	view := diff.MaterializedViewsModified[0]
	c.Assert(view.ViewName, qt.Equals, "daily")
	c.Assert(view.Changes, qt.HasLen, 0)
	c.Assert(view.FeatureChanges, qt.HasLen, 1)
	c.Assert(view.FeatureChanges[0].Subject.Kind, qt.Equals, objectidentity.KindMatView)
	c.Assert(view.FeatureChanges[0].Subject.Name.Source, qt.Equals, "daily")
	c.Assert(view.Desired.Facets, qt.DeepEquals, desired.MaterializedViews[0].Facets)
	c.Assert(view.Replaces(), qt.IsFalse)
	c.Assert(diff.HasChanges(), qt.IsTrue)
}

// A change its owner applies only by replacing the view marks the entry as a
// replacement, as a changed definition does.
func TestViewFacetChangeThatReplacesTheViewMarksItsEntry(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, viewFacetProvider(changeEachView(func() schemaext.ChangeValue { return &replacingChange{Number: 1} })))
	desired, current := viewFacetSchemas()
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime))
	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(diff.MaterializedViewsModified[0].Changes, qt.HasLen, 0)
	c.Assert(diff.MaterializedViewsModified[0].Replaces(), qt.IsTrue)
}

// A view whose definition and settings both changed is one entry holding both.
func TestViewFacetChangesShareTheEntryOfAChangedDefinition(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, viewFacetProvider(changeEachView(func() schemaext.ChangeValue { return &comparedChange{Number: 1} })))
	desired, current := viewFacetSchemas()
	desired.MaterializedViews[0].Body = "SELECT 2"
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime))
	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
	view := diff.MaterializedViewsModified[0]
	c.Assert(view.Changes, qt.HasLen, 1)
	c.Assert(view.FeatureChanges, qt.HasLen, 1)
	c.Assert(view.Replaces(), qt.IsTrue)
}

// Settings the comparison adopts from the server travel in the entry's
// declaration, so a replacement recreates the view with them, while the
// caller's schema is left as it was.
func TestViewFacetAdoptionReachesTheEntryWithoutRewritingTheSource(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, viewFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return schemaext.FacetComparisonResult{Complete: true, Desired: schemaext.FacetState{Records: r.Current.Records}}, nil
	})))
	desired, current := viewFacetSchemas()
	desired.MaterializedViews[0].Facets = schemaext.Facets{}
	desired.MaterializedViews[0].Body = "SELECT 2"
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime))
	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(diff.MaterializedViewsModified[0].FeatureChanges, qt.HasLen, 0)
	c.Assert(diff.MaterializedViewsModified[0].Desired.Facets, qt.DeepEquals, current.MatViews[0].Facets)
	c.Assert(desired.MaterializedViews[0].Facets.IsZero(), qt.IsTrue)
}

// Only a materialized view can be replaced for an attached setting. A table
// change that asks for it is refused rather than planned as if the table could
// keep its rows.
func TestTableFacetChangeThatReplacesItsTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, withReplacingChanges(facetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired, Changes: []schemaext.FacetChange{{
			Kind: r.Kinds[0], Change: schemaext.ChangeRecord{Subject: r.Owners[0].Subject, Value: &replacingChange{Number: 1}},
		}}}, nil
	}))))
	desired, current := facetSchemas()
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*requires replacing an object this host cannot replace.*`)
	c.Assert(diff, qt.IsNil)
}
