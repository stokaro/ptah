package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/migration/schemadiff"
)

func indexFacetSchemas() (*schemamodel.Database, *catalog.Database) {
	desired, current := facetSchemas()
	desired.Tables[0].Facets = desired.Tables[0].Facets.Without(conversionSecond)
	current.Tables[0].Facets = current.Tables[0].Facets.Without(conversionSecond)
	desired.Indexes = []schemamodel.Index{{Name: "by.status", StructName: "Orders", Fields: []string{"status"},
		Facets: must.Must(schemaext.NewFacets(&conversionValue{ID: conversionSecond, Number: 2}))}}
	current.Indexes = []catalog.Index{{Name: "by.status", TableName: "orders", Columns: []string{"status"},
		Facets: must.Must(schemaext.NewFacets(&conversionValue{ID: conversionSecond, Number: 1}))}}
	return desired, current
}

func TestIndexFacetComparisonCapturesChangesOnTheirTable(t *testing.T) {
	c := qt.New(t)
	calls := 0
	runtime := mustRuntime(c, mixedFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		calls++
		c.Assert(r.Owners, qt.HasLen, 1)
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired, Changes: []schemaext.FacetChange{{
			Kind: r.Kinds[0], Change: schemaext.ChangeRecord{Subject: r.Owners[0].Subject, Value: &comparedChange{Number: 1}},
		}}}, nil
	})))
	desired, current := indexFacetSchemas()
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime))
	c.Assert(calls, qt.Equals, 2)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	table := diff.TablesModified[0]
	c.Assert(table.FeatureChanges, qt.HasLen, 2)
	c.Assert(table.Desired.Indexes, qt.HasLen, 1)
	c.Assert(table.Current.Indexes, qt.HasLen, 1)
	c.Assert(table.Desired.Indexes[0].Facets, qt.DeepEquals, desired.Indexes[0].Facets)
	c.Assert(table.Current.Indexes[0].Facets, qt.DeepEquals, current.Indexes[0].Facets)
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
	c.Assert(table.FeatureChanges[0].Subject.Kind, qt.Equals, objectidentity.KindIndex)
	c.Assert(table.FeatureChanges[0].Subject.Name.Source, qt.Equals, "by.status")
	c.Assert(table.FeatureChanges[0].Subject.Parent.Source, qt.Equals, "orders")
}

func TestIndexFacetAdoptionIsCapturedWithoutRewritingTheSource(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, mixedFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		desired := map[schemaext.Kind]schemaext.FacetState{
			conversionFirst: r.Desired, conversionSecond: {Records: r.Current.Records},
		}[r.Kinds[0]]
		return schemaext.FacetComparisonResult{Complete: true, Desired: desired}, nil
	})))
	desired, current := indexFacetSchemas()
	desired.Indexes[0].Facets = schemaext.Facets{}
	desired.Tables[0].Comment = "capture the effective index settings"
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime))
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].Desired.Indexes[0].Facets, qt.DeepEquals, current.Indexes[0].Facets)
	c.Assert(desired.Indexes[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(diff.TablePreparation.Source[0].Desired.Indexes[0].Facets.IsZero(), qt.IsTrue)
}

func TestIndexFacetIdentitiesKeepLiteralDotsAndTableNamespaces(t *testing.T) {
	c := qt.New(t)
	var owners []schemaext.ParentState
	provider := facetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		owners = r.Owners
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
	}))
	provider.FacetComparisons[0].OwnerKinds = []objectidentity.Kind{objectidentity.KindIndex}
	runtime := mustRuntime(c, provider)
	values := must.Must(schemaext.NewFacets(&conversionValue{ID: conversionFirst}))
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "orders.a", StructName: "First"}, {Name: "orders", StructName: "Second"}},
		Indexes: []schemamodel.Index{
			{Name: "by.status", StructName: "First", Fields: []string{"status"}, Facets: values},
			{Name: "a.by.status", StructName: "Second", Fields: []string{"status"}, Facets: values},
		}}
	current := &catalog.Database{Tables: []catalog.Table{{Name: "orders.a"}, {Name: "orders"}}, Indexes: []catalog.Index{
		{Name: "by.status", TableName: "orders.a", Columns: []string{"status"}, Facets: values},
		{Name: "a.by.status", TableName: "orders", Columns: []string{"status"}, Facets: values},
	}}
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime))
	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(owners, qt.HasLen, 2)
	for _, owner := range owners {
		c.Assert(owner.Desired, qt.IsTrue)
		c.Assert(owner.Current, qt.IsTrue)
	}
	c.Assert(owners[0].Subject.Key(), qt.Not(qt.Equals), owners[1].Subject.Key())
}

func TestIndexFacetCoverageBindsTheConnectionDatabase(t *testing.T) {
	c := qt.New(t)
	received := make(map[schemaext.Kind]schemaext.FacetComparisonRequest)
	provider := mixedFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		received[r.Kinds[0]] = r
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
	}))
	runtime := mustRuntime(c, provider)
	desired, current := indexFacetSchemas()
	semantics := identifier.ForDialect("custom")
	subject := objectidentity.NewBuilder(semantics).IndexParts("", "orders", "by.status")
	coverage := facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Uninspected, schemaext.SubjectCoverage{
		Kind: conversionSecond, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "partial source"},
	})
	desired.FeatureCoverage = coverage
	semantics.DefaultSchema = "live.database"
	opts := &config.CompareOptions{Dialect: "custom", IdentifierSemantics: &semantics}
	_, err := schemadiff.CompareWithOptions(t.Context(), desired, current, opts, runtime)
	c.Assert(err, qt.IsNil)
	request := received[conversionSecond]
	c.Assert(request.Owners, qt.HasLen, 1)
	bound := request.Desired.Coverage.SubjectRecords()[0]
	c.Assert(bound.Subject.Key(), qt.Equals, request.Owners[0].Subject.Key())
	c.Assert(bound.Subject.Schema.Normalized, qt.Equals, "live.database")
	c.Assert(bound.Knowledge, qt.DeepEquals, coverage.SubjectRecords()[0].Knowledge)
	c.Assert(desired.FeatureCoverage, qt.DeepEquals, coverage)
}

func TestSchemaScopedIndexFacetChangeKeepsItsCommonIdentity(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, mixedFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		changes := map[schemaext.Kind][]schemaext.FacetChange{conversionSecond: {{Kind: conversionSecond,
			Change: schemaext.ChangeRecord{Subject: r.Owners[0].Subject, Value: &comparedChange{Number: 2}}}}}
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired, Changes: changes[r.Kinds[0]]}, nil
	})))
	desired, current := indexFacetSchemas()
	semantics := identifier.ForDialect("custom")
	semantics.IndexNamespace = identifier.IndexNamespaceSchema
	opts := &config.CompareOptions{Dialect: "custom", IdentifierSemantics: &semantics}
	diff := must.Must(schemadiff.CompareWithOptions(t.Context(), desired, current, opts, runtime))
	c.Assert(diff.FeatureChanges, qt.HasLen, 1)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.FeatureChanges[0].Subject.Kind, qt.Equals, objectidentity.KindIndex)
	c.Assert(diff.FeatureChanges[0].Subject.Parent.Empty(), qt.IsTrue)
	c.Assert(diff.FeatureChanges[0].Subject.Name.Source, qt.Equals, "by.status")
}

func TestIndexCoverageBindingRefusesCollidingClaimsBeforeDispatch(t *testing.T) {
	for _, side := range []schemaext.Representation{schemaext.Desired, schemaext.Observed} {
		t.Run(string(side), func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			runtime := mustRuntime(c, mixedFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
				calls++
				return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
			})))
			desired, current := indexFacetSchemas()
			semantics := identifier.ForDialect("custom")
			builder := objectidentity.NewBuilder(semantics)
			var claims []schemaext.SubjectCoverage
			for _, schema := range []string{"", "connection_database"} {
				claims = append(claims, schemaext.SubjectCoverage{Kind: conversionSecond, Subject: builder.IndexParts(schema, "orders", "by.status"),
					Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "partial source"}})
			}
			coverage := facetCoverage(runtime.Codecs(), side, schemaext.Uninspected, claims...)
			states := map[schemaext.Representation]*schemaext.Coverage{schemaext.Desired: &desired.FeatureCoverage, schemaext.Observed: &current.FeatureCoverage}
			*states[side] = coverage
			semantics.DefaultSchema = "connection_database"
			opts := &config.CompareOptions{Dialect: "custom", IdentifierSemantics: &semantics}
			diff, err := schemadiff.CompareWithOptions(t.Context(), desired, current, opts, runtime)
			c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
			c.Assert(diff, qt.IsNil)
			c.Assert(calls, qt.Equals, 0)
			c.Assert(*states[side], qt.DeepEquals, coverage)
		})
	}
}

func TestPreparedIndexFacetsReachComparisonAndCapturesWithoutSourceChanges(t *testing.T) {
	c := qt.New(t)
	received := make(map[schemaext.Kind]schemaext.FacetComparisonRequest)
	provider := mixedFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		received[r.Kinds[0]] = r
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
	}))
	provider.Targets[0].Preparation = preparationFunc(func(_ context.Context, r schemapreparation.Request) (schemapreparation.Result, error) {
		table := &r.Tables[0]
		index := objectidentity.NewBuilder(r.Identifiers).IndexParts(table.Subject.Schema.Source, table.Subject.Name.Source, table.Desired.Indexes[0].Name)
		table.ResolvedFacets = []schemaext.FacetRecord{{Subject: index, Values: must.Must(schemaext.NewFacets(&conversionValue{ID: conversionSecond, Number: 33}))}}
		return schemapreparation.Result{Complete: true, Tables: r.Tables}, nil
	})
	runtime := mustRuntime(c, provider)
	desired, current := indexFacetSchemas()
	desired.Indexes[0].Facets = must.Must(desired.Indexes[0].Facets.WithTargetScope(conversionSecond, "custom"))
	desired.Tables[0].Comment = "capture the prepared index"
	source := desired.Indexes[0].Facets
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime))
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	prepared := diff.TablesModified[0].Desired.Indexes[0].Facets
	c.Assert(prepared.TargetScope(conversionSecond), qt.DeepEquals, []string{"custom"})
	value, found, err := schemaext.FacetAs[*conversionValue](prepared, conversionSecond)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.Number, qt.Equals, 33)
	c.Assert(received[conversionSecond].Desired.Records[0].Values, qt.DeepEquals, prepared)
	c.Assert(desired.Indexes[0].Facets, qt.DeepEquals, source)
	c.Assert(diff.TablePreparation.Source[0].Desired.Indexes[0].Facets, qt.DeepEquals, source)
	c.Assert(diff.TablePreparation.Prepared[0].Desired.Indexes[0].Facets, qt.DeepEquals, source)
	c.Assert(diff.TablesModified[0].Current.Indexes[0].Facets, qt.DeepEquals, current.Indexes[0].Facets)
}

func TestPreparedIndexFacetsKeepStructuralOwnerAcrossComparison(t *testing.T) {
	c := qt.New(t)
	provider := mixedFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
	}))
	provider.Targets[0].Preparation = preparationFunc(func(_ context.Context, r schemapreparation.Request) (schemapreparation.Result, error) {
		table := &r.Tables[0]
		index := objectidentity.NewBuilder(r.Identifiers).IndexParts(table.Subject.Schema.Source, table.Subject.Name.Source, table.Desired.Indexes[0].Name)
		table.ResolvedFacets = []schemaext.FacetRecord{{Subject: index, Values: must.Must(schemaext.NewFacets(&conversionValue{ID: conversionSecond, Number: 33}))}}
		return schemapreparation.Result{Complete: true, Tables: r.Tables}, nil
	})
	desired, current := indexFacetSchemas()
	desired.Tables[0].Schema = "tenant.archive"
	desired.Tables[0].Name = "orders.2026"
	desired.Tables[0].Comment = "capture the prepared index"
	current.Tables[0].Schema = "tenant.archive"
	current.Tables[0].Name = "orders.2026"
	current.Indexes[0].Schema = "tenant.archive"
	current.Indexes[0].TableName = "orders.2026"
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", mustRuntime(c, provider))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].Desired.Indexes, qt.HasLen, 1)
	value, found, err := schemaext.FacetAs[*conversionValue](diff.TablesModified[0].Desired.Indexes[0].Facets, conversionSecond)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.Number, qt.Equals, 33)
}
