package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

func tableFacetReversalProvider() engine.Provider {
	provider := facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return facetReply(request), nil
	}))
	provider.Targets[0].Name, provider.Targets[0].Aliases = "ydb", nil
	provider.Targets[0].Validation = validationFunc(func(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
		return schemavalidation.Result{Complete: true}, nil
	})
	provider.FacetComparisons[0].Target = "ydb"
	provider.Codecs = append(provider.Codecs, planningCodec())
	provider.Conversions = []engine.Conversion{{Target: "ydb", Kinds: []schemaext.Kind{conversionFirst, conversionSecond}, Service: conversionFunc(func(_ context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
		return request.Values, nil
	})}}
	provider.Reversals = []engine.Reversal{{Target: "ydb", Kinds: []schemaext.Kind{comparedKind}, Service: reversalFunc(func(_ context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
		var results []schemaext.Reversal
		for _, change := range request.Changes {
			results = append(results, schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: change.Subject, Value: &comparedChange{Number: -change.Value.(*comparedChange).Number}}, Strategy: "restore captured table settings",
				ForwardState: []schemaext.ProjectedValue{
					{Placement: schemaext.FacetPlacement, Kind: conversionFirst, Value: &conversionValue{ID: conversionFirst, Number: 1}},
					{Placement: schemaext.FacetPlacement, Kind: conversionSecond, Value: &conversionValue{ID: conversionSecond, Number: 2}},
				}})
		}
		return results, nil
	})}}
	provider.Planning = []engine.Planning{{Target: "ydb", Kinds: []schemaext.Kind{comparedKind}, OperationKinds: []schemaext.Kind{planningOperationKind}, Service: planningFunc(func(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
		result := featureplan.Result{Complete: true}
		contribution := plangraph.Contribution[featureplan.Operation]{Owner: "example.org/converter"}
		for _, change := range request.Changes {
			step := plangraph.StepID{Owner: contribution.Owner, Name: change.Subject.Name.Source}
			contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: step,
				Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: change.Subject, Payload: &planningOperation{Values: []int{change.Value.(*comparedChange).Number}}},
				Effects: []plangraph.Effect{{Subject: change.Subject, Action: plangraph.Alter}}, Transaction: plangraph.TransactionForbidden,
			})
			result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: change.Subject, Kind: change.Value.Kind(), Strategy: "apply captured table settings", Steps: []plangraph.StepID{step}})
		}
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
		return result, nil
	})}}
	return provider
}

func TestTableFacetReversalProjectsAcceptedStateIntoRollback(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, tableFacetReversalProvider())
	desired, current := facetSchemas()
	desired.Fields = []schemamodel.Field{{StructName: "Orders", Name: "id", Type: "uint64", Primary: true}}
	current.Tables[0].Columns = []catalog.Column{{Name: "id", DataType: "Uint64", IsPrimaryKey: true}}
	current.FeatureCoverage = facetCoverage(runtime.Codecs(), schemaext.Observed, schemaext.Uninspected)
	current.Tables[0].Facets = must.Must(current.Tables[0].Facets.WithTargetScope(conversionFirst, "ydb"))
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	original := diff.TablesModified[0].Current.Clone()
	// Planning must use accepted changes, even after the caller edits its source.
	desired.Tables[0].Facets = must.Must(desired.Tables[0].Facets.Replace(&conversionValue{ID: conversionFirst, Number: 99}))
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "ydb", Capabilities: capability.YDB262(),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Reverse.Diff.TablesModified, qt.HasLen, 1)
	reversed := plan.Reverse.Diff.TablesModified[0]
	c.Assert(reversed.FeatureChanges[0].Value.(*comparedChange).Number, qt.Equals, -9)
	for _, test := range []struct {
		kind   schemaext.Kind
		number int
	}{{conversionFirst, 1}, {conversionSecond, 2}} {
		value, found, err := schemaext.FacetAs[*conversionValue](reversed.Current.Table.Facets, test.kind)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		c.Assert(value.Number, qt.Equals, test.number)
		c.Assert(reversed.Current.FeatureCoverage.Lookup(test.kind, reversed.FeatureChanges[0].Subject).State, qt.Equals, schemaext.Complete)
	}
	c.Assert(reversed.Current.Table.Facets.TargetScope(conversionFirst), qt.DeepEquals, []string{"ydb"})
	c.Assert(diff.TablesModified[0].Current, qt.DeepEquals, original)
	c.Assert(current.Tables[0].Facets, qt.DeepEquals, original.Table.Facets)
	c.Assert(plan.Forward.Nodes, qt.HasLen, 1)
	c.Assert(plan.Reverse.Nodes, qt.HasLen, 2)
}
