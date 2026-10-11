package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// carryingIndexes reads every index's options as equal, and carries a facet
// of kind carried through a rename.
type carryingIndexes struct {
	carried schemaext.Kind
}

func (carryingIndexes) OptionsEqual(schemamodel.Index, catalog.Index) bool { return true }

func (r carryingIndexes) RenameCarries(kind schemaext.Kind) bool { return kind == r.carried }

// acceptedRuntime is a runtime whose validation accepts every declaration, so
// a comparison through [schemadiff.CompareWithDatabaseInfo] reaches the
// target's own services.
type acceptedRuntime struct {
	*engine.Runtime
}

func (acceptedRuntime) ValidateSchema(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
	return schemavalidation.Result{Complete: true}, nil
}

// renamedFacetIndexes is index old_status, which carries a facet of
// conversionSecond, renamed to by_status with the same facet.
func renamedFacetIndexes() (*schemamodel.Database, *catalog.Database) {
	desired, current := facetSchemas()
	desired.Tables[0].Facets = desired.Tables[0].Facets.Without(conversionSecond)
	current.Tables[0].Facets = current.Tables[0].Facets.Without(conversionSecond)
	facets := must.Must(schemaext.NewFacets(&conversionValue{ID: conversionSecond, Number: 2}))
	desired.Indexes = []schemamodel.Index{{Name: "by_status", StructName: "Orders", Fields: []string{"status"}, Facets: facets}}
	current.Indexes = []catalog.Index{{Name: "old_status", TableName: "orders", Columns: []string{"status"}, Facets: facets}}
	return desired, current
}

// TestCompare_IndexRulesDecideWhatARenameCarries shows that a comparison asks
// the selected target's index rules whether a rename keeps an index's facet:
// an index whose facet the rules carry is renamed, and one whose facet they
// do not is dropped and built again.
func TestCompare_IndexRulesDecideWhatARenameCarries(t *testing.T) {
	keep := func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}, nil
	}
	cases := []struct {
		name        string
		carried     schemaext.Kind
		wantRenamed []difftypes.IndexRename
		wantAdded   []difftypes.IndexRef
	}{
		{name: "carried", carried: conversionSecond,
			wantRenamed: []difftypes.IndexRename{{TableName: "orders", From: "old_status", To: "by_status"}}},
		{name: "not carried", carried: conversionFirst,
			wantAdded: []difftypes.IndexRef{{TableName: "orders", Name: "by_status"}}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			provider := mixedFacetProvider(facetComparisonFunc(keep))
			provider.Targets[0].Comparison = schemacomparison.Rules{Indexes: carryingIndexes{carried: tt.carried}}
			runtime := acceptedRuntime{Runtime: mustRuntime(c, provider)}
			desired, current := renamedFacetIndexes()

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current,
				catalog.ServerInfo{Dialect: "custom", Capabilities: capability.Capabilities{capability.IndexRename: true}}, nil, runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.IndexesRenamed, qt.DeepEquals, tt.wantRenamed)
			c.Assert(diff.IndexAdditions(), qt.DeepEquals, tt.wantAdded)
		})
	}
}
