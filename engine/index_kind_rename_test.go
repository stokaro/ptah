package engine_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/migration/schemadiff"
)

// heldKindRules compare a declared index's type with the held index's
// method exactly, and keep the index facet across a rename.
type heldKindRules struct{}

func (heldKindRules) OptionsEqual(desired schemamodel.Index, current catalog.Index) bool {
	return desired.Type == current.Method
}

func (heldKindRules) RenameCarries(kind schemaext.Kind) bool { return kind == conversionSecond }

// asyncKinds prepares every table as it is and states async, the held
// index's spelling, as the kind of each declared index.
var asyncKinds = preparationFunc(func(_ context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
	result := schemapreparation.Result{Complete: true, Tables: request.Clone().Tables}
	for i := range result.Tables {
		result.Tables[i].IndexMethods = make(map[string]string, len(result.Tables[i].Desired.Indexes))
		for _, index := range result.Tables[i].Desired.Indexes {
			result.Tables[i].IndexMethods[index.Name] = "async"
		}
	}
	return result, nil
})

// An index renamed under the kind its table's preparation states is one
// index to the feature comparison too: the comparison reads the renamed
// index, declared and held, under its declared name. Read under its old name,
// the held facet would belong to an index the declaration does not have.
func TestCompare_PreparedIndexKindsPairARenameForTheFeatureComparison(t *testing.T) {
	c := qt.New(t)
	var owners []schemaext.ParentState
	provider := mixedFacetProvider(facetComparisonFunc(func(_ context.Context, r schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		owners = append(owners, r.Owners...)
		return schemaext.FacetComparisonResult{Complete: true, Desired: r.Desired}, nil
	}))
	provider.Targets[0].Preparation = asyncKinds
	provider.Targets[0].Comparison = schemacomparison.Rules{Indexes: heldKindRules{}}
	runtime := acceptedRuntime{Runtime: mustRuntime(c, provider)}
	desired, current := renamedFacetIndexes()
	desired.Indexes[0].Type = "GLOBAL ASYNC"
	current.Indexes[0].Method = "async"

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current,
		catalog.ServerInfo{Dialect: "custom", Capabilities: capability.Of(map[capability.Capability]bool{capability.IndexRename: true})}, nil, runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexesRenamed, qt.DeepEquals, []difftypes.IndexRename{{TableName: "orders", From: "old_status", To: "by_status"}})
	indexes := slices.DeleteFunc(owners, func(owner schemaext.ParentState) bool { return owner.Subject.Kind != objectidentity.KindIndex })
	c.Assert(indexes, qt.HasLen, 1)
	c.Assert(indexes[0].Subject.Name.Source, qt.Equals, "by_status")
	c.Assert(indexes[0].Desired, qt.IsTrue)
	c.Assert(indexes[0].Current, qt.IsTrue)
}
