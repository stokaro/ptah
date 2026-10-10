package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
)

// TestFeatureComparisonPassesTheDatabasePath hands each object owner the
// database path of the comparison it serves, the path a read carried, so the
// owner can read a path an object writes absolute against it.
func TestFeatureComparisonPassesTheDatabasePath(t *testing.T) {
	c := qt.New(t)
	var received []string
	runtime := mustRuntime(c, combinedProvider(comparisonFunc(func(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		received = append(received, request.DatabasePath)
		return changedObject(ctx, request)
	}), facetComparisonFunc(changedFacet)))
	request := combinedRequest(c)
	request.Desired.Coverage = facetCoverage(runtime.Codecs(), schemaext.Desired, schemaext.Complete)
	request.DatabasePath = "/local"

	must.Must(runtime.CompareFeatures(t.Context(), request))

	c.Assert(received, qt.DeepEquals, []string{"/local"})
}

// TestPlanningPassesTheDatabasePath hands each planning owner the database
// path of the plan, through the snapshot every batch is sent as.
func TestPlanningPassesTheDatabasePath(t *testing.T) {
	c := qt.New(t)
	var received []string
	runtime := mustRuntime(c, planningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		received = append(received, request.DatabasePath)
		return plannedFixture(ctx, request)
	})))
	request := planningRequest()
	request.DatabasePath = "/local"

	must.Must(runtime.PlanFeatures(t.Context(), request))

	c.Assert(received, qt.DeepEquals, []string{"/local"})
}
