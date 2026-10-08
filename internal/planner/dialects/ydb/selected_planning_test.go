package ydb_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
)

type selectedPlanning struct {
	*engine.Runtime
	plan func(context.Context, featureplan.Request) (featureplan.Result, error)
}

func (s selectedPlanning) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return s.plan(ctx, request)
}

func TestPlannerUsesSelectedServiceAndCallerContext(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	failure := errors.New("selected planner unavailable")
	calls := 0
	selected := selectedPlanning{Runtime: runtime, plan: func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "ydb")
		c.Assert(request.Changes, qt.HasLen, 1)
		c.Assert(request.Tables, qt.HasLen, 1)
		c.Assert(request.Tables[0].Desired.HasTable(), qt.IsTrue)
		c.Assert(request.Tables[0].Current.HasTable(), qt.IsTrue)
		c.Assert(request.Tables[0].Desired.OwnedObjects.Len(), qt.Equals, 1)
		c.Assert(request.Capabilities.Has(capability.Changefeeds), qt.IsTrue)
		return featureplan.Result{}, failure
	}}
	diff := changefeedsChanged(t, []ydbschema.ChangefeedSpec{{Name: "updates", Mode: "UPDATES", Format: "JSON"}}, nil)
	// The diff also requests an ordinary column operation. A feature service
	// failure must not expose that operation as a usable partial plan.
	diff.TablesModified[0].ColumnsAdded = append(diff.TablesModified[0].ColumnsAdded, field("note", "TEXT", true))
	nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(t.Context(), selected, diff)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(nodes, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
}

func TestPlannerSchedulesSelectedContributionsBeforeReturningNodes(t *testing.T) {
	for _, test := range []struct {
		name string
		edge func(plangraph.StepID) plangraph.Dependency
		want error
	}{
		{"missing dependency", func(step plangraph.StepID) plangraph.Dependency {
			return plangraph.Dependency{Before: plangraph.StepID{Owner: "host", Name: "missing"}, After: step}
		}, plangraph.ErrInvalid},
		{"cycle", func(step plangraph.StepID) plangraph.Dependency {
			return plangraph.Dependency{Before: step, After: step}
		}, plangraph.ErrCycle},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			selected := selectedPlanning{Runtime: runtime, plan: func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
				result, err := runtime.PlanFeatures(ctx, request)
				c.Assert(err, qt.IsNil)
				step := result.Contributions[0].Steps[0].ID
				result.Contributions[0].Dependencies = append(result.Contributions[0].Dependencies, test.edge(step))
				return result, nil
			}}
			diff := changefeedsChanged(t, []ydbschema.ChangefeedSpec{{Name: "updates", Mode: "UPDATES", Format: "JSON"}}, nil)
			nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(t.Context(), selected, diff)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestPlannerCancellationAfterSelectedServiceReturnsNoPlan(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	selected := selectedPlanning{Runtime: runtime, plan: func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		result, err := runtime.PlanFeatures(ctx, request)
		cancel()
		return result, err
	}}
	diff := changefeedsChanged(t, []ydbschema.ChangefeedSpec{{Name: "updates", Mode: "UPDATES", Format: "JSON", Consumers: []ast.TopicConsumerSpec{{Name: "reader"}}}}, nil)
	nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(ctx, selected, diff)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(nodes, qt.IsNil)
}
