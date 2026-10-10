package chplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chplan"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
)

func refreshView() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "analytics", "totals")
}

func hourly() chschema.Schedule {
	return chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR"}
}

// refreshRequest changes one view's schedule beside the given host steps.
func refreshRequest(change *chdiff.Refresh, steps ...featureplan.CommonStep) featureplan.Request {
	return featureplan.Request{
		Target: "clickhouse", Identifiers: identifier.ForDialect("clickhouse"),
		Changes:     []schemaext.ChangeRecord{{Subject: refreshView(), Value: change}},
		CommonSteps: steps,
	}
}

// viewReplacement is the host's drop and create of the view, which is how it
// replaces one.
func viewReplacement() []featureplan.CommonStep {
	return []featureplan.CommonStep{
		{ID: plangraph.StepID{Owner: "example.org/common", Name: "drop"}, Effects: []plangraph.Effect{{Subject: refreshView(), Action: plangraph.Drop}}},
		{ID: plangraph.StepID{Owner: "example.org/common", Name: "create"}, Effects: []plangraph.Effect{{Subject: refreshView(), Action: plangraph.Create}}},
	}
}

// A schedule changed to another is set in place under the ALTER that names the
// view, outside a transaction, and the view keeps its rows.
func TestRefreshPlanChangesTheScheduleInPlace(t *testing.T) {
	c := qt.New(t)
	after := chschema.Schedule{Mode: chschema.RefreshAfter, Interval: "30 MINUTE", DependsOn: []string{"analytics.source"}}
	request := refreshRequest(&chdiff.Refresh{Before: &chschema.ObservedRefresh{Schedule: hourly()}, After: &chschema.DesiredRefresh{Schedule: after}})

	result, err := must.Must(builtin.New()).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Strategy, qt.Equals, "change the refresh schedule in place; the view keeps its rows")
	c.Assert(result.Contributions, qt.HasLen, 1)
	c.Assert(result.Contributions[0].Steps, qt.HasLen, 1)
	step := result.Contributions[0].Steps[0]
	c.Assert(result.Changes[0].Steps, qt.DeepEquals, []plangraph.StepID{step.ID})
	c.Assert(step.Payload.Role, qt.Equals, ast.AlterExtension)
	c.Assert(step.Payload.Parent, qt.Equals, request.Changes[0].Subject)
	c.Assert(step.Payload.Payload, qt.DeepEquals, &chast.ModifyRefresh{Schedule: after})
	c.Assert(step.Effects, qt.DeepEquals, []plangraph.Effect{{Subject: request.Changes[0].Subject, Action: plangraph.Alter}})
	c.Assert(step.Transaction, qt.Equals, plangraph.TransactionForbidden)
	c.Assert(step.Impact.Impact, qt.Equals, schemaext.Behavioral)
}

// A change that replaces the view is made by the host's drop and create, which
// recreates the view with its declared schedule. The owner accounts for it
// through that replacement and adds no step of its own.
func TestRefreshPlanYieldsToTheViewReplacement(t *testing.T) {
	tests := []struct {
		name   string
		change *chdiff.Refresh
	}{
		{name: "a schedule gained", change: &chdiff.Refresh{After: &chschema.DesiredRefresh{Schedule: hourly()}}},
		{name: "a schedule lost", change: &chdiff.Refresh{Before: &chschema.ObservedRefresh{Schedule: hourly()}}},
		{name: "APPEND added", change: &chdiff.Refresh{
			Before: &chschema.ObservedRefresh{Schedule: hourly()},
			After:  &chschema.DesiredRefresh{Schedule: chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR", Append: true}},
		}},
		{name: "an in-place change on a replaced view", change: &chdiff.Refresh{
			Before: &chschema.ObservedRefresh{Schedule: hourly()},
			After:  &chschema.DesiredRefresh{Schedule: chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "2 HOUR"}},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := refreshRequest(test.change, viewReplacement()...)

			result, err := (chplan.RefreshService{}).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Steps, qt.HasLen, 0)
			c.Assert(result.Changes[0].Strategy, qt.Equals, "the materialized view replacement recreates the view with its declared refresh schedule; the rows it held are discarded")
		})
	}
}

// A change only a replacement can make, on a view the plan keeps, is refused
// rather than sent as a MODIFY REFRESH the server would refuse.
func TestRefreshPlan_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		change *chdiff.Refresh
	}{
		{name: "a schedule gained", change: &chdiff.Refresh{After: &chschema.DesiredRefresh{Schedule: hourly()}}},
		{name: "a schedule lost", change: &chdiff.Refresh{Before: &chschema.ObservedRefresh{Schedule: hourly()}}},
		{name: "APPEND removed", change: &chdiff.Refresh{
			Before: &chschema.ObservedRefresh{Schedule: chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR", Append: true}},
			After:  &chschema.DesiredRefresh{Schedule: hourly()},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := refreshRequest(test.change)

			result, err := (chplan.RefreshService{}).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Code, qt.Equals, schemavalidation.UnsupportedFeature)
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, "which only replacing the view does, and the plan keeps the view")
			c.Assert(result.Diagnostics[0].Change, qt.DeepEquals, new(0))
		})
	}
}

// A host plan that drops the view without creating it again, or alters it,
// contradicts a comparison that found the view on both sides with a changed
// schedule, so the request is refused as invalid rather than planned.
func TestRefreshPlanRefusesAContradictoryHostPlan(t *testing.T) {
	for _, test := range []struct {
		name   string
		action plangraph.Action
	}{
		{"a drop alone", plangraph.Drop},
		{"a create alone", plangraph.Create},
		{"a common alter", plangraph.Alter},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := refreshRequest(&chdiff.Refresh{After: &chschema.DesiredRefresh{Schedule: hourly()}}, featureplan.CommonStep{
				ID: plangraph.StepID{Owner: "example.org/common", Name: "only"}, Effects: []plangraph.Effect{{Subject: refreshView(), Action: test.action}},
			})

			result, err := (chplan.RefreshService{}).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Code, qt.Equals, schemavalidation.InvalidSchema)
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Contains, "whose settings change")
		})
	}
}
