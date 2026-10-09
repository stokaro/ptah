package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

// viewPlanningRequest changes a setting attached to a materialized view. A
// view has no table capture, so the change is what makes it a parent an ALTER
// operation may name.
func viewPlanningRequest() (featureplan.Request, objectidentity.ID) {
	request := planningRequest()
	view := objectidentity.NewBuilder(request.Identifiers).SchemaScopedParts(objectidentity.KindMatView, "", "totals")
	request.Changes = []schemaext.ChangeRecord{{Subject: view, Value: &reversalChange{ID: conversionFirst, Number: 3}}}
	return request, view
}

// viewPlanned answers every change with one ALTER operation in the envelope
// that names parent.
func viewPlanned(parent objectidentity.ID) planningFunc {
	return func(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
		step := plangraph.StepID{Owner: "example.org/reverser", Name: "alter"}
		change := request.Changes[0]
		return featureplan.Result{
			Complete: true,
			Changes:  []featureplan.ChangePlan{{Subject: change.Subject, Kind: change.Value.Kind(), Strategy: "change in place", Steps: []plangraph.StepID{step}}},
			Contributions: []plangraph.Contribution[featureplan.Operation]{{Owner: step.Owner, Steps: []plangraph.Step[featureplan.Operation]{{
				ID:          step,
				Payload:     featureplan.Operation{Role: ast.AlterExtension, Parent: parent, Payload: &planningOperation{Values: []int{3}}},
				Effects:     []plangraph.Effect{{Subject: change.Subject, Action: plangraph.Alter}},
				Transaction: plangraph.TransactionForbidden,
			}}}},
		}, nil
	}
}

// An owner changes a view's setting with an ALTER that names the view, which
// is the only object such an operation can sit under: the view has no table
// capture of its own.
func TestPlanningAcceptsAnAlterUnderAChangedView(t *testing.T) {
	c := qt.New(t)
	request, view := viewPlanningRequest()

	result, err := mustRuntime(c, planningProvider(viewPlanned(view))).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Contributions, qt.HasLen, 1)
	c.Assert(result.Contributions[0].Steps[0].Payload.Parent, qt.Equals, view)
}

// The view must be one the request changes. Naming another view, or a view
// spelled as a table, would let a reply attach work to an object the host
// never handed the owner.
func TestPlanningRefusesAnAlterUnderAnUnchangedView(t *testing.T) {
	builder := func(request featureplan.Request) objectidentity.Builder {
		return objectidentity.NewBuilder(request.Identifiers)
	}
	for _, test := range []struct {
		name   string
		parent func(featureplan.Request) objectidentity.ID
	}{
		{"another view", func(r featureplan.Request) objectidentity.ID {
			return builder(r).SchemaScopedParts(objectidentity.KindMatView, "", "other")
		}},
		{"the view named as a table", func(r featureplan.Request) objectidentity.ID {
			return builder(r).TableParts("", "totals")
		}},
		{"a plain view", func(r featureplan.Request) objectidentity.ID {
			return builder(r).SchemaScopedParts(objectidentity.KindView, "", "totals")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := viewPlanningRequest()

			result, err := mustRuntime(c, planningProvider(viewPlanned(test.parent(request)))).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, `.*operation has no captured parent.*`)
			c.Assert(result, qt.DeepEquals, featureplan.Result{})
		})
	}
}
