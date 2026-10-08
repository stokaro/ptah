package featurehost_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/planner/featurehost"
)

type selectedRuntime struct {
	*engine.Runtime
	plan func(context.Context, featureplan.Request) (featureplan.Result, error)
}

func (r selectedRuntime) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	return r.plan(ctx, request)
}

type operation struct{ Values []string }

func (*operation) Kind() schemaext.Kind { return "example.org/host-operation" }
func (v *operation) CloneExtension() ast.ExtensionPayload {
	return &operation{Values: slices.Clone(v.Values)}
}

func fixture() (featureplan.Request, featureplan.Result, map[objectidentity.Key]string) {
	semantics := identifier.ForDialect("postgres")
	parent := objectidentity.NewBuilder(semantics).Table(`public."Orders.Log"`)
	step := plangraph.StepID{Owner: "example.org/host-operation", Name: "change"}
	request := featureplan.Request{Target: "postgres", Identifiers: semantics, Tables: []featureplan.Table{{Subject: parent}}}
	result := featureplan.Result{Complete: true, Contributions: []plangraph.Contribution[featureplan.Operation]{{
		Owner: step.Owner, Dependencies: []plangraph.Dependency{{Before: plangraph.StepID{Owner: "example.org/host", Name: "before"}, After: step}},
		Steps: []plangraph.Step[featureplan.Operation]{{ID: step,
			Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: parent, Payload: &operation{Values: []string{"captured"}}, Notes: []string{"owner note"}},
			Effects: []plangraph.Effect{{Subject: parent, Action: plangraph.Alter}}, Transaction: plangraph.TransactionForbidden,
			Impact: schemaext.Effect{Reason: "owner has not assessed recovery"},
		}},
	}}}
	return request, result, map[objectidentity.Key]string{parent.Key(): `public."Orders.Log"`}
}

func TestHostRetainsSourceNamesAndCompleteGraphMetadata(t *testing.T) {
	c := qt.New(t)
	request, reply, names := fixture()
	selected := selectedRuntime{Runtime: must.Must(engine.New()), plan: func(ctx context.Context, got featureplan.Request) (featureplan.Result, error) {
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(got, qt.DeepEquals, request)
		names[request.Tables[0].Subject.Key()] = "mutated"
		return reply, nil
	}}
	result, err := featurehost.Plan(t.Context(), selected, request, names)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.HasLen, 1)
	c.Assert(result[0].Owner, qt.Equals, reply.Contributions[0].Owner)
	step := result[0].Steps[0]
	original := reply.Contributions[0].Steps[0]
	c.Assert(step.ID, qt.Equals, original.ID)
	c.Assert(step.Effects, qt.DeepEquals, original.Effects)
	c.Assert(step.Transaction, qt.Equals, original.Transaction)
	c.Assert(step.Impact, qt.Equals, original.Impact)
	c.Assert(result[0].Dependencies, qt.DeepEquals, reply.Contributions[0].Dependencies)
	c.Assert(step.Payload, qt.HasLen, 2)
	c.Assert(step.Payload[0], qt.DeepEquals, ast.NewComment("owner note"))
	alter := step.Payload[1].(*ast.AlterTableNode)
	c.Assert(alter.Name, qt.Equals, `public."Orders.Log"`)
	payload := alter.Operations[0].(*ast.ExtensionAlterOperation).Payload.(*operation)
	original.Payload.Payload.(*operation).Values[0] = "mutated"
	reply.Contributions[0].Steps[0].Effects[0].Action = plangraph.Drop
	reply.Contributions[0].Dependencies[0].Before.Name = "mutated"
	c.Assert(payload.Values, qt.DeepEquals, []string{"captured"})
	c.Assert(step.Effects[0].Action, qt.Equals, plangraph.Alter)
	c.Assert(result[0].Dependencies[0].Before.Name, qt.Equals, "before")
	common := plangraph.Contribution[[]ast.Node]{Owner: "example.org/host", Steps: []plangraph.Step[[]ast.Node]{{ID: plangraph.StepID{Owner: "example.org/host", Name: "before"}, Payload: []ast.Node{ast.NewComment("common")}}}}
	plan, err := plangraph.Schedule(t.Context(), append(result, common)...)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps[0].ID.Owner, qt.Equals, "example.org/host")
	c.Assert(plan.Steps[1].Transaction, qt.Equals, plangraph.TransactionForbidden)
	_, err = plangraph.Schedule(t.Context(), result...)
	c.Assert(err, qt.ErrorIs, plangraph.ErrInvalid)
}

func TestHostLowersStandaloneOperationsWithoutTableBindings(t *testing.T) {
	c := qt.New(t)
	request, reply, _ := fixture()
	reply.Contributions[0].Steps[0].Payload.Role = ast.StatementExtension
	reply.Contributions[0].Steps[0].Payload.Parent = objectidentity.ID{}
	selected := selectedRuntime{Runtime: must.Must(engine.New()), plan: func(context.Context, featureplan.Request) (featureplan.Result, error) { return reply, nil }}
	result, err := featurehost.Plan(t.Context(), selected, request, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(result[0].Steps[0].Payload[1], qt.DeepEquals, &ast.ExtensionStatement{Payload: &operation{Values: []string{"captured"}}})
}

func TestHostRejectsInvalidEmissionBindingsBeforeDispatch(t *testing.T) {
	for _, name := range []string{"", "another_table"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			request, reply, names := fixture()
			names[request.Tables[0].Subject.Key()] = name
			called := false
			selected := selectedRuntime{Runtime: must.Must(engine.New()), plan: func(context.Context, featureplan.Request) (featureplan.Result, error) {
				called = true
				return reply, nil
			}}
			result, err := featurehost.Plan(t.Context(), selected, request, names)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.IsNil)
			c.Assert(called, qt.IsFalse)
		})
	}
}

func TestHostRejectsIncompleteAndUnlowerableReplies(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*featureplan.Result)
	}{
		{name: "incomplete", edit: func(r *featureplan.Result) { r.Complete = false }},
		{name: "unknown role", edit: func(r *featureplan.Result) { r.Contributions[0].Steps[0].Payload.Role = "unknown" }},
		{name: "unbound parent", edit: func(r *featureplan.Result) { r.Contributions[0].Steps[0].Payload.Parent.Name.Normalized = "other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, reply, names := fixture()
			test.edit(&reply)
			selected := selectedRuntime{Runtime: must.Must(engine.New()), plan: func(context.Context, featureplan.Request) (featureplan.Result, error) { return reply, nil }}
			result, err := featurehost.Plan(t.Context(), selected, request, names)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestHostFailureAndCancellationDiscardOperations(t *testing.T) {
	c := qt.New(t)
	request, reply, names := fixture()
	failure := errors.New("selected provider failed")
	selected := selectedRuntime{Runtime: must.Must(engine.New()), plan: func(context.Context, featureplan.Request) (featureplan.Result, error) { return reply, failure }}
	result, err := featurehost.Plan(t.Context(), selected, request, names)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(result, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	selected.plan = func(context.Context, featureplan.Request) (featureplan.Result, error) { cancel(); return reply, nil }
	result, err = featurehost.Plan(ctx, selected, request, names)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
}
