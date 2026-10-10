package plangraph_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

const owner = "example.org/common"

func id(name string) plangraph.StepID { return plangraph.StepID{Owner: owner, Name: name} }

func table(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("postgres")).TablePartsVerbatim(schema, name)
}

func operation(name string, action plangraph.Action) plangraph.Step[string] {
	return plangraph.Step[string]{ID: id(name), Payload: name, Effects: []plangraph.Effect{{Subject: table("public", "items"), Action: action}}}
}

func edge(before, after string) plangraph.Dependency {
	return plangraph.Dependency{Before: id(before), After: id(after)}
}

func TestScheduleCombinesOwnersAndRetainsIndependentMetadata(t *testing.T) {
	c := qt.New(t)
	childID := plangraph.StepID{Owner: "example.org/features", Name: "policy"}
	parent := table("public", "items")
	policy := objectidentity.NewBuilder(identifier.ForDialect("postgres")).Policy("public.items", "readers")
	common := plangraph.Contribution[string]{Owner: owner, Steps: []plangraph.Step[string]{operation("table", plangraph.Create)}}
	feature := plangraph.Contribution[string]{Owner: childID.Owner, Steps: []plangraph.Step[string]{{
		ID: childID, Payload: "policy", Transaction: plangraph.TransactionRequired,
		Impact:  schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes row access"},
		Effects: []plangraph.Effect{{Subject: parent, Action: plangraph.Read}, {Subject: policy, Action: plangraph.Create}},
	}}, Dependencies: []plangraph.Dependency{{Before: id("table"), After: childID}}}
	forward, err := plangraph.Schedule(t.Context(), common, feature)
	c.Assert(err, qt.IsNil)
	reordered, err := plangraph.Schedule(t.Context(), feature, common)
	c.Assert(err, qt.IsNil)
	c.Assert(reordered, qt.DeepEquals, forward)
	c.Assert(forward.Steps, qt.HasLen, 2)
	c.Assert(forward.Steps[0].ID, qt.Equals, id("table"))
	c.Assert(forward.Steps[1].ID, qt.Equals, childID)
	c.Assert(forward.Steps[0].Transaction, qt.Equals, plangraph.TransactionUnknown)
	c.Assert(forward.Steps[1].Transaction, qt.Equals, plangraph.TransactionRequired)
	c.Assert(forward.Steps[0].Impact, qt.Equals, schemaext.Effect{})
	c.Assert(forward.Steps[1].Impact, qt.Equals, feature.Steps[0].Impact)
	feature.Steps[0].Effects[0].Action = plangraph.Drop
	feature.Dependencies[0].Before = childID
	c.Assert(forward.Steps[1].Effects[0].Action, qt.Equals, plangraph.Read)
	c.Assert(forward.Dependencies[0].Before, qt.Equals, id("table"))
}

func TestScheduleDeterministicOrderingAndLifecycles(t *testing.T) {
	cases := []struct {
		name  string
		steps []plangraph.Step[string]
		edges []plangraph.Dependency
		want  []string
	}{
		{name: "empty"},
		{name: "independent order", steps: []plangraph.Step[string]{{ID: id("z"), Payload: "z"}, {ID: id("a"), Payload: "a"}}, want: []string{"a", "z"}},
		{name: "replacement via intermediate dependency", steps: []plangraph.Step[string]{operation("add", plangraph.Create), operation("drop", plangraph.Drop), {ID: id("middle"), Payload: "middle"}},
			edges: []plangraph.Dependency{edge("drop", "middle"), edge("middle", "add"), edge("drop", "middle")}, want: []string{"drop", "middle", "add"}},
		{name: "independent readers", steps: []plangraph.Step[string]{operation("z", plangraph.Read), operation("a", plangraph.Read)}, want: []string{"a", "z"}},
		{name: "create alter drop", steps: []plangraph.Step[string]{operation("z", plangraph.Create), operation("b", plangraph.Alter), operation("a", plangraph.Drop)},
			edges: []plangraph.Dependency{edge("z", "b"), edge("b", "a")}, want: []string{"z", "b", "a"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			contribution := plangraph.Contribution[string]{Owner: owner, Steps: test.steps, Dependencies: test.edges}
			plan, err := plangraph.Schedule(t.Context(), contribution)
			c.Assert(err, qt.IsNil)
			var got []string
			for _, step := range plan.Steps {
				got = append(got, step.Payload)
			}
			c.Assert(got, qt.DeepEquals, test.want)
			contribution.Steps = slices.Clone(test.steps)
			slices.Reverse(contribution.Steps)
			shuffled, err := plangraph.Schedule(t.Context(), contribution)
			c.Assert(err, qt.IsNil)
			c.Assert(shuffled, qt.DeepEquals, plan)
		})
	}
}

// TestSchedulePlacesEarlyStepsFirstAmongReadySteps runs an early step as soon
// as its dependencies allow and ahead of every other ready step, whatever its
// name, but never before a step it depends on.
func TestSchedulePlacesEarlyStepsFirstAmongReadySteps(t *testing.T) {
	tests := []struct {
		name  string
		steps []plangraph.Step[string]
		edges []plangraph.Dependency
		want  []string
	}{
		{name: "ready at the start", steps: []plangraph.Step[string]{{ID: id("a"), Payload: "a"}, {ID: id("z"), Payload: "z", Placement: plangraph.PlacementEarly}},
			want: []string{"z", "a"}},
		{name: "ready after a dependency", steps: []plangraph.Step[string]{{ID: id("a"), Payload: "a"}, {ID: id("m"), Payload: "m"},
			{ID: id("z"), Payload: "z", Placement: plangraph.PlacementEarly}}, edges: []plangraph.Dependency{edge("a", "z")},
			want: []string{"a", "z", "m"}},
		{name: "two early steps by name", steps: []plangraph.Step[string]{{ID: id("z"), Payload: "z", Placement: plangraph.PlacementEarly},
			{ID: id("y"), Payload: "y", Placement: plangraph.PlacementEarly}, {ID: id("a"), Payload: "a"}},
			want: []string{"y", "z", "a"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, err := plangraph.Schedule(t.Context(), plangraph.Contribution[string]{Owner: owner, Steps: test.steps, Dependencies: test.edges})
			c.Assert(err, qt.IsNil)
			var order []string
			for _, step := range plan.Steps {
				order = append(order, step.Payload)
			}
			c.Assert(order, qt.DeepEquals, test.want)
		})
	}
}

func TestScheduleKeepsStructuredObjectScopesDistinct(t *testing.T) {
	c := qt.New(t)
	first, second := operation("a", plangraph.Create), operation("b", plangraph.Create)
	first.Effects[0].Subject = table("a.b", "c")
	second.Effects[0].Subject = table("a", "b.c")
	plan, err := plangraph.Schedule(t.Context(), plangraph.Contribution[string]{Owner: owner, Steps: []plangraph.Step[string]{first, second}})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 2)
}

func TestScheduleRejectsInvalidGraphsWithoutPartialPlan(t *testing.T) {
	cases := []struct {
		name  string
		steps []plangraph.Step[string]
		edges []plangraph.Dependency
		want  error
	}{
		{name: "duplicate step", steps: []plangraph.Step[string]{operation("a", plangraph.Create), operation("a", plangraph.Drop)}, want: plangraph.ErrInvalid},
		{name: "missing step", steps: []plangraph.Step[string]{operation("a", plangraph.Create)}, edges: []plangraph.Dependency{edge("a", "b")}, want: plangraph.ErrInvalid},
		{name: "cycle with valid prefix", steps: []plangraph.Step[string]{{ID: id("prefix")}, {ID: id("a")}, {ID: id("b")}}, edges: []plangraph.Dependency{edge("a", "b"), edge("b", "a")}, want: plangraph.ErrCycle},
		{name: "self cycle", steps: []plangraph.Step[string]{{ID: id("a")}}, edges: []plangraph.Dependency{edge("a", "a")}, want: plangraph.ErrCycle},
		{name: "unordered writers", steps: []plangraph.Step[string]{operation("a", plangraph.Drop), operation("b", plangraph.Create)}, want: plangraph.ErrConflict},
		{name: "unordered reader and writer", steps: []plangraph.Step[string]{operation("a", plangraph.Read), operation("b", plangraph.Alter)}, want: plangraph.ErrConflict},
		{name: "double creation", steps: []plangraph.Step[string]{operation("a", plangraph.Create), operation("b", plangraph.Create)}, edges: []plangraph.Dependency{edge("a", "b")}, want: plangraph.ErrConflict},
		{name: "double removal", steps: []plangraph.Step[string]{operation("a", plangraph.Drop), operation("b", plangraph.Drop)}, edges: []plangraph.Dependency{edge("a", "b")}, want: plangraph.ErrConflict},
		{name: "alter removed object", steps: []plangraph.Step[string]{operation("a", plangraph.Drop), operation("b", plangraph.Alter)}, edges: []plangraph.Dependency{edge("a", "b")}, want: plangraph.ErrConflict},
		{name: "read before creation", steps: []plangraph.Step[string]{operation("a", plangraph.Read), operation("b", plangraph.Create)}, edges: []plangraph.Dependency{edge("a", "b")}, want: plangraph.ErrConflict},
		{name: "unnamed step", steps: []plangraph.Step[string]{operation("", plangraph.Read)}, want: plangraph.ErrInvalid},
		{name: "invalid transaction", steps: []plangraph.Step[string]{{ID: id("a"), Transaction: "automatic"}}, want: plangraph.ErrInvalid},
		{name: "invalid placement", steps: []plangraph.Step[string]{{ID: id("a"), Placement: "first"}}, want: plangraph.ErrInvalid},
		{name: "missing object", steps: []plangraph.Step[string]{{ID: id("a"), Effects: []plangraph.Effect{{Action: plangraph.Read}}}}, want: plangraph.ErrInvalid},
		{name: "missing column parent", steps: []plangraph.Step[string]{{ID: id("a"), Effects: []plangraph.Effect{{Subject: objectidentity.ID{Kind: objectidentity.KindColumn, Name: objectidentity.Part{Source: "id", Normalized: "id"}}, Action: plangraph.Read}}}}, want: plangraph.ErrInvalid},
		{name: "invalid action", steps: []plangraph.Step[string]{operation("a", "other")}, want: plangraph.ErrInvalid},
		{name: "duplicate object effect", steps: []plangraph.Step[string]{{ID: id("a"), Effects: append(operation("a", plangraph.Read).Effects, operation("a", plangraph.Alter).Effects...)}}, want: plangraph.ErrInvalid},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan, err := plangraph.Schedule(t.Context(), plangraph.Contribution[string]{Owner: owner, Steps: test.steps, Dependencies: test.edges})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(plan, qt.DeepEquals, plangraph.Plan[string]{})
		})
	}
}

func TestScheduleRejectsCompetingEmittersEvenWhenOrdered(t *testing.T) {
	c := qt.New(t)
	first, second := operation("a", plangraph.Alter), operation("b", plangraph.Alter)
	second.ID.Owner = "example.org/other"
	plan, err := plangraph.Schedule(t.Context(),
		plangraph.Contribution[string]{Owner: owner, Steps: []plangraph.Step[string]{first}},
		plangraph.Contribution[string]{Owner: second.ID.Owner, Steps: []plangraph.Step[string]{second}, Dependencies: []plangraph.Dependency{{Before: first.ID, After: second.ID}}},
	)
	c.Assert(err, qt.ErrorIs, plangraph.ErrConflict)
	c.Assert(plan, qt.DeepEquals, plangraph.Plan[string]{})
}

func TestScheduleRejectsMisownedSteps(t *testing.T) {
	c := qt.New(t)
	plan, err := plangraph.Schedule(t.Context(), plangraph.Contribution[string]{Owner: "example.org/other", Steps: []plangraph.Step[string]{operation("a", plangraph.Create)}})
	c.Assert(err, qt.ErrorIs, plangraph.ErrInvalid)
	c.Assert(plan, qt.DeepEquals, plangraph.Plan[string]{})
}

func TestSchedulePropagatesCancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	plan, err := plangraph.Schedule[string](ctx)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(plan, qt.DeepEquals, plangraph.Plan[string]{})
}
