package plangraph_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/plangraph"
)

// scheme owns every step these tests contribute. Schedule refuses two owners
// writing one subject, so contributions that hand a subject over share an
// owner, as one provider's services do; the dependencies are derived per
// contribution, not per owner.
const scheme = "example.org/scheme"

// stepOf is a step with one effect on public.items.
func stepOf(name string, action plangraph.Action, placement plangraph.Placement) plangraph.Step[string] {
	return plangraph.Step[string]{ID: plangraph.StepID{Owner: scheme, Name: name}, Payload: name, Placement: placement,
		Effects: []plangraph.Effect{{Subject: table("public", "items"), Action: action}}}
}

// single is a contribution of one step and the given dependencies.
func single(step plangraph.Step[string], edges ...plangraph.Dependency) plangraph.Contribution[string] {
	return plangraph.Contribution[string]{Owner: step.ID.Owner, Steps: []plangraph.Step[string]{step}, Dependencies: edges}
}

func ordered(before, after string) plangraph.Dependency {
	return plangraph.Dependency{Before: plangraph.StepID{Owner: scheme, Name: before}, After: plangraph.StepID{Owner: scheme, Name: after}}
}

// TestLifecycleDependencies_HappyPath orders two contributions' effects on one
// subject the way its lifecycle allows, whichever contribution comes first,
// and the schedule then runs them in that order. The names sort against the
// expected order, so the order is the dependency's and not the tie-break's.
func TestLifecycleDependencies_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		a, b      plangraph.Step[string]
		want      []plangraph.Dependency
		wantOrder []string
	}{
		{name: "a drop hands the subject to a creation",
			a: stepOf("z-drop", plangraph.Drop, ""), b: stepOf("a-create", plangraph.Create, ""),
			want: []plangraph.Dependency{ordered("z-drop", "a-create")}, wantOrder: []string{"z-drop", "a-create"}},
		{name: "a creation before a read",
			a: stepOf("z-create", plangraph.Create, ""), b: stepOf("a-read", plangraph.Read, ""),
			want: []plangraph.Dependency{ordered("z-create", "a-read")}, wantOrder: []string{"z-create", "a-read"}},
		{name: "a read before a drop",
			a: stepOf("a-drop", plangraph.Drop, ""), b: stepOf("z-read", plangraph.Read, ""),
			want: []plangraph.Dependency{ordered("z-read", "a-drop")}, wantOrder: []string{"z-read", "a-drop"}},
		{name: "an alteration before a reader of what it makes",
			a: stepOf("z-alter", plangraph.Alter, ""), b: stepOf("a-read", plangraph.Read, ""),
			want: []plangraph.Dependency{ordered("z-alter", "a-read")}, wantOrder: []string{"z-alter", "a-read"}},
		{name: "an early reader of what an alteration replaces",
			a: stepOf("a-alter", plangraph.Alter, plangraph.PlacementEarly), b: stepOf("z-read", plangraph.Read, plangraph.PlacementEarly),
			want: []plangraph.Dependency{ordered("z-read", "a-alter")}, wantOrder: []string{"z-read", "a-alter"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			forward, err := plangraph.LifecycleDependencies(t.Context(), single(test.a), single(test.b))
			c.Assert(err, qt.IsNil)
			c.Assert(forward, qt.DeepEquals, test.want)
			backward, err := plangraph.LifecycleDependencies(t.Context(), single(test.b), single(test.a))
			c.Assert(err, qt.IsNil)
			c.Assert(backward, qt.DeepEquals, test.want)
			plan, err := plangraph.Schedule(t.Context(), single(test.a, forward...), single(test.b))
			c.Assert(err, qt.IsNil)
			var order []string
			for _, step := range plan.Steps {
				order = append(order, step.Payload)
			}
			c.Assert(order, qt.DeepEquals, test.wantOrder)
		})
	}
}

// TestLifecycleDependencies_OrdersEachPairOnce orders a drop, a creation and
// a read of one subject from three contributions without a cycle. Each edge it
// derives orders the pairs after it: once the read precedes the drop, which
// precedes the creation, the read and the creation are ordered already, so
// the read is not also placed after the creation.
func TestLifecycleDependencies_OrdersEachPairOnce(t *testing.T) {
	c := qt.New(t)
	drop, create, read := stepOf("a-drop", plangraph.Drop, ""), stepOf("b-create", plangraph.Create, ""), stepOf("c-read", plangraph.Read, "")

	edges, err := plangraph.LifecycleDependencies(t.Context(), single(read), single(create), single(drop))
	c.Assert(err, qt.IsNil)
	plan, err := plangraph.Schedule(t.Context(), single(drop, edges...), single(create), single(read))

	c.Assert(err, qt.IsNil)
	c.Assert(edges, qt.DeepEquals, []plangraph.Dependency{ordered("a-drop", "b-create"), ordered("c-read", "a-drop")})
	c.Assert(plan.Steps, qt.HasLen, 3)
}

// TestLifecycleDependencies_LeavesPairsItDoesNotDecide adds nothing for steps
// of one contribution, which orders its own, for a pair already ordered either
// way, even through a third step, for reads alone, and for two writes the
// lifecycle gives no order, which Schedule refuses.
func TestLifecycleDependencies_LeavesPairsItDoesNotDecide(t *testing.T) {
	middle := plangraph.Step[string]{ID: plangraph.StepID{Owner: scheme, Name: "middle"}, Payload: "middle"}
	tests := []struct {
		name          string
		contributions []plangraph.Contribution[string]
	}{
		{name: "one contribution", contributions: []plangraph.Contribution[string]{{Owner: scheme,
			Steps: []plangraph.Step[string]{stepOf("create", plangraph.Create, ""), stepOf("read", plangraph.Read, "")}}}},
		{name: "ordered against the lifecycle", contributions: []plangraph.Contribution[string]{
			single(stepOf("create", plangraph.Create, ""), ordered("read", "create")), single(stepOf("read", plangraph.Read, ""))}},
		{name: "ordered through a third step", contributions: []plangraph.Contribution[string]{
			single(stepOf("drop", plangraph.Drop, ""), ordered("drop", "middle")),
			single(stepOf("create", plangraph.Create, ""), ordered("middle", "create")), single(middle)}},
		{name: "two reads", contributions: []plangraph.Contribution[string]{
			single(stepOf("read-a", plangraph.Read, "")), single(stepOf("read-b", plangraph.Read, ""))}},
		{name: "two creations", contributions: []plangraph.Contribution[string]{
			single(stepOf("create-a", plangraph.Create, "")), single(stepOf("create-b", plangraph.Create, ""))}},
		{name: "an alteration and a drop", contributions: []plangraph.Contribution[string]{
			single(stepOf("alter", plangraph.Alter, "")), single(stepOf("drop", plangraph.Drop, ""))}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			edges, err := plangraph.LifecycleDependencies(t.Context(), test.contributions...)
			c.Assert(err, qt.IsNil)
			c.Assert(edges, qt.HasLen, 0)
		})
	}
}

// TestLifecycleDependencies_FailurePath returns no dependencies for
// contributions Schedule would refuse, and none once the caller has given up.
func TestLifecycleDependencies_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	misowned := stepOf("drop", plangraph.Drop, "")
	misowned.ID.Owner = "example.org/other"
	tests := []struct {
		name          string
		ctx           context.Context
		contributions []plangraph.Contribution[string]
		wantIs        error
	}{
		{name: "a dependency on a missing step", ctx: t.Context(),
			contributions: []plangraph.Contribution[string]{single(stepOf("drop", plangraph.Drop, ""), ordered("gone", "drop"))},
			wantIs:        plangraph.ErrInvalid},
		{name: "a misowned step", ctx: t.Context(),
			contributions: []plangraph.Contribution[string]{{Owner: scheme, Steps: []plangraph.Step[string]{misowned}}},
			wantIs:        plangraph.ErrInvalid},
		{name: "a canceled context", ctx: canceled,
			contributions: []plangraph.Contribution[string]{single(stepOf("drop", plangraph.Drop, "")), single(stepOf("create", plangraph.Create, ""))},
			wantIs:        context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			edges, err := plangraph.LifecycleDependencies(test.ctx, test.contributions...)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(edges, qt.IsNil)
		})
	}
}
