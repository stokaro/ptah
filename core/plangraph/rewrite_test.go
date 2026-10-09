package plangraph_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
)

func rewriteFixture() (common, feature plangraph.Contribution[string], rewrites []plangraph.Rewrite) {
	common = plangraph.Contribution[string]{Owner: owner,
		Steps:        []plangraph.Step[string]{operation("before", plangraph.Read), operation("change", plangraph.Alter), operation("after", plangraph.Read)},
		Dependencies: []plangraph.Dependency{edge("before", "change"), edge("change", "after")},
	}
	replacement := plangraph.StepID{Owner: "example.org/feature", Name: "compound"}
	feature = plangraph.Contribution[string]{Owner: replacement.Owner, Steps: []plangraph.Step[string]{{
		ID: replacement, Payload: "compound operation", Effects: slices.Clone(common.Steps[1].Effects),
		Transaction: plangraph.TransactionForbidden, Impact: schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes retained state"},
	}}}
	return common, feature, []plangraph.Rewrite{{Sources: []plangraph.StepID{id("change")}, Replacement: replacement}}
}

func TestRewriteTransfersEmissionAndPreservesBoundaryDependencies(t *testing.T) {
	c := qt.New(t)
	common, feature, rewrites := rewriteFixture()
	plan, err := plangraph.ScheduleRewritten(t.Context(), common, rewrites, feature)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 3)
	c.Assert(plan.Steps[0].ID, qt.Equals, id("before"))
	c.Assert(plan.Steps[1], qt.DeepEquals, feature.Steps[0])
	c.Assert(plan.Steps[2].ID, qt.Equals, id("after"))
	c.Assert(plan.Dependencies, qt.ContentEquals, []plangraph.Dependency{
		{Before: id("before"), After: feature.Steps[0].ID}, {Before: feature.Steps[0].ID, After: id("after")},
	})
	c.Assert(common.Steps, qt.HasLen, 3)
	c.Assert(common.Dependencies, qt.DeepEquals, []plangraph.Dependency{edge("before", "change"), edge("change", "after")})
	feature.Steps[0].Effects[0].Action = plangraph.Drop
	c.Assert(plan.Steps[1].Effects[0].Action, qt.Equals, plangraph.Alter)
}

func TestRewriteConsumesOnlyInternalEdgesOfAGroup(t *testing.T) {
	c := qt.New(t)
	common, feature, rewrites := rewriteFixture()
	rewrites[0].Sources = []plangraph.StepID{id("before"), id("change")}
	plan, err := plangraph.ScheduleRewritten(t.Context(), common, rewrites, feature)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Steps, qt.HasLen, 2)
	c.Assert(plan.Dependencies, qt.DeepEquals, []plangraph.Dependency{{Before: feature.Steps[0].ID, After: id("after")}})
}

func TestRewriteRejectsAmbiguousOrIncompleteEmissionClaims(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*plangraph.Contribution[string], *plangraph.Contribution[string], *[]plangraph.Rewrite)
	}{
		{"empty group", func(_, _ *plangraph.Contribution[string], r *[]plangraph.Rewrite) { (*r)[0].Sources = nil }},
		{"missing source", func(_, _ *plangraph.Contribution[string], r *[]plangraph.Rewrite) { (*r)[0].Sources[0] = id("missing") }},
		{"foreign source", func(_, f *plangraph.Contribution[string], r *[]plangraph.Rewrite) { (*r)[0].Sources[0] = f.Steps[0].ID }},
		{"repeated source", func(_, _ *plangraph.Contribution[string], r *[]plangraph.Rewrite) {
			(*r)[0].Sources = append((*r)[0].Sources, id("change"))
		}},
		{"repeated replacement", func(_, _ *plangraph.Contribution[string], r *[]plangraph.Rewrite) { *r = append(*r, (*r)[0]) }},
		{"missing replacement", func(_, _ *plangraph.Contribution[string], r *[]plangraph.Rewrite) {
			(*r)[0].Replacement.Name = "missing"
		}},
		{"host replacement", func(_, _ *plangraph.Contribution[string], r *[]plangraph.Rewrite) { (*r)[0].Replacement = id("after") }},
		{"unknown source footprint", func(h, _ *plangraph.Contribution[string], _ *[]plangraph.Rewrite) { h.Steps[1].Effects = nil }},
		{"invalid source footprint", func(h, _ *plangraph.Contribution[string], _ *[]plangraph.Rewrite) {
			h.Steps[1].Effects[0].Action = "invalid"
		}},
		{"duplicate source footprint", func(h, _ *plangraph.Contribution[string], _ *[]plangraph.Rewrite) {
			h.Steps[1].Effects = append(h.Steps[1].Effects, h.Steps[1].Effects[0])
		}},
		{"lost footprint", func(_, f *plangraph.Contribution[string], _ *[]plangraph.Rewrite) { f.Steps[0].Effects = nil }},
		{"write hidden as read", func(_, f *plangraph.Contribution[string], _ *[]plangraph.Rewrite) {
			f.Steps[0].Effects[0].Action = plangraph.Read
		}},
		{"changed logical write", func(_, f *plangraph.Contribution[string], _ *[]plangraph.Rewrite) {
			f.Steps[0].Effects[0].Action = plangraph.Drop
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			common, feature, rewrites := rewriteFixture()
			test.edit(&common, &feature, &rewrites)
			plan, err := plangraph.ScheduleRewritten(t.Context(), common, rewrites, feature)
			c.Assert(err, qt.ErrorIs, plangraph.ErrInvalid)
			c.Assert(plan, qt.DeepEquals, plangraph.Plan[string]{})
		})
	}
}

func TestRewriteCannotHideACycleAcrossTheGroupBoundary(t *testing.T) {
	c := qt.New(t)
	common, feature, rewrites := rewriteFixture()
	rewrites[0].Sources = []plangraph.StepID{id("before"), id("after")}
	plan, err := plangraph.ScheduleRewritten(t.Context(), common, rewrites, feature)
	c.Assert(err, qt.ErrorIs, plangraph.ErrCycle)
	c.Assert(plan, qt.DeepEquals, plangraph.Plan[string]{})
}

func TestRewriteRetainsOtherEmittersAsConflicts(t *testing.T) {
	c := qt.New(t)
	common, feature, rewrites := rewriteFixture()
	common.Steps[2].Effects[0].Action = plangraph.Alter
	plan, err := plangraph.ScheduleRewritten(t.Context(), common, rewrites, feature)
	c.Assert(err, qt.ErrorIs, plangraph.ErrConflict)
	c.Assert(plan, qt.DeepEquals, plangraph.Plan[string]{})
}

func TestRewriteCancellationDiscardsTheWholePlan(t *testing.T) {
	c := qt.New(t)
	common, feature, rewrites := rewriteFixture()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	plan, err := plangraph.ScheduleRewritten(ctx, common, rewrites, feature)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(plan, qt.DeepEquals, plangraph.Plan[string]{})
}

func TestRewriteCannotConsumeAnInvalidSelfDependency(t *testing.T) {
	c := qt.New(t)
	common, feature, rewrites := rewriteFixture()
	common.Dependencies = append(common.Dependencies, edge("change", "change"))
	plan, err := plangraph.ScheduleRewritten(t.Context(), common, rewrites, feature)
	c.Assert(err, qt.ErrorIs, plangraph.ErrCycle)
	c.Assert(plan, qt.DeepEquals, plangraph.Plan[string]{})
}

func TestRewriteDoesNotClaimStepsOutsideTheSuppliedHost(t *testing.T) {
	c := qt.New(t)
	common, feature, rewrites := rewriteFixture()
	foreign := plangraph.Contribution[string]{Owner: owner, Steps: []plangraph.Step[string]{operation("outside", plangraph.Alter)}}
	rewrites[0].Sources[0] = id("outside")
	plan, err := plangraph.ScheduleRewritten(t.Context(), common, rewrites, feature, foreign)
	c.Assert(err, qt.ErrorIs, plangraph.ErrInvalid)
	c.Assert(plan, qt.DeepEquals, plangraph.Plan[string]{})
}
