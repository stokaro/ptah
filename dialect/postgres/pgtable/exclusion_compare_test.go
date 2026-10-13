package pgtable_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

func exclusionSubject() objectidentity.ID {
	return pgtable.ExclusionRef("app.rooms", "no_overlap", identifier.ForDialect("postgres"))
}

func exclusionObjects(value schemaext.Value) schemaext.Objects {
	if value == nil {
		return schemaext.Objects{}
	}
	return must.Must(schemaext.NewObjects(schemaext.Object{Ref: exclusionSubject(), Value: value}))
}

func exclusionComparison(desired, current schemaext.Value) schemaext.ObjectComparisonRequest {
	return schemaext.ObjectComparisonRequest{Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Kinds: []schemaext.Kind{pgtable.ExclusionKind},
		Parents: []schemaext.ParentState{{Subject: pgtable.ExclusionTable(exclusionSubject()), Desired: true, Current: true}},
		Desired: schemaext.ObjectState{Objects: exclusionObjects(desired), Coverage: must.Must(pgtable.ExclusionCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))},
		Current: schemaext.ObjectState{Objects: exclusionObjects(current), Coverage: must.Must(pgtable.ExclusionCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))},
	}
}

func TestExclusionComparison_Changes(t *testing.T) {
	for _, test := range []struct {
		name        string
		before      *pgtable.ObservedExclusion
		after       *pgtable.DesiredExclusion
		commentOnly bool
	}{
		{name: "create", after: &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}},
		{name: "drop", before: new(pgtable.ObservedExclusion(exclusionDefinition()))},
		{name: "replace", before: new(pgtable.ObservedExclusion(exclusionDefinition())), after: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "during WITH &&"}}},
		{name: "comment", before: new(pgtable.ObservedExclusion(exclusionDefinition())), after: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =, during WITH &&", Comment: "No overlapping rooms"}}, commentOnly: true},
		{name: "create with predicate", after: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =, during WITH &&", WhereCondition: "is_active = true"}}},
		{name: "drop with predicate", before: &pgtable.ObservedExclusion{UsingMethod: "gist", Elements: "room WITH =, during WITH &&", WhereCondition: "is_active = true"}},
		{name: "method changes", before: new(pgtable.ObservedExclusion(exclusionDefinition())), after: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "btree", Elements: "room WITH =, during WITH &&"}}},
		{name: "predicate changes", before: &pgtable.ObservedExclusion{UsingMethod: "gist", Elements: "room WITH =, during WITH &&", WhereCondition: "is_active = true"},
			after: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =, during WITH &&", WhereCondition: "status = 'confirmed'"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := exclusionComparison(nil, nil)
			request.Desired.Objects = desiredExclusionObjects(test.after)
			request.Current.Objects = observedExclusionObjects(test.before)
			result, err := (pgtable.ExclusionService{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Subject, qt.DeepEquals, exclusionSubject())
			change := result.Changes[0].Value.(*pgtable.ExclusionChange)
			c.Assert(change, qt.DeepEquals, &pgtable.ExclusionChange{Before: test.before, After: test.after, CommentOnly: test.commentOnly})
			c.Assert(pgtable.ValidateExclusionChange(change), qt.IsNil)
		})
	}
}

func desiredExclusionObjects(value *pgtable.DesiredExclusion) schemaext.Objects {
	if value == nil {
		return schemaext.Objects{}
	}
	return exclusionObjects(value)
}

func TestExclusionComparison_EqualDefinitions(t *testing.T) {
	c := qt.New(t)
	desired := &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =, during WITH &&", WhereCondition: "is_active = true"}}
	current := pgtable.ObservedExclusion(desired.Exclusion)
	result, err := (pgtable.ExclusionService{}).CompareObjects(t.Context(), exclusionComparison(desired, &current))
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
}

func observedExclusionObjects(value *pgtable.ObservedExclusion) schemaext.Objects {
	if value == nil {
		return schemaext.Objects{}
	}
	return exclusionObjects(value)
}

func TestExclusionComparison_NormalizedDefinitions(t *testing.T) {
	c := qt.New(t)
	desired := &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}
	desired.WhereCondition = "state = 'open'"
	observed := pgtable.ObservedExclusion(desired.Exclusion)
	observed.WhereCondition = "state = 'open'::text"
	desired.Normalized = &pgtable.NormalizedExclusion{SourceKey: pgtable.ExclusionComparisonKey(desired.Exclusion), Elements: observed.Elements, WhereCondition: observed.WhereCondition}
	request := exclusionComparison(desired, &observed)
	result, err := (pgtable.ExclusionService{}).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(desired.WhereCondition, qt.Equals, "state = 'open'")
}

func TestExclusionComparison_UnknownObservation(t *testing.T) {
	for _, state := range []schemaext.KnowledgeState{schemaext.Uninspected, schemaext.Unrepresentable} {
		t.Run(string(state), func(t *testing.T) {
			c := qt.New(t)
			request := exclusionComparison(&pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}, nil)
			request.Current.Coverage = must.Must(pgtable.ExclusionCoverage(schemaext.Observed, schemaext.Knowledge{State: state, Reason: "reader limit"}, nil))
			result, err := (pgtable.ExclusionService{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 2)
			c.Assert(result.Undecided[0].Subject, qt.DeepEquals, exclusionSubject())
			c.Assert(result.Undecided[1].Subject, qt.DeepEquals, pgtable.ExclusionTable(exclusionSubject()))
		})
	}
}

func TestExclusionComparison_AdoptsUnspecifiedObservation(t *testing.T) {
	c := qt.New(t)
	observed := pgtable.ObservedExclusion(exclusionDefinition())
	observed.Dependencies = []objectidentity.ID{exclusionDependency("btree_gist")}
	request := exclusionComparison(nil, &observed)
	request.Desired.Coverage = schemaext.Coverage{}
	result, err := (pgtable.ExclusionService{}).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 0)
	adopted, err := result.Desired.Objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(adopted, qt.HasLen, 1)
	c.Assert(adopted[0].Value, qt.DeepEquals, &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion(observed)})
	c.Assert(result.Desired.Coverage.Lookup(pgtable.ExclusionKind, exclusionSubject()).State, qt.Equals, schemaext.Complete)
	c.Assert(request.Desired.Objects.Refs(), qt.HasLen, 0)
}

func TestExclusionComparison_ParentTransitions(t *testing.T) {
	for _, test := range []struct {
		name             string
		desired, current bool
	}{
		{"created", true, false}, {"dropped", false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := exclusionComparison(nil, nil)
			request.Parents[0].Desired, request.Parents[0].Current = test.desired, test.current
			values := map[bool]schemaext.Value{true: &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}, false: nil}
			request.Desired.Objects = exclusionObjects(values[test.desired])
			values = map[bool]schemaext.Value{true: new(pgtable.ObservedExclusion(exclusionDefinition())), false: nil}
			request.Current.Objects = exclusionObjects(values[test.current])
			result, err := (pgtable.ExclusionService{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

func TestExclusionComparison_RequiredConstraint(t *testing.T) {
	c := qt.New(t)
	request := exclusionComparison(nil, new(pgtable.ObservedExclusion(exclusionDefinition())))
	request.Required = []objectidentity.ID{exclusionSubject()}
	result, err := (pgtable.ExclusionService{}).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Reason, qt.Matches, ".*requires this exclusion constraint.*")
}

func TestExclusionComparison_InvalidRequest(t *testing.T) {
	for _, test := range []struct {
		name             string
		desired, current schemaext.Value
	}{
		{"observation on desired side", new(pgtable.ObservedExclusion(exclusionDefinition())), nil},
		{"declaration on observed side", nil, &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}},
		{"missing index method", &pgtable.DesiredExclusion{}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := exclusionComparison(test.desired, test.current)
			result, err := (pgtable.ExclusionService{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
		})
	}
}

func TestExclusionComparison_Canceled(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (pgtable.ExclusionService{}).CompareObjects(ctx, exclusionComparison(nil, nil))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
}
