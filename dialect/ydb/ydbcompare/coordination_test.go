package ydbcompare_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/engine"
)

func coordinationRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}},
		Codecs: append(ydbcoordination.Codecs(), ydbdiff.Codecs()...),
		Comparisons: []engine.ObjectComparison{{Target: "ydb", Kinds: []schemaext.Kind{ydbcoordination.Kind},
			ChangeKinds: []schemaext.Kind{ydbdiff.CoordinationNodeKind}, Service: ydbcompare.CoordinationService{}}},
	})
	c.Assert(err, qt.IsNil)
	return runtime
}

func coordinationState(c *qt.C, direction schemaext.Representation, knowledge schemaext.KnowledgeState, spec *ydbcoordination.Spec, limits ...schemaext.SubjectCoverage) schemaext.ObjectState {
	c.Helper()
	coverage, err := ydbcoordination.Coverage(direction, schemaext.Knowledge{State: knowledge, Reason: "namespace evidence"}, limits)
	c.Assert(err, qt.IsNil)
	state := schemaext.ObjectState{Coverage: coverage}
	if spec != nil {
		var value schemaext.Value = &ydbcoordination.Desired{Spec: *spec}
		if direction == schemaext.Observed {
			value = &ydbcoordination.Observed{Spec: *spec}
		}
		state.Objects, err = schemaext.NewObjects(schemaext.Object{Ref: ydbcoordination.Ref("app", "locks"), Value: value})
		c.Assert(err, qt.IsNil)
	}
	return state
}

func TestCoordinationComparisonNeedsNoTableAndRespectsEvidence(t *testing.T) {
	empty := &ydbcoordination.Spec{}
	strict := &ydbcoordination.Spec{ReadConsistencyMode: "strict"}
	defaults := new(ydbcoordination.Defaults())
	limit := func(state schemaext.KnowledgeState) []schemaext.SubjectCoverage {
		return []schemaext.SubjectCoverage{{Kind: ydbcoordination.Kind, Subject: ydbcoordination.Ref("app", "locks"),
			Knowledge: schemaext.Knowledge{State: state, Reason: "unreadable setting"}}}
	}
	cases := []struct {
		name                               string
		desired, current                   *ydbcoordination.Spec
		desiredKnowledge, currentKnowledge schemaext.KnowledgeState
		desiredLimits, currentLimits       []schemaext.SubjectCoverage
		changes, diagnostics, objects      int
	}{
		{name: "create", desired: empty, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, changes: 1, objects: 1},
		{name: "remove", current: strict, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, changes: 1},
		{name: "reset to default", desired: empty, current: strict, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, changes: 1, objects: 1},
		{name: "explicit defaults are equivalent", desired: defaults, current: empty, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, objects: 1},
		{name: "unknown source adopts raw state", current: strict, desiredKnowledge: schemaext.Uninspected, currentKnowledge: schemaext.Complete, objects: 1},
		{name: "unknown observation withholds creation", desired: empty, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Uninspected, objects: 1, diagnostics: 2},
		{name: "unknown empty observation is a namespace limit", desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Uninspected, diagnostics: 1},
		{name: "positive observation survives partial enumeration", desired: empty, current: empty, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Uninspected, objects: 1, diagnostics: 1},
		{name: "unknown current object withholds removal", current: strict, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, currentLimits: limit(schemaext.Unrepresentable), diagnostics: 1},
		{name: "unknown current object without value", desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, currentLimits: limit(schemaext.Unrepresentable), diagnostics: 1},
		{name: "incomplete explicit declaration withholds alteration", desired: empty, current: strict, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, desiredLimits: limit(schemaext.Unrepresentable), diagnostics: 1, objects: 1},
		{name: "explicit absence removes despite incomplete source", current: strict, desiredKnowledge: schemaext.Uninspected, currentKnowledge: schemaext.Complete, desiredLimits: limit(schemaext.Absent), changes: 1},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := coordinationRuntime(c)
			request := schemaext.ObjectComparisonRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
				Capabilities: capability.Capabilities{capability.CoordinationNodes: true}, Kinds: []schemaext.Kind{ydbcoordination.Kind},
				Desired: coordinationState(c, schemaext.Desired, test.desiredKnowledge, test.desired, test.desiredLimits...),
				Current: coordinationState(c, schemaext.Observed, test.currentKnowledge, test.current, test.currentLimits...),
			}
			result, err := runtime.CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.diagnostics)
			c.Assert(result.Desired.Objects.Refs(), qt.HasLen, test.objects)
			for _, record := range result.Changes {
				c.Assert(record.Value, qt.DeepEquals, coordinationChange(test.current, test.desired))
				c.Assert(record.Subject.Parent.Empty(), qt.IsTrue)
			}
		})
	}
}

func TestCoordinationComparisonRefusesInvalidInputsWithoutPartialOutput(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*schemaext.ObjectComparisonRequest)
		want   error
	}{
		{name: "wrong target", mutate: func(r *schemaext.ObjectComparisonRequest) { r.Target = "postgres" }, want: ptaherr.ErrUnsupportedDialect},
		{name: "missing capability", mutate: func(r *schemaext.ObjectComparisonRequest) { r.Capabilities = capability.Capabilities{} }, want: ptaherr.ErrUnsupportedFeature},
		{name: "wrong identifiers", mutate: func(r *schemaext.ObjectComparisonRequest) { r.Identifiers = identifier.ForDialect("postgres") }, want: schemaext.ErrInvalidValue},
		{name: "table parent", mutate: func(r *schemaext.ObjectComparisonRequest) {
			ref := ydbcoordination.Ref("app", "locks")
			ref.Parent = objectidentity.Part{Source: "table", Normalized: "table"}
			r.Desired.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &ydbcoordination.Desired{}}))
		}, want: schemaext.ErrInvalidValue},
		{name: "reserved lock", mutate: func(r *schemaext.ObjectComparisonRequest) {
			r.Desired.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbcoordination.Ref("", ydbcoordination.LockNode), Value: &ydbcoordination.Desired{}}))
		}, want: schemaext.ErrInvalidValue},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemaext.ObjectComparisonRequest{Target: "ydb", Kinds: []schemaext.Kind{ydbcoordination.Kind}, Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Capabilities{capability.CoordinationNodes: true},
				Desired: coordinationState(c, schemaext.Desired, schemaext.Complete, &ydbcoordination.Spec{}),
				Current: coordinationState(c, schemaext.Observed, schemaext.Complete, nil)}
			test.mutate(&request)
			result, err := (ydbcompare.CoordinationService{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
		})
	}
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (ydbcompare.CoordinationService{}).CompareObjects(ctx, schemaext.ObjectComparisonRequest{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
}

// coordinationChange constructs expected complete operands without applying defaults.
func coordinationChange(before, after *ydbcoordination.Spec) *ydbdiff.CoordinationNode {
	change := &ydbdiff.CoordinationNode{}
	if before != nil {
		change.Before = &ydbcoordination.Observed{Spec: *before}
	}
	if after != nil {
		change.After = &ydbcoordination.Desired{Spec: *after}
	}
	return change
}

func TestCoordinationComparisonAdoptsRawStateFromAnUnknownSource(t *testing.T) {
	c := qt.New(t)
	observed := &ydbcoordination.Spec{ReadConsistencyMode: "strict"}
	request := schemaext.ObjectComparisonRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
		Capabilities: capability.YDB262(), Kinds: []schemaext.Kind{ydbcoordination.Kind},
		Desired: coordinationState(c, schemaext.Desired, schemaext.Uninspected, nil),
		Current: coordinationState(c, schemaext.Observed, schemaext.Complete, observed),
	}
	result, err := coordinationRuntime(c).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	values, err := result.Desired.Objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 1)
	c.Assert(values[0].Value, qt.DeepEquals, &ydbcoordination.Desired{Spec: *observed})
	c.Assert(result.Desired.Coverage.Lookup(ydbcoordination.Kind, values[0].Ref).State, qt.Equals, schemaext.Complete)
}

func TestCoordinationComparisonKeepsServerOwnedLimits(t *testing.T) {
	c := qt.New(t)
	hidden := ydbcoordination.Ref("", ".hidden")
	request := schemaext.ObjectComparisonRequest{Target: "ydb", Kinds: []schemaext.Kind{ydbcoordination.Kind},
		Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		Desired: coordinationState(c, schemaext.Desired, schemaext.Complete, nil),
		Current: coordinationState(c, schemaext.Observed, schemaext.Complete, nil, schemaext.SubjectCoverage{
			Kind: ydbcoordination.Kind, Subject: hidden, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "server-owned node"},
		}),
	}
	result, err := coordinationRuntime(c).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Subject, qt.DeepEquals, hidden)
	c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
}

func TestCoordinationComparisonKeepsCaseAndDirectoryIdentity(t *testing.T) {
	tests := []struct{ name, desiredSchema, desiredName, currentSchema, currentName string }{
		{name: "case", desiredName: "Locks", currentName: "locks"},
		{name: "directory", desiredSchema: "a", desiredName: "locks", currentSchema: "b", currentName: "locks"},
		{name: "literal dot", desiredName: "app.locks", currentSchema: "app", currentName: "locks"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := coordinationState(c, schemaext.Desired, schemaext.Complete, nil)
			current := coordinationState(c, schemaext.Observed, schemaext.Complete, nil)
			desired.Objects = must.Must(schemaext.NewObjects(ydbcoordination.DesiredObject(test.desiredSchema, test.desiredName, "", ydbcoordination.Spec{})))
			observed := ydbcoordination.Spec{SelfCheckPeriodMillis: 2500, ReadConsistencyMode: "strict"}
			current.Objects = must.Must(schemaext.NewObjects(ydbcoordination.ObservedObject(test.currentSchema, test.currentName, observed)))
			result, err := coordinationRuntime(c).CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "ydb", Kinds: []schemaext.Kind{ydbcoordination.Kind}, Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(), Desired: desired, Current: current})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.ContentEquals, []schemaext.ChangeRecord{
				{Subject: ydbcoordination.Ref(test.desiredSchema, test.desiredName), Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}},
				{Subject: ydbcoordination.Ref(test.currentSchema, test.currentName), Value: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{Spec: observed}}},
			})
		})
	}
}
