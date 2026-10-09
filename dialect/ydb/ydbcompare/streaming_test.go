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
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine"
)

func streamingRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}},
		Codecs: append(ydbstreaming.Codecs(), ydbdiff.StreamingCodec()),
		Comparisons: []engine.ObjectComparison{{Target: "ydb", Kinds: []schemaext.Kind{ydbstreaming.Kind},
			ChangeKinds: []schemaext.Kind{ydbdiff.StreamingQueryKind}, Service: ydbcompare.StreamingService{}}},
	})
	c.Assert(err, qt.IsNil)
	return runtime
}

func streamingState(c *qt.C, direction schemaext.Representation, knowledge schemaext.KnowledgeState, spec *ydbstreaming.Spec, limits ...schemaext.SubjectCoverage) schemaext.ObjectState {
	c.Helper()
	coverage, err := ydbstreaming.Coverage(direction, schemaext.Knowledge{State: knowledge, Reason: "namespace evidence"}, limits)
	c.Assert(err, qt.IsNil)
	state := schemaext.ObjectState{Coverage: coverage}
	if spec != nil {
		var value schemaext.Value = &ydbstreaming.Desired{Spec: *spec}
		if direction == schemaext.Observed {
			value = &ydbstreaming.Observed{Spec: *spec}
		}
		state.Objects, err = schemaext.NewObjects(schemaext.Object{Ref: ydbstreaming.Ref("app", "locks"), Value: value})
		c.Assert(err, qt.IsNil)
	}
	return state
}

func TestStreamingComparisonNeedsNoTableAndRespectsEvidence(t *testing.T) {
	empty := &ydbstreaming.Spec{Text: "SELECT 1;"}
	strict := &ydbstreaming.Spec{Text: "SELECT 1;", Run: new(false)}
	defaults := &ydbstreaming.Spec{Text: "SELECT 1;", Run: new(true), ResourcePool: "default"}
	limit := func(state schemaext.KnowledgeState) []schemaext.SubjectCoverage {
		return []schemaext.SubjectCoverage{{Kind: ydbstreaming.Kind, Subject: ydbstreaming.Ref("app", "locks"),
			Knowledge: schemaext.Knowledge{State: state, Reason: "unreadable setting"}}}
	}
	cases := []struct {
		name                               string
		desired, current                   *ydbstreaming.Spec
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
			runtime := streamingRuntime(c)
			request := schemaext.ObjectComparisonRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
				Capabilities: capability.Capabilities{capability.StreamingQueries: true}, Kinds: []schemaext.Kind{ydbstreaming.Kind},
				Desired: streamingState(c, schemaext.Desired, test.desiredKnowledge, test.desired, test.desiredLimits...),
				Current: streamingState(c, schemaext.Observed, test.currentKnowledge, test.current, test.currentLimits...),
			}
			result, err := runtime.CompareObjects(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.diagnostics)
			c.Assert(result.Desired.Objects.Refs(), qt.HasLen, test.objects)
			for _, record := range result.Changes {
				c.Assert(record.Value, qt.DeepEquals, streamingChange(test.current, test.desired))
				c.Assert(record.Subject.Parent.Empty(), qt.IsTrue)
			}
		})
	}
}

func TestStreamingComparisonRefusesInvalidInputsWithoutPartialOutput(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*schemaext.ObjectComparisonRequest)
		want   error
	}{
		{name: "wrong target", mutate: func(r *schemaext.ObjectComparisonRequest) { r.Target = "postgres" }, want: ptaherr.ErrUnsupportedDialect},
		{name: "missing capability", mutate: func(r *schemaext.ObjectComparisonRequest) { r.Capabilities = capability.Capabilities{} }, want: ptaherr.ErrUnsupportedFeature},
		{name: "wrong identifiers", mutate: func(r *schemaext.ObjectComparisonRequest) { r.Identifiers = identifier.ForDialect("postgres") }, want: schemaext.ErrInvalidValue},
		{name: "table parent", mutate: func(r *schemaext.ObjectComparisonRequest) {
			ref := ydbstreaming.Ref("app", "locks")
			ref.Parent = objectidentity.Part{Source: "table", Normalized: "table"}
			r.Desired.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}}}))
		}, want: schemaext.ErrInvalidValue},
		{name: "unsplit leaf", mutate: func(r *schemaext.ObjectComparisonRequest) {
			r.Desired.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbstreaming.Ref("", "app/query"), Value: &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}}}))
		}, want: schemaext.ErrInvalidValue},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemaext.ObjectComparisonRequest{Target: "ydb", Kinds: []schemaext.Kind{ydbstreaming.Kind}, Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Capabilities{capability.StreamingQueries: true},
				Desired: streamingState(c, schemaext.Desired, schemaext.Complete, &ydbstreaming.Spec{Text: "SELECT 1;"}),
				Current: streamingState(c, schemaext.Observed, schemaext.Complete, nil)}
			test.mutate(&request)
			result, err := (ydbcompare.StreamingService{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
		})
	}
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (ydbcompare.StreamingService{}).CompareObjects(ctx, schemaext.ObjectComparisonRequest{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
}

// streamingChange constructs expected complete operands without applying defaults.
func streamingChange(before, after *ydbstreaming.Spec) *ydbdiff.StreamingQuery {
	change := &ydbdiff.StreamingQuery{}
	if before != nil {
		change.Before = &ydbstreaming.Observed{Spec: *before}
	}
	if after != nil {
		change.After = &ydbstreaming.Desired{Spec: *after}
	}
	return change
}

func TestStreamingComparisonAdoptsRawStateFromAnUnknownSource(t *testing.T) {
	c := qt.New(t)
	observed := &ydbstreaming.Spec{Text: "SELECT 1;", Run: new(false)}
	request := schemaext.ObjectComparisonRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
		Capabilities: capability.Capabilities{capability.StreamingQueries: true}, Kinds: []schemaext.Kind{ydbstreaming.Kind},
		Desired: streamingState(c, schemaext.Desired, schemaext.Uninspected, nil),
		Current: streamingState(c, schemaext.Observed, schemaext.Complete, observed),
	}
	result, err := streamingRuntime(c).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	values, err := result.Desired.Objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.HasLen, 1)
	c.Assert(values[0].Value, qt.DeepEquals, &ydbstreaming.Desired{Spec: *observed})
	c.Assert(result.Desired.Coverage.Lookup(ydbstreaming.Kind, values[0].Ref).State, qt.Equals, schemaext.Complete)
}

func TestStreamingComparisonKeepsServerOwnedLimits(t *testing.T) {
	c := qt.New(t)
	hidden := ydbstreaming.Ref("", ".hidden")
	request := schemaext.ObjectComparisonRequest{Target: "ydb", Kinds: []schemaext.Kind{ydbstreaming.Kind},
		Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Capabilities{capability.StreamingQueries: true},
		Desired: streamingState(c, schemaext.Desired, schemaext.Complete, nil),
		Current: streamingState(c, schemaext.Observed, schemaext.Complete, nil, schemaext.SubjectCoverage{
			Kind: ydbstreaming.Kind, Subject: hidden, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "server-owned node"},
		}),
	}
	result, err := streamingRuntime(c).CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Subject, qt.DeepEquals, hidden)
	c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
}

func TestStreamingComparisonKeepsCaseAndDirectoryIdentity(t *testing.T) {
	tests := []struct{ name, desiredSchema, desiredName, currentSchema, currentName string }{
		{name: "case", desiredName: "Locks", currentName: "locks"},
		{name: "directory", desiredSchema: "a", desiredName: "locks", currentSchema: "b", currentName: "locks"},
		{name: "literal dot", desiredName: "app.locks", currentSchema: "app", currentName: "locks"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := streamingState(c, schemaext.Desired, schemaext.Complete, nil)
			current := streamingState(c, schemaext.Observed, schemaext.Complete, nil)
			desired.Objects = must.Must(schemaext.NewObjects(ydbstreaming.DesiredObject(test.desiredSchema, test.desiredName, "", ydbstreaming.Spec{Text: "SELECT 1;"}, false)))
			observed := ydbstreaming.Spec{Text: "SELECT 1;", Run: new(false)}
			current.Objects = must.Must(schemaext.NewObjects(ydbstreaming.ObservedObject(test.currentSchema, test.currentName, observed)))
			result, err := streamingRuntime(c).CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "ydb", Kinds: []schemaext.Kind{ydbstreaming.Kind}, Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Capabilities{capability.StreamingQueries: true}, Desired: desired, Current: current})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.ContentEquals, []schemaext.ChangeRecord{
				{Subject: ydbstreaming.Ref(test.desiredSchema, test.desiredName), Value: &ydbdiff.StreamingQuery{After: &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}}}},
				{Subject: ydbstreaming.Ref(test.currentSchema, test.currentName), Value: &ydbdiff.StreamingQuery{Before: &ydbstreaming.Observed{Spec: observed}}},
			})
		})
	}
}
