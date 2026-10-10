package ydbplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine"
)

func topicPlanningRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	codecs := append(ydbtopic.Codecs(), ydbdiff.TopicCodec(), ydbast.TopicCodec())
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: codecs,
		Declarations: []engine.DeclarationPlanning{{Target: "ydb", Kinds: []schemaext.Kind{ydbtopic.Kind}, OperationKinds: []schemaext.Kind{ydbast.TopicKind}, Service: ydbplan.TopicService{}}},
		Planning:     []engine.Planning{{Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.TopicKind}, OperationKinds: []schemaext.Kind{ydbast.TopicKind}, Service: ydbplan.TopicService{}}}})
	c.Assert(err, qt.IsNil)
	return runtime
}

// topicOperationName names a scheduled topic statement by what it does and
// the topic's path.
func topicOperationName(operation *ydbast.Topic) string {
	switch {
	case operation.Change.Before == nil:
		return "create " + operation.Path()
	case operation.Change.After == nil:
		return "drop " + operation.Path()
	default:
		return "alter " + operation.Path()
	}
}

// topicChange is a change of the topic app/events from before to after; a nil
// side is the topic's absence.
func topicChange(before, after *ydbtopic.Spec) *ydbdiff.Topic {
	change := &ydbdiff.Topic{}
	if before != nil {
		change.Before = &ydbtopic.Observed{Spec: *before}
	}
	if after != nil {
		change.After = &ydbtopic.Desired{Spec: *after}
	}
	return change
}

// TestTopicPlan_LowersEachChange writes one statement per created, changed or
// dropped topic, carrying the topic's identity and its path as effects and
// the change's effect as its impact.
func TestTopicPlan_LowersEachChange(t *testing.T) {
	c := qt.New(t)
	empty := ydbtopic.Spec{}
	request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		Changes: []schemaext.ChangeRecord{
			{Subject: ydbtopic.Ref("app", "created"), Value: topicChange(nil, &ydbtopic.Spec{RetentionPeriod: "PT2H"})},
			{Subject: ydbtopic.Ref("app", "changed"), Value: topicChange(&empty, &ydbtopic.Spec{RetentionPeriod: "PT3H"})},
			{Subject: ydbtopic.Ref("", "dropped.v1"), Value: topicChange(&empty, nil)},
		}}

	result, err := topicPlanningRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	plan, err := plangraph.Schedule(t.Context(), result.Contributions...)
	c.Assert(err, qt.IsNil)
	var names []string
	for _, step := range plan.Steps {
		operation := step.Payload.Payload.(*ydbast.Topic)
		names = append(names, topicOperationName(operation))
		c.Assert(step.Transaction, qt.Equals, plangraph.TransactionForbidden)
		c.Assert(step.Effects[0].Subject, qt.DeepEquals, operation.Subject())
		c.Assert(step.Effects[1].Subject, qt.DeepEquals, ydbscheme.Path(operation.Schema, operation.Name))
		c.Assert(step.Impact, qt.DeepEquals, operation.Effect())
	}
	c.Assert(names, qt.DeepEquals, []string{"drop dropped.v1", "alter app/changed", "create app/created"})
	c.Assert([]string{result.Changes[0].Strategy, result.Changes[1].Strategy, result.Changes[2].Strategy}, qt.DeepEquals, []string{
		"create the topic with its consumers and declared settings",
		"change the topic's settings and consumers in place",
		"drop the topic, every message it holds and every consumer's position in it",
	})
}

// TestTopicPlan_PlacesEachStatement orders a topic against the host's
// statements: first, unless a drop frees its path or a directory above it; a
// creation or change before every statement that reads it, and a drop after
// every such statement.
func TestTopicPlan_PlacesEachStatement(t *testing.T) {
	slot := ydbscheme.Path("app", "events")
	reads := []plangraph.Effect{{Subject: ydbtopic.Ref("app", "events"), Action: plangraph.Read}}
	empty := ydbtopic.Spec{}
	tests := []struct {
		name    string
		change  *ydbdiff.Topic
		effects [][]plangraph.Effect
		want    []string
	}{
		{name: "a creation runs first and before its reader", change: topicChange(nil, &empty),
			effects: [][]plangraph.Effect{nil, nil, reads},
			want:    []string{"create app/events", "a", "b", "c"}},
		{name: "a creation follows the drop that frees its path", change: topicChange(nil, &empty),
			effects: [][]plangraph.Effect{nil, {{Subject: slot, Action: plangraph.Drop}}, nil},
			want:    []string{"a", "b", "create app/events", "c"}},
		{name: "a creation follows the drop of its directory", change: topicChange(nil, &empty),
			effects: [][]plangraph.Effect{nil, {{Subject: ydbscheme.Path("", "app"), Action: plangraph.Drop}}, nil},
			want:    []string{"a", "b", "create app/events", "c"}},
		{name: "a drop runs before a creation at its path", change: topicChange(&empty, nil),
			effects: [][]plangraph.Effect{nil, {{Subject: slot, Action: plangraph.Create}}},
			want:    []string{"drop app/events", "a", "b"}},
		{name: "a drop follows the statement that reads it", change: topicChange(&empty, nil),
			effects: [][]plangraph.Effect{nil, reads, nil},
			want:    []string{"a", "b", "drop app/events", "c"}},
		{name: "a change runs before its reader", change: topicChange(&empty, &ydbtopic.Spec{RetentionPeriod: "PT2H"}),
			effects: [][]plangraph.Effect{nil, reads},
			want:    []string{"alter app/events", "a", "b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			chain := commonChain(test.effects...)
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
				CommonSteps: chain.steps, Changes: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("app", "events"), Value: test.change}}}
			result, err := topicPlanningRuntime(c).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			c.Assert(scheduledNames(c, chain, result), qt.DeepEquals, test.want)
		})
	}
}

// TestTopicPlan_RefusesTheWholeBatch returns no statement when one change is
// refused: a target without the topics capability, a change YDB makes in no
// line, and a creation at a path another object keeps.
func TestTopicPlan_RefusesTheWholeBatch(t *testing.T) {
	two := ydbtopic.Spec{MinActivePartitions: 2}
	empty := ydbtopic.Spec{}
	tests := []struct {
		name    string
		caps    capability.Capabilities
		change  *ydbdiff.Topic
		effects [][]plangraph.Effect
		code    schemavalidation.Code
		wantErr string
	}{
		{name: "a line without topics", caps: capability.YDB262().With(capability.Topics, false), change: topicChange(nil, &empty),
			code: schemavalidation.UnsupportedFeature, wantErr: "topic first, which requires target capability topics, unavailable on this ydb target\n" +
				"topic app/events, which requires target capability topics, unavailable on this ydb target"},
		{name: "fewer partitions", caps: capability.YDB262(), change: topicChange(&two, &empty),
			code: schemavalidation.UnsupportedFeature, wantErr: "(?s).*topic app/events: it has 2 partitions and is declared with 1, .*"},
		{name: "a path a table takes", caps: capability.YDB262(), change: topicChange(nil, &empty),
			effects: [][]plangraph.Effect{{{Subject: ydbscheme.Path("app", "events"), Action: plangraph.Create}}},
			code:    schemavalidation.InvalidSchema, wantErr: ".*topic create conflicts with create at scheme path.*"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: test.caps,
				CommonSteps: commonChain(test.effects...).steps, Changes: []schemaext.ChangeRecord{
					{Subject: ydbtopic.Ref("", "first"), Value: topicChange(nil, &empty)},
					{Subject: ydbtopic.Ref("app", "events"), Value: test.change},
				}}
			result, err := topicPlanningRuntime(c).PlanFeatures(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.Not(qt.HasLen), 0)
			c.Assert(result.Diagnostics[0].Problem.Code, qt.Equals, test.code)
			c.Assert(result.Err(request), qt.ErrorMatches, test.wantErr)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// TestTopicDeclarations_CreateEachDeclaredTopic derives one CREATE TOPIC per
// declared topic, before the statements that read it.
func TestTopicDeclarations_CreateEachDeclaredTopic(t *testing.T) {
	c := qt.New(t)
	chain := commonChain(nil, []plangraph.Effect{{Subject: ydbtopic.Ref("app", "events"), Action: plangraph.Read}})
	request := featureplan.DeclarationRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		CommonSteps: chain.steps, Objects: []schemaext.Object{ydbtopic.DesiredObject("app", "events", "", ydbtopic.Spec{})}}

	result, err := topicPlanningRuntime(c).PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Declarations, qt.HasLen, 1)
	c.Assert(result.Declarations[0].Subject, qt.DeepEquals, ydbtopic.Ref("app", "events"))
	c.Assert(scheduledNames(c, chain, featureplan.Result{Contributions: result.Contributions}), qt.DeepEquals, []string{"create app/events", "a", "b"})
}

// TestTopicDeclarations_RefusesAPathATableTakes refuses a declared topic whose
// path a declared table holds, since YDB keeps one object at a path, naming
// the topic model.
func TestTopicDeclarations_RefusesAPathATableTakes(t *testing.T) {
	c := qt.New(t)
	table := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("app", "events")
	chain := commonChain([]plangraph.Effect{{Subject: ydbscheme.Path("app", "events"), Action: plangraph.Create}, {Subject: table, Action: plangraph.Create}})
	request := featureplan.DeclarationRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		CommonSteps: chain.steps, Objects: []schemaext.Object{ydbtopic.DesiredObject("app", "events", "", ydbtopic.Spec{})}}

	result, err := topicPlanningRuntime(c).PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Problem.Kind, qt.Equals, string(ydbtopic.Kind))
	c.Assert(result.Err(request), qt.ErrorMatches, ".*topic create conflicts with create at scheme path.*")
	c.Assert(result.Contributions, qt.HasLen, 0)
}
