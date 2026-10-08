package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// Dropping a topic, or a consumer of one, loses messages or a reader's
// position, so each is destructive and passes a destructive gate only when
// asked to. Adding a consumer loses nothing, and a changed setting is a
// warning: it can shorten how long the topic keeps a message.
func TestAssessRendered_Topics(t *testing.T) {
	consumers := func(names ...string) ast.TopicSpec {
		spec := ast.TopicSpec{}
		for _, name := range names {
			spec.Consumers = append(spec.Consumers, ast.TopicConsumerSpec{Name: name})
		}
		return spec
	}
	tests := []struct {
		name     string
		node     ast.Node
		severity safety.Severity
		reason   string
	}{
		{name: "a created topic", node: ast.NewCreateTopic("events", ast.TopicSpec{}),
			severity: safety.Safe, reason: "does not remove data or tighten constraints"},
		{name: "a dropped topic", node: ast.NewDropTopic("events"), severity: safety.Destructive,
			reason: "DROP TOPIC removes the topic, every message it holds and every consumer's position in it"},
		{name: "a dropped consumer", node: ast.NewAlterTopic("events", consumers(), consumers("gone")),
			severity: safety.Destructive, reason: "DROP CONSUMER removes a topic consumer and its position in the topic"},
		{name: "an added consumer", node: ast.NewAlterTopic("events", consumers("fresh"), consumers()),
			severity: safety.Safe, reason: "does not remove data or tighten constraints"},
		{name: "a changed setting", node: ast.NewAlterTopic("events", ast.TopicSpec{RetentionPeriod: "PT1H"}, ast.TopicSpec{}),
			severity: safety.Warning, reason: "ALTER TOPIC can shorten how long the topic keeps a message, or move where a consumer reads from"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			assessments, err := safety.AssessRenderedWithCapabilities(c.Context(), must.Must(builtin.New()), []ast.Node{test.node}, platform.YDB, capability.YDB262())
			c.Assert(err, qt.IsNil)
			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.severity)
			c.Assert(assessments[0].Reason, qt.Equals, test.reason)
		})
	}
}

// The AST classifier reads the same verdicts off the nodes, without rendering
// them: a consumer YDB changes only by dropping it is destructive there too.
func TestAssess_Topics(t *testing.T) {
	codecs := ast.TopicSpec{Consumers: []ast.TopicConsumerSpec{{Name: "c", SupportedCodecs: []string{"raw"}}}}
	plain := ast.TopicSpec{Consumers: []ast.TopicConsumerSpec{{Name: "c"}}}
	tests := []struct {
		name     string
		node     ast.Node
		severity safety.Severity
		reason   string
	}{
		{name: "a dropped topic", node: ast.NewDropTopic("events"), severity: safety.Destructive,
			reason: "DROP TOPIC removes the topic, every message it holds and every consumer's position in it"},
		{name: "a dropped consumer", node: ast.NewAlterTopic("events", ast.TopicSpec{}, plain), severity: safety.Destructive,
			reason: "DROP CONSUMER removes a topic consumer and its position in the topic"},
		{name: "a consumer dropped and added again", node: ast.NewAlterTopic("events", plain, codecs),
			severity: safety.Destructive, reason: "DROP CONSUMER removes a topic consumer and its position in the topic"},
		{name: "an added consumer", node: ast.NewAlterTopic("events", plain, ast.TopicSpec{}), severity: safety.Safe,
			reason: "does not remove data or tighten constraints"},
		{name: "a changed setting", node: ast.NewAlterTopic("events", ast.TopicSpec{RetentionPeriod: "PT1H"}, ast.TopicSpec{}),
			severity: safety.Warning, reason: "ALTER TOPIC can shorten how long the topic keeps a message, or move where a consumer reads from"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			assessments := safety.Assess([]ast.Node{test.node})
			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.severity)
			c.Assert(assessments[0].Reason, qt.Equals, test.reason)
			c.Assert(safety.Classify(test.node), qt.Equals, test.severity)
		})
	}
}

// A consumer YDB changes only by dropping it renders as two statements, and
// the one that drops it is destructive on its own words.
func TestAssessRendered_TopicConsumerRestarted(t *testing.T) {
	c := qt.New(t)
	node := ast.NewAlterTopic("events", ast.TopicSpec{Consumers: []ast.TopicConsumerSpec{{Name: "c"}}},
		ast.TopicSpec{Consumers: []ast.TopicConsumerSpec{{Name: "c", SupportedCodecs: []string{"raw"}}}})

	assessments, err := safety.AssessRenderedWithCapabilities(c.Context(), must.Must(builtin.New()), []ast.Node{node}, platform.YDB, capability.YDB262())

	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 2)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(assessments[0].Reason, qt.Equals, "DROP CONSUMER removes a topic consumer and its position in the topic")
	c.Assert(assessments[1].Severity, qt.Equals, safety.Safe)
	c.Assert(safety.HasDestructiveAssessment(assessments), qt.IsTrue)
}

// A statement read as text is judged the same way, so a hand-written
// migration that drops a topic or a consumer is held by the same gate.
func TestAssessSQL_Topics(t *testing.T) {
	tests := []struct {
		statement string
		severity  safety.Severity
	}{
		{statement: "DROP TOPIC `events`", severity: safety.Destructive},
		{statement: "ALTER TOPIC `events` DROP CONSUMER `gone`", severity: safety.Destructive},
		{statement: "ALTER TOPIC `events` ADD CONSUMER `fresh`", severity: safety.Safe},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(safety.AssessSQL(test.statement).Severity, qt.Equals, test.severity)
		})
	}
}

func TestClassifySchemaDiff_Topics(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TopicsAdded:   difftypes.TopicChanges{{Name: "fresh"}},
		TopicsRemoved: difftypes.TopicChanges{{Name: "gone"}},
		TopicsModified: []difftypes.TopicDiff{
			{Name: "events", ConsumersRemoved: []string{"a"}, ConsumersRestarted: []string{"b"}},
			{Name: "audit", SettingsChanged: true},
		},
	}

	c.Assert(safety.ClassifySchemaDiff(diff), qt.DeepEquals, []safety.Finding{
		{Category: "topic_consumers_removed", Count: 2, Severity: safety.Destructive},
		{Category: "topics_removed", Count: 1, Severity: safety.Destructive},
		{Category: "topics_modified", Count: 2, Severity: safety.Warning},
		{Category: "topics_added", Count: 1, Severity: safety.Safe},
	})
}
