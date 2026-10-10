package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// consumers is a topic spec holding a consumer of each name.
func consumers(names ...string) ydbtopic.Spec {
	spec := ydbtopic.Spec{}
	for _, name := range names {
		spec.Consumers = append(spec.Consumers, ydbtopic.ConsumerSpec{Name: name})
	}
	return spec
}

// topicChange is a change of the topic events from before to after; a nil
// side is the topic's absence.
func topicChange(before, after *ydbtopic.Spec) ydbdiff.Topic {
	var change ydbdiff.Topic
	if before != nil {
		change.Before = &ydbtopic.Observed{Spec: *before}
	}
	if after != nil {
		change.After = &ydbtopic.Desired{Spec: *after}
	}
	return change
}

// TestClassify_Topic judges what a topic statement loses, from the owner's
// effect, before and after rendering alike. Dropping a topic, or a consumer of
// one -- one YDB changes only by dropping it and adding it again included --
// loses messages or a reader's position, so each is destructive. Creating a
// topic or adding a consumer loses nothing, and a changed setting is a
// warning: it can shorten how long the topic keeps a message.
func TestClassify_Topic(t *testing.T) {
	codecs := ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "c", SupportedCodecs: []string{"raw"}}}}
	plain, none := consumers("c"), consumers()
	tests := []struct {
		name     string
		change   ydbdiff.Topic
		severity safety.Severity
		reason   string
	}{
		{name: "a created topic", change: topicChange(nil, &none), severity: safety.Safe, reason: "CREATE TOPIC adds a topic"},
		{name: "a dropped topic", change: topicChange(&none, nil), severity: safety.Destructive,
			reason: "DROP TOPIC removes the topic, every message it holds and every consumer's position in it"},
		{name: "a dropped consumer", change: topicChange(&plain, &none), severity: safety.Destructive,
			reason: "DROP CONSUMER removes a topic consumer and its position in the topic"},
		{name: "a consumer dropped and added again", change: topicChange(&codecs, &plain), severity: safety.Destructive,
			reason: "DROP CONSUMER removes a topic consumer and its position in the topic"},
		{name: "an added consumer", change: topicChange(&none, &plain), severity: safety.Safe, reason: "ALTER TOPIC adds consumers"},
		{name: "a changed setting", change: topicChange(&none, &ydbtopic.Spec{RetentionPeriod: "PT1H"}), severity: safety.Warning,
			reason: "ALTER TOPIC can shorten how long the topic keeps a message, or move where a consumer reads from"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.ExtensionStatement{Payload: &ydbast.Topic{Name: "events", Change: test.change}}

			assessments := safety.Assess([]ast.Node{node})
			rendered, err := safety.AssessRenderedWithCapabilities(c.Context(), must.Must(builtin.New()), []ast.Node{node}, platform.YDB, capability.YDB262())

			c.Assert(err, qt.IsNil)
			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.severity)
			c.Assert(assessments[0].Reason, qt.Equals, test.reason)
			c.Assert(rendered, qt.Not(qt.HasLen), 0)
			c.Assert(safety.Highest(severities(rendered)), qt.Equals, test.severity)
		})
	}
}

// severities lists the severity of each assessment as a finding, so the
// highest of them can be read.
func severities(assessments []safety.StatementAssessment) []safety.Finding {
	findings := make([]safety.Finding, 0, len(assessments))
	for _, assessment := range assessments {
		findings = append(findings, safety.Finding{Severity: assessment.Severity})
	}
	return findings
}

// TestAssessSQL_Topics judges a statement read as text the same way, so a
// hand-written migration that drops a topic or a consumer is held by the same
// gate.
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

// TestClassifySchemaDiff_Topics reads a diff's topic changes through the
// owner's change effect: a dropped topic or consumer is destructive, a changed
// setting a warning, and a created topic safe.
func TestClassifySchemaDiff_Topics(t *testing.T) {
	none, plain := consumers(), consumers("a")
	tests := []struct {
		name   string
		change ydbdiff.Topic
		want   safety.Severity
	}{
		{name: "dropped", change: topicChange(&none, nil), want: safety.Destructive},
		{name: "a consumer dropped", change: topicChange(&plain, &none), want: safety.Destructive},
		{name: "a setting changed", change: topicChange(&none, &ydbtopic.Spec{RetentionPeriod: "PT1H"}), want: safety.Warning},
		{name: "created", change: topicChange(nil, &none), want: safety.Safe},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("", "events"), Value: &test.change}}}
			c.Assert(safety.ClassifySchemaDiff(diff), qt.DeepEquals, []safety.Finding{
				{Category: "feature_changes:" + string(ydbdiff.TopicKind), Count: 1, Severity: test.want},
			})
		})
	}
}
