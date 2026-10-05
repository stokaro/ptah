package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// readTopic is a topic as the reader describes one created with a retention
// of two hours and important consumer billing, measured on 26.2.1.14.
func readTopic(name string) catalog.Topic {
	return catalog.Topic{Name: name, Schema: "app", Spec: ast.TopicSpec{
		MinActivePartitions: 1, AutoPartitioningStrategy: "disabled", RetentionPeriod: "PT2H",
		PartitionWriteSpeedBytesPerSecond: 1048576, PartitionWriteBurstBytes: 1048576,
		Consumers: []ast.TopicConsumerSpec{{Name: "billing", Important: true}},
	}}
}

func topicsDeclared(topics ...schemamodel.Topic) *schemamodel.Database {
	return &schemamodel.Database{Topics: topics}
}

// A declaration and a read of the same topic compare equal however the
// declaration spells its settings, and a difference is reported once per
// topic, with what changed.
func TestCompare_Topics(t *testing.T) {
	declared := func(spec ast.TopicSpec) schemamodel.Topic {
		return schemamodel.Topic{Name: "events", Schema: "app", Spec: spec}
	}
	billing := []ast.TopicConsumerSpec{{Name: "billing", Important: true}}
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		current  *catalog.Database
		added    []string
		removed  []string
		modified []difftypes.TopicDiff
	}{
		{
			name:    "the same topic",
			desired: topicsDeclared(declared(ast.TopicSpec{RetentionPeriod: "PT120M", Consumers: billing})),
			current: &catalog.Database{Topics: []catalog.Topic{readTopic("events")}},
		},
		{
			name:    "a topic only the declaration has",
			desired: topicsDeclared(declared(ast.TopicSpec{})),
			current: &catalog.Database{},
			added:   []string{"app.events"},
		},
		{
			name:    "a topic only the database has",
			desired: topicsDeclared(),
			current: &catalog.Database{Topics: []catalog.Topic{readTopic("events")}},
			removed: []string{"app.events"},
		},
		{
			name:    "another retention and another consumer",
			desired: topicsDeclared(declared(ast.TopicSpec{RetentionPeriod: "PT3H", Consumers: []ast.TopicConsumerSpec{{Name: "audit"}}})),
			current: &catalog.Database{Topics: []catalog.Topic{readTopic("events")}},
			modified: []difftypes.TopicDiff{{Name: "app.events", SettingsChanged: true,
				ConsumersAdded: []string{"audit"}, ConsumersRemoved: []string{"billing"}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(test.desired, test.current, platform.YDB)

			c.Assert(diff.TopicsAdded.Names(), qt.DeepEquals, test.added)
			c.Assert(diff.TopicsRemoved.Names(), qt.DeepEquals, test.removed)
			c.Assert(diff.TopicsModified, qt.HasLen, len(test.modified))
			for i, change := range test.modified {
				got := diff.TopicsModified[i]
				c.Assert([]any{got.Name, got.SettingsChanged, got.ConsumersAdded, got.ConsumersRemoved, got.ConsumersChanged},
					qt.DeepEquals, []any{change.Name, change.SettingsChanged, change.ConsumersAdded, change.ConsumersRemoved, change.ConsumersChanged})
			}
			c.Assert(diff.HasChanges(), qt.Equals, len(test.added)+len(test.removed)+len(test.modified) > 0)
		})
	}
}

// A description that does not describe topics plans no removal of the topics
// the database holds, and a read that did not describe a topic does not
// decide that a declared one is missing: the addition is withheld and
// reported, since CREATE TOPIC over a topic that is there fails.
func TestCompare_Topics_Coverage(t *testing.T) {
	c := qt.New(t)
	undescribed := coverage.Set{}.With(coverage.Object{Kind: coverage.Topic})
	desired := topicsDeclared()
	desired.NotDescribed = undescribed

	removal := schemadiff.CompareWithDialect(desired, &catalog.Database{Topics: []catalog.Topic{readTopic("events")}}, platform.YDB)
	addition, undecided := schemadiff.CompareReportingUndecidedAdditions(
		topicsDeclared(schemamodel.Topic{Name: "events", Schema: "app"}),
		&catalog.Database{NotDescribed: coverage.Set{}.With(coverage.Object{Kind: coverage.Topic, Name: "app.events"})},
		&config.CompareOptions{Dialect: platform.YDB},
	)

	c.Assert(removal.TopicsRemoved, qt.HasLen, 0)
	c.Assert(addition.TopicsAdded, qt.HasLen, 0)
	c.Assert(undecided, qt.DeepEquals, []coverage.Object{{Kind: coverage.Topic, Name: "app.events"}})
}
