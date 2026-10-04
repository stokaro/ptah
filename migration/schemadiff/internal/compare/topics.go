package compare

import (
	"sort"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// Topics compares declared YDB topics against the ones the database reports,
// by directory and name.
//
// A topic both sides hold is compared through [ydbtopic]: each side's settings
// resolve to the values a new topic takes for the ones it leaves out, and the
// consumers compare by name, so a declaration naming a default and one leaving
// it out are the same topic. A topic only the database holds is a removal only
// where the desired state claims to describe topics, and one only the
// declaration holds is a creation only where the read looked; CREATE TOPIC
// carries no guard Ptah writes, so an undecided creation is withheld and
// recorded rather than planned.
func Topics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	declared := make(map[string]schemamodel.Topic, len(desired.Topics))
	for _, topic := range desired.Topics {
		declared[topic.QualifiedName()] = topic
	}
	held := make(map[string]catalog.Topic, len(database.Topics))
	for _, topic := range database.Topics {
		held[topic.QualifiedName()] = topic
	}

	var added difftypes.TopicChanges
	for name, topic := range declared {
		current, exists := held[name]
		if !exists {
			added = append(added, topic)
			continue
		}
		if change, differs := difftypes.NewTopicDiff(name, topic.Spec, current.Spec); differs {
			diff.TopicsModified = append(diff.TopicsModified, change)
		}
	}
	for name, topic := range held {
		if _, ok := declared[name]; ok {
			continue
		}
		if !cov.PlansRemoval(coverage.Topic, topic.Schema, topic.Name, name) {
			continue
		}
		diff.TopicsRemoved = append(diff.TopicsRemoved, schemamodel.Topic{
			Name:   topic.Name,
			Schema: topic.Schema,
			Spec:   topic.Spec.Clone(),
		})
	}

	kept, withheld := keepPlannedAdditions(cov, coverage.Topic, added,
		func(topic schemamodel.Topic) (string, []string) {
			return topic.Schema, []string{topic.Name, topic.QualifiedName()}
		},
		func(topic schemamodel.Topic) string { return topic.QualifiedName() },
		unguardedCreations(),
	)
	cov.recordUndecidedAdditions(withheld)
	diff.TopicsAdded = kept

	sortTopics(diff.TopicsAdded)
	sortTopics(diff.TopicsRemoved)
	sort.Slice(diff.TopicsModified, func(i, j int) bool {
		return diff.TopicsModified[i].Name < diff.TopicsModified[j].Name
	})
}

// sortTopics orders topics by their canonical reference.
func sortTopics(topics difftypes.TopicChanges) {
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].QualifiedName() < topics[j].QualifiedName()
	})
}
