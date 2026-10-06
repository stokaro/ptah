package sqlschema

import (
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

func appendChangefeed(target alterTarget, spec ast.ChangefeedSpec) error {
	if slices.ContainsFunc(target.table.Changefeeds, func(held ast.ChangefeedSpec) bool { return held.Name == spec.Name }) {
		return fmt.Errorf("changefeed %q is declared twice on table %s", spec.Name, target.qualified)
	}
	target.table.Changefeeds = append(target.table.Changefeeds, spec.Clone())
	return nil
}

func appendTopicConsumer(database, base *schemamodel.Database, node *ast.AddTopicConsumerNode) error {
	for _, source := range []*schemamodel.Database{database, base} {
		if source == nil {
			continue
		}
		consumers := declaredTopicConsumers(source, node.Name)
		if consumers == nil {
			continue
		}
		if slices.ContainsFunc(*consumers, func(held ast.TopicConsumerSpec) bool { return held.Name == node.Consumer.Name }) {
			return fmt.Errorf("consumer %q is declared twice on topic %s", node.Consumer.Name, node.Name)
		}
		*consumers = append(*consumers, node.Consumer.Clone())
		return nil
	}
	return fmt.Errorf("%w: ALTER TOPIC %s ADD CONSUMER names no topic or changefeed this schema declares", ErrUnmodeledStatement, node.Name)
}

func declaredTopicConsumers(database *schemamodel.Database, name string) *[]ast.TopicConsumerSpec {
	for i := range database.Topics {
		topic := &database.Topics[i]
		if topic.QualifiedName() == name {
			return &topic.Spec.Consumers
		}
	}
	for i := range database.Tables {
		table := &database.Tables[i]
		directory := table.Name
		if table.Schema != "" {
			directory = table.Schema + "/" + table.Name
		}
		for j := range table.Changefeeds {
			feed := &table.Changefeeds[j]
			if tableref.Canonical(directory, feed.Name) == name {
				return &feed.Consumers
			}
		}
	}
	return nil
}
