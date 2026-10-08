package sqlschema

import (
	"errors"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/tableref"
)

func appendChangefeed(target alterTarget, spec ydbschema.ChangefeedSpec) error {
	object := ydbschema.DesiredObject(target.table.Schema, target.table.Name, spec)
	for _, database := range target.databases {
		_, found, err := database.FeatureObjects.Get(object.Ref)
		if err != nil {
			return err
		}
		if found {
			return fmt.Errorf("changefeed %q is declared twice on table %s", spec.Name, target.qualified)
		}
	}
	// A later file adds the object to its own result, even when its parent was
	// declared by an earlier file. The document merge retains one named object.
	database := target.databases[0]
	objects, err := database.FeatureObjects.With(object)
	if err != nil {
		return err
	}
	database.FeatureObjects = objects
	return nil
}

func appendTopicConsumer(database, base *schemamodel.Database, node *ast.AddTopicConsumerNode) error {
	for _, source := range []*schemamodel.Database{database, base} {
		if source == nil {
			continue
		}
		found, err := appendDeclaredTopicConsumer(source, node)
		if err != nil {
			return err
		}
		if found {
			return nil
		}
	}
	return fmt.Errorf("%w: ALTER TOPIC %s ADD CONSUMER names no topic or changefeed this schema declares", ErrUnmodeledStatement, node.Name)
}

func appendDeclaredTopicConsumer(database *schemamodel.Database, node *ast.AddTopicConsumerNode) (bool, error) {
	for i := range database.Topics {
		topic := &database.Topics[i]
		if topic.QualifiedName() != node.Name {
			continue
		}
		consumers, err := withTopicConsumer(topic.Spec.Consumers, node)
		if err != nil {
			return false, err
		}
		topic.Spec.Consumers = consumers
		return true, nil
	}
	for _, ref := range database.FeatureObjects.Refs() {
		if ref.Kind != objectidentity.Kind(ydbschema.ChangefeedKind) {
			continue
		}
		directory := ref.Parent.Source
		if ref.Schema.Source != "" {
			directory = ref.Schema.Source + "/" + directory
		}
		if tableref.Canonical(directory, ref.Name.Source) != node.Name {
			continue
		}
		object, found, err := database.FeatureObjects.Get(ref)
		if err != nil {
			return false, err
		}
		if !found {
			return false, errors.New("changefeed disappeared from an immutable schema snapshot")
		}
		feed, ok := object.Value.(*ydbschema.DesiredChangefeed)
		if !ok {
			return false, fmt.Errorf("%w: expected desired changefeed, got %T", schemaext.ErrInvalidValue, object.Value)
		}
		consumers, err := withTopicConsumer(feed.Spec.Consumers, node)
		if err != nil {
			return false, err
		}
		feed.Spec.Consumers = consumers
		objects, err := database.FeatureObjects.Replace(object)
		if err != nil {
			return false, err
		}
		database.FeatureObjects = objects
		return true, nil
	}
	return false, nil
}

func withTopicConsumer(consumers []ast.TopicConsumerSpec, node *ast.AddTopicConsumerNode) ([]ast.TopicConsumerSpec, error) {
	if slices.ContainsFunc(consumers, func(held ast.TopicConsumerSpec) bool { return held.Name == node.Consumer.Name }) {
		return nil, fmt.Errorf("consumer %q is declared twice on topic %s", node.Consumer.Name, node.Name)
	}
	return append(slices.Clone(consumers), node.Consumer.Clone()), nil
}
