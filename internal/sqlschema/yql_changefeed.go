package sqlschema

import (
	"errors"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
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

// appendTopicConsumer applies `ALTER TOPIC <path> ADD CONSUMER ...` to the
// topic or the changefeed this schema, or the one it extends, declares at the
// path.
func appendTopicConsumer(database, base *schemamodel.Database, node *ydbast.TopicConsumer) error {
	if err := node.Validate(); err != nil {
		return err
	}
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
	return fmt.Errorf("%w: ALTER TOPIC %s ADD CONSUMER names no topic or changefeed this schema declares", ErrUnmodeledStatement, node.Path())
}

func appendDeclaredTopicConsumer(database *schemamodel.Database, node *ydbast.TopicConsumer) (bool, error) {
	object, found, err := database.FeatureObjects.Get(ydbtopic.Ref(node.Schema, node.Name))
	if err != nil {
		return false, err
	}
	if found {
		topic, ok := object.Value.(*ydbtopic.Desired)
		if !ok {
			return false, fmt.Errorf("%w: expected desired topic, got %T", schemaext.ErrInvalidValue, object.Value)
		}
		consumers, err := withTopicConsumer(topic.Spec.Consumers, node)
		if err != nil {
			return false, err
		}
		topic.Spec.Consumers = consumers
		database.FeatureObjects, err = database.FeatureObjects.Replace(object)
		return true, err
	}
	for _, ref := range database.FeatureObjects.Refs() {
		if ref.Kind != objectidentity.Kind(ydbschema.ChangefeedKind) {
			continue
		}
		directory := ref.Parent.Source
		if ref.Schema.Source != "" {
			directory = ref.Schema.Source + "/" + directory
		}
		if directory != node.Schema || ref.Name.Source != node.Name {
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

func withTopicConsumer(consumers []ydbtopic.ConsumerSpec, node *ydbast.TopicConsumer) ([]ydbtopic.ConsumerSpec, error) {
	if slices.ContainsFunc(consumers, func(held ydbtopic.ConsumerSpec) bool { return held.Name == node.Consumer.Name }) {
		return nil, fmt.Errorf("consumer %q is declared twice on topic %s", node.Consumer.Name, node.Path())
	}
	return append(slices.Clone(consumers), node.Consumer.Clone()), nil
}
