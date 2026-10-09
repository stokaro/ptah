package sqlschema

import (
	"fmt"

	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbstreaming"
)

func appendStreamingQuery(database, base *schemamodel.Database, node *ydbast.StreamingQuery) error {
	if err := node.Validate(); err != nil {
		return err
	}
	if node.Operation != ydbast.StreamingCreate {
		return fmt.Errorf("%w: only CREATE declares a streaming query", ErrUnmodeledStatement)
	}
	object := ydbstreaming.DesiredObject(node.Schema, node.Name, "", node.Spec, node.Creation.OrReplace && !node.Creation.IfNotExists)
	if err := ydbstreaming.ValidateIdentity(object.Ref); err != nil {
		return err
	}
	for _, source := range []*schemamodel.Database{database, base} {
		if source == nil {
			continue
		}
		_, found, err := source.FeatureObjects.Get(object.Ref)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		switch {
		case node.Creation.IfNotExists:
			return nil
		case node.Creation.OrReplace:
			source.FeatureObjects, err = source.FeatureObjects.Replace(object)
			return err
		default:
			return fmt.Errorf("streaming query %q is declared twice", node.QualifiedName())
		}
	}
	var err error
	database.FeatureObjects, err = database.FeatureObjects.With(object)
	return err
}
