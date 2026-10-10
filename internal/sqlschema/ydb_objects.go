package sqlschema

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/yqlparse"
)

func appendYDBDeclaration(database *schemamodel.Database, document *Document, statement ast.Node, sourcePlatform string) (bool, error) {
	if handled, err := appendYDBPrincipal(database, document, statement, sourcePlatform); handled {
		return true, err
	}
	switch node := statement.(type) {
	case *ast.AlterSequenceNode:
		if platform.NormalizeDialect(sourcePlatform) != platform.YDB {
			return false, nil
		}
		return true, alterYDBSequence(database, document, node)
	case *ast.ExtensionStatement:
		if handled, err := appendPoolDeclaration(database, node.Payload); handled {
			return true, err
		}
		if handled, err := appendTopicDeclaration(database, document.base, node.Payload); handled {
			return true, err
		}
		if handled, err := appendExternalDeclaration(database, node.Payload); handled {
			return true, err
		}
		if handled, err := appendReplicationDeclaration(database, node.Payload); handled {
			return true, err
		}
		if value, ok := node.Payload.(*ydbast.StreamingQuery); ok {
			return true, appendStreamingQuery(database, document.base, value)
		}
		if value, ok := node.Payload.(*ydbast.Secret); ok {
			return true, appendSecret(database, value)
		}
		value, ok := node.Payload.(*ydbast.CoordinationNode)
		if !ok || value == nil || value.Change.Before != nil || value.Change.After == nil {
			return false, nil
		}
		var err error
		database.FeatureObjects, err = database.FeatureObjects.With(ydbcoordination.DesiredObject(value.Schema, value.Name, value.Change.After.StructName, value.Change.After.Spec))
		return true, err
	case *yqlparse.ReplicationSettings:
		return true, applyReplicationSettings(database, document.base, node)
	default:
		return false, nil
	}
}

// appendTopicDeclaration declares a topic a CREATE TOPIC names, and adds a
// consumer an ALTER TOPIC ... ADD CONSUMER names to its topic or changefeed.
func appendTopicDeclaration(database, base *schemamodel.Database, payload ast.ExtensionPayload) (bool, error) {
	switch value := payload.(type) {
	case *ydbast.Topic:
		if value.Change.Before != nil || value.Change.After == nil {
			return false, nil
		}
		var err error
		database.FeatureObjects, err = ydbtopic.Declare(database.FeatureObjects, value.Schema, value.Name, "", value.Change.After.Spec)
		return true, err
	case *ydbast.TopicConsumer:
		return true, appendTopicConsumer(database, base, value)
	default:
		return false, nil
	}
}

func appendPoolDeclaration(database *schemamodel.Database, payload ast.ExtensionPayload) (bool, error) {
	switch value := payload.(type) {
	case *ydbast.ResourcePool:
		if err := value.Validate(); err != nil {
			return true, err
		}
		if value.Operation != ydbast.PoolCreate {
			return false, nil
		}
		var err error
		database.FeatureObjects, err = database.FeatureObjects.With(ydbworkload.DesiredPoolObject(value.Name, "", *value.Spec))
		return true, err
	case *ydbast.ResourcePoolClassifier:
		if err := value.Validate(); err != nil {
			return true, err
		}
		if value.Operation != ydbast.PoolCreate {
			return false, nil
		}
		var err error
		database.FeatureObjects, err = database.FeatureObjects.With(ydbworkload.DesiredClassifierObject(value.Name, "", *value.Spec))
		return true, err
	default:
		return false, nil
	}
}

// appendSecret declares the secret a CREATE SECRET statement names, with the
// variable its value comes from. A YQL document declares a secret once.
func appendSecret(database *schemamodel.Database, node *ydbast.Secret) error {
	if err := node.Validate(); err != nil {
		return err
	}
	if node.Operation != ydbast.SecretCreate {
		return fmt.Errorf("%w: only CREATE declares a secret", ErrUnmodeledStatement)
	}
	var err error
	database.FeatureObjects, err = ydbsecret.Declare(database.FeatureObjects, node.Schema, node.Name, "", node.ValueEnv)
	return err
}
