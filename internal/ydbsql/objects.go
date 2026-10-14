package ydbsql

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/yqlparse"
)

// convertStatement converts a YDB statement the shared conversion does not
// model, and reports whether it did. Any other statement is the shared
// conversion's, which refuses one it does not model either.
func convertStatement(scope sqlschema.Scope, statement ast.Node) (bool, error) {
	if handled, err := appendPrivilege(scope, statement); handled {
		return true, err
	}
	if handled, err := appendPrincipal(scope, statement); handled {
		return true, err
	}
	database := scope.Database()
	switch node := statement.(type) {
	case *ast.CommentNode:
		return true, applyComment(scope, node.Text)
	case *ast.AlterSequenceNode:
		return true, alterSequence(scope, node)
	case *ast.ExtensionStatement:
		return appendExtension(scope, node.Payload)
	case *yqlparse.ReplicationSettings:
		return true, applyReplicationSettings(database, scope.Base(), node)
	default:
		return false, nil
	}
}

// appendExtension declares the YDB object an extension statement names.
func appendExtension(scope sqlschema.Scope, payload ast.ExtensionPayload) (bool, error) {
	database, base := scope.Database(), scope.Base()
	if handled, err := appendPoolDeclaration(database, payload); handled {
		return true, err
	}
	if handled, err := appendTopicDeclaration(database, base, payload); handled {
		return true, err
	}
	if handled, err := appendExternalDeclaration(database, payload); handled {
		return true, err
	}
	if handled, err := appendReplicationDeclaration(database, payload); handled {
		return true, err
	}
	if value, ok := payload.(*ydbast.StreamingQuery); ok {
		return true, appendStreamingQuery(database, base, value)
	}
	if value, ok := payload.(*ydbast.Secret); ok {
		return true, appendSecret(database, value)
	}
	value, ok := payload.(*ydbast.CoordinationNode)
	if !ok || value == nil || value.Change.Before != nil || value.Change.After == nil {
		return false, nil
	}
	var err error
	database.FeatureObjects, err = database.FeatureObjects.With(ydbcoordination.DesiredObject(value.Schema, value.Name, value.Change.After.StructName, value.Change.After.Spec))
	return true, err
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
		return fmt.Errorf("%w: only CREATE declares a secret", sqlschema.ErrUnmodeledStatement)
	}
	var err error
	database.FeatureObjects, err = ydbsecret.Declare(database.FeatureObjects, node.Schema, node.Name, "", node.ValueEnv)
	return err
}
