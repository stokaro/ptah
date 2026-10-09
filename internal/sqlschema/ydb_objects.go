package sqlschema

import (
	"errors"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbworkload"
)

func appendYDBDeclaration(database *schemamodel.Database, document *Document, statement ast.Node, sourcePlatform string) (bool, error) {
	if handled, err := appendYDBPrincipal(database, document, statement, sourcePlatform); handled {
		return true, err
	}
	if appendYDBExternalDeclaration(database, statement) {
		return true, nil
	}
	switch node := statement.(type) {
	case *ast.AlterSequenceNode:
		if platform.NormalizeDialect(sourcePlatform) != platform.YDB {
			return false, nil
		}
		return true, alterYDBSequence(database, document, node)
	case *ast.CreateTopicNode:
		schema, name := normalizeSQLTableIdentifier(sourcePlatform, node.Name)
		database.Topics = append(database.Topics, schemamodel.Topic{Name: name, Schema: schema, Spec: node.Spec.Clone()})
	case *ast.ExtensionStatement:
		if handled, err := appendPoolDeclaration(database, node.Payload); handled {
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
	default:
		return appendYDBReplication(database, document, statement, sourcePlatform)
	}
	return true, nil
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
	database.FeatureObjects, err = database.FeatureObjects.With(ydbsecret.DesiredObject(node.Schema, node.Name, "", node.ValueEnv))
	if errors.Is(err, schemaext.ErrDuplicate) {
		return fmt.Errorf("secret %q is declared twice", node.Path())
	}
	return err
}
