package sqlschema

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/internal/tableref"
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
	case *ast.CreateSecretNode:
		ref, _ := tableref.Parse(node.Name)
		database.Secrets = append(database.Secrets, schemamodel.Secret{Name: ref.Name, Schema: ref.Schema, ValueEnv: node.ValueEnv})
	case *ast.CreateTopicNode:
		schema, name := normalizeSQLTableIdentifier(sourcePlatform, node.Name)
		database.Topics = append(database.Topics, schemamodel.Topic{Name: name, Schema: schema, Spec: node.Spec.Clone()})
	case *ast.ExtensionStatement:
		if value, ok := node.Payload.(*ydbast.StreamingQuery); ok {
			return true, appendStreamingQuery(database, document.base, value)
		}
		value, ok := node.Payload.(*ydbast.CoordinationNode)
		if !ok || value == nil || value.Change.Before != nil || value.Change.After == nil {
			return false, nil
		}
		var err error
		database.FeatureObjects, err = database.FeatureObjects.With(ydbcoordination.DesiredObject(value.Schema, value.Name, value.Change.After.StructName, value.Change.After.Spec))
		return true, err
	case *ast.CreateResourcePoolNode:
		database.ResourcePools = append(database.ResourcePools, schemamodel.ResourcePool{Name: node.Name, Spec: node.Spec.Clone()})
	case *ast.CreateResourcePoolClassifierNode:
		database.ResourcePoolClassifiers = append(database.ResourcePoolClassifiers, schemamodel.ResourcePoolClassifier{Name: node.Name, Spec: node.Spec})
	default:
		return appendYDBReplication(database, document, statement, sourcePlatform)
	}
	return true, nil
}
