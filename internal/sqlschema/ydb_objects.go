package sqlschema

import (
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
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
	case *ast.CreateTopicNode:
		schema, name := normalizeSQLTableIdentifier(sourcePlatform, node.Name)
		database.Topics = append(database.Topics, schemamodel.Topic{Name: name, Schema: schema, Spec: node.Spec.Clone()})
	case *ast.CreateCoordinationNodeNode:
		schema, name := normalizeSQLTableIdentifier(sourcePlatform, node.Name)
		database.CoordinationNodes = append(database.CoordinationNodes, schemamodel.CoordinationNode{Name: name, Schema: schema, Spec: node.Spec})
	case *ast.CreateResourcePoolNode:
		database.ResourcePools = append(database.ResourcePools, schemamodel.ResourcePool{Name: node.Name, Spec: node.Spec.Clone()})
	case *ast.CreateResourcePoolClassifierNode:
		database.ResourcePoolClassifiers = append(database.ResourcePoolClassifiers, schemamodel.ResourcePoolClassifier{Name: node.Name, Spec: node.Spec})
	default:
		return false, nil
	}
	return true, nil
}
