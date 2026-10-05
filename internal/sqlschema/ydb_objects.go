package sqlschema

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbstream"
)

func appendYDBDeclaration(database, base *schemamodel.Database, statement ast.Node, sourcePlatform string) (bool, error) {
	switch node := statement.(type) {
	case *ydbstream.Node:
		return true, appendStreamingQuery(database, base, node)
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
