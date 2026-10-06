package sqlschema

import (
	"maps"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

func appendYDBExternalDeclaration(database *schemamodel.Database, statement ast.Node) bool {
	switch node := statement.(type) {
	case *ast.CreateExternalDataSourceNode:
		ref, _ := tableref.Parse(node.Name)
		database.ExternalDataSources = append(database.ExternalDataSources, schemamodel.ExternalDataSource{
			Name: ref.Name, Schema: ref.Schema, SourceType: node.SourceType,
			Location: node.Location, AuthMethod: node.AuthMethod, Options: maps.Clone(node.Options),
		})
	case *ast.CreateExternalTableNode:
		ref, _ := tableref.Parse(node.Name)
		table := schemamodel.ExternalTable{
			Name: ref.Name, Schema: ref.Schema, DataSource: node.DataSource,
			Location: node.Location, Options: maps.Clone(node.Options),
		}
		for _, column := range node.Columns {
			table.Columns = append(table.Columns, schemamodel.ExternalColumn{
				Name: column.Name, Type: column.Type, NotNull: column.NotNull,
			})
		}
		database.ExternalTables = append(database.ExternalTables, table)
	default:
		return false
	}
	return true
}
