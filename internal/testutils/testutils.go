// Package testutils provides canned goschema fixtures and small lookup helpers
// shared by dbschema-related unit tests.
package testutils

import (
	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
)

// CreateTestParseResult creates a minimal PackageParseResult for testing
func CreateTestParseResult() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "test_table", StructName: "TestTable"},
		},
		Fields: []schemamodel.Field{
			{Name: "id", Type: "int", StructName: "TestTable"},
			{Name: "name", Type: "string", StructName: "TestTable"},
		},
		Indexes: make([]schemamodel.Index, 0),
		Enums: []schemamodel.Enum{
			{Name: "test_status", Values: []string{"active", "inactive"}},
		},
		EmbeddedFields: make([]schemamodel.EmbeddedField, 0),
	}
}

func FindColumn(columns []catalog.Column, name string) *catalog.Column {
	for i := range columns {
		if columns[i].Name == name {
			return &columns[i]
		}
	}
	return nil
}
