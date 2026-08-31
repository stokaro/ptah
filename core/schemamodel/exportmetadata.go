package schemamodel

import (
	"fmt"
	"reflect"
	"sort"
)

// ExportMetadata names one export-only attribute a declaration carries.
//
// "Export-only" means the attribute describes the API contract a schema
// projects rather than the storage it defines: [Table.APIName],
// [Field.APIType] and the rest steer the OpenAPI, GraphQL and Protobuf output
// and reach no DDL. A format that carries the storage faithfully can therefore
// still lose all of it.
type ExportMetadata struct {
	// Kind is the declaration the attribute sits on, "table" or "column".
	Kind string
	// Name identifies that declaration, qualified by its table for a column.
	Name string
	// Attribute is the authored spelling, so a diagnostic names the thing the
	// author would search their sources for.
	Attribute string
	// Value is what they wrote.
	Value string
}

// ExportMetadataIn names every export-only attribute the database declares, in
// a stable order.
//
// It exists because HCL has no spelling for any of them (stokaro/ptah#2607).
// Rendering a document that carries them is a real loss, and a loss nothing
// reports is what let `ptah schema export --to hcl --cleanup-go-annotations`
// delete the Go annotations holding an API contract and exit 0 -- measured, the
// published OpenAPI schema went from `AccountDoc` with property `emailDoc` and
// `readOnly: true` to `users` with property `email_addr` and no exposure, with
// nothing said and nowhere left to read the original.
func ExportMetadataIn(db *Database) []ExportMetadata {
	if db == nil {
		return nil
	}
	found := make([]ExportMetadata, 0)
	for _, table := range db.Tables {
		found = append(found, tableExportMetadata(table)...)
	}
	for _, field := range db.Fields {
		found = append(found, fieldExportMetadata(field, db)...)
	}
	sort.SliceStable(found, func(i, j int) bool {
		if found[i].Name != found[j].Name {
			return found[i].Name < found[j].Name
		}
		return found[i].Attribute < found[j].Attribute
	})
	return found
}

func tableExportMetadata(table Table) []ExportMetadata {
	name := table.QualifiedName()
	found := namedExportMetadata("table", name, map[string]string{
		"api_name": table.APIName,
	})
	return append(found, targetNameMetadata("table", name, table.APINames)...)
}

func fieldExportMetadata(field Field, db *Database) []ExportMetadata {
	name := fmt.Sprintf("%s.%s", fieldTableName(field, db), field.Name)
	found := namedExportMetadata("column", name, map[string]string{
		"api_name":   field.APIName,
		"api_type":   field.APIType,
		"api_expose": field.APIExpose,
	})
	return append(found, targetNameMetadata("column", name, field.APINames)...)
}

// targetNameMetadata reads [TargetNames] by reflection rather than by listing
// its fields.
//
// A per-target name that is added to the struct and forgotten here would be
// lost exactly the way every attribute in this file already was, and silently:
// the loss report is the only thing standing between such an attribute and a
// cleanup that deletes it. Reflection makes the list impossible to fall behind.
// TestTargetNamesAreAllReported holds the two spellings together.
func targetNameMetadata(kind, name string, names TargetNames) []ExportMetadata {
	values := reflect.ValueOf(names)
	fields := values.Type()
	declared := make(map[string]string, fields.NumField())
	for i := range fields.NumField() {
		declared[TargetNameAttribute(fields.Field(i).Name)] = values.Field(i).String()
	}
	return namedExportMetadata(kind, name, declared)
}

// TargetNameAttribute spells the annotation attribute a [TargetNames] field is
// authored as.
func TargetNameAttribute(fieldName string) string {
	switch fieldName {
	case "OpenAPI":
		return "openapi_name"
	case "GraphQL":
		return "graphql_name"
	case "Protobuf":
		return "proto_name"
	default:
		return ""
	}
}

func namedExportMetadata(kind, name string, attributes map[string]string) []ExportMetadata {
	found := make([]ExportMetadata, 0, len(attributes))
	for attribute, value := range attributes {
		if attribute == "" || value == "" {
			continue
		}
		found = append(found, ExportMetadata{
			Kind:      kind,
			Name:      name,
			Attribute: attribute,
			Value:     value,
		})
	}
	return found
}

func fieldTableName(field Field, db *Database) string {
	for _, table := range db.Tables {
		if table.StructName == field.StructName {
			return table.QualifiedName()
		}
	}
	return field.StructName
}
