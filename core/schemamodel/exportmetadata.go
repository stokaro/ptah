package schemamodel

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
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
// It is the shared census for every boundary that must either preserve this
// metadata or refuse it: HCL and OCI preserve the attributes, strict Atlas CE
// rejects the Ptah extension, and formats without a lossless spelling reject a
// conversion before producing output. Export-only model fields carry a
// ptah_export tag, and the census reads that tag rather than a parallel list.
// The model test requires every API-prefixed field to carry one, so a newly
// added attribute fails the guard until each boundary makes an explicit
// preservation decision (stokaro/ptah#2607).
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
	return taggedExportMetadata("table", table.QualifiedName(), reflect.ValueOf(table))
}

func fieldExportMetadata(field Field, db *Database) []ExportMetadata {
	name := fmt.Sprintf("%s.%s", fieldTableName(field, db), field.Name)
	return taggedExportMetadata("column", name, reflect.ValueOf(field))
}

func taggedExportMetadata(kind, name string, value reflect.Value) []ExportMetadata {
	fields := value.Type()
	found := make([]ExportMetadata, 0)
	for i := range fields.NumField() {
		field := fields.Field(i)
		attribute, tagged := field.Tag.Lookup("ptah_export")
		if !tagged {
			continue
		}
		if attribute == ",inline" {
			found = append(found, taggedExportMetadata(kind, name, value.Field(i))...)
			continue
		}
		if field.Type.Kind() != reflect.String {
			panic(fmt.Sprintf("schemamodel: ptah_export field %s.%s is not a string or inline struct", fields, field.Name))
		}
		if declared := value.Field(i).String(); declared != "" {
			found = append(found, ExportMetadata{
				Kind: kind, Name: name, Attribute: attribute, Value: declared,
			})
		}
	}
	return found
}

// TargetNameAttribute spells the annotation attribute a [TargetNames] field is
// authored as.
func TargetNameAttribute(fieldName string) string {
	field, ok := reflect.TypeFor[TargetNames]().FieldByName(fieldName)
	if !ok {
		return ""
	}
	return strings.TrimSpace(field.Tag.Get("ptah_export"))
}

func fieldTableName(field Field, db *Database) string {
	for _, table := range db.Tables {
		if table.StructName == field.StructName {
			return table.QualifiedName()
		}
	}
	return field.StructName
}
