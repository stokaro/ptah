// Package generatedschema provides reusable views over generated schema data.
package generatedschema

import (
	"go.5x5.cz/ptah/core/schemamodel"
)

// FieldsForTable returns direct and embedded columns for table.
func FieldsForTable(
	database *schemamodel.Database,
	table schemamodel.Table,
) []schemamodel.Field {
	if database == nil {
		return nil
	}
	fields := make([]schemamodel.Field, 0)
	for _, field := range database.Fields {
		if field.StructName == table.StructName {
			fields = append(fields, field)
		}
	}
	desired := embeddedFieldsForStruct(
		database.EmbeddedFields,
		database.Fields,
		table.StructName,
	)
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		seen[field.Name] = struct{}{}
	}
	for _, field := range desired {
		if _, exists := seen[field.Name]; exists {
			continue
		}
		seen[field.Name] = struct{}{}
		fields = append(fields, field)
	}
	return fields
}

func embeddedFieldsForStruct(
	embeddedFields []schemamodel.EmbeddedField,
	allFields []schemamodel.Field,
	structName string,
) []schemamodel.Field {
	var generatedFields []schemamodel.Field
	for _, embedded := range embeddedFields {
		if embedded.StructName != structName {
			continue
		}
		switch embedded.Mode {
		case "json":
			generatedFields = append(
				generatedFields,
				jsonEmbeddedField(embedded, structName),
			)
		case "relation":
			generatedFields = append(
				generatedFields,
				relationEmbeddedField(embedded, structName),
			)
		case "skip":
			continue
		default:
			generatedFields = appendInlineEmbeddedFields(
				generatedFields,
				embedded,
				allFields,
				embeddedFields,
				structName,
			)
		}
	}
	return generatedFields
}

func jsonEmbeddedField(
	embedded schemamodel.EmbeddedField,
	structName string,
) schemamodel.Field {
	return schemamodel.Field{
		StructName: structName,
		FieldName:  embedded.EmbeddedTypeName,
		Name:       embedded.Name,
		Type:       embedded.Type,
		Nullable:   embedded.Nullable,
		Comment:    embedded.Comment,
	}
}

func relationEmbeddedField(
	embedded schemamodel.EmbeddedField,
	structName string,
) schemamodel.Field {
	return schemamodel.Field{
		StructName: structName,
		FieldName:  embedded.EmbeddedTypeName + "ID",
		Name:       embedded.Field,
		Type:       "INTEGER",
		Foreign:    embedded.Ref,
		OnDelete:   embedded.OnDelete,
		OnUpdate:   embedded.OnUpdate,
		Comment:    embedded.Comment,
		Overrides: map[string]map[string]string{
			"mysql":   {"type": "INT"},
			"mariadb": {"type": "INT"},
		},
	}
}

func appendInlineEmbeddedFields(
	generatedFields []schemamodel.Field,
	embedded schemamodel.EmbeddedField,
	allFields []schemamodel.Field,
	allEmbeddedFields []schemamodel.EmbeddedField,
	structName string,
) []schemamodel.Field {
	for _, field := range allFields {
		if field.StructName != embedded.EmbeddedTypeName {
			continue
		}
		newField := field
		newField.StructName = structName
		if embedded.Prefix != "" {
			newField.Name = embedded.Prefix + field.Name
		}
		generatedFields = append(generatedFields, newField)
	}

	for _, nested := range allEmbeddedFields {
		if nested.StructName != embedded.EmbeddedTypeName ||
			nested.Mode != "inline" {
			continue
		}
		nested.StructName = structName
		nested.Prefix = embedded.Prefix + nested.Prefix
		generatedFields = appendInlineEmbeddedFields(
			generatedFields,
			nested,
			allFields,
			allEmbeddedFields,
			structName,
		)
	}
	return generatedFields
}
