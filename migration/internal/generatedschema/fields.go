// Package generatedschema provides reusable views over generated schema data.
package generatedschema

import "github.com/stokaro/ptah/core/goschema"

// FieldsForTable returns direct and embedded columns for table.
func FieldsForTable(
	database *goschema.Database,
	table goschema.Table,
) []goschema.Field {
	if database == nil {
		return nil
	}
	fields := make([]goschema.Field, 0)
	for _, field := range database.Fields {
		if field.StructName == table.StructName {
			fields = append(fields, field)
		}
	}
	generated := embeddedFieldsForStruct(
		database.EmbeddedFields,
		database.Fields,
		table.StructName,
	)
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		seen[field.Name] = struct{}{}
	}
	for _, field := range generated {
		if _, exists := seen[field.Name]; exists {
			continue
		}
		seen[field.Name] = struct{}{}
		fields = append(fields, field)
	}
	return fields
}

func embeddedFieldsForStruct(
	embeddedFields []goschema.EmbeddedField,
	allFields []goschema.Field,
	structName string,
) []goschema.Field {
	var generatedFields []goschema.Field
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
	embedded goschema.EmbeddedField,
	structName string,
) goschema.Field {
	return goschema.Field{
		StructName: structName,
		FieldName:  embedded.EmbeddedTypeName,
		Name:       embedded.Name,
		Type:       embedded.Type,
		Nullable:   embedded.Nullable,
		Comment:    embedded.Comment,
	}
}

func relationEmbeddedField(
	embedded goschema.EmbeddedField,
	structName string,
) goschema.Field {
	return goschema.Field{
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
	generatedFields []goschema.Field,
	embedded goschema.EmbeddedField,
	allFields []goschema.Field,
	allEmbeddedFields []goschema.EmbeddedField,
	structName string,
) []goschema.Field {
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
