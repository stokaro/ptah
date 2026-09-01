package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
)

func TestProcessEmbeddedFieldsKeepsOneConcreteColumn(t *testing.T) {
	c := qt.New(t)
	embedded := []schemamodel.EmbeddedField{{
		StructName:       "User",
		Mode:             "json",
		Name:             "metadata",
		Type:             "JSONB",
		EmbeddedTypeName: "UserMetadata",
	}}
	fields := []schemamodel.Field{{
		StructName: "User",
		FieldName:  "UserMetadata",
		Name:       "metadata",
		Type:       "JSONB",
	}}

	got := schemamodel.ProcessEmbeddedFields(embedded, fields)

	c.Assert(got, qt.DeepEquals, fields)
}

func TestProcessEmbeddedFieldsKeepsFinalizedGeneratedColumn(t *testing.T) {
	c := qt.New(t)
	embedded := []schemamodel.EmbeddedField{{
		StructName:       "User",
		Mode:             "json",
		Name:             "metadata",
		Type:             "JSONB",
		EmbeddedTypeName: "UserMetadata",
	}}
	fields := []schemamodel.Field{{
		StructName:            "User",
		FieldName:             "UserMetadata",
		Name:                  "metadata",
		Type:                  "JSONB",
		GeneratedFromEmbedded: true,
	}}

	got := schemamodel.ProcessEmbeddedFields(embedded, fields)

	c.Assert(got, qt.DeepEquals, fields)
}

func TestProcessEmbeddedFieldsPreservesGeneratedConflictsForValidation(t *testing.T) {
	c := qt.New(t)
	embedded := []schemamodel.EmbeddedField{
		{StructName: "User", Mode: "inline", EmbeddedTypeName: "TextMetadata"},
		{StructName: "User", Mode: "inline", EmbeddedTypeName: "UUIDMetadata"},
	}
	fields := []schemamodel.Field{
		{StructName: "TextMetadata", Name: "code", Type: "TEXT"},
		{StructName: "UUIDMetadata", Name: "code", Type: "UUID"},
	}

	got := schemamodel.ProcessEmbeddedFields(embedded, fields)
	generated := fieldsNamed(got, "User", "code")
	c.Assert(generated, qt.HasLen, 2)
	c.Assert(generated[0].Type, qt.Equals, "TEXT")
	c.Assert(generated[1].Type, qt.Equals, "UUID")
}

func fieldsNamed(fields []schemamodel.Field, structName, name string) []schemamodel.Field {
	var matched []schemamodel.Field
	for _, field := range fields {
		if field.StructName == structName && field.Name == name {
			matched = append(matched, field)
		}
	}
	return matched
}
