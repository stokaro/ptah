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
