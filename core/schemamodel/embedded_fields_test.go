package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
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

// fieldsOf is the fields of structName, in order.
func fieldsOf(fields []schemamodel.Field, structName string) []schemamodel.Field {
	var owned []schemamodel.Field
	for _, field := range fields {
		if field.StructName == structName {
			owned = append(owned, field)
		}
	}
	return owned
}

// StructFields gives each struct the fields ProcessEmbeddedFields gives it, in
// the same order: through inline, relation, JSON and skipped embeddings,
// nested ones, a prefix, a cycle, a column the struct declares itself, and an
// input that already holds generated fields, as a finalized schema does.
func TestStructFieldsIsTheStructsPartOfProcessEmbeddedFields(t *testing.T) {
	embedded := []schemamodel.EmbeddedField{
		{StructName: "Order", EmbeddedTypeName: "Ownership", Mode: "inline", Prefix: "owner_"},
		{StructName: "Order", EmbeddedTypeName: "Audit", Mode: "skip"},
		{StructName: "Ownership", EmbeddedTypeName: "Tenant", Mode: "relation", Field: "tenant_id", Ref: "tenants(id)"},
		{StructName: "Ownership", EmbeddedTypeName: "Metadata", Mode: "json", Name: "metadata"},
		{StructName: "Ownership", EmbeddedTypeName: "Cycle", Mode: "inline"},
		{StructName: "Cycle", EmbeddedTypeName: "Ownership", Mode: "inline"},
		{StructName: "User", EmbeddedTypeName: "UserMetadata", Mode: "json", Name: "metadata", Type: "JSONB"},
	}
	fields := []schemamodel.Field{
		{StructName: "Tenant", Name: "id", Type: "INTEGER", Primary: true},
		{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
		{StructName: "Order", Name: "owner_label", Type: "VARCHAR(10)"},
		{StructName: "Ownership", Name: "label", Type: "TEXT"},
		{StructName: "Cycle", Name: "code", Type: "TEXT"},
		{StructName: "User", Name: "metadata", Type: "JSONB", GeneratedFromEmbedded: true},
		{StructName: "Order", Name: "owner_code", Type: "TEXT", GeneratedFromEmbedded: true},
	}
	all := schemamodel.ProcessEmbeddedFields(embedded, fields)
	for _, structName := range []string{"Tenant", "Order", "Ownership", "Cycle", "User", "Missing"} {
		t.Run(structName, func(t *testing.T) {
			c := qt.New(t)

			got := schemamodel.StructFields(embedded, fields, structName)

			c.Assert(got, qt.DeepEquals, fieldsOf(all, structName))
		})
	}
}
