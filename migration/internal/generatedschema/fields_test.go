package generatedschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/migration/internal/generatedschema"
)

func TestFieldsForTable_NilDatabase(t *testing.T) {
	c := qt.New(t)

	got := generatedschema.FieldsForTable(
		nil,
		goschema.Table{StructName: "User"},
	)

	c.Assert(got, qt.IsNil)
}

func TestFieldsForTable_DirectAndEmbeddedFields(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Fields: []goschema.Field{
			{StructName: "User", FieldName: "ID", Name: "id", Type: "BIGINT"},
			{
				StructName: "Timestamps",
				FieldName:  "CreatedAt",
				Name:       "created_at",
				Type:       "TIMESTAMP",
			},
			{
				StructName: "Audit",
				FieldName:  "Actor",
				Name:       "actor",
				Type:       "VARCHAR(64)",
			},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			{
				StructName:       "User",
				Mode:             "inline",
				Prefix:           "ts_",
				EmbeddedTypeName: "Timestamps",
			},
			{
				StructName:       "Timestamps",
				Mode:             "inline",
				Prefix:           "audit_",
				EmbeddedTypeName: "Audit",
			},
			{
				StructName:       "User",
				Mode:             "json",
				Name:             "metadata",
				Type:             "JSONB",
				Nullable:         true,
				Comment:          "User metadata",
				EmbeddedTypeName: "Metadata",
			},
			{
				StructName:       "User",
				Mode:             "relation",
				Field:            "company_id",
				Ref:              "companies(id)",
				OnDelete:         "CASCADE",
				OnUpdate:         "NO ACTION",
				Comment:          "Owning company",
				EmbeddedTypeName: "Company",
			},
			{
				StructName:       "User",
				Mode:             "skip",
				EmbeddedTypeName: "Ignored",
			},
		},
	}

	got := generatedschema.FieldsForTable(
		database,
		goschema.Table{StructName: "User"},
	)

	c.Assert(got, qt.DeepEquals, []goschema.Field{
		{StructName: "User", FieldName: "ID", Name: "id", Type: "BIGINT"},
		{
			StructName: "User",
			FieldName:  "CreatedAt",
			Name:       "ts_created_at",
			Type:       "TIMESTAMP",
		},
		{
			StructName: "User",
			FieldName:  "Actor",
			Name:       "ts_audit_actor",
			Type:       "VARCHAR(64)",
		},
		{
			StructName: "User",
			FieldName:  "Metadata",
			Name:       "metadata",
			Type:       "JSONB",
			Nullable:   true,
			Comment:    "User metadata",
		},
		{
			StructName: "User",
			FieldName:  "CompanyID",
			Name:       "company_id",
			Type:       "INTEGER",
			Foreign:    "companies(id)",
			OnDelete:   "CASCADE",
			OnUpdate:   "NO ACTION",
			Comment:    "Owning company",
			Overrides: map[string]map[string]string{
				"mysql":   {"type": "INT"},
				"mariadb": {"type": "INT"},
			},
		},
	})
}

func TestFieldsForTable_UnspecifiedModeIsInline(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Fields: []goschema.Field{
			{
				StructName: "Address",
				FieldName:  "City",
				Name:       "city",
				Type:       "VARCHAR(128)",
			},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			{
				StructName:       "User",
				Prefix:           "billing_",
				EmbeddedTypeName: "Address",
			},
		},
	}

	got := generatedschema.FieldsForTable(
		database,
		goschema.Table{StructName: "User"},
	)

	c.Assert(got, qt.DeepEquals, []goschema.Field{
		{
			StructName: "User",
			FieldName:  "City",
			Name:       "billing_city",
			Type:       "VARCHAR(128)",
		},
	})
}
