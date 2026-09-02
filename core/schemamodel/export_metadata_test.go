package schemamodel_test

import (
	"reflect"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
)

func TestExportMetadataModelFieldsCarryCensusTags(t *testing.T) {
	for _, model := range []reflect.Type{
		reflect.TypeFor[schemamodel.Table](),
		reflect.TypeFor[schemamodel.Field](),
		reflect.TypeFor[schemamodel.TargetNames](),
	} {
		for field := range model.Fields() {
			if model != reflect.TypeFor[schemamodel.TargetNames]() &&
				!strings.HasPrefix(field.Name, "API") {
				continue
			}
			t.Run(model.Name()+"."+field.Name, func(t *testing.T) {
				c := qt.New(t)
				attribute, tagged := field.Tag.Lookup("ptah_export")
				c.Assert(tagged, qt.IsTrue)
				c.Assert(attribute, qt.Not(qt.Equals), "")
			})
		}
	}
}

// TestTargetNamesAreAllReported guards the tag-driven metadata census.
//
// A per-target name added to TargetNames and forgotten by the metadata census
// could cross a format boundary silently. This asserts every field of the
// struct has an authored spelling, so adding one without a ptah_export tag
// fails here rather than in somebody's contract.
func TestTargetNamesAreAllReported(t *testing.T) {
	fields := reflect.TypeFor[schemamodel.TargetNames]()

	for field := range fields.Fields() {
		t.Run(field.Name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemamodel.TargetNameAttribute(field.Name), qt.Not(qt.Equals), "")
		})
	}
}

// TestExportMetadataIn_ReportsEveryTargetName drives the same struct through the
// census, so a field with a spelling that is never read is caught as well.
func TestExportMetadataIn_ReportsEveryTargetName(t *testing.T) {
	fields := reflect.TypeFor[schemamodel.TargetNames]()

	for i := range fields.NumField() {
		t.Run(fields.Field(i).Name, func(t *testing.T) {
			c := qt.New(t)
			names := reflect.New(fields).Elem()
			names.Field(i).SetString("declared")
			database := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "U", Name: "users"}},
				Fields: []schemamodel.Field{{
					StructName: "U", Name: "a", Type: "TEXT",
					APINames: names.Interface().(schemamodel.TargetNames),
				}},
			}

			carried := schemamodel.ExportMetadataIn(database)

			c.Assert(carried, qt.HasLen, 1)
			c.Assert(carried[0].Attribute, qt.Equals, schemamodel.TargetNameAttribute(fields.Field(i).Name))
			c.Assert(carried[0].Value, qt.Equals, "declared")
		})
	}
}

// TestExportMetadataIn_ReportsNothingWithoutMetadata is the control for both:
// a census that returned an entry per column would satisfy them.
func TestExportMetadataIn_ReportsNothingWithoutMetadata(t *testing.T) {
	c := qt.New(t)

	carried := schemamodel.ExportMetadataIn(&schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "U", Name: "users"}},
		Fields: []schemamodel.Field{{StructName: "U", Name: "a", Type: "TEXT"}},
	})

	c.Assert(carried, qt.HasLen, 0)
}

func TestExportMetadataIn_ReportsEveryCurrentAttribute(t *testing.T) {
	c := qt.New(t)

	carried := schemamodel.ExportMetadataIn(&schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "U", Name: "users", APIName: "accounts",
			APINames: schemamodel.TargetNames{
				OpenAPI: "account_documents", GraphQL: "account_records", Protobuf: "account_records",
			},
		}},
		Fields: []schemamodel.Field{{
			StructName: "U", Name: "stored_amount", APIName: "amount",
			APINames: schemamodel.TargetNames{
				OpenAPI: "amount_value", GraphQL: "amountMinor", Protobuf: "amount_minor",
			},
			APIType: "TEXT", APIExpose: "read",
		}},
	})

	c.Assert(carried, qt.DeepEquals, []schemamodel.ExportMetadata{
		{Kind: "table", Name: "users", Attribute: "api_name", Value: "accounts"},
		{Kind: "table", Name: "users", Attribute: "graphql_name", Value: "account_records"},
		{Kind: "table", Name: "users", Attribute: "openapi_name", Value: "account_documents"},
		{Kind: "table", Name: "users", Attribute: "proto_name", Value: "account_records"},
		{Kind: "column", Name: "users.stored_amount", Attribute: "api_expose", Value: "read"},
		{Kind: "column", Name: "users.stored_amount", Attribute: "api_name", Value: "amount"},
		{Kind: "column", Name: "users.stored_amount", Attribute: "api_type", Value: "TEXT"},
		{Kind: "column", Name: "users.stored_amount", Attribute: "graphql_name", Value: "amountMinor"},
		{Kind: "column", Name: "users.stored_amount", Attribute: "openapi_name", Value: "amount_value"},
		{Kind: "column", Name: "users.stored_amount", Attribute: "proto_name", Value: "amount_minor"},
	})
}
