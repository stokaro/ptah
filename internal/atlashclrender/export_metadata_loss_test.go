package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

func exportMetadataDatabase() *schemamodel.Database {
	db := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Tables: []schemamodel.Table{{
			StructName: "U",
			Name:       "users",
			Schema:     "public",
			APIName:    "Account",
			APINames: schemamodel.TargetNames{
				OpenAPI:  "AccountDocument",
				GraphQL:  "AccountNode",
				Protobuf: "account_record",
			},
		}},
		Fields: []schemamodel.Field{{
			StructName: "U",
			FieldName:  "Email",
			Name:       "email_addr",
			Type:       "TEXT",
			APIName:    "email",
			APINames: schemamodel.TargetNames{
				OpenAPI:  "emailDocument",
				GraphQL:  "emailNode",
				Protobuf: "email_value",
			},
			APIType:   "string",
			APIExpose: "read",
		}},
	}
	schemamodel.Finalize(db)
	return db
}

func TestRender_ExportMetadataRoundTripsWithoutLoss(t *testing.T) {
	c := qt.New(t)

	first, err := atlashclrender.Render(exportMetadataDatabase())
	c.Assert(err, qt.IsNil)
	c.Assert(first.Diagnostics, qt.HasLen, 0)
	c.Assert(string(first.Data), qt.Contains, `api_name = "Account"`)
	c.Assert(string(first.Data), qt.Contains, `graphql_name = "emailNode"`)
	c.Assert(string(first.Data), qt.Contains, `api_expose = "read"`)

	parsed, err := atlashcl.Parse(first.Data, "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Tables, qt.HasLen, 1)
	c.Assert(parsed.Tables[0].APIName, qt.Equals, "Account")
	c.Assert(parsed.Tables[0].APINames, qt.DeepEquals, schemamodel.TargetNames{
		OpenAPI:  "AccountDocument",
		GraphQL:  "AccountNode",
		Protobuf: "account_record",
	})
	c.Assert(parsed.Fields, qt.HasLen, 1)
	c.Assert(parsed.Fields[0].APIName, qt.Equals, "email")
	c.Assert(parsed.Fields[0].APINames, qt.DeepEquals, schemamodel.TargetNames{
		OpenAPI:  "emailDocument",
		GraphQL:  "emailNode",
		Protobuf: "email_value",
	})
	c.Assert(parsed.Fields[0].APIType, qt.Equals, "string")
	c.Assert(parsed.Fields[0].APIExpose, qt.Equals, "read")

	second, err := atlashclrender.Render(parsed)
	c.Assert(err, qt.IsNil)
	c.Assert(second.Diagnostics, qt.HasLen, 0)
	c.Assert(second.Data, qt.DeepEquals, first.Data)
}

func TestRender_ASchemaWithoutExportMetadataReportsNothing(t *testing.T) {
	c := qt.New(t)
	db := exportMetadataDatabase()
	db.Tables[0].APIName = ""
	db.Tables[0].APINames = schemamodel.TargetNames{}
	db.Fields[0].APIName = ""
	db.Fields[0].APINames = schemamodel.TargetNames{}
	db.Fields[0].APIType = ""
	db.Fields[0].APIExpose = ""

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)
}
