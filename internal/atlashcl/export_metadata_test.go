package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
)

func TestParseExportMetadata(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "public" {}

table "users" {
  schema       = schema.public
  api_name     = "Account"
  openapi_name = "AccountDocument"
  graphql_name = "AccountNode"
  proto_name   = "account_record"

  column "email_addr" {
    type         = text
    api_name     = "email"
    openapi_name = "emailDocument"
    graphql_name = "emailNode"
    proto_name   = "email_value"
    api_type     = "string"
    api_expose   = "read"
  }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].APIName, qt.Equals, "Account")
	c.Assert(db.Tables[0].APINames, qt.DeepEquals, schemamodel.TargetNames{
		OpenAPI:  "AccountDocument",
		GraphQL:  "AccountNode",
		Protobuf: "account_record",
	})
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].APIName, qt.Equals, "email")
	c.Assert(db.Fields[0].APINames, qt.DeepEquals, schemamodel.TargetNames{
		OpenAPI:  "emailDocument",
		GraphQL:  "emailNode",
		Protobuf: "email_value",
	})
	c.Assert(db.Fields[0].APIType, qt.Equals, "string")
	c.Assert(db.Fields[0].APIExpose, qt.Equals, "read")
}

func TestParseExportMetadataRejectsInvalidAttributes(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "table value is not a string",
			body:    `api_name = true`,
			wantErr: `(?s).*table attribute "api_name" must be a string.*`,
		},
		{
			name:    "unknown table attribute",
			body:    `api_nam = "Account"`,
			wantErr: `(?s).*unsupported table attribute "api_nam".*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := atlashcl.Parse([]byte("table \"users\" {\n"+test.body+"\n}\n"), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
