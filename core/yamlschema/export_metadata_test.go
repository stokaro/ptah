package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/core/yamlschema"
)

func TestParseExportMetadata(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse([]byte(`
tables:
  users:
    api_name: Account
    openapi_name: AccountDocument
    graphql_name: AccountNode
    proto_name: account_record
    columns:
      email_addr:
        type: TEXT
        api_name: email
        openapi_name: emailDocument
        graphql_name: emailNode
        proto_name: email_value
        api_type: string
        api_expose: read
`))

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

func TestParseExportMetadataRejectsUnknownAndNonScalarAttributes(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		wantErr string
	}{
		{
			name: "unknown attribute",
			source: `
tables:
  users:
    api_nam: Account
`,
			wantErr: `(?s).*field api_nam not found.*`,
		},
		{
			name: "non-scalar attribute",
			source: `
tables:
  users:
    columns:
      email:
        type: TEXT
        api_expose: [read]
`,
			wantErr: `(?s).*expected scalar.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := yamlschema.Parse([]byte(test.source))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
