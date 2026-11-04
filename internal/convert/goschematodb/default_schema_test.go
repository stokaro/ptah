package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
)

// TestWithDefaultSchemaBaseline_DescribesTheSchemaTheServerCreated converts a
// document and adds what the server gives the default schema of every
// database: on PostgreSQL, USAGE for PUBLIC on public and the comment the
// schema is created with. A privilege or a comment the document states itself
// replaces the server's.
func TestWithDefaultSchemaBaseline_DescribesTheSchemaTheServerCreated(t *testing.T) {
	usage := catalog.Grant{Role: "PUBLIC", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "public", Implicit: true}
	tests := []struct {
		name        string
		dialect     string
		db          *schemamodel.Database
		wantGrants  []catalog.Grant
		wantSchemas []catalog.Schema
	}{
		{
			name:        "a document that names no schema",
			dialect:     platform.Postgres,
			db:          &schemamodel.Database{},
			wantGrants:  []catalog.Grant{usage},
			wantSchemas: make([]catalog.Schema, 0),
		},
		{
			name:        "a document that declares public without a comment",
			dialect:     platform.Postgres,
			db:          &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "public"}, {Name: "app"}}},
			wantGrants:  []catalog.Grant{usage},
			wantSchemas: []catalog.Schema{{Name: "public", Comment: "standard public schema"}, {Name: "app"}},
		},
		{
			name:        "a document that comments public",
			dialect:     platform.Postgres,
			db:          &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "public", Comment: "mine"}}},
			wantGrants:  []catalog.Grant{usage},
			wantSchemas: []catalog.Schema{{Name: "public", Comment: "mine"}},
		},
		{
			name:    "a document that revokes the privilege",
			dialect: platform.Postgres,
			db: &schemamodel.Database{RevokedGrants: []schemamodel.Grant{
				{Role: "PUBLIC", Privileges: []string{"usage"}, OnSchema: "public"},
			}},
			wantSchemas: make([]catalog.Schema, 0),
		},
		{
			name:    "a document that grants it",
			dialect: platform.Postgres,
			db: &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "PUBLIC", Privileges: []string{"ALL"}, OnSchema: "public"},
			}},
			wantGrants:  []catalog.Grant{{Role: "PUBLIC", Privilege: "ALL", ObjectType: "SCHEMA", ObjectName: "public"}},
			wantSchemas: make([]catalog.Schema, 0),
		},
		{
			name:        "a revoke on another schema",
			dialect:     platform.Postgres,
			db:          &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "PUBLIC", Privileges: []string{"USAGE"}, OnSchema: "app"}}},
			wantGrants:  []catalog.Grant{usage},
			wantSchemas: make([]catalog.Schema, 0),
		},
		{
			name:        "another dialect",
			dialect:     platform.MySQL,
			db:          &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "public"}}},
			wantSchemas: []catalog.Schema{{Name: "public"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			converted := must.Must(goschematodb.ToDBSchema(t.Context(), test.db, test.dialect, must.Must(builtin.New())))

			got := goschematodb.WithDefaultSchemaBaseline(converted, test.db, test.dialect)

			c.Assert(got.Grants, qt.DeepEquals, test.wantGrants)
			c.Assert(got.Schemas, qt.DeepEquals, test.wantSchemas)
		})
	}
}

// TestWithDefaultSchemaBaseline_LeavesTheConvertedCatalogAlone pins that the
// catalog handed in keeps what the conversion made of the document.
func TestWithDefaultSchemaBaseline_LeavesTheConvertedCatalogAlone(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "public"}}}
	converted := must.Must(goschematodb.ToDBSchema(t.Context(), db, platform.Postgres, must.Must(builtin.New())))

	goschematodb.WithDefaultSchemaBaseline(converted, db, platform.Postgres)

	c.Assert(converted.Grants, qt.IsNil)
	c.Assert(converted.Schemas, qt.DeepEquals, []catalog.Schema{{Name: "public"}})
}
