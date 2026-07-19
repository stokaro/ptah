package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/atlashcl"
	"github.com/stokaro/ptah/core/atlashclrender"
	"github.com/stokaro/ptah/core/goschema"
)

func TestRenderTablesIndexesConstraintsAndDiagnostics(t *testing.T) {
	c := qt.New(t)
	falseValue := false
	db := &goschema.Database{
		Schemas: []goschema.Schema{{Name: "auth", Comment: "Authentication objects"}},
		Enums:   []goschema.Enum{{Name: "enum_user_status", Values: []string{"active", "disabled"}}},
		Tables: []goschema.Table{
			{StructName: "Account", Name: "accounts", Schema: "auth", PrimaryKey: []string{"id"}},
			{StructName: "Team", Name: "teams", Schema: "auth", PrimaryKey: []string{"id"}},
			{StructName: "User", Name: "users", Schema: "auth", Comment: "User accounts"},
		},
		Fields: []goschema.Field{
			{StructName: "Account", FieldName: "ID", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Team", FieldName: "ID", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", FieldName: "ID", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", FieldName: "AccountID", Name: "account_id", Type: "INTEGER", Foreign: "auth.accounts(id)", ForeignKeyName: "users_account_fk", OnDelete: "CASCADE"},
			{StructName: "User", FieldName: "TeamID", Name: "team_id", Type: "INTEGER", Foreign: "auth.teams(id)", OnUpdate: "CASCADE"},
			{StructName: "User", FieldName: "Status", Name: "status", Type: "enum_user_status", Default: "active", DefaultSet: true},
			{StructName: "User", FieldName: "CreatedAt", Name: "created_at", Type: "TIMESTAMP", DefaultExpr: "CURRENT_TIMESTAMP", Check: "created_at IS NOT NULL"},
		},
		Indexes: []goschema.Index{{
			StructName:     "User",
			TableName:      "auth.users",
			Name:           "users_status_idx",
			Fields:         []string{"status"},
			Unique:         true,
			NullsDistinct:  &falseValue,
			IncludeColumns: []string{"created_at"},
		}},
		Constraints: []goschema.Constraint{
			{StructName: "User", Name: "users_status_check", Type: "CHECK", Table: "auth.users", CheckExpression: "status <> ''"},
			{StructName: "User", Name: "users_account_fk", Type: "FOREIGN KEY", Table: "auth.users", Columns: []string{"account_id"}, ForeignTable: "auth.accounts", ForeignColumn: "id", OnDelete: "CASCADE"},
		},
		Functions: []goschema.Function{{Name: "set_tenant_context", Body: "BEGIN END;"}},
	}

	first, err := atlashclrender.Render(db)
	c.Assert(err, qt.IsNil)
	second, err := atlashclrender.Render(db)
	c.Assert(err, qt.IsNil)
	c.Assert(string(first.Data), qt.Equals, string(second.Data))
	c.Assert(string(first.Data), qt.Contains, `schema "auth"`)
	c.Assert(string(first.Data), qt.Contains, `table "users"`)
	c.Assert(string(first.Data), qt.Contains, `default = sql("CURRENT_TIMESTAMP")`)
	c.Assert(string(first.Data), qt.Contains, `foreign_key "users_account_fk"`)
	c.Assert(string(first.Data), qt.Contains, `foreign_key "fk_users_team_id"`)
	c.Assert(string(first.Data), qt.Contains, `check "users_created_at_check"`)
	c.Assert(string(first.Data), qt.Contains, `nulls_distinct = false`)
	c.Assert(first.Diagnostics, qt.HasLen, 1)
	c.Assert(first.Diagnostics[0].Path, qt.Equals, "functions")

	parsed, err := atlashcl.Parse(first.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", string(first.Data)))
	c.Assert(parsed.Tables, qt.HasLen, 3)
	c.Assert(parsed.Enums, qt.HasLen, 1)
	c.Assert(parsed.Constraints, qt.HasLen, 2)
	c.Assert(fieldByName(parsed.Fields, "account_id").Foreign, qt.Equals, "accounts(id)")
	c.Assert(fieldByName(parsed.Fields, "account_id").ForeignKeyName, qt.Equals, "users_account_fk")
	c.Assert(fieldByName(parsed.Fields, "team_id").Foreign, qt.Equals, "teams(id)")
	c.Assert(fieldByName(parsed.Fields, "team_id").OnUpdate, qt.Equals, "CASCADE")
}

func TestRenderCollapsesEmbeddedFieldsToConcreteColumns(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", FieldName: "CreatedAt", Name: "created_at", Type: "TIMESTAMP"},
			{StructName: "User", FieldName: "UpdatedAt", Name: "updated_at", Type: "TIMESTAMP"},
		},
		EmbeddedFields: []goschema.EmbeddedField{{StructName: "User", Mode: "inline", EmbeddedTypeName: "Timestamps"}},
	}

	result, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	hcl := string(result.Data)
	c.Assert(hcl, qt.Contains, `column "created_at"`)
	c.Assert(hcl, qt.Contains, `column "updated_at"`)
	c.Assert(hcl, qt.Not(qt.Contains), "embedded")
}

func fieldByName(fields []goschema.Field, name string) goschema.Field {
	for _, field := range fields {
		if field.Name == name {
			return field
		}
	}
	return goschema.Field{}
}
