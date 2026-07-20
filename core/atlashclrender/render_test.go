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
	goschema.Finalize(db)

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
	c.Assert(string(first.Data), qt.Contains, `function "set_tenant_context"`)
	c.Assert(first.Diagnostics, qt.HasLen, 0)

	parsed, err := atlashcl.Parse(first.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", string(first.Data)))
	c.Assert(parsed.Tables, qt.HasLen, 3)
	c.Assert(parsed.Enums, qt.HasLen, 1)
	c.Assert(parsed.Constraints, qt.HasLen, 2)
	c.Assert(parsed.Functions, qt.HasLen, 1)
	c.Assert(fieldByName(parsed.Fields, "account_id").Foreign, qt.Equals, "accounts(id)")
	c.Assert(fieldByName(parsed.Fields, "account_id").ForeignKeyName, qt.Equals, "users_account_fk")
	c.Assert(fieldByName(parsed.Fields, "team_id").Foreign, qt.Equals, "teams(id)")
	c.Assert(fieldByName(parsed.Fields, "team_id").OnUpdate, qt.Equals, "CASCADE")
}

func TestRenderFixture023SchemaObjectsRoundTrip(t *testing.T) {
	c := qt.New(t)
	db, err := goschema.ParseDir("../../integration/fixtures/entities/023-go-annotations-objects")
	c.Assert(err, qt.IsNil)

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	hcl := string(rendered.Data)
	c.Assert(hcl, qt.Contains, `extension "pg_trgm"`)
	c.Assert(hcl, qt.Contains, `role "fixture_app_user"`)
	c.Assert(hcl, qt.Contains, `row_security {`)
	c.Assert(hcl, qt.Contains, `function "get_fixture_tenant_id"`)
	c.Assert(hcl, qt.Contains, `view "active_users"`)
	c.Assert(hcl, qt.Contains, `materialized "user_stats"`)
	c.Assert(hcl, qt.Contains, `trigger "users_set_updated_at"`)
	c.Assert(hcl, qt.Contains, `policy "users_tenant_policy"`)
	c.Assert(hcl, qt.Contains, `permission {`)
	c.Assert(diagnosticPaths(rendered.Diagnostics), qt.DeepEquals, []string{"extensions.pg_trgm", "rls_enabled_tables.users"})

	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", hcl))
	c.Assert(parsed.Extensions, qt.HasLen, 1)
	c.Assert(parsed.Extensions[0].Name, qt.Equals, "pg_trgm")
	c.Assert(parsed.Extensions[0].Comment, qt.Equals, "Fixture extension")
	c.Assert(parsed.Extensions[0].IfNotExists, qt.IsFalse)
	c.Assert(parsed.Functions, qt.HasLen, 1)
	c.Assert(parsed.Functions[0].Name, qt.Equals, "get_fixture_tenant_id")
	c.Assert(parsed.Functions[0].Returns, qt.Equals, "text")
	c.Assert(parsed.Functions[0].Language, qt.Equals, "sql")
	c.Assert(parsed.Functions[0].Body, qt.Equals, "SELECT current_setting('app.tenant_id', true)")
	c.Assert(parsed.Views, qt.HasLen, 1)
	c.Assert(parsed.Views[0].Name, qt.Equals, "active_users")
	c.Assert(parsed.Views[0].Body, qt.Equals, "SELECT id, email FROM users WHERE deleted_at IS NULL")
	c.Assert(parsed.MaterializedViews, qt.HasLen, 1)
	c.Assert(parsed.MaterializedViews[0].Name, qt.Equals, "user_stats")
	c.Assert(parsed.MaterializedViews[0].Body, qt.Equals, "SELECT COUNT(*) as cnt FROM users")
	c.Assert(parsed.Triggers, qt.HasLen, 1)
	c.Assert(parsed.Triggers[0].Name, qt.Equals, "users_set_updated_at")
	c.Assert(parsed.Triggers[0].Table, qt.Equals, "users")
	c.Assert(parsed.Triggers[0].Timing, qt.Equals, "BEFORE")
	c.Assert(parsed.Triggers[0].Event, qt.Equals, "UPDATE")
	c.Assert(parsed.Triggers[0].ForEach, qt.Equals, "ROW")
	c.Assert(parsed.Triggers[0].Body, qt.Equals, "NEW.updated_at = NOW(); RETURN NEW;")
	c.Assert(parsed.RLSPolicies, qt.HasLen, 1)
	c.Assert(parsed.RLSPolicies[0].Name, qt.Equals, "users_tenant_policy")
	c.Assert(parsed.RLSPolicies[0].Table, qt.Equals, "users")
	c.Assert(parsed.RLSPolicies[0].PolicyFor, qt.Equals, "SELECT")
	c.Assert(parsed.RLSPolicies[0].ToRoles, qt.Equals, "fixture_app_user")
	c.Assert(parsed.RLSPolicies[0].UsingExpression, qt.Equals, "get_fixture_tenant_id() IS NOT NULL")
	c.Assert(parsed.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(parsed.RLSEnabledTables[0].Table, qt.Equals, "users")
	c.Assert(parsed.Roles, qt.HasLen, 1)
	c.Assert(parsed.Roles[0].Name, qt.Equals, "fixture_app_user")
	c.Assert(parsed.Roles[0].Inherit, qt.IsTrue)
	c.Assert(parsed.Grants, qt.HasLen, 3)
}

func TestRenderReportsLossyObjectDetails(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Roles:  []goschema.Role{{Name: "app_user", Password: "secret"}},
		Functions: []goschema.Function{{
			Name:       "filter_user",
			Parameters: "OUT tenant_id text",
			Returns:    "text",
			Language:   "sql",
			Body:       "SELECT tenant_id",
		}},
		MaterializedViews: []goschema.MaterializedView{{
			Name:            "user_stats",
			Body:            "SELECT count(*) FROM users",
			RefreshStrategy: "concurrently",
		}},
		Triggers: []goschema.Trigger{{
			Name:   "bad_event",
			Table:  "users",
			Timing: "BEFORE",
			Event:  "ALTER",
			Body:   "RETURN NEW;",
		}},
		Grants: []goschema.Grant{{Role: "app_user", Privileges: []string{"SELECT"}}},
	}

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(diagnosticPaths(rendered.Diagnostics), qt.DeepEquals, []string{
		"role app_user",
		"function filter_user",
		"materialized_views.user_stats",
		"triggers.bad_event",
		"grants.app_user",
	})
	_, err = atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", string(rendered.Data)))
}

func TestRenderReportsPlatformOverrideDiagnostics(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "User",
			Name:       "users",
			Overrides: map[string]map[string]string{
				"mysql": {"engine": "InnoDB"},
			},
		}},
		Fields: []goschema.Field{{
			StructName: "User",
			FieldName:  "ID",
			Name:       "id",
			Type:       "SERIAL",
			Primary:    true,
			Overrides: map[string]map[string]string{
				"mysql": {"type": "INT AUTO_INCREMENT"},
			},
		}},
	}

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(diagnosticPaths(rendered.Diagnostics), qt.DeepEquals, []string{
		"table users",
		"column User.id",
	})
	c.Assert(string(rendered.Data), qt.Contains, `table "users"`)
	c.Assert(string(rendered.Data), qt.Contains, `column "id"`)
	_, err = atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", string(rendered.Data)))
}

func TestRenderPreservesQualifiedTargetsAndRoleInheritance(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Schemas: []goschema.Schema{{Name: "auth"}},
		Tables:  []goschema.Table{{StructName: "User", Name: "users", Schema: "auth"}},
		Roles: []goschema.Role{
			{Name: "inheriting", Inherit: true},
			{Name: "isolated", Inherit: false},
		},
		Triggers: []goschema.Trigger{{
			Name:    "users_touch",
			Table:   "auth.users",
			Timing:  "BEFORE",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    "RETURN NEW;",
		}},
		RLSPolicies: []goschema.RLSPolicy{{
			Name:            "users_policy",
			Table:           "auth.users",
			PolicyFor:       "SELECT",
			ToRoles:         "isolated",
			UsingExpression: "true",
		}},
		Grants: []goschema.Grant{{
			Role:       "isolated",
			Privileges: []string{"SELECT"},
			OnTable:    "auth.users",
		}},
	}
	goschema.Finalize(db)

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	hcl := string(rendered.Data)
	c.Assert(hcl, qt.Contains, `inherit = true`)
	c.Assert(hcl, qt.Contains, `inherit = false`)
	c.Assert(hcl, qt.Contains, `on = table.auth.users`)
	c.Assert(hcl, qt.Contains, `for = table.auth.users`)
	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", hcl))
	c.Assert(roleByName(parsed.Roles, "inheriting").Inherit, qt.IsTrue)
	c.Assert(roleByName(parsed.Roles, "isolated").Inherit, qt.IsFalse)
	c.Assert(parsed.Triggers[0].Table, qt.Equals, "auth.users")
	c.Assert(parsed.RLSPolicies[0].Table, qt.Equals, "auth.users")
	c.Assert(parsed.Grants[0].OnTable, qt.Equals, "auth.users")
}

func TestRenderSkipsIncompleteObjectsWithDiagnostics(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Functions:         []goschema.Function{{Name: "missing_body"}},
		Views:             []goschema.View{{Name: "missing_body"}},
		MaterializedViews: []goschema.MaterializedView{{Name: "missing_body"}},
		Triggers:          []goschema.Trigger{{Name: "missing_table", Timing: "BEFORE", Event: "UPDATE", Body: "RETURN NEW;"}},
		RLSPolicies:       []goschema.RLSPolicy{{Name: "missing_table"}},
	}

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(diagnosticPaths(rendered.Diagnostics), qt.DeepEquals, []string{
		"function missing_body",
		"views.missing_body",
		"materialized_views.missing_body",
		"triggers.missing_table",
		"rls_policies.missing_table",
	})
	_, err = atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", string(rendered.Data)))
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

func diagnosticPaths(diagnostics []atlashclrender.Diagnostic) []string {
	paths := make([]string, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		paths = append(paths, diagnostic.Path)
	}
	return paths
}

func roleByName(roles []goschema.Role, name string) goschema.Role {
	for _, role := range roles {
		if role.Name == name {
			return role
		}
	}
	return goschema.Role{}
}
