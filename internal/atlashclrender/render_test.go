package atlashclrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

func TestRenderColumnUniqueExprAndIdentityOptionsRoundTrip(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{
				StructName:         "T",
				Name:               "id",
				Type:               "bigint",
				IdentityGeneration: "ALWAYS",
				IdentityOptions:    "START WITH 100 INCREMENT BY 5",
			},
			{
				StructName: "T",
				Name:       "email",
				Type:       "text",
				UniqueExpr: "lower(email)",
			},
		},
	}
	goschema.Finalize(db)

	rendered, err := atlashclrender.Render(db)
	c.Assert(err, qt.IsNil)
	hcl := string(rendered.Data)
	c.Assert(hcl, qt.Contains, `options = "START WITH 100 INCREMENT BY 5"`)
	c.Assert(hcl, qt.Contains, `unique_expr = "lower(email)"`)

	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", hcl))
	c.Assert(fieldByName(parsed.Fields, "id").IdentityOptions, qt.Equals, "START WITH 100 INCREMENT BY 5")
	c.Assert(fieldByName(parsed.Fields, "email").UniqueExpr, qt.Equals, "lower(email)")
}

func TestRenderPrimaryKeyColumnIsNotNullable(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "User",
			Name:       "users",
			PrimaryKey: []string{"id"},
		}},
		Fields: []goschema.Field{{
			StructName: "User",
			Name:       "id",
			Type:       "BIGINT",
			Nullable:   true,
		}},
	}

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(string(rendered.Data), qt.Not(qt.Contains), "null = true")
	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Fields, qt.HasLen, 1)
	c.Assert(parsed.Fields[0].Nullable, qt.IsFalse)
}

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
			{StructName: "User", Name: "users_email_key", Type: "UNIQUE", Table: "auth.users", Columns: []string{"account_id"}, IncludeColumns: []string{"created_at"}, NullsDistinct: &falseValue},
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
	c.Assert(string(first.Data), qt.Contains, `check "users_status_check"`)
	c.Assert(string(first.Data), qt.Contains, `unique "users_email_key"`)
	c.Assert(string(first.Data), qt.Contains, `foreign_key "fk_users_team_id"`)
	c.Assert(string(first.Data), qt.Contains, `check = "created_at IS NOT NULL"`)
	c.Assert(string(first.Data), qt.Contains, `include = [column.created_at]`)
	c.Assert(string(first.Data), qt.Contains, `nulls_distinct = false`)
	c.Assert(string(first.Data), qt.Contains, `function "set_tenant_context"`)
	c.Assert(strings.Count(string(first.Data), `foreign_key "users_account_fk"`), qt.Equals, 1)
	c.Assert(first.Diagnostics, qt.HasLen, 0)

	parsed, err := atlashcl.Parse(first.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", string(first.Data)))
	c.Assert(parsed.Tables, qt.HasLen, 3)
	c.Assert(parsed.Enums, qt.HasLen, 1)
	c.Assert(parsed.Constraints, qt.HasLen, 2)
	c.Assert(parsed.Functions, qt.HasLen, 1)
	c.Assert(constraintByName(parsed.Constraints, "users_email_key").IncludeColumns, qt.DeepEquals, []string{"created_at"})
	// `accounts`, not `auth.accounts`: the reference names the `accounts` block,
	// which the same document declares in `auth`, so the schema is written once
	// on that block instead of on every reference to it. The declaring table is
	// in `auth` too, which is what makes the short name unambiguous to every
	// reader of this IR -- tablelookup.ResolveReference, the one the DDL path
	// calls, resolves it to `auth.accounts` from these same tables. Only a
	// reference the document cannot resolve keeps its schema; see
	// TestRenderKeepsQualifiedReferenceWhenTargetTableIsAbsent.
	c.Assert(fieldByName(parsed.Fields, "account_id").Foreign, qt.Equals, "accounts(id)")
	c.Assert(fieldByName(parsed.Fields, "account_id").ForeignKeyName, qt.Equals, "users_account_fk")
	c.Assert(fieldByName(parsed.Fields, "team_id").Foreign, qt.Equals, "teams(id)")
	c.Assert(fieldByName(parsed.Fields, "team_id").OnUpdate, qt.Equals, "CASCADE")
}

func TestRender_ExplicitTableReferencePreservesStructuralIdentity(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
		},
		Fields: []goschema.Field{
			{StructName: "Literal", Name: "literal_id", Type: "INTEGER"},
			{StructName: "Qualified", Name: "qualified_id", Type: "INTEGER"},
		},
		Indexes: []goschema.Index{
			{
				StructName: "Literal",
				Name:       "literal_lookup",
				TableName:  `"tenant.data"`,
				Fields:     []string{"literal_id"},
			},
			{
				StructName: "Literal",
				Name:       "qualified_lookup",
				TableName:  "tenant.data",
				Fields:     []string{"qualified_id"},
			},
		},
		Constraints: []goschema.Constraint{
			{
				StructName:    "Literal",
				Name:          "literal_self_fk",
				Type:          "FOREIGN KEY",
				Table:         `"tenant.data"`,
				Columns:       []string{"literal_id"},
				ForeignTable:  `"tenant.data"`,
				ForeignColumn: "literal_id",
			},
			{
				StructName:    "Qualified",
				Name:          "qualified_self_fk",
				Type:          "FOREIGN KEY",
				Table:         "tenant.data",
				Columns:       []string{"qualified_id"},
				ForeignTable:  "tenant.data",
				ForeignColumn: "qualified_id",
			},
		},
		Triggers: []goschema.Trigger{
			{
				Name:    "literal_trigger",
				Table:   `"tenant.data"`,
				Timing:  "AFTER",
				Event:   "INSERT",
				ForEach: "ROW",
				Body:    "SELECT 1",
			},
			{
				Name:    "qualified_trigger",
				Table:   "tenant.data",
				Timing:  "AFTER",
				Event:   "INSERT",
				ForEach: "ROW",
				Body:    "SELECT 1",
			},
		},
		Functions: []goschema.Function{
			{Name: `"tenant.data"`, Returns: "integer", Language: "sql", Body: "SELECT 1"},
			{Name: "tenant.data", Returns: "integer", Language: "sql", Body: "SELECT 1"},
		},
		Views: []goschema.View{
			{Name: `"tenant.data"`, Body: "SELECT 1"},
			{Name: "tenant.data", Body: "SELECT 1"},
		},
		MaterializedViews: []goschema.MaterializedView{
			{Name: `"tenant.data"`, Body: "SELECT 1"},
			{Name: "tenant.data", Body: "SELECT 1"},
		},
	}

	rendered, err := atlashclrender.Render(db)
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", string(rendered.Data)))

	c.Assert(parsed.Tables, qt.HasLen, 2)
	c.Assert(parsed.Tables[0].QualifiedName(), qt.Equals, `"tenant.data"`)
	c.Assert(parsed.Tables[1].QualifiedName(), qt.Equals, "tenant.data")
	c.Assert(parsed.Fields, qt.HasLen, 2)
	c.Assert(parsed.Fields[0].StructName, qt.Equals, `"tenant.data"`)
	c.Assert(parsed.Fields[1].StructName, qt.Equals, "tenant.data")
	c.Assert(parsed.Indexes, qt.HasLen, 2)
	c.Assert(parsed.Indexes[0].TableName, qt.Equals, `"tenant.data"`)
	c.Assert(parsed.Indexes[1].TableName, qt.Equals, "tenant.data")
	// The two identities stay apart, which is what this test is for. The literal
	// name keeps its quotes and its dot -- it is one name, not a qualification,
	// and nothing shortens it. The qualified one loses the schema from the
	// REFERENCE, because the `data` block the document declares carries it and
	// the declaring table is in `tenant` as well; what it must not do is come
	// back looking like the literal one.
	c.Assert(fieldByName(parsed.Fields, "literal_id").Foreign, qt.Equals, `"tenant.data"(literal_id)`)
	c.Assert(fieldByName(parsed.Fields, "qualified_id").Foreign, qt.Equals, "data(qualified_id)")
	c.Assert(parsed.Triggers, qt.HasLen, 2)
	c.Assert(parsed.Triggers[0].Table, qt.Equals, `"tenant.data"`)
	c.Assert(parsed.Triggers[1].Table, qt.Equals, "tenant.data")
	c.Assert(parsed.Functions, qt.HasLen, 2)
	c.Assert(parsed.Functions[0].Name, qt.Equals, `"tenant.data"`)
	c.Assert(parsed.Functions[1].Name, qt.Equals, "tenant.data")
	c.Assert(parsed.Views, qt.HasLen, 2)
	c.Assert(parsed.Views[0].Name, qt.Equals, `"tenant.data"`)
	c.Assert(parsed.Views[1].Name, qt.Equals, "tenant.data")
	c.Assert(parsed.MaterializedViews, qt.HasLen, 2)
	c.Assert(parsed.MaterializedViews[0].Name, qt.Equals, `"tenant.data"`)
	c.Assert(parsed.MaterializedViews[1].Name, qt.Equals, "tenant.data")
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
	c.Assert(diagnosticPaths(rendered.Diagnostics), qt.HasLen, 0)

	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", hcl))
	c.Assert(parsed.Extensions, qt.HasLen, 1)
	c.Assert(parsed.Extensions[0].Name, qt.Equals, "pg_trgm")
	c.Assert(parsed.Extensions[0].Comment, qt.Equals, "Fixture extension")
	c.Assert(parsed.Extensions[0].IfNotExists, qt.IsTrue)
	c.Assert(parsed.Sequences, qt.HasLen, 1)
	c.Assert(parsed.Sequences[0].Name, qt.Equals, "fixture_order_seq")
	c.Assert(parsed.Domains, qt.HasLen, 1)
	c.Assert(parsed.Domains[0].Name, qt.Equals, "fixture_email")
	c.Assert(parsed.CompositeTypes, qt.HasLen, 1)
	c.Assert(parsed.CompositeTypes[0].Name, qt.Equals, "fixture_address")
	c.Assert(parsed.Ranges, qt.HasLen, 1)
	c.Assert(parsed.Ranges[0].Name, qt.Equals, "fixture_floatrange")
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
	c.Assert(parsed.RLSEnabledTables[0].Comment, qt.Equals, "Enable RLS for fixture users")
	c.Assert(parsed.Roles, qt.HasLen, 1)
	c.Assert(parsed.Roles[0].Name, qt.Equals, "fixture_app_user")
	c.Assert(parsed.Roles[0].Inherit, qt.IsTrue)
	c.Assert(parsed.Grants, qt.HasLen, 4)
	c.Assert(parsed.ManagedData, qt.HasLen, 1)
	c.Assert(parsed.ManagedData[0].Table, qt.Equals, "users")
}

func TestRenderPreservesSensitiveAndComplexValuesWhileReportingInvalidObjects(t *testing.T) {
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
		`triggers["users"]["bad_event"]`,
		"grants.app_user",
	})
	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", string(rendered.Data)))
	c.Assert(parsed.Roles, qt.HasLen, 1)
	c.Assert(parsed.Roles[0].Password, qt.Equals, "secret")
	c.Assert(parsed.Functions, qt.HasLen, 1)
	c.Assert(parsed.Functions[0].Parameters, qt.Equals, "out tenant_id text")
}

func TestRenderPreservesTableChecksAndManagedDataWhileReportingOrphans(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "User",
			Name:       "users",
			Checks:     []string{"id > 0"},
		}},
		ManagedData: []goschema.ManagedData{{
			StructName: "User",
			Table:      "users",
			Keys:       []string{"id"},
			File:       "users.yaml",
		}},
		Indexes: []goschema.Index{{
			Name:      "missing_table_idx",
			TableName: "missing",
			Fields:    []string{"id"},
		}},
		Constraints: []goschema.Constraint{{
			Name:    "missing_table_check",
			Table:   "missing",
			Type:    "CHECK",
			Columns: []string{"id"},
		}},
	}

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{
		{
			Severity: atlashclrender.SeverityWarning,
			Path:     "index missing_table_idx",
			Message:  "index cannot be rendered because the target table is absent from the exported schema",
		},
		{
			Severity: atlashclrender.SeverityWarning,
			Path:     "constraint missing_table_check",
			Message:  "constraint cannot be rendered because the target table is absent from the exported schema",
		},
	})
	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", string(rendered.Data)))
	c.Assert(parsed.Tables, qt.HasLen, 1)
	c.Assert(parsed.Tables[0].Checks, qt.DeepEquals, []string{"id > 0"})
	c.Assert(parsed.ManagedData, qt.HasLen, 1)
	c.Assert(parsed.ManagedData[0].Keys, qt.DeepEquals, []string{"id"})
	c.Assert(parsed.ManagedData[0].File, qt.Equals, "users.yaml")
}

func TestRenderMaterializedViewRefreshStrategyRoundTrip(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name:            "user_stats",
			Body:            "SELECT count(*) FROM users",
			RefreshStrategy: "CONCURRENTLY",
		}},
	}

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(diagnosticPaths(rendered.Diagnostics), qt.HasLen, 0)
	hcl := string(rendered.Data)
	c.Assert(hcl, qt.Contains, `refresh_strategy = "concurrently"`)

	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", hcl))
	c.Assert(parsed.MaterializedViews, qt.HasLen, 1)
	c.Assert(parsed.MaterializedViews[0].RefreshStrategy, qt.Equals, "concurrently")
}

func TestRenderMaterializedViewManualStrategyOmitsAttribute(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name: "user_stats",
			Body: "SELECT count(*) FROM users",
		}},
	}

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(diagnosticPaths(rendered.Diagnostics), qt.HasLen, 0)
	c.Assert(string(rendered.Data), qt.Not(qt.Contains), "refresh_strategy")
}

func TestRenderIndexCommentRoundTrip(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{{StructName: "User", FieldName: "Email", Name: "email", Type: "text"}},
		Indexes: []goschema.Index{{
			StructName: "User",
			TableName:  "users",
			Name:       "idx_users_email",
			Fields:     []string{"email"},
			Comment:    "lookup by email",
		}},
	}
	goschema.Finalize(db)

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(diagnosticPaths(rendered.Diagnostics), qt.HasLen, 0)
	hcl := string(rendered.Data)
	c.Assert(hcl, qt.Contains, `comment = "lookup by email"`)

	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", hcl))
	c.Assert(parsed.Indexes, qt.HasLen, 1)
	c.Assert(parsed.Indexes[0].Comment, qt.Equals, "lookup by email")
}

func TestRenderIndexGranularityRoundTrip(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Event", Name: "events"}},
		Fields: []goschema.Field{{StructName: "Event", FieldName: "Payload", Name: "payload", Type: "String"}},
		Indexes: []goschema.Index{{
			StructName:  "Event",
			TableName:   "events",
			Name:        "idx_events_payload",
			Fields:      []string{"payload"},
			Type:        "bloom_filter",
			Granularity: 64,
		}},
	}
	goschema.Finalize(db)

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(diagnosticPaths(rendered.Diagnostics), qt.HasLen, 0)
	hcl := string(rendered.Data)
	c.Assert(hcl, qt.Contains, `granularity = 64`)

	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", hcl))
	c.Assert(parsed.Indexes, qt.HasLen, 1)
	c.Assert(parsed.Indexes[0].Granularity, qt.Equals, 64)
}

func TestRenderIndexZeroGranularityOmitsAttribute(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Event", Name: "events"}},
		Fields: []goschema.Field{{StructName: "Event", FieldName: "Payload", Name: "payload", Type: "String"}},
		Indexes: []goschema.Index{{
			StructName: "Event",
			TableName:  "events",
			Name:       "idx_events_payload",
			Fields:     []string{"payload"},
		}},
	}
	goschema.Finalize(db)

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(diagnosticPaths(rendered.Diagnostics), qt.HasLen, 0)
	c.Assert(string(rendered.Data), qt.Not(qt.Contains), "granularity")
}

func TestRenderColumnParityExtensionsRoundTrip(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Enums: []goschema.Enum{{
			Name:   "enum_user_status",
			Values: []string{"active", "disabled"},
		}},
		Tables: []goschema.Table{{
			StructName: "User",
			Name:       "users",
			Overrides: map[string]map[string]string{
				"mysql": {"engine": "InnoDB"},
			},
		}},
		Fields: []goschema.Field{
			{
				StructName: "User",
				FieldName:  "ID",
				Name:       "id",
				Type:       "SERIAL",
				Primary:    true,
				Overrides: map[string]map[string]string{
					"mysql": {"type": "INT AUTO_INCREMENT"},
				},
			},
			{
				StructName: "User",
				FieldName:  "Status",
				Name:       "status",
				Type:       "enum_user_status",
				Enum:       []string{"active", "disabled"},
			},
		},
	}

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(diagnosticPaths(rendered.Diagnostics), qt.HasLen, 0)
	c.Assert(string(rendered.Data), qt.Contains, `table "users"`)
	c.Assert(string(rendered.Data), qt.Contains, `column "id"`)
	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", string(rendered.Data)))
	c.Assert(parsed.Tables, qt.HasLen, 1)
	c.Assert(parsed.Tables[0].Overrides, qt.DeepEquals, db.Tables[0].Overrides)
	c.Assert(parsed.Fields, qt.HasLen, 2)
	c.Assert(parsed.Fields[0].Overrides, qt.DeepEquals, db.Fields[0].Overrides)
	c.Assert(parsed.Fields[1].Enum, qt.DeepEquals, db.Fields[1].Enum)
}

func TestRenderStringTemplateIntroducersRoundTrip(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "Template",
			Name:       "templates",
			Comment:    "literal ${tenant} and %{if enabled}",
		}},
		Fields: []goschema.Field{{
			StructName: "Template",
			FieldName:  "Body",
			Name:       "body",
			Type:       "TEXT",
			Default:    "${literal}",
			DefaultSet: true,
		}},
		Roles: []goschema.Role{{Name: "role${literal}"}},
		Grants: []goschema.Grant{{
			Role:       "role${literal}",
			Privileges: []string{"SELECT"},
			OnTable:    "templates",
		}},
	}

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)
	c.Assert(string(rendered.Data), qt.Contains, `$${tenant}`)
	c.Assert(string(rendered.Data), qt.Contains, `%%{if enabled}`)
	c.Assert(string(rendered.Data), qt.Contains, `$${literal}`)
	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Tables[0].Comment, qt.Equals, db.Tables[0].Comment)
	c.Assert(parsed.Fields[0].Default, qt.Equals, db.Fields[0].Default)
	c.Assert(parsed.Roles[0].Name, qt.Equals, db.Roles[0].Name)
	c.Assert(parsed.Grants[0].Role, qt.Equals, db.Grants[0].Role)
}

func TestRenderQuotedIdentifiersRoundTrip(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Schemas: []goschema.Schema{{Name: "tenant.data"}},
		Tables: []goschema.Table{{
			StructName: "Record",
			Name:       "user-records",
			Schema:     "tenant.data",
			PrimaryKey: []string{"tenant-id"},
		}},
		Fields: []goschema.Field{
			{StructName: "Record", FieldName: "TenantID", Name: "tenant-id", Type: "TEXT"},
			{StructName: "Record", FieldName: "DisplayName", Name: "display.name", Type: "TEXT"},
		},
		Indexes: []goschema.Index{{
			StructName: "Record",
			Name:       "display-name-index",
			Fields:     []string{"display.name"},
		}},
	}

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)
	hcl := string(rendered.Data)
	c.Assert(hcl, qt.Contains, `schema["tenant.data"]`)
	c.Assert(hcl, qt.Contains, `column["tenant-id"]`)
	c.Assert(hcl, qt.Contains, `column["display.name"]`)
	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Tables[0].Schema, qt.Equals, "tenant.data")
	c.Assert(parsed.Tables[0].PrimaryKey, qt.DeepEquals, []string{"tenant-id"})
	c.Assert(parsed.Indexes[0].Fields, qt.DeepEquals, []string{"display.name"})
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
	// The reference names the `users` block, which this document declares, and a
	// block is named by its label alone. `table.auth.users` is what the pinned
	// Atlas community binary v1.3.0 refuses with `Unsupported attribute; This
	// object does not have an attribute named "auth"` -- measured with the
	// target in the SAME schema as the referring table too, so this is not a
	// cross-schema special case. What has to survive is the IR below, and it
	// does: the schema is written on the table block and read back off it.
	c.Assert(hcl, qt.Contains, `on = table.users`)
	c.Assert(hcl, qt.Contains, `for = table.users`)
	c.Assert(hcl, qt.Contains, `table "users" {`)
	c.Assert(hcl, qt.Contains, `schema = schema.auth`)
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
		"functions.missing_body",
		"views.missing_body",
		"materialized_views.missing_body",
		`triggers[""]["missing_table"]`,
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

func constraintByName(constraints []goschema.Constraint, name string) goschema.Constraint {
	for _, constraint := range constraints {
		if constraint.Name == name {
			return constraint
		}
	}
	return goschema.Constraint{}
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
