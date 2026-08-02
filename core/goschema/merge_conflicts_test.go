package goschema_test

import (
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
)

func TestMerge_RejectsEveryConflictingNamedObject(t *testing.T) {
	tests := []struct {
		name    string
		first   *goschema.Database
		second  *goschema.Database
		wantErr string
	}{
		{
			name:    "schema",
			first:   &goschema.Database{Schemas: []goschema.Schema{{Name: "auth", Comment: "Authentication"}}},
			second:  &goschema.Database{Schemas: []goschema.Schema{{Name: "auth", Comment: "Identity"}}},
			wantErr: `conflicting schema "auth" definitions`,
		},
		{
			name:    "table",
			first:   &goschema.Database{Tables: []goschema.Table{{StructName: "User", Name: "users", Comment: "Accounts"}}},
			second:  &goschema.Database{Tables: []goschema.Table{{StructName: "Account", Name: "users", Comment: "Identities"}}},
			wantErr: `conflicting table "users" definitions`,
		},
		{
			name: "field",
			first: &goschema.Database{
				Tables: []goschema.Table{{StructName: "User", Name: "users"}},
				Fields: []goschema.Field{{StructName: "User", FieldName: "ID", Name: "id", Type: "BIGINT"}},
			},
			second: &goschema.Database{
				Tables: []goschema.Table{{StructName: "Account", Name: "users"}},
				Fields: []goschema.Field{{StructName: "Account", FieldName: "Identifier", Name: "id", Type: "UUID"}},
			},
			wantErr: `conflicting field "id" definitions on table "users"`,
		},
		{
			name: "index",
			first: &goschema.Database{
				Tables:  []goschema.Table{{StructName: "User", Name: "users"}},
				Indexes: []goschema.Index{{StructName: "User", Name: "idx_users_email", Fields: []string{"email"}}},
			},
			second: &goschema.Database{
				Tables:  []goschema.Table{{StructName: "Account", Name: "users"}},
				Indexes: []goschema.Index{{StructName: "Account", Name: "idx_users_email", Fields: []string{"email"}, Unique: true}},
			},
			wantErr: `conflicting index "idx_users_email" definitions on table "users"`,
		},
		{
			name: "constraint",
			first: &goschema.Database{Constraints: []goschema.Constraint{{
				Name: "users_email_check", Table: "users", Type: "CHECK", CheckExpression: "email <> ''",
			}}},
			second: &goschema.Database{Constraints: []goschema.Constraint{{
				Name: "users_email_check", Table: "users", Type: "CHECK", CheckExpression: "length(email) > 3",
			}}},
			wantErr: `conflicting constraint "users_email_check" definitions in scope "users"`,
		},
		{
			name:    "enum",
			first:   &goschema.Database{Enums: []goschema.Enum{{Name: "status", Values: []string{"active", "disabled"}}}},
			second:  &goschema.Database{Enums: []goschema.Enum{{Name: "status", Values: []string{"active", "archived"}}}},
			wantErr: `conflicting enum "status" definitions`,
		},
		{
			name: "embedded field",
			first: &goschema.Database{
				Tables:         []goschema.Table{{StructName: "User", Name: "users"}},
				EmbeddedFields: []goschema.EmbeddedField{{StructName: "User", EmbeddedTypeName: "Metadata", Mode: "inline"}},
			},
			second: &goschema.Database{
				Tables:         []goschema.Table{{StructName: "Account", Name: "users"}},
				EmbeddedFields: []goschema.EmbeddedField{{StructName: "Account", EmbeddedTypeName: "Metadata", Mode: "json"}},
			},
			wantErr: `conflicting embedded field "Metadata" definitions on table "users"`,
		},
		{
			name:    "extension",
			first:   &goschema.Database{Extensions: []goschema.Extension{{Name: "postgis", Version: "3.5"}}},
			second:  &goschema.Database{Extensions: []goschema.Extension{{Name: "postgis", Version: "3.6"}}},
			wantErr: `conflicting extension "postgis" definitions`,
		},
		{
			name:    "function",
			first:   &goschema.Database{Functions: []goschema.Function{{Name: "active_user", Returns: "BOOLEAN", Body: "SELECT true"}}},
			second:  &goschema.Database{Functions: []goschema.Function{{Name: "active_user", Returns: "boolean", Body: "SELECT false"}}},
			wantErr: `conflicting function "active_user" definitions`,
		},
		{
			name:    "sequence",
			first:   &goschema.Database{Sequences: []goschema.Sequence{{Name: "order_seq", AsType: "bigint"}}},
			second:  &goschema.Database{Sequences: []goschema.Sequence{{Name: "order_seq", AsType: "integer"}}},
			wantErr: `conflicting sequence "order_seq" definitions`,
		},
		{
			name:    "domain",
			first:   &goschema.Database{Domains: []goschema.Domain{{Name: "email", BaseType: "TEXT"}}},
			second:  &goschema.Database{Domains: []goschema.Domain{{Name: "email", BaseType: "VARCHAR(255)"}}},
			wantErr: `conflicting domain "email" definitions`,
		},
		{
			name: "composite type",
			first: &goschema.Database{CompositeTypes: []goschema.CompositeType{{
				Name: "address", Fields: []goschema.CompositeTypeField{{Name: "city", Type: "TEXT"}},
			}}},
			second: &goschema.Database{CompositeTypes: []goschema.CompositeType{{
				Name: "address", Fields: []goschema.CompositeTypeField{{Name: "city", Type: "VARCHAR(255)"}},
			}}},
			wantErr: `conflicting composite type "address" definitions`,
		},
		{
			name:    "range",
			first:   &goschema.Database{Ranges: []goschema.Range{{Name: "amount_range", Subtype: "numeric"}}},
			second:  &goschema.Database{Ranges: []goschema.Range{{Name: "amount_range", Subtype: "bigint"}}},
			wantErr: `conflicting range "amount_range" definitions`,
		},
		{
			name:    "view",
			first:   &goschema.Database{Views: []goschema.View{{Name: "active_users", Body: "SELECT id FROM users"}}},
			second:  &goschema.Database{Views: []goschema.View{{Name: "active_users", Body: "SELECT email FROM users"}}},
			wantErr: `conflicting view "active_users" definitions`,
		},
		{
			name: "materialized view",
			first: &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
				Name: "user_stats", Body: "SELECT count(*) FROM users",
			}}},
			second: &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
				Name: "user_stats", Body: "SELECT count(id) FROM users",
			}}},
			wantErr: `conflicting materialized view "user_stats" definitions`,
		},
		{
			name: "trigger",
			first: &goschema.Database{Triggers: []goschema.Trigger{{
				Name: "set_updated_at", Table: "users", Timing: "BEFORE", Event: "UPDATE", Body: "RETURN NEW",
			}}},
			second: &goschema.Database{Triggers: []goschema.Trigger{{
				Name: "set_updated_at", Table: "users", Timing: "BEFORE", Event: "UPDATE", Body: "RETURN OLD",
			}}},
			wantErr: `conflicting trigger "set_updated_at" definitions on table "users"`,
		},
		{
			name: "RLS policy",
			first: &goschema.Database{RLSPolicies: []goschema.RLSPolicy{{
				Name: "tenant", Table: "users", PolicyFor: "ALL", UsingExpression: "tenant_id = current_user",
			}}},
			second: &goschema.Database{RLSPolicies: []goschema.RLSPolicy{{
				Name: "tenant", Table: "users", PolicyFor: "ALL", UsingExpression: "tenant_id = session_user",
			}}},
			wantErr: `conflicting RLS policy "tenant" definitions on table "users"`,
		},
		{
			name:    "RLS enablement",
			first:   &goschema.Database{RLSEnabledTables: []goschema.RLSEnabledTable{{Table: "users", Comment: "Tenant isolation"}}},
			second:  &goschema.Database{RLSEnabledTables: []goschema.RLSEnabledTable{{Table: "users", Comment: "Account isolation"}}},
			wantErr: `conflicting RLS enablement definitions on table "users"`,
		},
		{
			name:    "role",
			first:   &goschema.Database{Roles: []goschema.Role{{Name: "app_user", Inherit: true}}},
			second:  &goschema.Database{Roles: []goschema.Role{{Name: "app_user", Login: true, Inherit: true}}},
			wantErr: `conflicting role "app_user" definitions`,
		},
		{
			name: "managed data keys",
			first: &goschema.Database{ManagedData: []goschema.ManagedData{{
				Table: "countries", Keys: []string{"code"}, SourceDir: "reference", File: "countries.yaml",
			}}},
			second: &goschema.Database{ManagedData: []goschema.ManagedData{{
				Table: "countries", Keys: []string{"tenant_id", "code"}, SourceDir: "reference", File: "countries.yaml",
			}}},
			wantErr: `conflicting managed data key definitions for file "reference/countries.yaml" on table "countries"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			merged, err := goschema.Merge(test.first, test.second)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(merged, qt.IsNil)
		})
	}
}

func TestMerge_DeduplicatesEveryIdenticalNamedObject(t *testing.T) {
	c := qt.New(t)

	source := &goschema.Database{
		Schemas:           []goschema.Schema{{Name: "auth"}},
		Tables:            []goschema.Table{{StructName: "User", Name: "users"}},
		Fields:            []goschema.Field{{StructName: "User", FieldName: "ID", Name: "id", Type: "BIGINT"}},
		Indexes:           []goschema.Index{{StructName: "User", Name: "idx_users_id", Fields: []string{"id"}}},
		Constraints:       []goschema.Constraint{{StructName: "User", Name: "users_id_check", Type: "CHECK", CheckExpression: "id > 0"}},
		Enums:             []goschema.Enum{{Name: "status", Values: []string{"active"}}},
		EmbeddedFields:    []goschema.EmbeddedField{{StructName: "User", EmbeddedTypeName: "Metadata", Mode: "skip"}},
		Extensions:        []goschema.Extension{{Name: "pg_trgm"}},
		Functions:         []goschema.Function{{Name: "active_user", Returns: "BOOLEAN", Body: "SELECT true"}},
		Sequences:         []goschema.Sequence{{Name: "order_seq", AsType: "bigint"}},
		Domains:           []goschema.Domain{{Name: "email", BaseType: "TEXT"}},
		CompositeTypes:    []goschema.CompositeType{{Name: "address", Fields: []goschema.CompositeTypeField{{Name: "city", Type: "TEXT"}}}},
		Ranges:            []goschema.Range{{Name: "amount_range", Subtype: "numeric"}},
		Views:             []goschema.View{{Name: "active_users", Body: "SELECT id FROM users"}},
		MaterializedViews: []goschema.MaterializedView{{Name: "user_stats", Body: "SELECT count(*) FROM users"}},
		Triggers:          []goschema.Trigger{{Name: "set_updated_at", Table: "users", Timing: "BEFORE", Event: "UPDATE", Body: "RETURN NEW"}},
		RLSPolicies:       []goschema.RLSPolicy{{Name: "tenant", Table: "users", PolicyFor: "ALL"}},
		RLSEnabledTables:  []goschema.RLSEnabledTable{{Table: "users"}},
		Roles:             []goschema.Role{{Name: "app_user", Inherit: true}},
		Grants:            []goschema.Grant{{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "users"}},
		ManagedData: []goschema.ManagedData{{
			Table: "users", Keys: []string{"id"}, SourceDir: "reference", File: "users.yaml",
		}},
	}

	merged, err := goschema.Merge(source, source)

	c.Assert(err, qt.IsNil)
	c.Assert(merged.Schemas, qt.HasLen, 1)
	c.Assert(merged.Tables, qt.HasLen, 1)
	c.Assert(merged.Fields, qt.HasLen, 1)
	c.Assert(merged.Indexes, qt.HasLen, 1)
	c.Assert(merged.Constraints, qt.HasLen, 1)
	c.Assert(merged.Enums, qt.HasLen, 1)
	c.Assert(merged.EmbeddedFields, qt.HasLen, 1)
	c.Assert(merged.Extensions, qt.HasLen, 1)
	c.Assert(merged.Functions, qt.HasLen, 1)
	c.Assert(merged.Sequences, qt.HasLen, 1)
	c.Assert(merged.Domains, qt.HasLen, 1)
	c.Assert(merged.CompositeTypes, qt.HasLen, 1)
	c.Assert(merged.Ranges, qt.HasLen, 1)
	c.Assert(merged.Views, qt.HasLen, 1)
	c.Assert(merged.MaterializedViews, qt.HasLen, 1)
	c.Assert(merged.Triggers, qt.HasLen, 1)
	c.Assert(merged.RLSPolicies, qt.HasLen, 1)
	c.Assert(merged.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(merged.Roles, qt.HasLen, 1)
	c.Assert(merged.Grants, qt.HasLen, 1)
	c.Assert(merged.ManagedData, qt.HasLen, 1)
}

func TestMerge_DeduplicatesTableScopedObjectsAcrossParserProvenance(t *testing.T) {
	c := qt.New(t)

	first := &goschema.Database{
		Tables:           []goschema.Table{{StructName: "User", Name: "users"}},
		Fields:           []goschema.Field{{StructName: "User", FieldName: "ID", Name: "id", Type: "BIGINT"}},
		Indexes:          []goschema.Index{{StructName: "User", Name: "idx_users_id", Fields: []string{"id"}}},
		Constraints:      []goschema.Constraint{{StructName: "User", Name: "users_id_check", Type: "CHECK", CheckExpression: "id > 0"}},
		EmbeddedFields:   []goschema.EmbeddedField{{StructName: "User", EmbeddedTypeName: "Metadata", Mode: "skip"}},
		RLSPolicies:      []goschema.RLSPolicy{{StructName: "User", Name: "tenant", PolicyFor: "ALL"}},
		RLSEnabledTables: []goschema.RLSEnabledTable{{StructName: "User"}},
	}
	second := &goschema.Database{
		Tables:           []goschema.Table{{StructName: "Account", Name: "users"}},
		Fields:           []goschema.Field{{StructName: "Account", FieldName: "Identifier", Name: "id", Type: "BIGINT"}},
		Indexes:          []goschema.Index{{StructName: "Account", Name: "idx_users_id", Fields: []string{"id"}}},
		Constraints:      []goschema.Constraint{{StructName: "Account", Name: "users_id_check", Type: "CHECK", CheckExpression: "id > 0"}},
		EmbeddedFields:   []goschema.EmbeddedField{{StructName: "Account", EmbeddedTypeName: "Metadata", Mode: "skip"}},
		RLSPolicies:      []goschema.RLSPolicy{{StructName: "Account", Name: "tenant", PolicyFor: "ALL"}},
		RLSEnabledTables: []goschema.RLSEnabledTable{{StructName: "Account"}},
	}

	merged, err := goschema.Merge(first, second)

	c.Assert(err, qt.IsNil)
	c.Assert(merged.Tables, qt.HasLen, 1)
	c.Assert(merged.Fields, qt.HasLen, 1)
	c.Assert(merged.Indexes, qt.HasLen, 1)
	c.Assert(merged.Constraints, qt.HasLen, 1)
	c.Assert(merged.EmbeddedFields, qt.HasLen, 1)
	c.Assert(merged.RLSPolicies, qt.HasLen, 1)
	c.Assert(merged.RLSEnabledTables, qt.HasLen, 1)
}

func TestMerge_PreservesTablesWithSameGoTypeNameInDifferentSchemas(t *testing.T) {
	c := qt.New(t)

	auth := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Schema: "auth", Name: "users"}},
		Fields: []goschema.Field{{StructName: "User", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true}},
	}
	billing := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Schema: "billing", Name: "users"}},
		Fields: []goschema.Field{{StructName: "User", FieldName: "ID", Name: "id", Type: "UUID", Primary: true}},
	}

	merged, err := goschema.Merge(auth, billing)

	c.Assert(err, qt.IsNil)
	c.Assert(merged.Tables, qt.HasLen, 2)
	c.Assert(merged.Fields, qt.HasLen, 2)
	c.Assert(merged.Tables[0].StructName, qt.Equals, "auth.users")
	c.Assert(merged.Tables[1].StructName, qt.Equals, "billing.users")
	c.Assert(merged.Fields[0].StructName, qt.Equals, "auth.users")
	c.Assert(merged.Fields[1].StructName, qt.Equals, "billing.users")

	statements, err := renderer.GetOrderedCreateStatements(merged, "postgres")
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	c.Assert(rendered, qt.Contains, `CREATE TABLE "auth"."users"`)
	c.Assert(rendered, qt.Contains, `"id" BIGINT PRIMARY KEY`)
	c.Assert(rendered, qt.Contains, `CREATE TABLE "billing"."users"`)
	c.Assert(rendered, qt.Contains, `"id" UUID PRIMARY KEY`)
}

func TestMerge_IsolatesSourceLocalNestedEmbeddedHelpers(t *testing.T) {
	c := qt.New(t)

	auth := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Schema: "auth", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Metadata", FieldName: "Label", Name: "auth_label", Type: "TEXT"},
			{StructName: "Audit", FieldName: "CreatedAt", Name: "auth_created_at", Type: "TIMESTAMPTZ"},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			{StructName: "User", EmbeddedTypeName: "Metadata", Mode: "inline"},
			{StructName: "Metadata", EmbeddedTypeName: "Audit", Mode: "inline"},
		},
	}
	billing := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Schema: "billing", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", FieldName: "ID", Name: "id", Type: "UUID", Primary: true},
			{StructName: "Metadata", FieldName: "Label", Name: "billing_label", Type: "VARCHAR(64)"},
			{StructName: "Audit", FieldName: "CreatedAt", Name: "billing_created_at", Type: "TIMESTAMP"},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			{StructName: "User", EmbeddedTypeName: "Metadata", Mode: "inline"},
			{StructName: "Metadata", EmbeddedTypeName: "Audit", Mode: "inline"},
		},
	}

	merged, err := goschema.Merge(auth, billing)
	c.Assert(err, qt.IsNil)
	c.Assert(merged.Fields, qt.HasLen, 6)
	c.Assert(merged.EmbeddedFields, qt.HasLen, 2)

	statements, err := renderer.GetOrderedCreateStatements(merged, "postgres")
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	c.Assert(strings.Count(rendered, `"auth_label"`), qt.Equals, 1)
	c.Assert(strings.Count(rendered, `"auth_created_at"`), qt.Equals, 1)
	c.Assert(strings.Count(rendered, `"billing_label"`), qt.Equals, 1)
	c.Assert(strings.Count(rendered, `"billing_created_at"`), qt.Equals, 1)
	c.Assert(rendered, qt.Not(qt.Contains), "composite-source-")
}

func TestMerge_PreservesNestedEmbeddedHelpersAcrossRefinalization(t *testing.T) {
	c := qt.New(t)
	auth := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Schema: "auth", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Metadata", Name: "label", Type: "TEXT"},
			{StructName: "Audit", Name: "created_at", Type: "TIMESTAMPTZ"},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			{StructName: "User", EmbeddedTypeName: "Metadata", Mode: "inline"},
			{StructName: "Metadata", EmbeddedTypeName: "Audit", Mode: "inline"},
		},
	}

	merged, err := goschema.Merge(auth)
	c.Assert(err, qt.IsNil)
	goschema.Finalize(merged)
	refinalizedSQL, err := renderer.GetOrderedCreateStatements(merged, "postgres")
	c.Assert(err, qt.IsNil)

	remerged, err := goschema.Merge(merged)
	c.Assert(err, qt.IsNil)
	remergedSQL, err := renderer.GetOrderedCreateStatements(remerged, "postgres")
	c.Assert(err, qt.IsNil)

	c.Assert(strings.Join(refinalizedSQL, "\n"), qt.Contains, `"label" TEXT`)
	c.Assert(strings.Join(refinalizedSQL, "\n"), qt.Contains, `"created_at" TIMESTAMPTZ`)
	c.Assert(remergedSQL, qt.DeepEquals, refinalizedSQL)
}

func TestMerge_DoesNotAttachSourceLocalHelperToSameNamedTable(t *testing.T) {
	c := qt.New(t)

	tableSource := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Metadata", Name: "metadata"}},
		Fields: []goschema.Field{{
			StructName: "Metadata",
			FieldName:  "ID",
			Name:       "id",
			Type:       "BIGINT",
			Primary:    true,
		}},
	}
	helperSource := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Profile", Name: "profiles"},
			{StructName: "Organization", Name: "organizations"},
		},
		Fields: []goschema.Field{
			{
				StructName: "Profile",
				FieldName:  "ID",
				Name:       "id",
				Type:       "BIGINT",
				Primary:    true,
			},
			{
				StructName: "Organization",
				FieldName:  "ID",
				Name:       "id",
				Type:       "BIGINT",
				Primary:    true,
			},
			{
				StructName: "Metadata",
				FieldName:  "OrganizationID",
				Name:       "organization_id",
				Type:       "BIGINT",
				Foreign:    "organizations(id)",
			},
		},
		EmbeddedFields: []goschema.EmbeddedField{{
			StructName:       "Profile",
			EmbeddedTypeName: "Metadata",
			Mode:             "inline",
		}},
	}

	merged, err := goschema.Merge(tableSource, helperSource)
	c.Assert(err, qt.IsNil)
	c.Assert(merged.Fields, qt.HasLen, 4)
	c.Assert(merged.Dependencies["metadata"], qt.HasLen, 0)
	c.Assert(merged.Dependencies["profiles"], qt.DeepEquals, []string{"organizations"})

	statements, err := renderer.GetOrderedCreateStatements(merged, "postgres")
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	c.Assert(strings.Count(rendered, `"organization_id"`), qt.Equals, 2)
}

func TestMerge_RebindsEmbeddedTableTypeAcrossParserNames(t *testing.T) {
	c := qt.New(t)

	canonicalSource := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{{
			StructName: "User",
			FieldName:  "ID",
			Name:       "id",
			Type:       "BIGINT",
		}},
	}
	alternateSource := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Account", Name: "users"},
			{StructName: "Profile", Name: "profiles"},
		},
		Fields: []goschema.Field{{
			StructName: "Account",
			FieldName:  "Email",
			Name:       "email",
			Type:       "TEXT",
		}},
		EmbeddedFields: []goschema.EmbeddedField{{
			StructName:       "Profile",
			EmbeddedTypeName: "Account",
			Mode:             "inline",
		}},
	}

	merged, err := goschema.Merge(canonicalSource, alternateSource)
	c.Assert(err, qt.IsNil)
	c.Assert(merged.Fields, qt.HasLen, 4)

	statements, err := renderer.GetOrderedCreateStatements(merged, "postgres")
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	c.Assert(strings.Count(rendered, `"id" BIGINT`), qt.Equals, 2)
	c.Assert(strings.Count(rendered, `"email" TEXT`), qt.Equals, 2)
}

func TestMerge_RejectsConflictingExpandedHelperFields(t *testing.T) {
	c := qt.New(t)

	first := &goschema.Database{
		Tables:         []goschema.Table{{StructName: "User", Name: "users"}},
		Fields:         []goschema.Field{{StructName: "Metadata", FieldName: "Code", Name: "code", Type: "TEXT"}},
		EmbeddedFields: []goschema.EmbeddedField{{StructName: "User", EmbeddedTypeName: "Metadata", Mode: "inline"}},
	}
	second := &goschema.Database{
		Tables:         []goschema.Table{{StructName: "Account", Name: "users"}},
		Fields:         []goschema.Field{{StructName: "Metadata", FieldName: "Code", Name: "code", Type: "UUID"}},
		EmbeddedFields: []goschema.EmbeddedField{{StructName: "Account", EmbeddedTypeName: "Metadata", Mode: "inline"}},
	}

	merged, err := goschema.Merge(first, second)

	c.Assert(err, qt.ErrorMatches, `conflicting field "code" definitions on table "users"`)
	c.Assert(merged, qt.IsNil)
}

func TestMerge_UnifiesTableOwnershipAcrossParserNames(t *testing.T) {
	c := qt.New(t)

	first := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{{StructName: "User", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true}},
	}
	second := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Account", Name: "users"}},
		Fields: []goschema.Field{{StructName: "Account", FieldName: "Email", Name: "email", Type: "TEXT"}},
	}

	merged, err := goschema.Merge(first, second)

	c.Assert(err, qt.IsNil)
	c.Assert(merged.Tables, qt.HasLen, 1)
	c.Assert(merged.Fields, qt.HasLen, 2)
	c.Assert(merged.Tables[0].StructName, qt.Equals, "User")
	c.Assert(merged.Fields[0].StructName, qt.Equals, "User")
	c.Assert(merged.Fields[1].StructName, qt.Equals, "User")

	statements, err := renderer.GetOrderedCreateStatements(merged, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `"email" TEXT`)
}

func TestParseFS_UsesTheSameStrictCollisionPolicyAsMerge(t *testing.T) {
	c := qt.New(t)
	firstSource := `package fixtures

//ptah:schema:table name="users" comment="Accounts"
type User struct{}
`
	secondSource := `package fixtures

//ptah:schema:table name="users" comment="Identities"
type Account struct{}
`
	fsys := fstest.MapFS{
		"first.go":  {Data: []byte(firstSource)},
		"second.go": {Data: []byte(secondSource)},
	}

	parsed, parseErr := goschema.ParseFS(fsys, ".")
	c.Assert(parseErr, qt.ErrorMatches, `conflicting table "users" definitions`)
	c.Assert(parsed, qt.IsNil)

	first, err := goschema.ParseSource("first.go", firstSource)
	c.Assert(err, qt.IsNil)
	second, err := goschema.ParseSource("second.go", secondSource)
	c.Assert(err, qt.IsNil)
	merged, mergeErr := goschema.Merge(&first, &second)
	c.Assert(mergeErr, qt.ErrorMatches, `conflicting table "users" definitions`)
	c.Assert(merged, qt.IsNil)
}
