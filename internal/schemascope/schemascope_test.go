package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemascope"
)

func TestSplitNames(t *testing.T) {
	c := qt.New(t)

	got := schemascope.SplitNames([]string{" auth, billing ", "auth", "", "metrics"})

	c.Assert(got, qt.DeepEquals, []string{"auth", "billing", "metrics"})
}

func TestFilterGeneratedScopesTablesAndDependentObjects(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Schemas: []schemamodel.Schema{
			{Name: "auth"},
			{Name: "billing"},
		},
		Extensions: []schemamodel.Extension{
			{Schema: "auth", Name: "pgcrypto"},
			{Schema: "billing", Name: "citext"},
		},
		Tables: []schemamodel.Table{
			{StructName: "AuthUser", Schema: "auth", Name: "users"},
			{StructName: "BillingInvoice", Schema: "billing", Name: "invoices"},
		},
		Fields: []schemamodel.Field{
			{StructName: "AuthUser", Name: "id", Type: "BIGINT"},
			{StructName: "AuthUser", Name: "status", Type: "enum_auth_user_status"},
			{StructName: "AuthUser", Name: "invoice_id", Type: "BIGINT", Foreign: "billing.invoices(id)", ForeignKeyName: "fk_users_invoice"},
			{StructName: "BillingInvoice", Name: "id", Type: "BIGINT"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "AuthUser", Name: "idx_users_status"},
			{StructName: "BillingInvoice", Name: "idx_invoices_total"},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "AuthUser", Name: "users_status_check", Type: "CHECK"},
			{StructName: "AuthUser", Name: "users_invoice_fk", Type: "FOREIGN KEY", ForeignTable: "billing.invoices"},
			{StructName: "BillingInvoice", Name: "invoices_total_check", Type: "CHECK"},
		},
		Enums: []schemamodel.Enum{
			{Name: "enum_auth_user_status", Values: []string{"active"}},
			{Name: "enum_billing_invoice_status", Values: []string{"paid"}},
		},
		Functions: []schemamodel.Function{
			{StructName: "AuthUser", Name: "auth.set_tenant"},
			{Name: "billing.set_invoice"},
		},
		Views: []schemamodel.View{
			{Name: "auth.active_users"},
			{Name: "billing.open_invoices"},
		},
		Triggers: []schemamodel.Trigger{
			{Name: "users_updated_at", Table: "auth.users"},
			{Name: "invoices_updated_at", Table: "billing.invoices"},
		},
		RLSPolicies: []schemamodel.RLSPolicy{
			{Name: "users_tenant", Table: "auth.users"},
			{Name: "invoices_tenant", Table: "billing.invoices"},
		},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{
			{Table: "auth.users"},
			{Table: "billing.invoices"},
		},
		Roles: []schemamodel.Role{
			{Name: "app_role"},
		},
		Grants: []schemamodel.Grant{
			{Role: "app_role", OnSchema: "auth"},
			{Role: "app_role", OnSchema: "billing"},
			{Role: "app_role", OnTable: "auth.users"},
		},
		Dependencies: map[string][]string{
			"auth.users":       {"billing.invoices"},
			"billing.invoices": nil,
		},
	}

	got := schemascope.FilterGenerated(db, []string{"auth"})

	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"auth.users"})
	c.Assert(generatedSchemaNames(got.Schemas), qt.DeepEquals, []string{"auth"})
	c.Assert(generatedExtensionNames(got.Extensions), qt.DeepEquals, []string{"auth.pgcrypto", "billing.citext"})
	c.Assert(generatedFieldNames(got.Fields), qt.DeepEquals, []string{"id", "status", "invoice_id"})
	c.Assert(got.Fields[2].Foreign, qt.Equals, "")
	c.Assert(got.Fields[2].ForeignKeyName, qt.Equals, "")
	c.Assert(generatedIndexNames(got.Indexes), qt.DeepEquals, []string{"idx_users_status"})
	c.Assert(generatedConstraintNames(got.Constraints), qt.DeepEquals, []string{"users_status_check"})
	c.Assert(generatedEnumNames(got.Enums), qt.DeepEquals, []string{"enum_auth_user_status"})
	c.Assert(generatedFunctionNames(got.Functions), qt.DeepEquals, []string{"auth.set_tenant"})
	c.Assert(generatedViewNames(got.Views), qt.DeepEquals, []string{"auth.active_users"})
	c.Assert(generatedTriggerNames(got.Triggers), qt.DeepEquals, []string{"users_updated_at"})
	c.Assert(generatedRLSPolicyNames(got.RLSPolicies), qt.DeepEquals, []string{"users_tenant"})
	c.Assert(generatedRLSTableNames(got.RLSEnabledTables), qt.DeepEquals, []string{"auth.users"})
	c.Assert(generatedGrantTargets(got.Grants), qt.DeepEquals, []string{"schema:auth", "table:auth.users"})
	c.Assert(got.Roles, qt.HasLen, 1)
	c.Assert(got.Dependencies, qt.DeepEquals, map[string][]string{"auth.users": {}})
}

func TestFilterGeneratedWithDefaultSchemaKeepsUnqualifiedPublicObjects(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
			{StructName: "AuditLog", Schema: "audit", Name: "logs"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "BIGINT"},
			{StructName: "AuditLog", Name: "id", Type: "BIGINT"},
		},
		Views: []schemamodel.View{
			{Name: "active_users"},
			{Name: "audit.recent_logs"},
		},
		Grants: []schemamodel.Grant{
			{OnSchema: "public", Role: "app_role"},
			{OnTable: "users", Role: "app_role"},
			{OnSchema: "audit", Role: "app_role"},
		},
	}

	got := schemascope.FilterGeneratedWithDefaultSchema(db, []string{"public"}, "public")

	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{"users"})
	c.Assert(generatedFieldNames(got.Fields), qt.DeepEquals, []string{"id"})
	c.Assert(generatedViewNames(got.Views), qt.DeepEquals, []string{"active_users"})
	c.Assert(generatedGrantTargets(got.Grants), qt.DeepEquals, []string{"schema:public", "table:users"})
}

func TestFilterGeneratedKeepsDatabaseWideExtensionsAcrossSchemaSelection(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "app"}, {Name: "extensions"}, {Name: "other"}},
		Extensions: []schemamodel.Extension{
			{Name: "pgcrypto"},
			{Name: "citext", Schema: "extensions", Provides: []string{"citext"}},
			{Name: "unrelated", Schema: "other"},
		},
		Tables: []schemamodel.Table{{StructName: "User", Schema: "app", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "email", Type: "extensions.citext"},
		},
	}

	got := schemascope.FilterGeneratedWithDefaultSchema(db, []string{"app"}, "public")

	c.Assert(generatedSchemaNames(got.Schemas), qt.DeepEquals, []string{"app"})
	c.Assert(generatedExtensionNames(got.Extensions), qt.DeepEquals,
		[]string{"pgcrypto", "extensions.citext", "other.unrelated"})
	c.Assert(generatedFieldNames(got.Fields), qt.DeepEquals, []string{"email"})
}

func TestFilterGenerated_PreservesStructuralTableIdentity(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
		},
		Triggers: []schemamodel.Trigger{
			{Name: "literal_trigger", Table: `"tenant.data"`},
			{Name: "qualified_trigger", Table: "tenant.data"},
		},
	}

	got := schemascope.FilterGeneratedWithDefaultSchema(db, []string{"public"}, "public")

	c.Assert(generatedTableNames(got.Tables), qt.DeepEquals, []string{`"tenant.data"`})
	c.Assert(generatedTriggerNames(got.Triggers), qt.DeepEquals, []string{"literal_trigger"})
}

func TestFilterDatabaseScopesIntrospectedObjects(t *testing.T) {
	c := qt.New(t)
	foreignTable := "billing.invoices"
	db := &catalog.Database{
		Tables: []catalog.Table{
			{
				Schema: "auth",
				Name:   "users",
				Columns: []catalog.Column{
					{Name: "status", DataType: "USER-DEFINED", UDTName: "user_status"},
				},
			},
			{
				Schema: "billing",
				Name:   "invoices",
				Columns: []catalog.Column{
					{Name: "status", DataType: "USER-DEFINED", UDTName: "invoice_status"},
				},
			},
		},
		Enums: []catalog.Enum{
			{Name: "user_status", Values: []string{"active"}},
			{Name: "invoice_status", Values: []string{"paid"}},
		},
		Indexes: []catalog.Index{
			{Schema: "auth", TableName: "users", Name: "idx_users_status"},
			{Schema: "billing", TableName: "invoices", Name: "idx_invoices_status"},
		},
		Constraints: []catalog.Constraint{
			{Schema: "auth", TableName: "users", Name: "users_status_check", Type: "CHECK"},
			{Schema: "auth", TableName: "users", Name: "users_invoice_fk", Type: "FOREIGN KEY", ForeignTable: &foreignTable, ForeignSchema: "billing"},
			{Schema: "billing", TableName: "invoices", Name: "invoices_status_check", Type: "CHECK"},
		},
		Extensions: []catalog.Extension{
			{Schema: "auth", Name: "pg_trgm"},
			{Schema: "billing", Name: "btree_gin"},
		},
		Views: []catalog.View{
			{Schema: "auth", Name: "active_users"},
			{Schema: "billing", Name: "open_invoices"},
		},
		MatViews: []catalog.MaterializedView{
			{Schema: "auth", Name: "user_stats"},
			{Schema: "billing", Name: "invoice_stats"},
		},
		Triggers: []catalog.Trigger{
			{Schema: "auth", Table: "users", Name: "users_updated_at"},
			{Schema: "billing", Table: "invoices", Name: "invoices_updated_at"},
		},
		RLSPolicies: []catalog.RLSPolicy{
			{Table: "users", Name: "users_tenant"},
			{Table: "billing.invoices", Name: "invoices_tenant"},
		},
		Grants: []catalog.Grant{
			{ObjectType: "SCHEMA", ObjectName: "auth", Role: "app_role"},
			{ObjectType: "SCHEMA", ObjectName: "billing", Role: "app_role"},
			{ObjectType: "TABLE", Schema: "auth", ObjectName: "users", Role: "app_role"},
		},
	}

	got := schemascope.FilterDatabase(db, []string{"auth"})

	c.Assert(databaseTableNames(got.Tables), qt.DeepEquals, []string{"auth.users"})
	c.Assert(databaseEnumNames(got.Enums), qt.DeepEquals, []string{"user_status"})
	c.Assert(databaseIndexNames(got.Indexes), qt.DeepEquals, []string{"idx_users_status"})
	c.Assert(databaseConstraintNames(got.Constraints), qt.DeepEquals, []string{"users_status_check"})
	c.Assert(databaseExtensionNames(got.Extensions), qt.DeepEquals, []string{"auth.pg_trgm", "billing.btree_gin"})
	c.Assert(databaseViewNames(got.Views), qt.DeepEquals, []string{"auth.active_users"})
	c.Assert(databaseMatViewNames(got.MatViews), qt.DeepEquals, []string{"auth.user_stats"})
	c.Assert(databaseTriggerNames(got.Triggers), qt.DeepEquals, []string{"users_updated_at"})
	c.Assert(databaseRLSPolicyNames(got.RLSPolicies), qt.DeepEquals, []string{"users_tenant"})
	c.Assert(databaseGrantTargets(got.Grants), qt.DeepEquals, []string{"schema:auth", "table:auth.users"})
}

func TestFilterDatabaseWithDefaultSchemaKeepsUnqualifiedPublicObjects(t *testing.T) {
	c := qt.New(t)
	db := &catalog.Database{
		Tables: []catalog.Table{
			{Schema: "", Name: "users"},
			{Schema: "audit", Name: "logs"},
		},
		Indexes: []catalog.Index{
			{Schema: "", TableName: "users", Name: "idx_users_id"},
			{Schema: "audit", TableName: "logs", Name: "idx_logs_id"},
		},
		Constraints: []catalog.Constraint{
			{Schema: "", TableName: "users", Name: "users_pkey", Type: "PRIMARY KEY"},
			{Schema: "audit", TableName: "logs", Name: "logs_pkey", Type: "PRIMARY KEY"},
		},
		Views: []catalog.View{
			{Schema: "", Name: "active_users"},
			{Schema: "audit", Name: "recent_logs"},
		},
		RLSPolicies: []catalog.RLSPolicy{
			{Table: "users", Name: "users_tenant"},
			{Table: "audit.logs", Name: "logs_tenant"},
		},
		Grants: []catalog.Grant{
			{ObjectType: "SCHEMA", ObjectName: "public", Role: "app_role"},
			{ObjectType: "TABLE", Schema: "", ObjectName: "users", Role: "app_role"},
			{ObjectType: "SCHEMA", ObjectName: "audit", Role: "app_role"},
		},
	}

	got := schemascope.FilterDatabaseWithDefaultSchema(db, []string{"public"}, "public")

	c.Assert(databaseTableNames(got.Tables), qt.DeepEquals, []string{"users"})
	c.Assert(databaseIndexNames(got.Indexes), qt.DeepEquals, []string{"idx_users_id"})
	c.Assert(databaseConstraintNames(got.Constraints), qt.DeepEquals, []string{"users_pkey"})
	c.Assert(databaseViewNames(got.Views), qt.DeepEquals, []string{"active_users"})
	c.Assert(databaseRLSPolicyNames(got.RLSPolicies), qt.DeepEquals, []string{"users_tenant"})
	c.Assert(databaseGrantTargets(got.Grants), qt.DeepEquals, []string{"schema:public", "table:users"})
}

func TestFilterDatabaseKeepsDatabaseWideExtensionsAcrossSchemaSelection(t *testing.T) {
	c := qt.New(t)
	db := &catalog.Database{
		Tables: []catalog.Table{
			{
				Schema: "app",
				Name:   "users",
				Columns: []catalog.Column{
					{Name: "email", DataType: "USER-DEFINED", UDTName: "citext", FormattedType: "extensions.citext"},
				},
			},
		},
		Extensions: []catalog.Extension{
			{Name: "pgcrypto", Schema: "public"},
			{Name: "citext", Schema: "extensions", Provides: []string{"citext"}},
			{Name: "unrelated", Schema: "other"},
		},
	}

	got := schemascope.FilterDatabaseWithDefaultSchema(db, []string{"app"}, "public")

	c.Assert(databaseTableNames(got.Tables), qt.DeepEquals, []string{"app.users"})
	c.Assert(databaseExtensionNames(got.Extensions), qt.DeepEquals,
		[]string{"public.pgcrypto", "extensions.citext", "other.unrelated"})
}

func TestFilterDatabase_PreservesStructuralTableIdentity(t *testing.T) {
	c := qt.New(t)
	db := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "tenant.data"},
			{Schema: "tenant", Name: "data"},
		},
		RLSPolicies: []catalog.RLSPolicy{
			{Name: "literal_policy", Table: `"tenant.data"`},
			{Name: "qualified_policy", Table: "tenant.data"},
		},
	}

	got := schemascope.FilterDatabaseWithDefaultSchema(db, []string{"public"}, "public")

	c.Assert(databaseTableNames(got.Tables), qt.DeepEquals, []string{`"tenant.data"`})
	c.Assert(databaseRLSPolicyNames(got.RLSPolicies), qt.DeepEquals, []string{"literal_policy"})
}

func generatedTableNames(tables []schemamodel.Table) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.QualifiedName())
	}
	return names
}

func generatedSchemaNames(schemas []schemamodel.Schema) []string {
	names := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		names = append(names, schema.Name)
	}
	return names
}

func generatedExtensionNames(extensions []schemamodel.Extension) []string {
	names := make([]string, 0, len(extensions))
	for _, extension := range extensions {
		names = append(names, catalog.QualifyTableName(extension.Schema, extension.Name))
	}
	return names
}

func generatedFieldNames(fields []schemamodel.Field) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Name)
	}
	return names
}

func generatedIndexNames(indexes []schemamodel.Index) []string {
	names := make([]string, 0, len(indexes))
	for _, index := range indexes {
		names = append(names, index.Name)
	}
	return names
}

func generatedConstraintNames(constraints []schemamodel.Constraint) []string {
	names := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		names = append(names, constraint.Name)
	}
	return names
}

func generatedEnumNames(enums []schemamodel.Enum) []string {
	names := make([]string, 0, len(enums))
	for _, enum := range enums {
		names = append(names, enum.Name)
	}
	return names
}

func generatedFunctionNames(functions []schemamodel.Function) []string {
	names := make([]string, 0, len(functions))
	for _, function := range functions {
		names = append(names, function.Name)
	}
	return names
}

func generatedViewNames(views []schemamodel.View) []string {
	names := make([]string, 0, len(views))
	for _, view := range views {
		names = append(names, view.Name)
	}
	return names
}

func generatedTriggerNames(triggers []schemamodel.Trigger) []string {
	names := make([]string, 0, len(triggers))
	for _, trigger := range triggers {
		names = append(names, trigger.Name)
	}
	return names
}

func generatedRLSPolicyNames(policies []schemamodel.RLSPolicy) []string {
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.Name)
	}
	return names
}

func generatedRLSTableNames(tables []schemamodel.RLSEnabledTable) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Table)
	}
	return names
}

func generatedGrantTargets(grants []schemamodel.Grant) []string {
	targets := make([]string, 0, len(grants))
	for _, grant := range grants {
		targets = append(targets, grantTarget(grant.OnSchema, grant.OnTable))
	}
	return targets
}

func grantTarget(schema, table string) string {
	if schema != "" {
		return "schema:" + schema
	}
	return "table:" + table
}

func databaseTableNames(tables []catalog.Table) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.QualifiedName())
	}
	return names
}

func databaseEnumNames(enums []catalog.Enum) []string {
	names := make([]string, 0, len(enums))
	for _, enum := range enums {
		names = append(names, enum.Name)
	}
	return names
}

func databaseIndexNames(indexes []catalog.Index) []string {
	names := make([]string, 0, len(indexes))
	for _, index := range indexes {
		names = append(names, index.Name)
	}
	return names
}

func databaseConstraintNames(constraints []catalog.Constraint) []string {
	names := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		names = append(names, constraint.Name)
	}
	return names
}

func databaseExtensionNames(extensions []catalog.Extension) []string {
	names := make([]string, 0, len(extensions))
	for _, extension := range extensions {
		names = append(names, catalog.QualifyTableName(extension.Schema, extension.Name))
	}
	return names
}

func databaseViewNames(views []catalog.View) []string {
	names := make([]string, 0, len(views))
	for _, view := range views {
		names = append(names, view.QualifiedName())
	}
	return names
}

func databaseMatViewNames(views []catalog.MaterializedView) []string {
	names := make([]string, 0, len(views))
	for _, view := range views {
		names = append(names, view.QualifiedName())
	}
	return names
}

func databaseTriggerNames(triggers []catalog.Trigger) []string {
	names := make([]string, 0, len(triggers))
	for _, trigger := range triggers {
		names = append(names, trigger.Name)
	}
	return names
}

func databaseRLSPolicyNames(policies []catalog.RLSPolicy) []string {
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.Name)
	}
	return names
}

func databaseGrantTargets(grants []catalog.Grant) []string {
	targets := make([]string, 0, len(grants))
	for _, grant := range grants {
		targets = append(targets, dbGrantTarget(grant))
	}
	return targets
}

func dbGrantTarget(grant catalog.Grant) string {
	if grant.ObjectType == "SCHEMA" {
		return "schema:" + grant.ObjectName
	}
	return "table:" + grant.QualifiedTarget()
}
