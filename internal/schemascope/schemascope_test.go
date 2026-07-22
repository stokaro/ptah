package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/schemascope"
)

func TestSplitNames(t *testing.T) {
	c := qt.New(t)

	got := schemascope.SplitNames([]string{" auth, billing ", "auth", "", "metrics"})

	c.Assert(got, qt.DeepEquals, []string{"auth", "billing", "metrics"})
}

func TestFilterGeneratedScopesTablesAndDependentObjects(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Schemas: []goschema.Schema{
			{Name: "auth"},
			{Name: "billing"},
		},
		Tables: []goschema.Table{
			{StructName: "AuthUser", Schema: "auth", Name: "users"},
			{StructName: "BillingInvoice", Schema: "billing", Name: "invoices"},
		},
		Fields: []goschema.Field{
			{StructName: "AuthUser", Name: "id", Type: "BIGINT"},
			{StructName: "AuthUser", Name: "status", Type: "enum_auth_user_status"},
			{StructName: "AuthUser", Name: "invoice_id", Type: "BIGINT", Foreign: "billing.invoices(id)", ForeignKeyName: "fk_users_invoice"},
			{StructName: "BillingInvoice", Name: "id", Type: "BIGINT"},
		},
		Indexes: []goschema.Index{
			{StructName: "AuthUser", Name: "idx_users_status"},
			{StructName: "BillingInvoice", Name: "idx_invoices_total"},
		},
		Constraints: []goschema.Constraint{
			{StructName: "AuthUser", Name: "users_status_check", Type: "CHECK"},
			{StructName: "AuthUser", Name: "users_invoice_fk", Type: "FOREIGN KEY", ForeignTable: "billing.invoices"},
			{StructName: "BillingInvoice", Name: "invoices_total_check", Type: "CHECK"},
		},
		Enums: []goschema.Enum{
			{Name: "enum_auth_user_status", Values: []string{"active"}},
			{Name: "enum_billing_invoice_status", Values: []string{"paid"}},
		},
		Functions: []goschema.Function{
			{StructName: "AuthUser", Name: "auth.set_tenant"},
			{Name: "billing.set_invoice"},
		},
		Views: []goschema.View{
			{Name: "auth.active_users"},
			{Name: "billing.open_invoices"},
		},
		Triggers: []goschema.Trigger{
			{Name: "users_updated_at", Table: "auth.users"},
			{Name: "invoices_updated_at", Table: "billing.invoices"},
		},
		RLSPolicies: []goschema.RLSPolicy{
			{Name: "users_tenant", Table: "auth.users"},
			{Name: "invoices_tenant", Table: "billing.invoices"},
		},
		RLSEnabledTables: []goschema.RLSEnabledTable{
			{Table: "auth.users"},
			{Table: "billing.invoices"},
		},
		Roles: []goschema.Role{
			{Name: "app_role"},
		},
		Grants: []goschema.Grant{
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
	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "AuditLog", Schema: "audit", Name: "logs"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "BIGINT"},
			{StructName: "AuditLog", Name: "id", Type: "BIGINT"},
		},
		Views: []goschema.View{
			{Name: "active_users"},
			{Name: "audit.recent_logs"},
		},
		Grants: []goschema.Grant{
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

func TestFilterDatabaseScopesIntrospectedObjects(t *testing.T) {
	c := qt.New(t)
	foreignTable := "billing.invoices"
	db := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{
				Schema: "auth",
				Name:   "users",
				Columns: []dbschematypes.DBColumn{
					{Name: "status", DataType: "USER-DEFINED", UDTName: "user_status"},
				},
			},
			{
				Schema: "billing",
				Name:   "invoices",
				Columns: []dbschematypes.DBColumn{
					{Name: "status", DataType: "USER-DEFINED", UDTName: "invoice_status"},
				},
			},
		},
		Enums: []dbschematypes.DBEnum{
			{Name: "user_status", Values: []string{"active"}},
			{Name: "invoice_status", Values: []string{"paid"}},
		},
		Indexes: []dbschematypes.DBIndex{
			{Schema: "auth", TableName: "users", Name: "idx_users_status"},
			{Schema: "billing", TableName: "invoices", Name: "idx_invoices_status"},
		},
		Constraints: []dbschematypes.DBConstraint{
			{Schema: "auth", TableName: "users", Name: "users_status_check", Type: "CHECK"},
			{Schema: "auth", TableName: "users", Name: "users_invoice_fk", Type: "FOREIGN KEY", ForeignTable: &foreignTable, ForeignSchema: "billing"},
			{Schema: "billing", TableName: "invoices", Name: "invoices_status_check", Type: "CHECK"},
		},
		Extensions: []dbschematypes.DBExtension{
			{Schema: "auth", Name: "pg_trgm"},
			{Schema: "billing", Name: "btree_gin"},
		},
		Views: []dbschematypes.DBView{
			{Schema: "auth", Name: "active_users"},
			{Schema: "billing", Name: "open_invoices"},
		},
		MatViews: []dbschematypes.DBMatView{
			{Schema: "auth", Name: "user_stats"},
			{Schema: "billing", Name: "invoice_stats"},
		},
		Triggers: []dbschematypes.DBTrigger{
			{Schema: "auth", Table: "users", Name: "users_updated_at"},
			{Schema: "billing", Table: "invoices", Name: "invoices_updated_at"},
		},
		RLSPolicies: []dbschematypes.DBRLSPolicy{
			{Table: "users", Name: "users_tenant"},
			{Table: "billing.invoices", Name: "invoices_tenant"},
		},
		Grants: []dbschematypes.DBGrant{
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
	c.Assert(databaseExtensionNames(got.Extensions), qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(databaseViewNames(got.Views), qt.DeepEquals, []string{"auth.active_users"})
	c.Assert(databaseMatViewNames(got.MatViews), qt.DeepEquals, []string{"auth.user_stats"})
	c.Assert(databaseTriggerNames(got.Triggers), qt.DeepEquals, []string{"users_updated_at"})
	c.Assert(databaseRLSPolicyNames(got.RLSPolicies), qt.DeepEquals, []string{"users_tenant"})
	c.Assert(databaseGrantTargets(got.Grants), qt.DeepEquals, []string{"schema:auth", "table:auth.users"})
}

func TestFilterDatabaseWithDefaultSchemaKeepsUnqualifiedPublicObjects(t *testing.T) {
	c := qt.New(t)
	db := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Schema: "", Name: "users"},
			{Schema: "audit", Name: "logs"},
		},
		Indexes: []dbschematypes.DBIndex{
			{Schema: "", TableName: "users", Name: "idx_users_id"},
			{Schema: "audit", TableName: "logs", Name: "idx_logs_id"},
		},
		Constraints: []dbschematypes.DBConstraint{
			{Schema: "", TableName: "users", Name: "users_pkey", Type: "PRIMARY KEY"},
			{Schema: "audit", TableName: "logs", Name: "logs_pkey", Type: "PRIMARY KEY"},
		},
		Views: []dbschematypes.DBView{
			{Schema: "", Name: "active_users"},
			{Schema: "audit", Name: "recent_logs"},
		},
		RLSPolicies: []dbschematypes.DBRLSPolicy{
			{Table: "users", Name: "users_tenant"},
			{Table: "audit.logs", Name: "logs_tenant"},
		},
		Grants: []dbschematypes.DBGrant{
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

func generatedTableNames(tables []goschema.Table) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.QualifiedName())
	}
	return names
}

func generatedSchemaNames(schemas []goschema.Schema) []string {
	names := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		names = append(names, schema.Name)
	}
	return names
}

func generatedFieldNames(fields []goschema.Field) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Name)
	}
	return names
}

func generatedIndexNames(indexes []goschema.Index) []string {
	names := make([]string, 0, len(indexes))
	for _, index := range indexes {
		names = append(names, index.Name)
	}
	return names
}

func generatedConstraintNames(constraints []goschema.Constraint) []string {
	names := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		names = append(names, constraint.Name)
	}
	return names
}

func generatedEnumNames(enums []goschema.Enum) []string {
	names := make([]string, 0, len(enums))
	for _, enum := range enums {
		names = append(names, enum.Name)
	}
	return names
}

func generatedFunctionNames(functions []goschema.Function) []string {
	names := make([]string, 0, len(functions))
	for _, function := range functions {
		names = append(names, function.Name)
	}
	return names
}

func generatedViewNames(views []goschema.View) []string {
	names := make([]string, 0, len(views))
	for _, view := range views {
		names = append(names, view.Name)
	}
	return names
}

func generatedTriggerNames(triggers []goschema.Trigger) []string {
	names := make([]string, 0, len(triggers))
	for _, trigger := range triggers {
		names = append(names, trigger.Name)
	}
	return names
}

func generatedRLSPolicyNames(policies []goschema.RLSPolicy) []string {
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.Name)
	}
	return names
}

func generatedRLSTableNames(tables []goschema.RLSEnabledTable) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Table)
	}
	return names
}

func generatedGrantTargets(grants []goschema.Grant) []string {
	targets := make([]string, 0, len(grants))
	for _, grant := range grants {
		targets = append(targets, grantTarget(grant.OnSchema, grant.OnTable))
	}
	return targets
}

func grantTarget(schema string, table string) string {
	if schema != "" {
		return "schema:" + schema
	}
	return "table:" + table
}

func databaseTableNames(tables []dbschematypes.DBTable) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.QualifiedName())
	}
	return names
}

func databaseEnumNames(enums []dbschematypes.DBEnum) []string {
	names := make([]string, 0, len(enums))
	for _, enum := range enums {
		names = append(names, enum.Name)
	}
	return names
}

func databaseIndexNames(indexes []dbschematypes.DBIndex) []string {
	names := make([]string, 0, len(indexes))
	for _, index := range indexes {
		names = append(names, index.Name)
	}
	return names
}

func databaseConstraintNames(constraints []dbschematypes.DBConstraint) []string {
	names := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		names = append(names, constraint.Name)
	}
	return names
}

func databaseExtensionNames(extensions []dbschematypes.DBExtension) []string {
	names := make([]string, 0, len(extensions))
	for _, extension := range extensions {
		names = append(names, extension.Name)
	}
	return names
}

func databaseViewNames(views []dbschematypes.DBView) []string {
	names := make([]string, 0, len(views))
	for _, view := range views {
		names = append(names, view.QualifiedName())
	}
	return names
}

func databaseMatViewNames(views []dbschematypes.DBMatView) []string {
	names := make([]string, 0, len(views))
	for _, view := range views {
		names = append(names, view.QualifiedName())
	}
	return names
}

func databaseTriggerNames(triggers []dbschematypes.DBTrigger) []string {
	names := make([]string, 0, len(triggers))
	for _, trigger := range triggers {
		names = append(names, trigger.Name)
	}
	return names
}

func databaseRLSPolicyNames(policies []dbschematypes.DBRLSPolicy) []string {
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.Name)
	}
	return names
}

func databaseGrantTargets(grants []dbschematypes.DBGrant) []string {
	targets := make([]string, 0, len(grants))
	for _, grant := range grants {
		targets = append(targets, dbGrantTarget(grant))
	}
	return targets
}

func dbGrantTarget(grant dbschematypes.DBGrant) string {
	if grant.ObjectType == "SCHEMA" {
		return "schema:" + grant.ObjectName
	}
	return "table:" + grant.QualifiedTarget()
}
