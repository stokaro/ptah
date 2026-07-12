package schemaops_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/schemaops"
	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
)

func TestFilterGeneratedTables_RemovesTableScopedObjects(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "AuditLog", Name: "audit_log"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER"},
			{StructName: "AuditLog", Name: "status", Type: "enum_auditlog_status"},
		},
		Indexes: []goschema.Index{
			{StructName: "AuditLog", Name: "idx_audit_log_status"},
			{StructName: "User", Name: "idx_users_id"},
		},
		Constraints: []goschema.Constraint{
			{StructName: "AuditLog", Name: "audit_log_status_check"},
		},
		Enums: []goschema.Enum{
			{Name: "enum_auditlog_status", Values: []string{"ok", "failed"}},
		},
		RLSEnabledTables: []goschema.RLSEnabledTable{
			{Table: "audit_log"},
		},
		Dependencies: map[string][]string{
			"audit_log": {"users"},
			"users":     {"audit_log"},
		},
	}

	filtered := schemaops.FilterGeneratedTables(db, []string{"audit_log"})

	c.Assert(filtered.Tables, qt.DeepEquals, []goschema.Table{{StructName: "User", Name: "users"}})
	c.Assert(filtered.Fields, qt.DeepEquals, []goschema.Field{{StructName: "User", Name: "id", Type: "INTEGER"}})
	c.Assert(filtered.Indexes, qt.DeepEquals, []goschema.Index{{StructName: "User", Name: "idx_users_id"}})
	c.Assert(filtered.Constraints, qt.HasLen, 0)
	c.Assert(filtered.Enums, qt.HasLen, 0)
	c.Assert(filtered.RLSEnabledTables, qt.HasLen, 0)
	c.Assert(filtered.Dependencies, qt.DeepEquals, map[string][]string{"users": {}})
}

func TestFilterGeneratedTables_RemovesSchemaQualifiedTableScopedObjects(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "auth", Name: "users"},
			{StructName: "Invoice", Schema: "billing", Name: "invoices"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "status", Type: "enum_user_status"},
			{StructName: "Invoice", Name: "id", Type: "INTEGER"},
		},
		Indexes: []goschema.Index{
			{StructName: "User", Name: "idx_users_status", TableName: "auth.users"},
			{StructName: "Invoice", Name: "idx_invoices_id", TableName: "billing.invoices"},
		},
		Constraints: []goschema.Constraint{
			{StructName: "User", Name: "users_status_check", Table: "auth.users"},
			{StructName: "Invoice", Name: "invoices_id_check", Table: "billing.invoices"},
		},
		Enums: []goschema.Enum{
			{Name: "enum_user_status", Values: []string{"active"}},
			{Name: "orphan_enum", Values: []string{"kept"}},
		},
		RLSEnabledTables: []goschema.RLSEnabledTable{
			{Table: "auth.users"},
			{Table: "billing.invoices"},
		},
		Dependencies: map[string][]string{
			"auth.users":       {"billing.invoices"},
			"billing.invoices": {"auth.users"},
		},
		SelfReferencingForeignKeys: map[string][]goschema.SelfReferencingFK{
			"auth.users":       {{FieldName: "parent_id"}},
			"billing.invoices": {{FieldName: "parent_id"}},
		},
	}

	filtered := schemaops.FilterGeneratedTables(db, []string{"auth.users"})

	c.Assert(filtered.Tables, qt.DeepEquals, []goschema.Table{{StructName: "Invoice", Schema: "billing", Name: "invoices"}})
	c.Assert(filtered.Fields, qt.DeepEquals, []goschema.Field{{StructName: "Invoice", Name: "id", Type: "INTEGER"}})
	c.Assert(filtered.Indexes, qt.DeepEquals, []goschema.Index{{StructName: "Invoice", Name: "idx_invoices_id", TableName: "billing.invoices"}})
	c.Assert(filtered.Constraints, qt.DeepEquals, []goschema.Constraint{{StructName: "Invoice", Name: "invoices_id_check", Table: "billing.invoices"}})
	c.Assert(filtered.Enums, qt.DeepEquals, []goschema.Enum{{Name: "orphan_enum", Values: []string{"kept"}}})
	c.Assert(filtered.RLSEnabledTables, qt.DeepEquals, []goschema.RLSEnabledTable{{Table: "billing.invoices"}})
	c.Assert(filtered.Dependencies, qt.DeepEquals, map[string][]string{"billing.invoices": {}})
	c.Assert(filtered.SelfReferencingForeignKeys, qt.DeepEquals, map[string][]goschema.SelfReferencingFK{
		"billing.invoices": {{FieldName: "parent_id"}},
	})
}

func TestFilterDatabaseTables_RemovesOnlyIgnoredTableEnums(t *testing.T) {
	c := qt.New(t)

	db := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{
				Name: "users",
				Columns: []dbschematypes.DBColumn{
					{Name: "status", DataType: "USER-DEFINED", UDTName: "enum_user_status"},
				},
			},
			{
				Name: "audit_log",
				Columns: []dbschematypes.DBColumn{
					{Name: "statuses", DataType: "ARRAY", UDTName: "_enum_auditlog_status"},
				},
			},
		},
		Enums: []dbschematypes.DBEnum{
			{Name: "enum_user_status", Values: []string{"active"}},
			{Name: "enum_auditlog_status", Values: []string{"ok"}},
			{Name: "orphan_enum", Values: []string{"kept"}},
		},
		Indexes: []dbschematypes.DBIndex{
			{Name: "idx_audit_log_status", TableName: "audit_log"},
		},
		Constraints: []dbschematypes.DBConstraint{
			{Name: "audit_log_status_check", TableName: "audit_log"},
		},
		RLSPolicies: []dbschematypes.DBRLSPolicy{
			{Name: "audit_rls", Table: "audit_log"},
		},
	}

	filtered := schemaops.FilterDatabaseTables(db, []string{"audit_log"})

	c.Assert(filtered.Tables, qt.HasLen, 1)
	c.Assert(filtered.Tables[0].Name, qt.Equals, "users")
	c.Assert(filtered.Indexes, qt.HasLen, 0)
	c.Assert(filtered.Constraints, qt.HasLen, 0)
	c.Assert(filtered.RLSPolicies, qt.HasLen, 0)
	c.Assert(filtered.Enums, qt.DeepEquals, []dbschematypes.DBEnum{
		{Name: "enum_user_status", Values: []string{"active"}},
		{Name: "orphan_enum", Values: []string{"kept"}},
	})
}

func TestFilterDatabaseTables_RemovesSchemaQualifiedTableScopedObjects(t *testing.T) {
	c := qt.New(t)

	db := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{
				Schema: "auth",
				Name:   "users",
				Columns: []dbschematypes.DBColumn{
					{Name: "status", DataType: "USER-DEFINED", UDTName: "enum_user_status"},
				},
			},
			{
				Schema: "billing",
				Name:   "invoices",
				Columns: []dbschematypes.DBColumn{
					{Name: "state", DataType: "USER-DEFINED", UDTName: "enum_invoice_state"},
				},
			},
		},
		Enums: []dbschematypes.DBEnum{
			{Name: "enum_user_status", Values: []string{"active"}},
			{Name: "enum_invoice_state", Values: []string{"open"}},
			{Name: "orphan_enum", Values: []string{"kept"}},
		},
		Indexes: []dbschematypes.DBIndex{
			{Name: "idx_users_status", Schema: "auth", TableName: "users"},
			{Name: "idx_invoices_state", Schema: "billing", TableName: "invoices"},
		},
		Constraints: []dbschematypes.DBConstraint{
			{Name: "users_status_check", Schema: "auth", TableName: "users"},
			{Name: "invoices_state_check", Schema: "billing", TableName: "invoices"},
		},
		RLSPolicies: []dbschematypes.DBRLSPolicy{
			{Name: "users_rls", Table: "auth.users"},
			{Name: "invoices_rls", Table: "billing.invoices"},
		},
	}

	filtered := schemaops.FilterDatabaseTables(db, []string{"auth.users"})

	c.Assert(filtered.Tables, qt.DeepEquals, []dbschematypes.DBTable{{
		Schema: "billing",
		Name:   "invoices",
		Columns: []dbschematypes.DBColumn{
			{Name: "state", DataType: "USER-DEFINED", UDTName: "enum_invoice_state"},
		},
	}})
	c.Assert(filtered.Indexes, qt.DeepEquals, []dbschematypes.DBIndex{{Name: "idx_invoices_state", Schema: "billing", TableName: "invoices"}})
	c.Assert(filtered.Constraints, qt.DeepEquals, []dbschematypes.DBConstraint{{Name: "invoices_state_check", Schema: "billing", TableName: "invoices"}})
	c.Assert(filtered.RLSPolicies, qt.DeepEquals, []dbschematypes.DBRLSPolicy{{Name: "invoices_rls", Table: "billing.invoices"}})
	c.Assert(filtered.Enums, qt.DeepEquals, []dbschematypes.DBEnum{
		{Name: "enum_invoice_state", Values: []string{"open"}},
		{Name: "orphan_enum", Values: []string{"kept"}},
	})
}

func TestFilterDatabaseTables_IgnoresNonEnumUDTNames(t *testing.T) {
	c := qt.New(t)

	db := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{
				Name: "audit_log",
				Columns: []dbschematypes.DBColumn{
					{Name: "status_text", DataType: "text", UDTName: "enum_auditlog_status"},
				},
			},
		},
		Enums: []dbschematypes.DBEnum{
			{Name: "enum_auditlog_status", Values: []string{"kept"}},
		},
	}

	filtered := schemaops.FilterDatabaseTables(db, []string{"audit_log"})

	c.Assert(filtered.Enums, qt.DeepEquals, db.Enums)
}
