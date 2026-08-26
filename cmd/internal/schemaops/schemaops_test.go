package schemaops_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/cmd/internal/schemaops"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/core/schemasource"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
)

const sqlServerDatabaseURL = "sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable"

func TestCompare_UsesDatabaseURLDialectForExternalSQL(t *testing.T) {
	c := qt.New(t)
	command := schemasource.Command{
		Args: []string{"go", "run", "./testdata/sqlserver-schema-command"},
	}

	_, err := schemasource.Run(context.Background(), command)
	c.Assert(err, qt.ErrorMatches, `parse schema command "go" output: unsupported CREATE OR ALTER outside SQL Server dialect at position \d+`)

	_, err = schemaops.Compare(t.Context(), schemaops.CompareOptions{
		Commands:       []schemasource.Command{command},
		DatabaseURL:    sqlServerDatabaseURL,
		ConnectTimeout: time.Nanosecond,
	})
	c.Assert(err, qt.ErrorMatches, `error connecting to database: .*`)
}

func TestCompare_ValidatesVirtualDropToggleBeforeExternalSchema(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "maybe")(t)

	_, err := schemaops.Compare(t.Context(), schemaops.CompareOptions{
		Commands:    []schemasource.Command{{Args: []string{"/path/that/does/not/exist"}}},
		DatabaseURL: "sqlite://test.db",
	})

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "maybe" for `+sqlitevirtual.AllowDropEnvVar)
}

func TestFilterGeneratedTables_RemovesTableScopedObjects(t *testing.T) {
	c := qt.New(t)

	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
			{StructName: "AuditLog", Name: "audit_log"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INTEGER"},
			{StructName: "AuditLog", Name: "status", Type: "enum_auditlog_status"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "AuditLog", Name: "idx_audit_log_status"},
			{StructName: "User", Name: "idx_users_id"},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "AuditLog", Name: "audit_log_status_check"},
		},
		Enums: []schemamodel.Enum{
			{Name: "enum_auditlog_status", Values: []string{"ok", "failed"}},
		},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{
			{Table: "audit_log"},
		},
		Dependencies: map[string][]string{
			"audit_log": {"users"},
			"users":     {"audit_log"},
		},
	}

	filtered := schemaops.FilterGeneratedTables(db, []string{"audit_log"})

	c.Assert(filtered.Tables, qt.DeepEquals, []schemamodel.Table{{StructName: "User", Name: "users"}})
	c.Assert(filtered.Fields, qt.DeepEquals, []schemamodel.Field{{StructName: "User", Name: "id", Type: "INTEGER"}})
	c.Assert(filtered.Indexes, qt.DeepEquals, []schemamodel.Index{{StructName: "User", Name: "idx_users_id"}})
	c.Assert(filtered.Constraints, qt.HasLen, 0)
	c.Assert(filtered.Enums, qt.HasLen, 0)
	c.Assert(filtered.RLSEnabledTables, qt.HasLen, 0)
	c.Assert(filtered.Dependencies, qt.DeepEquals, map[string][]string{"users": {}})
}

func TestFilterGeneratedTables_RemovesSchemaQualifiedTableScopedObjects(t *testing.T) {
	c := qt.New(t)

	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Schema: "auth", Name: "users"},
			{StructName: "Invoice", Schema: "billing", Name: "invoices"},
		},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "status", Type: "enum_user_status"},
			{StructName: "Invoice", Name: "id", Type: "INTEGER"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "User", Name: "idx_users_status", TableName: "auth.users"},
			{StructName: "Invoice", Name: "idx_invoices_id", TableName: "billing.invoices"},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "User", Name: "users_status_check", Table: "auth.users"},
			{StructName: "Invoice", Name: "invoices_id_check", Table: "billing.invoices"},
		},
		Enums: []schemamodel.Enum{
			{Name: "enum_user_status", Values: []string{"active"}},
			{Name: "orphan_enum", Values: []string{"kept"}},
		},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{
			{Table: "auth.users"},
			{Table: "billing.invoices"},
		},
		Dependencies: map[string][]string{
			"auth.users":       {"billing.invoices"},
			"billing.invoices": {"auth.users"},
		},
		SelfReferencingForeignKeys: map[string][]schemamodel.SelfReferencingFK{
			"auth.users":       {{FieldName: "parent_id"}},
			"billing.invoices": {{FieldName: "parent_id"}},
		},
	}

	filtered := schemaops.FilterGeneratedTables(db, []string{"auth.users"})

	c.Assert(filtered.Tables, qt.DeepEquals, []schemamodel.Table{{StructName: "Invoice", Schema: "billing", Name: "invoices"}})
	c.Assert(filtered.Fields, qt.DeepEquals, []schemamodel.Field{{StructName: "Invoice", Name: "id", Type: "INTEGER"}})
	c.Assert(filtered.Indexes, qt.DeepEquals, []schemamodel.Index{{StructName: "Invoice", Name: "idx_invoices_id", TableName: "billing.invoices"}})
	c.Assert(filtered.Constraints, qt.DeepEquals, []schemamodel.Constraint{{StructName: "Invoice", Name: "invoices_id_check", Table: "billing.invoices"}})
	c.Assert(filtered.Enums, qt.DeepEquals, []schemamodel.Enum{{Name: "orphan_enum", Values: []string{"kept"}}})
	c.Assert(filtered.RLSEnabledTables, qt.DeepEquals, []schemamodel.RLSEnabledTable{{Table: "billing.invoices"}})
	c.Assert(filtered.Dependencies, qt.DeepEquals, map[string][]string{"billing.invoices": {}})
	c.Assert(filtered.SelfReferencingForeignKeys, qt.DeepEquals, map[string][]schemamodel.SelfReferencingFK{
		"billing.invoices": {{FieldName: "parent_id"}},
	})
}

func TestFilterDatabaseTables_RemovesOnlyIgnoredTableEnums(t *testing.T) {
	c := qt.New(t)

	db := &catalog.Database{
		Tables: []catalog.Table{
			{
				Name: "users",
				Columns: []catalog.Column{
					{Name: "status", DataType: "USER-DEFINED", UDTName: "enum_user_status"},
				},
			},
			{
				Name: "audit_log",
				Columns: []catalog.Column{
					{Name: "statuses", DataType: "ARRAY", UDTName: "_enum_auditlog_status"},
				},
			},
		},
		Enums: []catalog.Enum{
			{Name: "enum_user_status", Values: []string{"active"}},
			{Name: "enum_auditlog_status", Values: []string{"ok"}},
			{Name: "orphan_enum", Values: []string{"kept"}},
		},
		Indexes: []catalog.Index{
			{Name: "idx_audit_log_status", TableName: "audit_log"},
		},
		Constraints: []catalog.Constraint{
			{Name: "audit_log_status_check", TableName: "audit_log"},
		},
		RLSPolicies: []catalog.RLSPolicy{
			{Name: "audit_rls", Table: "audit_log"},
		},
	}

	filtered := schemaops.FilterDatabaseTables(db, []string{"audit_log"})

	c.Assert(filtered.Tables, qt.HasLen, 1)
	c.Assert(filtered.Tables[0].Name, qt.Equals, "users")
	c.Assert(filtered.Indexes, qt.HasLen, 0)
	c.Assert(filtered.Constraints, qt.HasLen, 0)
	c.Assert(filtered.RLSPolicies, qt.HasLen, 0)
	c.Assert(filtered.Enums, qt.DeepEquals, []catalog.Enum{
		{Name: "enum_user_status", Values: []string{"active"}},
		{Name: "orphan_enum", Values: []string{"kept"}},
	})
}

func TestFilterDatabaseTables_RemovesSchemaQualifiedTableScopedObjects(t *testing.T) {
	c := qt.New(t)

	db := &catalog.Database{
		Tables: []catalog.Table{
			{
				Schema: "auth",
				Name:   "users",
				Columns: []catalog.Column{
					{Name: "status", DataType: "USER-DEFINED", UDTName: "enum_user_status"},
				},
			},
			{
				Schema: "billing",
				Name:   "invoices",
				Columns: []catalog.Column{
					{Name: "state", DataType: "USER-DEFINED", UDTName: "enum_invoice_state"},
				},
			},
		},
		Enums: []catalog.Enum{
			{Name: "enum_user_status", Values: []string{"active"}},
			{Name: "enum_invoice_state", Values: []string{"open"}},
			{Name: "orphan_enum", Values: []string{"kept"}},
		},
		Indexes: []catalog.Index{
			{Name: "idx_users_status", Schema: "auth", TableName: "users"},
			{Name: "idx_invoices_state", Schema: "billing", TableName: "invoices"},
		},
		Constraints: []catalog.Constraint{
			{Name: "users_status_check", Schema: "auth", TableName: "users"},
			{Name: "invoices_state_check", Schema: "billing", TableName: "invoices"},
		},
		RLSPolicies: []catalog.RLSPolicy{
			{Name: "users_rls", Table: "auth.users"},
			{Name: "invoices_rls", Table: "billing.invoices"},
		},
	}

	filtered := schemaops.FilterDatabaseTables(db, []string{"auth.users"})

	c.Assert(filtered.Tables, qt.DeepEquals, []catalog.Table{{
		Schema: "billing",
		Name:   "invoices",
		Columns: []catalog.Column{
			{Name: "state", DataType: "USER-DEFINED", UDTName: "enum_invoice_state"},
		},
	}})
	c.Assert(filtered.Indexes, qt.DeepEquals, []catalog.Index{{Name: "idx_invoices_state", Schema: "billing", TableName: "invoices"}})
	c.Assert(filtered.Constraints, qt.DeepEquals, []catalog.Constraint{{Name: "invoices_state_check", Schema: "billing", TableName: "invoices"}})
	c.Assert(filtered.RLSPolicies, qt.DeepEquals, []catalog.RLSPolicy{{Name: "invoices_rls", Table: "billing.invoices"}})
	c.Assert(filtered.Enums, qt.DeepEquals, []catalog.Enum{
		{Name: "enum_invoice_state", Values: []string{"open"}},
		{Name: "orphan_enum", Values: []string{"kept"}},
	})
}

func TestFilterDatabaseTables_IgnoresNonEnumUDTNames(t *testing.T) {
	c := qt.New(t)

	db := &catalog.Database{
		Tables: []catalog.Table{
			{
				Name: "audit_log",
				Columns: []catalog.Column{
					{Name: "status_text", DataType: "text", UDTName: "enum_auditlog_status"},
				},
			},
		},
		Enums: []catalog.Enum{
			{Name: "enum_auditlog_status", Values: []string{"kept"}},
		},
	}

	filtered := schemaops.FilterDatabaseTables(db, []string{"audit_log"})

	c.Assert(filtered.Enums, qt.DeepEquals, db.Enums)
}
