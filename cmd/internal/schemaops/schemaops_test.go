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

func TestFilterDatabaseTables_RemovesOnlyIgnoredTableEnums(t *testing.T) {
	c := qt.New(t)

	db := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{
				Name: "users",
				Columns: []dbschematypes.DBColumn{
					{Name: "status", UDTName: "enum_user_status"},
				},
			},
			{
				Name: "audit_log",
				Columns: []dbschematypes.DBColumn{
					{Name: "status", UDTName: "enum_auditlog_status"},
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
