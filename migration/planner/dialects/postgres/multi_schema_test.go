package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_GenerateMigrationAST_MultiSchemaTablesAndFKs(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users", Schema: "auth"},
			{StructName: "Invoice", Name: "invoices", Schema: "billing"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "user_id", Type: "INTEGER", Foreign: "auth.users(id)"},
		},
		SelfReferencingForeignKeys: map[string][]goschema.SelfReferencingFK{},
	}
	diff := &types.SchemaDiff{
		TablesAdded: []string{"auth.users", "billing.invoices"},
	}

	nodes := postgres.New().GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sql, qt.Contains, "CREATE SCHEMA IF NOT EXISTS auth;")
	c.Assert(sql, qt.Contains, "CREATE SCHEMA IF NOT EXISTS billing;")
	c.Assert(sql, qt.Contains, "CREATE TABLE auth.users")
	c.Assert(sql, qt.Contains, "CREATE TABLE billing.invoices")
	c.Assert(sql, qt.Contains, "ALTER TABLE billing.invoices ADD CONSTRAINT fk_invoices_user_id FOREIGN KEY (user_id) REFERENCES auth.users(id);")
}

func TestPlanner_GenerateMigrationAST_TrimsSchemaPreconditions(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users", Schema: " auth "},
			{StructName: "Account", Name: "accounts", Schema: "auth"},
			{StructName: "Blank", Name: "blank", Schema: "   "},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Account", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Blank", Name: "id", Type: "SERIAL", Primary: true},
		},
		SelfReferencingForeignKeys: map[string][]goschema.SelfReferencingFK{},
	}
	diff := &types.SchemaDiff{
		TablesAdded: []string{"auth.users", "auth.accounts", "blank"},
	}

	nodes := postgres.New().GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sql, qt.Contains, "CREATE SCHEMA IF NOT EXISTS auth;")
	c.Assert(sql, qt.Not(qt.Contains), "CREATE SCHEMA IF NOT EXISTS  auth ;")
	c.Assert(sql, qt.Not(qt.Contains), "CREATE SCHEMA IF NOT EXISTS    ;")
	c.Assert(countSQLLine(sql, "CREATE SCHEMA IF NOT EXISTS auth;"), qt.Equals, 1)
}

func TestPlanner_GenerateMigrationAST_DoesNotQualifyAmbiguousLeafFK(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "AuthUser", Name: "users", Schema: "auth"},
			{StructName: "CrmUser", Name: "users", Schema: "crm"},
			{StructName: "Invoice", Name: "invoices", Schema: "billing"},
		},
		Fields: []goschema.Field{
			{StructName: "AuthUser", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "CrmUser", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Invoice", Name: "user_id", Type: "INTEGER", Foreign: "users(id)"},
		},
		SelfReferencingForeignKeys: map[string][]goschema.SelfReferencingFK{},
	}
	diff := &types.SchemaDiff{
		TablesAdded: []string{"auth.users", "crm.users", "billing.invoices"},
	}

	nodes := postgres.New().GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sql, qt.Contains, "ALTER TABLE billing.invoices ADD CONSTRAINT fk_invoices_user_id FOREIGN KEY (user_id) REFERENCES users(id);")
	c.Assert(sql, qt.Not(qt.Contains), "REFERENCES auth.users(id)")
	c.Assert(sql, qt.Not(qt.Contains), "REFERENCES crm.users(id)")
}

func countSQLLine(sql, line string) int {
	count := 0
	for sqlLine := range strings.SplitSeq(sql, "\n") {
		if sqlLine == line {
			count++
		}
	}
	return count
}
