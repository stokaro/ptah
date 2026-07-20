package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_GenerateMigrationAST_MySQLFamilyOrdersFKChainTables(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			generated := dependencyOrderSchema()
			diff := &types.SchemaDiff{
				TablesAdded: []string{
					"ptah_fk_order_tasks",
					"ptah_fk_order_projects",
					"ptah_fk_order_accounts",
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			assertContainsBefore(c, sql, "CREATE TABLE ptah_fk_order_accounts", "CREATE TABLE ptah_fk_order_projects")
			assertContainsBefore(c, sql, "CREATE TABLE ptah_fk_order_projects", "CREATE TABLE ptah_fk_order_tasks")
			assertContainsBefore(c, sql, "CREATE TABLE ptah_fk_order_tasks", "ALTER TABLE ptah_fk_order_projects ADD CONSTRAINT")
		})
	}
}

func TestPlanner_GenerateMigrationAST_MySQLFamilyOrdersFKDiamondTables(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			generated := dependencyOrderSchema()
			diff := &types.SchemaDiff{
				TablesAdded: []string{
					"ptah_fk_order_tasks",
					"ptah_fk_order_projects",
					"ptah_fk_order_memberships",
					"ptah_fk_order_accounts",
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			assertContainsBefore(c, sql, "CREATE TABLE ptah_fk_order_accounts", "CREATE TABLE ptah_fk_order_projects")
			assertContainsBefore(c, sql, "CREATE TABLE ptah_fk_order_accounts", "CREATE TABLE ptah_fk_order_memberships")
			assertContainsBefore(c, sql, "CREATE TABLE ptah_fk_order_projects", "CREATE TABLE ptah_fk_order_tasks")
			assertContainsBefore(c, sql, "CREATE TABLE ptah_fk_order_memberships", "CREATE TABLE ptah_fk_order_tasks")
		})
	}
}

func TestPlanner_GenerateMigrationAST_MySQLFamilyDropsFKDiamondTablesInDependencyOrder(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			generated := dependencyOrderSchema()
			diff := &types.SchemaDiff{
				TablesRemoved: []string{
					"ptah_fk_order_accounts",
					"ptah_fk_order_projects",
					"ptah_fk_order_memberships",
					"ptah_fk_order_tasks",
				},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			assertContainsBefore(c, sql, "DROP TABLE IF EXISTS ptah_fk_order_tasks", "DROP TABLE IF EXISTS ptah_fk_order_projects")
			assertContainsBefore(c, sql, "DROP TABLE IF EXISTS ptah_fk_order_tasks", "DROP TABLE IF EXISTS ptah_fk_order_memberships")
			assertContainsBefore(c, sql, "DROP TABLE IF EXISTS ptah_fk_order_projects", "DROP TABLE IF EXISTS ptah_fk_order_accounts")
			assertContainsBefore(c, sql, "DROP TABLE IF EXISTS ptah_fk_order_memberships", "DROP TABLE IF EXISTS ptah_fk_order_accounts")
		})
	}
}

func TestPlanner_GenerateMigrationAST_MySQLFamilyAddsReferencedUniqueIndexBeforeNewTableFKs(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			generated := referencedUniqueKeySchema()
			diff := &types.SchemaDiff{
				TablesAdded:  []string{"ptah_fk_order_children", "ptah_fk_order_parents"},
				IndexesAdded: []string{"uq_ptah_fk_order_parents_code_idx"},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			assertContainsBefore(c, sql, "CREATE TABLE ptah_fk_order_parents", "CREATE TABLE ptah_fk_order_children")
			assertContainsBefore(c, sql, "CREATE UNIQUE INDEX uq_ptah_fk_order_parents_code_idx", "ALTER TABLE ptah_fk_order_children ADD CONSTRAINT")
		})
	}
}

func TestPlanner_GenerateMigrationAST_MySQLFamilyAddsReferencedUniqueConstraintBeforeNewTableFKs(t *testing.T) {
	for _, dialect := range mysqlFamilyDialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			generated := referencedUniqueKeySchema()
			diff := &types.SchemaDiff{
				TablesAdded:      []string{"ptah_fk_order_children", "ptah_fk_order_parents"},
				ConstraintsAdded: []string{"uq_ptah_fk_order_parents_code"},
				ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{{
					Name:      "uq_ptah_fk_order_parents_code",
					TableName: "ptah_fk_order_parents",
					Type:      "UNIQUE",
					Columns:   []string{"code"},
				}},
			}

			sql := renderMySQLFamily(c, dialect, diff, generated)

			assertContainsBefore(c, sql, "ALTER TABLE ptah_fk_order_parents ADD CONSTRAINT uq_ptah_fk_order_parents_code", "ALTER TABLE ptah_fk_order_children ADD CONSTRAINT")
		})
	}
}

func dependencyOrderSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "PtahFKOrderTask", Name: "ptah_fk_order_tasks"},
			{StructName: "PtahFKOrderProject", Name: "ptah_fk_order_projects"},
			{StructName: "PtahFKOrderMembership", Name: "ptah_fk_order_memberships"},
			{StructName: "PtahFKOrderAccount", Name: "ptah_fk_order_accounts"},
		},
		Fields: []goschema.Field{
			{StructName: "PtahFKOrderAccount", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "PtahFKOrderProject", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{
				StructName:     "PtahFKOrderProject",
				Name:           "account_id",
				Type:           "VARCHAR(36)",
				Foreign:        "ptah_fk_order_accounts(id)",
				ForeignKeyName: "fk_ptah_fk_order_projects_account",
			},
			{StructName: "PtahFKOrderMembership", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{
				StructName:     "PtahFKOrderMembership",
				Name:           "account_id",
				Type:           "VARCHAR(36)",
				Foreign:        "ptah_fk_order_accounts(id)",
				ForeignKeyName: "fk_ptah_fk_order_memberships_account",
			},
			{StructName: "PtahFKOrderTask", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{
				StructName:     "PtahFKOrderTask",
				Name:           "project_id",
				Type:           "VARCHAR(36)",
				Foreign:        "ptah_fk_order_projects(id)",
				ForeignKeyName: "fk_ptah_fk_order_tasks_project",
			},
			{
				StructName:     "PtahFKOrderTask",
				Name:           "membership_id",
				Type:           "VARCHAR(36)",
				Foreign:        "ptah_fk_order_memberships(id)",
				ForeignKeyName: "fk_ptah_fk_order_tasks_membership",
			},
		},
	}
}

func referencedUniqueKeySchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "PtahFKOrderChild", Name: "ptah_fk_order_children"},
			{StructName: "PtahFKOrderParent", Name: "ptah_fk_order_parents"},
		},
		Fields: []goschema.Field{
			{StructName: "PtahFKOrderParent", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "PtahFKOrderParent", Name: "code", Type: "VARCHAR(36)"},
			{StructName: "PtahFKOrderChild", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{
				StructName:     "PtahFKOrderChild",
				Name:           "parent_code",
				Type:           "VARCHAR(36)",
				Foreign:        "ptah_fk_order_parents(code)",
				ForeignKeyName: "fk_ptah_fk_order_children_parent_code",
			},
		},
		Indexes: []goschema.Index{{
			StructName: "PtahFKOrderParent",
			Name:       "uq_ptah_fk_order_parents_code_idx",
			Fields:     []string{"code"},
			Unique:     true,
		}},
	}
}
