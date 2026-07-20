package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/internal/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_GenerateMigrationAST_OrdersFKChainTables(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()
	generated := dependencyOrderSchema()
	diff := &types.SchemaDiff{
		TablesAdded: []string{
			"ptah_fk_order_tasks",
			"ptah_fk_order_projects",
			"ptah_fk_order_accounts",
		},
	}

	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	assertBefore(t, sql, "CREATE TABLE ptah_fk_order_accounts", "CREATE TABLE ptah_fk_order_projects")
	assertBefore(t, sql, "CREATE TABLE ptah_fk_order_projects", "CREATE TABLE ptah_fk_order_tasks")
	assertBefore(t, sql, "CREATE TABLE ptah_fk_order_tasks", "ALTER TABLE ptah_fk_order_projects ADD CONSTRAINT")
}

func TestPlanner_GenerateMigrationAST_OrdersFKDiamondTables(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()
	generated := dependencyOrderSchema()
	diff := &types.SchemaDiff{
		TablesAdded: []string{
			"ptah_fk_order_tasks",
			"ptah_fk_order_projects",
			"ptah_fk_order_memberships",
			"ptah_fk_order_accounts",
		},
	}

	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	assertBefore(t, sql, "CREATE TABLE ptah_fk_order_accounts", "CREATE TABLE ptah_fk_order_projects")
	assertBefore(t, sql, "CREATE TABLE ptah_fk_order_accounts", "CREATE TABLE ptah_fk_order_memberships")
	assertBefore(t, sql, "CREATE TABLE ptah_fk_order_projects", "CREATE TABLE ptah_fk_order_tasks")
	assertBefore(t, sql, "CREATE TABLE ptah_fk_order_memberships", "CREATE TABLE ptah_fk_order_tasks")
}

func TestPlanner_GenerateMigrationAST_DropsFKDiamondTablesInDependencyOrder(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()
	generated := dependencyOrderSchema()
	diff := &types.SchemaDiff{
		TablesRemoved: []string{
			"ptah_fk_order_accounts",
			"ptah_fk_order_projects",
			"ptah_fk_order_memberships",
			"ptah_fk_order_tasks",
		},
	}

	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	assertBefore(t, sql, "DROP TABLE IF EXISTS ptah_fk_order_tasks", "DROP TABLE IF EXISTS ptah_fk_order_projects")
	assertBefore(t, sql, "DROP TABLE IF EXISTS ptah_fk_order_tasks", "DROP TABLE IF EXISTS ptah_fk_order_memberships")
	assertBefore(t, sql, "DROP TABLE IF EXISTS ptah_fk_order_projects", "DROP TABLE IF EXISTS ptah_fk_order_accounts")
	assertBefore(t, sql, "DROP TABLE IF EXISTS ptah_fk_order_memberships", "DROP TABLE IF EXISTS ptah_fk_order_accounts")
}

func TestPlanner_GenerateMigrationAST_AddsReferencedUniqueIndexBeforeNewTableFKs(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()
	generated := referencedUniqueKeySchema()
	diff := &types.SchemaDiff{
		TablesAdded:  []string{"ptah_fk_order_children", "ptah_fk_order_parents"},
		IndexesAdded: []string{"uq_ptah_fk_order_parents_code_idx"},
	}

	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	assertBefore(t, sql, "CREATE TABLE ptah_fk_order_parents", "CREATE TABLE ptah_fk_order_children")
	assertBefore(t, sql, "CREATE UNIQUE INDEX IF NOT EXISTS uq_ptah_fk_order_parents_code_idx", "ALTER TABLE ptah_fk_order_children ADD CONSTRAINT")
}

func TestPlanner_GenerateMigrationAST_AddsReferencedUniqueConstraintBeforeNewTableFKs(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()
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

	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	assertBefore(t, sql, "ALTER TABLE ptah_fk_order_parents ADD CONSTRAINT uq_ptah_fk_order_parents_code", "ALTER TABLE ptah_fk_order_children ADD CONSTRAINT")
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
