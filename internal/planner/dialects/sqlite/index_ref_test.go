package sqlite_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/internal/planner/dialects/sqlite"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_IndexRefs_AttributesAdditionsToExactTables(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_orders_reference", TableName: "orders"},
			{Name: "idx_users_email", TableName: "users"},
		},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "orders", StructName: "Order"},
		},
		Indexes: []goschema.Index{
			{Name: "idx_users_email", StructName: "User", Fields: []string{"email"}},
			{Name: "idx_orders_reference", StructName: "Order", Fields: []string{"reference"}},
		},
	}

	nodes, err := sqlite.New().GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)

	c.Assert(nodes, qt.HasLen, 2)
	ordersIndex, ok := nodes[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(ordersIndex.Table, qt.Equals, "orders")
	c.Assert(ordersIndex.Columns, qt.DeepEquals, []string{"reference"})
	usersIndex, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(usersIndex.Table, qt.Equals, "users")
	c.Assert(usersIndex.Columns, qt.DeepEquals, []string{"email"})
}

func TestPlanner_IndexRefs_PreservesAttachedSchema(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_users_email", TableName: "tenant.users"},
		},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", Schema: "tenant", StructName: "TenantUser"},
		},
		Indexes: []goschema.Index{
			{Name: "idx_users_email", StructName: "TenantUser", Fields: []string{"email"}},
		},
	}

	nodes, err := sqlite.New().GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)

	c.Assert(nodes, qt.HasLen, 1)
	index, ok := nodes[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_email")
	c.Assert(index.Table, qt.Equals, "tenant.users")
	c.Assert(index.Columns, qt.DeepEquals, []string{"email"})
}

func TestPlanner_IndexRefs_DropsSameSchemaNameBeforeMovingIndex(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_shared", TableName: "tenant.orders"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_shared", TableName: "tenant.users"},
		},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "orders", Schema: "tenant", StructName: "Order"},
		},
		Indexes: []goschema.Index{
			{Name: "idx_shared", StructName: "Order", Fields: []string{"reference"}},
		},
	}

	nodes, err := sqlite.New().GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)

	c.Assert(nodes, qt.HasLen, 2)
	drop, ok := nodes[0].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "idx_shared")
	c.Assert(drop.Table, qt.Equals, "tenant.users")
	create, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Table, qt.Equals, "tenant.orders")
}

func TestPlanner_IndexRefs_ReplacesExactGlobalIndexBeforeCreate(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded:   []types.IndexRef{{Name: "idx_users_email", TableName: "users"}},
		IndexesRemoved: []types.IndexRef{{Name: "idx_users_email", TableName: "users"}},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{{Name: "users", StructName: "User"}},
		Indexes: []goschema.Index{{
			Name:       "idx_users_email",
			StructName: "User",
			Fields:     []string{"email"},
			Unique:     true,
		}},
	}

	nodes, err := sqlite.New().GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)

	c.Assert(nodes, qt.HasLen, 2)
	drop, ok := nodes[0].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "idx_users_email")
	c.Assert(drop.Table, qt.Equals, "users")
	c.Assert(drop.IfExists, qt.IsTrue)
	create, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Name, qt.Equals, "idx_users_email")
	c.Assert(create.Table, qt.Equals, "users")
	c.Assert(create.Unique, qt.IsTrue)
}

func TestPlanner_IndexRefs_UsesCanonicalOwnerWithDuplicateStructNames(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_users_email", TableName: "tenant.users"},
		},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Shared", Schema: "tenant", Name: "users"},
			{StructName: "Shared", Schema: "archive", Name: "records"},
		},
		Indexes: []goschema.Index{
			{
				Name:       "idx_users_email",
				StructName: "Shared",
				TableName:  "tenant.users",
				Fields:     []string{"email"},
			},
		},
	}

	nodes, err := sqlite.New().GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)

	index, ok := nodes[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.Name, qt.Equals, "idx_users_email")
	c.Assert(index.Table, qt.Equals, "tenant.users")
	c.Assert(generated.Indexes[0].TableName, qt.Equals, "tenant.users")
}

func TestPlanner_IndexRefs_CaseInsensitiveReplacementExecutesOnSQLite(t *testing.T) {
	c := qt.New(t)
	db, err := sql.Open("sqlite", ":memory:")
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE users (email TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE INDEX "IDX_Users_Email" ON users (email)`)
	c.Assert(err, qt.IsNil)

	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_users_email", TableName: "users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "IDX_Users_Email", TableName: "users"},
		},
	}
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{
				Name:      "idx_users_email",
				TableName: "users",
				Fields:    []string{"email"},
				Unique:    true,
			},
		},
	}

	nodes, err := sqlite.New().GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)

	dropSQL, err := renderer.RenderSQL(platform.SQLite, nodes[0])
	c.Assert(err, qt.IsNil)
	createSQL, err := renderer.RenderSQL(platform.SQLite, nodes[1])
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(dropSQL)
	c.Assert(err, qt.IsNil, qt.Commentf("execute replacement drop: %s", dropSQL))
	_, err = db.Exec(createSQL)
	c.Assert(err, qt.IsNil, qt.Commentf("execute replacement create: %s", createSQL))

	var name, definition string
	err = db.QueryRow(
		`SELECT name, sql FROM sqlite_master WHERE type = 'index' AND lower(name) = lower(?)`,
		"idx_users_email",
	).Scan(&name, &definition)
	c.Assert(err, qt.IsNil)
	c.Assert(name, qt.Equals, "idx_users_email")
	c.Assert(definition, qt.Contains, "CREATE UNIQUE INDEX")
}
