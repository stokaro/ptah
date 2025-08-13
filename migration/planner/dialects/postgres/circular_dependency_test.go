package postgres_test

import (
	"strings"
	"testing"

	"github.com/frankban/quicktest"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
	"github.com/stokaro/ptah/core/renderer"
)

// TestTwoPhaseTableCreationWithSelfReference tests that the PostgreSQL planner
// generates separate CREATE TABLE and ALTER TABLE statements for self-referencing foreign keys
func TestTwoPhaseTableCreationWithSelfReference(t *testing.T) {
	c := quicktest.New(t)

	// Create a schema with self-referencing foreign key (like the issue #51 scenario)
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "User", Name: "parent_id", Type: "TEXT", Foreign: "users(id)", ForeignKeyName: "fk_users_parent", Nullable: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
		},
		SelfReferencingForeignKeys: map[string][]goschema.SelfReferencingFK{
			"users": {
				{
					FieldName:      "parent_id",
					Foreign:        "users(id)",
					ForeignKeyName: "fk_users_parent",
				},
			},
		},
	}

	// Create a schema diff that adds the users table
	diff := &types.SchemaDiff{
		TablesAdded: []string{"users"},
	}

	// Generate AST nodes using PostgreSQL planner
	planner := &postgres.Planner{}
	nodes := planner.GenerateMigrationAST(diff, generated)

	// Render the nodes to SQL
	r := renderer.NewRenderer("postgresql")
	var sqlStatements []string
	for _, node := range nodes {
		sql, err := r.Render(node)
		c.Assert(err, quicktest.IsNil)
		sqlStatements = append(sqlStatements, sql)
	}

	// Verify we have exactly 2 statements: CREATE TABLE and ALTER TABLE
	c.Assert(sqlStatements, quicktest.HasLen, 2)

	// First statement should be CREATE TABLE without foreign key constraint
	createTableSQL := sqlStatements[0]
	c.Assert(createTableSQL, quicktest.Contains, "CREATE TABLE users")
	c.Assert(createTableSQL, quicktest.Contains, "id TEXT")
	c.Assert(createTableSQL, quicktest.Contains, "parent_id TEXT")
	c.Assert(createTableSQL, quicktest.Contains, "email TEXT")
	// Should NOT contain foreign key constraint in CREATE TABLE
	c.Assert(createTableSQL, quicktest.Not(quicktest.Contains), "FOREIGN KEY")
	c.Assert(createTableSQL, quicktest.Not(quicktest.Contains), "REFERENCES")

	// Second statement should be ALTER TABLE ADD CONSTRAINT for the self-referencing FK
	alterTableSQL := sqlStatements[1]
	c.Assert(alterTableSQL, quicktest.Contains, "ALTER TABLE users")
	c.Assert(alterTableSQL, quicktest.Contains, "ADD CONSTRAINT fk_users_parent")
	c.Assert(alterTableSQL, quicktest.Contains, "FOREIGN KEY (parent_id)")
	c.Assert(alterTableSQL, quicktest.Contains, "REFERENCES users(id)")
}

// TestComplexDependencyChainTwoPhase tests the complex scenario from issue #51
func TestComplexDependencyChainTwoPhase(t *testing.T) {
	c := quicktest.New(t)

	// Recreate the scenario from issue #51
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "tenants", StructName: "Tenant"},
			{Name: "users", StructName: "User"},
			{Name: "locations", StructName: "Location"},
			{Name: "areas", StructName: "Area"},
		},
		Fields: []goschema.Field{
			// Tenants table
			{StructName: "Tenant", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Tenant", Name: "name", Type: "TEXT"},

			// Users table (self-referencing)
			{StructName: "User", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "User", Name: "tenant_id", Type: "TEXT", Foreign: "tenants(id)", ForeignKeyName: "fk_user_tenant"},
			{StructName: "User", Name: "user_id", Type: "TEXT", Foreign: "users(id)", ForeignKeyName: "fk_user_parent", Nullable: true},
			{StructName: "User", Name: "email", Type: "TEXT"},

			// Locations table
			{StructName: "Location", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Location", Name: "tenant_id", Type: "TEXT", Foreign: "tenants(id)", ForeignKeyName: "fk_location_tenant"},
			{StructName: "Location", Name: "user_id", Type: "TEXT", Foreign: "users(id)", ForeignKeyName: "fk_location_user"},
			{StructName: "Location", Name: "name", Type: "TEXT"},

			// Areas table
			{StructName: "Area", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Area", Name: "tenant_id", Type: "TEXT", Foreign: "tenants(id)", ForeignKeyName: "fk_area_tenant"},
			{StructName: "Area", Name: "user_id", Type: "TEXT", Foreign: "users(id)", ForeignKeyName: "fk_area_user"},
			{StructName: "Area", Name: "location_id", Type: "TEXT", Foreign: "locations(id)", ForeignKeyName: "fk_area_location"},
			{StructName: "Area", Name: "name", Type: "TEXT"},
		},
		SelfReferencingForeignKeys: map[string][]goschema.SelfReferencingFK{
			"users": {
				{
					FieldName:      "user_id",
					Foreign:        "users(id)",
					ForeignKeyName: "fk_user_parent",
				},
			},
		},
	}

	// Create a schema diff that adds all tables
	diff := &types.SchemaDiff{
		TablesAdded: []string{"tenants", "users", "locations", "areas"},
	}

	// Generate AST nodes using PostgreSQL planner
	planner := &postgres.Planner{}
	nodes := planner.GenerateMigrationAST(diff, generated)

	// Render the nodes to SQL
	r := renderer.NewRenderer("postgresql")
	var sqlStatements []string
	for _, node := range nodes {
		sql, err := r.Render(node)
		c.Assert(err, quicktest.IsNil)
		sqlStatements = append(sqlStatements, sql)
	}

	// Should have 4 CREATE TABLE statements + multiple ALTER TABLE statements for foreign keys
	// Count CREATE TABLE statements
	createTableCount := 0
	alterTableCount := 0
	for _, sql := range sqlStatements {
		if strings.Contains(sql, "CREATE TABLE") {
			createTableCount++
		}
		if strings.Contains(sql, "ALTER TABLE") && strings.Contains(sql, "ADD CONSTRAINT") {
			alterTableCount++
		}
	}

	c.Assert(createTableCount, quicktest.Equals, 4) // 4 tables
	c.Assert(alterTableCount, quicktest.Equals, 7)  // 7 foreign key constraints total

	// Verify that CREATE TABLE statements don't contain foreign key constraints
	for _, sql := range sqlStatements {
		if strings.Contains(sql, "CREATE TABLE") {
			c.Assert(sql, quicktest.Not(quicktest.Contains), "FOREIGN KEY")
			c.Assert(sql, quicktest.Not(quicktest.Contains), "REFERENCES")
		}
	}

	// Verify that all foreign key constraints are in ALTER TABLE statements
	fkConstraints := []string{
		"fk_user_tenant", "fk_user_parent", "fk_location_tenant", "fk_location_user",
		"fk_area_tenant", "fk_area_user", "fk_area_location",
	}

	for _, fkName := range fkConstraints {
		found := false
		for _, sql := range sqlStatements {
			if strings.Contains(sql, "ALTER TABLE") && strings.Contains(sql, fkName) {
				found = true
				break
			}
		}
		c.Assert(found, quicktest.IsTrue, quicktest.Commentf("Foreign key constraint %s not found in ALTER TABLE statements", fkName))
	}
}

// TestNoForeignKeysInCreateTable tests that CREATE TABLE statements never contain foreign key constraints
func TestNoForeignKeysInCreateTable(t *testing.T) {
	c := quicktest.New(t)

	// Create a simple schema with regular foreign keys
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
			{Name: "posts", StructName: "Post"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
			{StructName: "Post", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Post", Name: "user_id", Type: "TEXT", Foreign: "users(id)", ForeignKeyName: "fk_posts_user"},
			{StructName: "Post", Name: "title", Type: "TEXT"},
		},
		SelfReferencingForeignKeys: make(map[string][]goschema.SelfReferencingFK),
	}

	// Create a schema diff that adds both tables
	diff := &types.SchemaDiff{
		TablesAdded: []string{"users", "posts"},
	}

	// Generate AST nodes using PostgreSQL planner
	planner := &postgres.Planner{}
	nodes := planner.GenerateMigrationAST(diff, generated)

	// Render the nodes to SQL
	r := renderer.NewRenderer("postgresql")
	var sqlStatements []string
	for _, node := range nodes {
		sql, err := r.Render(node)
		c.Assert(err, quicktest.IsNil)
		sqlStatements = append(sqlStatements, sql)
	}

	// Verify that CREATE TABLE statements don't contain foreign key constraints
	for _, sql := range sqlStatements {
		if strings.Contains(sql, "CREATE TABLE") {
			c.Assert(sql, quicktest.Not(quicktest.Contains), "FOREIGN KEY")
			c.Assert(sql, quicktest.Not(quicktest.Contains), "REFERENCES")
		}
	}

	// Verify that foreign key constraint is in ALTER TABLE statement
	found := false
	for _, sql := range sqlStatements {
		if strings.Contains(sql, "ALTER TABLE posts") && strings.Contains(sql, "fk_posts_user") {
			found = true
			break
		}
	}
	c.Assert(found, quicktest.IsTrue, quicktest.Commentf("Foreign key constraint fk_posts_user not found in ALTER TABLE statements"))
}
