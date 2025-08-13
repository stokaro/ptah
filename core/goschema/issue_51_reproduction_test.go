package goschema_test

import (
	"strings"
	"testing"

	"github.com/frankban/quicktest"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// TestIssue51ExactReproduction reproduces the exact scenario described in GitHub issue #51
// This test verifies that the circular dependency issue has been fixed and that
// migrations are generated correctly for complex dependency chains with self-referencing foreign keys
func TestIssue51ExactReproduction(t *testing.T) {
	c := quicktest.New(t)

	// Recreate the exact scenario from issue #51
	// This represents the TenantAwareEntityID embedded struct pattern
	db := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "tenants", StructName: "Tenant"},
			{Name: "users", StructName: "User"},
			{Name: "locations", StructName: "Location"},
			{Name: "areas", StructName: "Area"},
		},
		Fields: []goschema.Field{
			// Tenants table (no dependencies)
			{StructName: "Tenant", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Tenant", Name: "name", Type: "TEXT"},

			// Users table (depends on tenants, self-references via user_id)
			{StructName: "User", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "User", Name: "tenant_id", Type: "TEXT", Foreign: "tenants(id)", ForeignKeyName: "fk_entity_tenant"},
			{StructName: "User", Name: "user_id", Type: "TEXT", Foreign: "users(id)", ForeignKeyName: "fk_entity_user", Nullable: true},
			{StructName: "User", Name: "email", Type: "TEXT"},

			// Locations table (depends on tenants and users)
			{StructName: "Location", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Location", Name: "tenant_id", Type: "TEXT", Foreign: "tenants(id)", ForeignKeyName: "fk_entity_tenant"},
			{StructName: "Location", Name: "user_id", Type: "TEXT", Foreign: "users(id)", ForeignKeyName: "fk_entity_user"},
			{StructName: "Location", Name: "name", Type: "TEXT"},
			{StructName: "Location", Name: "address", Type: "TEXT"},

			// Areas table (depends on tenants, users, and locations)
			{StructName: "Area", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Area", Name: "tenant_id", Type: "TEXT", Foreign: "tenants(id)", ForeignKeyName: "fk_entity_tenant"},
			{StructName: "Area", Name: "user_id", Type: "TEXT", Foreign: "users(id)", ForeignKeyName: "fk_entity_user"},
			{StructName: "Area", Name: "location_id", Type: "TEXT", Foreign: "locations(id)", ForeignKeyName: "fk_area_location"},
			{StructName: "Area", Name: "name", Type: "TEXT"},
		},
		Dependencies:               make(map[string][]string),
		SelfReferencingForeignKeys: make(map[string][]goschema.SelfReferencingFK),
	}

	// Build dependency graph using the new algorithm
	buildDependencyGraphIssue51(db)

	// Verify that self-referencing FK is tracked separately
	c.Assert(db.SelfReferencingForeignKeys["users"], quicktest.HasLen, 1)
	selfRefFK := db.SelfReferencingForeignKeys["users"][0]
	c.Assert(selfRefFK.FieldName, quicktest.Equals, "user_id")
	c.Assert(selfRefFK.Foreign, quicktest.Equals, "users(id)")
	c.Assert(selfRefFK.ForeignKeyName, quicktest.Equals, "fk_entity_user")

	// Verify regular dependencies (without self-references)
	c.Assert(db.Dependencies["tenants"], quicktest.HasLen, 0)
	c.Assert(db.Dependencies["users"], quicktest.DeepEquals, []string{"tenants"})
	c.Assert(db.Dependencies["locations"], quicktest.DeepEquals, []string{"tenants", "users"})
	c.Assert(db.Dependencies["areas"], quicktest.DeepEquals, []string{"tenants", "users", "locations"})

	// Sort tables by dependencies - this should work without circular dependency warnings
	sortTablesByDependenciesIssue51(db)

	// Verify correct table order
	expectedOrder := []string{"tenants", "users", "locations", "areas"}
	actualOrder := make([]string, len(db.Tables))
	for i, table := range db.Tables {
		actualOrder[i] = table.Name
	}
	c.Assert(actualOrder, quicktest.DeepEquals, expectedOrder)

	// Now test the migration generation
	diff := &types.SchemaDiff{
		TablesAdded: []string{"tenants", "users", "locations", "areas"},
	}

	// Generate migration using PostgreSQL planner
	planner := &postgres.Planner{}
	nodes := planner.GenerateMigrationAST(diff, db)

	// Render to SQL
	r := renderer.NewRenderer("postgresql")
	var sqlStatements []string
	for _, node := range nodes {
		sql, err := r.Render(node)
		c.Assert(err, quicktest.IsNil)
		sqlStatements = append(sqlStatements, sql)
	}

	// Verify we have the correct number of statements:
	// 4 CREATE TABLE + 7 ALTER TABLE (6 regular FKs + 1 self-referencing FK)
	c.Assert(sqlStatements, quicktest.HasLen, 11)

	// Verify CREATE TABLE statements don't contain foreign key constraints
	createTableCount := 0
	alterTableCount := 0
	for _, sql := range sqlStatements {
		if strings.Contains(sql, "CREATE TABLE") {
			createTableCount++
			// Verify no foreign key constraints in CREATE TABLE
			c.Assert(sql, quicktest.Not(quicktest.Contains), "FOREIGN KEY")
			c.Assert(sql, quicktest.Not(quicktest.Contains), "REFERENCES")
		}
		if strings.Contains(sql, "ALTER TABLE") && strings.Contains(sql, "ADD CONSTRAINT") {
			alterTableCount++
		}
	}

	c.Assert(createTableCount, quicktest.Equals, 4) // 4 tables
	c.Assert(alterTableCount, quicktest.Equals, 7)  // 7 foreign key constraints

	// Verify that all expected foreign key constraints are present
	expectedConstraints := []string{
		"fk_entity_tenant", // users -> tenants
		"fk_entity_user",   // users -> users (self-reference)
		"fk_entity_tenant", // locations -> tenants
		"fk_entity_user",   // locations -> users
		"fk_entity_tenant", // areas -> tenants
		"fk_entity_user",   // areas -> users
		"fk_area_location", // areas -> locations
	}

	for _, constraint := range expectedConstraints {
		found := false
		for _, sql := range sqlStatements {
			if strings.Contains(sql, "ALTER TABLE") && strings.Contains(sql, constraint) {
				found = true
				break
			}
		}
		c.Assert(found, quicktest.IsTrue, quicktest.Commentf("Foreign key constraint %s not found", constraint))
	}

	// Verify that the self-referencing constraint is handled correctly
	selfRefConstraintFound := false
	for _, sql := range sqlStatements {
		if strings.Contains(sql, "ALTER TABLE users") &&
			strings.Contains(sql, "fk_entity_user") &&
			strings.Contains(sql, "REFERENCES users(id)") {
			selfRefConstraintFound = true
			break
		}
	}
	c.Assert(selfRefConstraintFound, quicktest.IsTrue, quicktest.Commentf("Self-referencing constraint not found"))

	// Verify table creation order in the SQL statements
	tableCreationOrder := []string{}
	for _, sql := range sqlStatements {
		if strings.Contains(sql, "CREATE TABLE") {
			for _, tableName := range []string{"tenants", "users", "locations", "areas"} {
				if strings.Contains(sql, "CREATE TABLE "+tableName) {
					tableCreationOrder = append(tableCreationOrder, tableName)
					break
				}
			}
		}
	}
	c.Assert(tableCreationOrder, quicktest.DeepEquals, expectedOrder)
}

// Helper functions for the test (simplified versions of the actual implementation)
func buildDependencyGraphIssue51(r *goschema.Database) {
	// Initialize dependencies map for all tables
	for _, table := range r.Tables {
		r.Dependencies[table.Name] = []string{}
	}

	// Initialize self-referencing foreign keys tracking
	if r.SelfReferencingForeignKeys == nil {
		r.SelfReferencingForeignKeys = make(map[string][]goschema.SelfReferencingFK)
	}

	// Analyze foreign key relationships
	for _, field := range r.Fields {
		if field.Foreign == "" {
			continue
		}
		// Parse foreign key reference (e.g., "users(id)" -> "users")
		refTable := strings.Split(field.Foreign, "(")[0]

		// Find the table that contains this field
		for _, table := range r.Tables {
			if table.StructName != field.StructName {
				continue
			}

			// Check if this is a self-referencing foreign key
			if table.Name == refTable {
				// Track self-referencing foreign key for deferred constraint creation
				r.SelfReferencingForeignKeys[table.Name] = append(r.SelfReferencingForeignKeys[table.Name], goschema.SelfReferencingFK{
					FieldName:      field.Name,
					Foreign:        field.Foreign,
					ForeignKeyName: field.ForeignKeyName,
				})
			} else {
				// Add dependency: table depends on refTable (only for non-self-referencing FKs)
				found := false
				for _, dep := range r.Dependencies[table.Name] {
					if dep == refTable {
						found = true
						break
					}
				}
				if !found {
					r.Dependencies[table.Name] = append(r.Dependencies[table.Name], refTable)
				}
			}
			break
		}
	}
}

func sortTablesByDependenciesIssue51(r *goschema.Database) {
	// Simple topological sort implementation
	var sorted []goschema.Table
	inDegree := make(map[string]int)

	for tableName := range r.Dependencies {
		inDegree[tableName] = 0
	}
	for tableName, deps := range r.Dependencies {
		inDegree[tableName] = len(deps)
	}

	var queue []string
	for tableName, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, tableName)
		}
	}

	tableMap := make(map[string]goschema.Table)
	for _, table := range r.Tables {
		tableMap[table.Name] = table
	}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if table, exists := tableMap[current]; exists {
			sorted = append(sorted, table)
		}

		for tableName, deps := range r.Dependencies {
			for _, dep := range deps {
				if dep != current {
					continue
				}
				inDegree[tableName]--
				if inDegree[tableName] == 0 {
					queue = append(queue, tableName)
				}
			}
		}
	}

	// Check for circular dependencies - should not happen with the fix
	if len(sorted) != len(r.Tables) {
		// Add remaining tables to the end
		for _, table := range r.Tables {
			found := false
			for _, sortedTable := range sorted {
				if sortedTable.Name == table.Name {
					found = true
					break
				}
			}
			if !found {
				sorted = append(sorted, table)
			}
		}
	}

	r.Tables = sorted
}
