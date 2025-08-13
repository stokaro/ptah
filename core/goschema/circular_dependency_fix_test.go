package goschema_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/frankban/quicktest"
	"github.com/stokaro/ptah/core/goschema"
)

// TestSelfReferencingForeignKeyDetection tests that self-referencing foreign keys
// are properly detected and tracked separately from regular dependencies
func TestSelfReferencingForeignKeyDetection(t *testing.T) {
	c := quicktest.New(t)

	// Create a database with a self-referencing table
	db := &goschema.Database{
		Tables: []goschema.Table{
			{
				Name:       "users",
				StructName: "User",
			},
		},
		Fields: []goschema.Field{
			{
				StructName:     "User",
				Name:           "id",
				Type:           "TEXT",
				Primary:        true,
			},
			{
				StructName:     "User",
				Name:           "parent_id",
				Type:           "TEXT",
				Foreign:        "users(id)",
				ForeignKeyName: "fk_users_parent",
				Nullable:       true,
			},
		},
		Dependencies:               make(map[string][]string),
		SelfReferencingForeignKeys: make(map[string][]goschema.SelfReferencingFK),
	}

	// Build dependency graph
	buildDependencyGraphTest(db)

	// Verify that self-referencing FK is tracked separately
	c.Assert(db.SelfReferencingForeignKeys, quicktest.HasLen, 1)
	c.Assert(db.SelfReferencingForeignKeys["users"], quicktest.HasLen, 1)
	
	selfRefFK := db.SelfReferencingForeignKeys["users"][0]
	c.Assert(selfRefFK.FieldName, quicktest.Equals, "parent_id")
	c.Assert(selfRefFK.Foreign, quicktest.Equals, "users(id)")
	c.Assert(selfRefFK.ForeignKeyName, quicktest.Equals, "fk_users_parent")

	// Verify that no circular dependency is created in regular dependencies
	c.Assert(db.Dependencies["users"], quicktest.HasLen, 0)
}

// TestComplexDependencyChainWithSelfReference tests the scenario from issue #51
// where we have a complex dependency chain with self-referencing foreign keys
func TestComplexDependencyChainWithSelfReference(t *testing.T) {
	c := quicktest.New(t)

	// Recreate the scenario from issue #51
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

			// Users table (depends on tenants, self-references)
			{StructName: "User", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "User", Name: "tenant_id", Type: "TEXT", Foreign: "tenants(id)", ForeignKeyName: "fk_user_tenant"},
			{StructName: "User", Name: "user_id", Type: "TEXT", Foreign: "users(id)", ForeignKeyName: "fk_user_parent", Nullable: true},
			{StructName: "User", Name: "email", Type: "TEXT"},

			// Locations table (depends on tenants and users)
			{StructName: "Location", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Location", Name: "tenant_id", Type: "TEXT", Foreign: "tenants(id)", ForeignKeyName: "fk_location_tenant"},
			{StructName: "Location", Name: "user_id", Type: "TEXT", Foreign: "users(id)", ForeignKeyName: "fk_location_user"},
			{StructName: "Location", Name: "name", Type: "TEXT"},

			// Areas table (depends on tenants, users, and locations)
			{StructName: "Area", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Area", Name: "tenant_id", Type: "TEXT", Foreign: "tenants(id)", ForeignKeyName: "fk_area_tenant"},
			{StructName: "Area", Name: "user_id", Type: "TEXT", Foreign: "users(id)", ForeignKeyName: "fk_area_user"},
			{StructName: "Area", Name: "location_id", Type: "TEXT", Foreign: "locations(id)", ForeignKeyName: "fk_area_location"},
			{StructName: "Area", Name: "name", Type: "TEXT"},
		},
		Dependencies:               make(map[string][]string),
		SelfReferencingForeignKeys: make(map[string][]goschema.SelfReferencingFK),
	}

	// Build dependency graph
	buildDependencyGraphTest(db)

	// Verify self-referencing FK is tracked separately
	c.Assert(db.SelfReferencingForeignKeys["users"], quicktest.HasLen, 1)
	selfRefFK := db.SelfReferencingForeignKeys["users"][0]
	c.Assert(selfRefFK.FieldName, quicktest.Equals, "user_id")
	c.Assert(selfRefFK.Foreign, quicktest.Equals, "users(id)")

	// Verify regular dependencies (without self-references)
	c.Assert(db.Dependencies["tenants"], quicktest.HasLen, 0)
	c.Assert(db.Dependencies["users"], quicktest.DeepEquals, []string{"tenants"})
	c.Assert(db.Dependencies["locations"], quicktest.DeepEquals, []string{"tenants", "users"})
	c.Assert(db.Dependencies["areas"], quicktest.DeepEquals, []string{"tenants", "users", "locations"})

	// Sort tables by dependencies
	sortTablesByDependenciesTest(db)

	// Verify correct table order (should be able to sort without circular dependency warning)
	expectedOrder := []string{"tenants", "users", "locations", "areas"}
	actualOrder := make([]string, len(db.Tables))
	for i, table := range db.Tables {
		actualOrder[i] = table.Name
	}
	c.Assert(actualOrder, quicktest.DeepEquals, expectedOrder)
}

// TestEmbeddedFieldSelfReference tests self-referencing foreign keys from embedded fields
func TestEmbeddedFieldSelfReference(t *testing.T) {
	c := quicktest.New(t)

	db := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "categories", StructName: "Category"},
		},
		Fields: []goschema.Field{
			{StructName: "Category", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Category", Name: "name", Type: "TEXT"},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			{
				StructName:       "Category",
				EmbeddedTypeName: "ParentCategory",
				Mode:             "relation",
				Field:            "parent_id",
				Ref:              "categories(id)",
				Nullable:         true,
			},
		},
		Dependencies:               make(map[string][]string),
		SelfReferencingForeignKeys: make(map[string][]goschema.SelfReferencingFK),
	}

	// Build dependency graph
	buildDependencyGraphTest(db)

	// Verify self-referencing FK from embedded field is tracked
	c.Assert(db.SelfReferencingForeignKeys["categories"], quicktest.HasLen, 1)
	selfRefFK := db.SelfReferencingForeignKeys["categories"][0]
	c.Assert(selfRefFK.FieldName, quicktest.Equals, "parent_id")
	c.Assert(selfRefFK.Foreign, quicktest.Equals, "categories(id)")
	c.Assert(selfRefFK.ForeignKeyName, quicktest.Equals, "fk_categories_parent_id")

	// Verify no circular dependency in regular dependencies
	c.Assert(db.Dependencies["categories"], quicktest.HasLen, 0)
}

// TestMultipleSelfReferencesInSameTable tests a table with multiple self-referencing foreign keys
func TestMultipleSelfReferencesInSameTable(t *testing.T) {
	c := quicktest.New(t)

	db := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "nodes", StructName: "Node"},
		},
		Fields: []goschema.Field{
			{StructName: "Node", Name: "id", Type: "TEXT", Primary: true},
			{StructName: "Node", Name: "parent_id", Type: "TEXT", Foreign: "nodes(id)", ForeignKeyName: "fk_nodes_parent", Nullable: true},
			{StructName: "Node", Name: "next_sibling_id", Type: "TEXT", Foreign: "nodes(id)", ForeignKeyName: "fk_nodes_next_sibling", Nullable: true},
			{StructName: "Node", Name: "name", Type: "TEXT"},
		},
		Dependencies:               make(map[string][]string),
		SelfReferencingForeignKeys: make(map[string][]goschema.SelfReferencingFK),
	}

	// Build dependency graph
	buildDependencyGraphTest(db)

	// Verify both self-referencing FKs are tracked
	c.Assert(db.SelfReferencingForeignKeys["nodes"], quicktest.HasLen, 2)
	
	// Check that both self-references are captured
	fieldNames := make([]string, 2)
	for i, selfRefFK := range db.SelfReferencingForeignKeys["nodes"] {
		fieldNames[i] = selfRefFK.FieldName
	}
	c.Assert(fieldNames, quicktest.Contains, "parent_id")
	c.Assert(fieldNames, quicktest.Contains, "next_sibling_id")

	// Verify no circular dependency in regular dependencies
	c.Assert(db.Dependencies["nodes"], quicktest.HasLen, 0)
}

// Helper functions (copied from utils.go for testing)
func buildDependencyGraphTest(r *goschema.Database) {
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
				if !slices.Contains(r.Dependencies[table.Name], refTable) {
					r.Dependencies[table.Name] = append(r.Dependencies[table.Name], refTable)
				}
			}
			break
		}
	}

	// Analyze embedded field relationships (relation mode)
	for _, embedded := range r.EmbeddedFields {
		if embedded.Mode != "relation" || embedded.Ref == "" {
			continue
		}

		// Parse embedded relation reference (e.g., "users(id)" -> "users")
		refTable := strings.Split(embedded.Ref, "(")[0]

		// Find the table that contains this embedded field
		for _, table := range r.Tables {
			if table.StructName != embedded.StructName {
				continue
			}
			
			// Check if this is a self-referencing foreign key
			if table.Name == refTable {
				// Track self-referencing foreign key for deferred constraint creation
				r.SelfReferencingForeignKeys[table.Name] = append(r.SelfReferencingForeignKeys[table.Name], goschema.SelfReferencingFK{
					FieldName:      embedded.Field,
					Foreign:        embedded.Ref,
					ForeignKeyName: "fk_" + strings.ToLower(table.Name) + "_" + strings.ToLower(embedded.Field),
				})
			} else {
				// Add dependency: table depends on refTable (only for non-self-referencing FKs)
				if !slices.Contains(r.Dependencies[table.Name], refTable) {
					r.Dependencies[table.Name] = append(r.Dependencies[table.Name], refTable)
				}
			}
			break
		}
	}
}

func sortTablesByDependenciesTest(r *goschema.Database) {
	// Simple topological sort implementation for testing
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

	// Check for circular dependencies
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
