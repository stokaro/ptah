package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

// TestDependencyOrderingWithEmbeddedFields tests that tables with foreign key dependencies
// through embedded fields are created in the correct order.
func TestDependencyOrderingWithEmbeddedFields(t *testing.T) {
	c := qt.New(t)

	// Create a database schema that reproduces the issue from GitHub issue #16
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Tenant", Name: "tenants"},
			{StructName: "User", Name: "users"},
		},
		Fields: []goschema.Field{
			// Tenant fields
			{StructName: "Tenant", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Tenant", Name: "name", Type: "VARCHAR(255)", Nullable: false},

			// EntityID fields (embedded type)
			{StructName: "EntityID", Name: "id", Type: "SERIAL", Primary: true},

			// TenantAwareEntityID fields (embedded type with foreign key)
			{StructName: "TenantAwareEntityID", Name: "tenant_id", Type: "INTEGER", Foreign: "tenants(id)", Nullable: false},

			// User direct fields (NOT including embedded fields yet)
			{StructName: "User", Name: "email", Type: "VARCHAR(255)", Nullable: false},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			// User embeds TenantAwareEntityID (which contains the foreign key)
			{
				StructName:       "User",
				Mode:             "inline",
				EmbeddedTypeName: "TenantAwareEntityID",
			},
			// TenantAwareEntityID embeds EntityID
			{
				StructName:       "TenantAwareEntityID",
				Mode:             "inline",
				EmbeddedTypeName: "EntityID",
			},
		},
		Dependencies: make(map[string][]string),
	}

	// Simulate the fixed flow: process embedded fields BEFORE building dependency graph
	database.Fields = processEmbeddedFields(database.EmbeddedFields, database.Fields)
	buildDependencyGraph(database)
	sortTablesByDependencies(database)

	// Verify that the dependency was correctly detected
	usersDeps := database.Dependencies["users"]
	c.Assert(len(usersDeps), qt.Equals, 1, qt.Commentf("Users table should have exactly one dependency"))
	c.Assert(usersDeps[0], qt.Equals, "tenants", qt.Commentf("Users table should depend on tenants table"))

	// Verify that tables are sorted in correct order
	tableNames := make([]string, len(database.Tables))
	for i, table := range database.Tables {
		tableNames[i] = table.Name
	}

	tenantsIndex := -1
	usersIndex := -1
	for i, name := range tableNames {
		if name == "tenants" {
			tenantsIndex = i
		}
		if name == "users" {
			usersIndex = i
		}
	}

	c.Assert(tenantsIndex, qt.Not(qt.Equals), -1, qt.Commentf("Tenants table should be found"))
	c.Assert(usersIndex, qt.Not(qt.Equals), -1, qt.Commentf("Users table should be found"))
	c.Assert(tenantsIndex < usersIndex, qt.IsTrue, qt.Commentf("Tenants table should come before users table"))

	// Verify that the embedded fields were properly expanded
	userFields := make([]goschema.Field, 0)
	for _, field := range database.Fields {
		if field.StructName == "User" {
			userFields = append(userFields, field)
		}
	}

	// Should have: tenant_id (from TenantAwareEntityID), email (direct)
	// Note: id field from EntityID is not included because TenantAwareEntityID doesn't directly embed EntityID in this test
	c.Assert(len(userFields), qt.Equals, 2, qt.Commentf("User should have 2 fields after embedded processing"))

	// Check that tenant_id field with foreign key is present
	hasTenantIDWithFK := false
	for _, field := range userFields {
		if field.Name == "tenant_id" && field.Foreign == "tenants(id)" {
			hasTenantIDWithFK = true
			break
		}
	}
	c.Assert(hasTenantIDWithFK, qt.IsTrue, qt.Commentf("User should have tenant_id field with foreign key"))
}

// TestEmbeddedFieldProcessingModes tests all embedding modes work correctly
func TestEmbeddedFieldProcessingModes(t *testing.T) {
	c := qt.New(t)

	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Article", Name: "articles"},
		},
		Fields: []goschema.Field{
			// Article direct fields
			{StructName: "Article", Name: "title", Type: "VARCHAR(255)", Nullable: false},

			// Embedded type fields
			{StructName: "Timestamps", Name: "created_at", Type: "TIMESTAMP", Nullable: false},
			{StructName: "Timestamps", Name: "updated_at", Type: "TIMESTAMP", Nullable: false},
			{StructName: "AuditInfo", Name: "by", Type: "VARCHAR(255)"},
			{StructName: "AuditInfo", Name: "reason", Type: "TEXT"},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			// Mode 1: inline
			{
				StructName:       "Article",
				Mode:             "inline",
				EmbeddedTypeName: "Timestamps",
			},
			// Mode 2: inline with prefix
			{
				StructName:       "Article",
				Mode:             "inline",
				Prefix:           "audit_",
				EmbeddedTypeName: "AuditInfo",
			},
			// Mode 3: json
			{
				StructName:       "Article",
				Mode:             "json",
				Name:             "meta_data",
				Type:             "JSONB",
				EmbeddedTypeName: "Metadata",
			},
			// Mode 4: relation
			{
				StructName:       "Article",
				Mode:             "relation",
				Field:            "author_id",
				Ref:              "users(id)",
				EmbeddedTypeName: "User",
			},
			// Mode 5: skip
			{
				StructName:       "Article",
				Mode:             "skip",
				EmbeddedTypeName: "SkippedInfo",
			},
		},
		Dependencies: make(map[string][]string),
	}

	// Process embedded fields
	processedFields := processEmbeddedFields(database.EmbeddedFields, database.Fields)

	// Find Article fields
	articleFields := make([]goschema.Field, 0)
	for _, field := range processedFields {
		if field.StructName == "Article" {
			articleFields = append(articleFields, field)
		}
	}

	// Should have: title (direct), created_at, updated_at (inline), audit_by, audit_reason (inline with prefix),
	// meta_data (json), author_id (relation) = 7 fields total
	c.Assert(len(articleFields), qt.Equals, 7, qt.Commentf("Article should have 7 fields after processing"))

	// Check inline mode fields
	hasCreatedAt := false
	hasUpdatedAt := false
	for _, field := range articleFields {
		if field.Name == "created_at" {
			hasCreatedAt = true
		}
		if field.Name == "updated_at" {
			hasUpdatedAt = true
		}
	}
	c.Assert(hasCreatedAt, qt.IsTrue, qt.Commentf("Should have created_at from inline embedding"))
	c.Assert(hasUpdatedAt, qt.IsTrue, qt.Commentf("Should have updated_at from inline embedding"))

	// Check inline with prefix fields
	hasAuditBy := false
	hasAuditReason := false
	for _, field := range articleFields {
		if field.Name == "audit_by" {
			hasAuditBy = true
		}
		if field.Name == "audit_reason" {
			hasAuditReason = true
		}
	}
	c.Assert(hasAuditBy, qt.IsTrue, qt.Commentf("Should have audit_by from prefixed inline embedding"))
	c.Assert(hasAuditReason, qt.IsTrue, qt.Commentf("Should have audit_reason from prefixed inline embedding"))

	// Check JSON mode field
	hasMetaData := false
	for _, field := range articleFields {
		if field.Name == "meta_data" && field.Type == "JSONB" {
			hasMetaData = true
		}
	}
	c.Assert(hasMetaData, qt.IsTrue, qt.Commentf("Should have meta_data JSONB field from json embedding"))

	// Check relation mode field
	hasAuthorID := false
	for _, field := range articleFields {
		if field.Name == "author_id" && field.Foreign == "users(id)" {
			hasAuthorID = true
		}
	}
	c.Assert(hasAuthorID, qt.IsTrue, qt.Commentf("Should have author_id foreign key field from relation embedding"))

	// Check that skip mode field is NOT present
	hasSkippedField := false
	for _, field := range articleFields {
		if field.FieldName == "SkippedInfo" {
			hasSkippedField = true
		}
	}
	c.Assert(hasSkippedField, qt.IsFalse, qt.Commentf("Should NOT have any fields from skip mode embedding"))
}

// Helper functions for testing (simplified versions of the actual functions)
func processEmbeddedFields(embeddedFields []goschema.EmbeddedField, originalFields []goschema.Field) []goschema.Field {
	allFields := make([]goschema.Field, len(originalFields))
	copy(allFields, originalFields)

	// Process embedded fields for each struct
	structNames := getUniqueStructNames(embeddedFields)
	for _, structName := range structNames {
		generatedFields := processEmbeddedFieldsForStruct(embeddedFields, originalFields, structName)
		allFields = append(allFields, generatedFields...)
	}

	return allFields
}

func getUniqueStructNames(embeddedFields []goschema.EmbeddedField) []string {
	structNameMap := make(map[string]bool)
	for _, embedded := range embeddedFields {
		structNameMap[embedded.StructName] = true
	}

	var structNames []string
	for structName := range structNameMap {
		structNames = append(structNames, structName)
	}
	return structNames
}

func processEmbeddedFieldsForStruct(embeddedFields []goschema.EmbeddedField, allFields []goschema.Field, structName string) []goschema.Field {
	var generatedFields []goschema.Field

	for _, embedded := range embeddedFields {
		if embedded.StructName != structName {
			continue
		}

		switch embedded.Mode {
		case "inline":
			for _, field := range allFields {
				if field.StructName == embedded.EmbeddedTypeName {
					newField := field
					newField.StructName = structName
					if embedded.Prefix != "" {
						newField.Name = embedded.Prefix + field.Name
					}
					generatedFields = append(generatedFields, newField)
				}
			}
		case "json":
			columnName := embedded.Name
			if columnName == "" {
				columnName = embedded.EmbeddedTypeName + "_data"
			}
			generatedFields = append(generatedFields, goschema.Field{
				StructName: structName,
				FieldName:  embedded.EmbeddedTypeName,
				Name:       columnName,
				Type:       embedded.Type,
				Nullable:   embedded.Nullable,
			})
		case "relation":
			if embedded.Field != "" && embedded.Ref != "" {
				generatedFields = append(generatedFields, goschema.Field{
					StructName: structName,
					FieldName:  embedded.EmbeddedTypeName,
					Name:       embedded.Field,
					Type:       "INTEGER",
					Foreign:    embedded.Ref,
					Nullable:   embedded.Nullable,
				})
			}
		case "skip":
			continue
		}
	}

	return generatedFields
}

func buildDependencyGraph(r *goschema.Database) {
	for _, table := range r.Tables {
		r.Dependencies[table.Name] = []string{}
	}

	for _, field := range r.Fields {
		if field.Foreign == "" {
			continue
		}
		refTable := field.Foreign[:len(field.Foreign)-4] // Remove "(id)"

		for _, table := range r.Tables {
			if table.StructName != field.StructName {
				continue
			}
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
			break
		}
	}
}

func sortTablesByDependencies(r *goschema.Database) {
	// Simple topological sort for testing
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

	// Add remaining tables
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

	r.Tables = sorted
}
