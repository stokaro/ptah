package goschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

func TestParseDir_ExtensionMerging(t *testing.T) {
	c := qt.New(t)

	// Create temporary directory structure
	tmpDir := c.TempDir()
	modelsDir := filepath.Join(tmpDir, "models")
	err := os.MkdirAll(modelsDir, 0755)
	c.Assert(err, qt.IsNil)

	// Create extensions.go file
	extensionsContent := `package models

//migrator:schema:extension name="pg_trgm" if_not_exists="true" comment="Enable trigram similarity search"
//migrator:schema:extension name="btree_gin" if_not_exists="true" comment="Enable GIN indexes on btree types"
type DatabaseExtensions struct{}`

	err = os.WriteFile(filepath.Join(modelsDir, "extensions.go"), []byte(extensionsContent), 0600)
	c.Assert(err, qt.IsNil)

	// Create entities.go file
	entitiesContent := `package models

import "time"

//migrator:schema:table name="products"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//migrator:schema:field name="tags" type="JSONB"
	Tags []string

	//migrator:schema:field name="deleted_at" type="TIMESTAMP"
	DeletedAt *time.Time
}

type ProductIndexes struct {
	//migrator:schema:index name="idx_product_tags" fields="tags" type="GIN" table="products"
	_ int

	//migrator:schema:index name="idx_product_name_trgm" fields="name" type="GIN" ops="gin_trgm_ops" table="products"
	_ int
}`

	err = os.WriteFile(filepath.Join(modelsDir, "entities.go"), []byte(entitiesContent), 0600)
	c.Assert(err, qt.IsNil)

	// Parse directory
	result, err := goschema.ParseDir(modelsDir)
	c.Assert(err, qt.IsNil)

	// Verify extensions are merged correctly
	c.Assert(len(result.Extensions), qt.Equals, 2)
	c.Assert(result.Extensions[0].Name, qt.Equals, "pg_trgm")
	c.Assert(result.Extensions[0].IfNotExists, qt.Equals, true)
	c.Assert(result.Extensions[0].Comment, qt.Equals, "Enable trigram similarity search")
	c.Assert(result.Extensions[1].Name, qt.Equals, "btree_gin")
	c.Assert(result.Extensions[1].IfNotExists, qt.Equals, true)
	c.Assert(result.Extensions[1].Comment, qt.Equals, "Enable GIN indexes on btree types")

	// Verify other entities are also merged
	c.Assert(len(result.Tables), qt.Equals, 1)
	c.Assert(result.Tables[0].Name, qt.Equals, "products")
	c.Assert(len(result.Indexes), qt.Equals, 2)
	c.Assert(len(result.Fields), qt.Equals, 4)
}

func TestParseDir_FileFiltering(t *testing.T) {
	tests := []struct {
		name           string
		files          map[string]string
		expectedTables int
		expectedFields int
	}{
		{
			name: "excludes test files",
			files: map[string]string{
				"entity.go": `package test
//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
				"entity_test.go": `package test
//migrator:schema:table name="test_users"
type TestUser struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
			},
			expectedTables: 1, // Only entity.go should be parsed
			expectedFields: 1,
		},
		{
			name: "excludes vendor directories",
			files: map[string]string{
				"entity.go": `package test
//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
				"vendor/lib/entity.go": `package lib
//migrator:schema:table name="vendor_users"
type VendorUser struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
			},
			expectedTables: 1, // Only non-vendor file should be parsed
			expectedFields: 1,
		},
		{
			name: "excludes non-go files",
			files: map[string]string{
				"entity.go": `package test
//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
				"README.md":   "# Documentation",
				"config.json": `{"key": "value"}`,
				"script.sh":   "#!/bin/bash\necho 'hello'",
			},
			expectedTables: 1, // Only .go file should be parsed
			expectedFields: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create temporary directory
			tmpDir := c.TempDir()

			// Create all test files
			for filename, content := range tt.files {
				fullPath := filepath.Join(tmpDir, filename)
				dir := filepath.Dir(fullPath)
				err := os.MkdirAll(dir, 0755)
				c.Assert(err, qt.IsNil)
				err = os.WriteFile(fullPath, []byte(content), 0600)
				c.Assert(err, qt.IsNil)
			}

			// Parse directory
			result, err := goschema.ParseDir(tmpDir)
			c.Assert(err, qt.IsNil)

			// Verify filtering worked correctly
			c.Assert(len(result.Tables), qt.Equals, tt.expectedTables)
			c.Assert(len(result.Fields), qt.Equals, tt.expectedFields)
		})
	}
}

func TestParseDir_MultipleFileTypes(t *testing.T) {
	c := qt.New(t)

	// Create temporary directory structure
	tmpDir := c.TempDir()
	err := os.MkdirAll(filepath.Join(tmpDir, "subdir"), 0755)
	c.Assert(err, qt.IsNil)

	// Create files with different entity types
	files := map[string]string{
		"extensions.go": `package test
//migrator:schema:extension name="pg_trgm" if_not_exists="true"
type DatabaseExtensions struct{}`,

		"tables.go": `package test
//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string
	//migrator:schema:field name="status" type="ENUM" enum="active,inactive,pending" not_null="true"
	Status string
}`,

		"indexes.go": `package test
type UserIndexes struct {
	//migrator:schema:index name="idx_users_name" fields="name" table="users"
	_ int
}`,

		"subdir/more_entities.go": `package test
//migrator:schema:table name="products"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
	}

	for filename, content := range files {
		fullPath := filepath.Join(tmpDir, filename)
		err := os.WriteFile(fullPath, []byte(content), 0600)
		c.Assert(err, qt.IsNil)
	}

	// Parse directory recursively
	result, err := goschema.ParseDir(tmpDir)
	c.Assert(err, qt.IsNil)

	// Verify all entity types are merged correctly
	c.Assert(len(result.Extensions), qt.Equals, 1)
	c.Assert(result.Extensions[0].Name, qt.Equals, "pg_trgm")

	c.Assert(len(result.Enums), qt.Equals, 1)
	c.Assert(result.Enums[0].Name, qt.Equals, "enum_user_status") // Auto-generated enum name

	c.Assert(len(result.Tables), qt.Equals, 2)
	tableNames := []string{result.Tables[0].Name, result.Tables[1].Name}
	c.Assert(tableNames, qt.Contains, "users")
	c.Assert(tableNames, qt.Contains, "products")

	c.Assert(len(result.Indexes), qt.Equals, 1)
	c.Assert(result.Indexes[0].Name, qt.Equals, "idx_users_name")

	c.Assert(len(result.Fields), qt.Equals, 4) // 3 from users, 1 from products
}

func TestParseDir_ErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		rootDir     string
		expectError bool
	}{
		{
			name:        "non-existent directory",
			rootDir:     "/non/existent/directory",
			expectError: true,
		},
		{
			name:        "empty directory",
			rootDir:     "", // Will be set to empty temp dir
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			rootDir := tt.rootDir
			if rootDir == "" {
				rootDir = c.TempDir() // Empty directory
			}

			result, err := goschema.ParseDir(rootDir)

			if tt.expectError {
				c.Assert(err, qt.IsNotNil)
				c.Assert(result, qt.IsNil)
			} else {
				c.Assert(err, qt.IsNil)
				c.Assert(result, qt.IsNotNil)
				// Empty directory should return empty but valid result
				c.Assert(len(result.Tables), qt.Equals, 0)
				c.Assert(len(result.Extensions), qt.Equals, 0)
				c.Assert(result.Dependencies, qt.IsNotNil)
			}
		})
	}
}

func TestParseDir_Deduplication(t *testing.T) {
	c := qt.New(t)

	// Create temporary directory
	tmpDir := c.TempDir()

	// Create files with duplicate entities (same table defined in multiple files)
	file1Content := `package test
//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string
}`

	file2Content := `package test
//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
	//migrator:schema:field name="email" type="VARCHAR(255)" unique="true"
	Email string
}`

	err := os.WriteFile(filepath.Join(tmpDir, "file1.go"), []byte(file1Content), 0600)
	c.Assert(err, qt.IsNil)
	err = os.WriteFile(filepath.Join(tmpDir, "file2.go"), []byte(file2Content), 0600)
	c.Assert(err, qt.IsNil)

	// Parse directory
	result, err := goschema.ParseDir(tmpDir)
	c.Assert(err, qt.IsNil)

	// Verify deduplication worked
	c.Assert(len(result.Tables), qt.Equals, 1) // Should have only one "users" table

	// Verify all fields are merged (should have id, name, email)
	userFields := []goschema.Field{}
	for _, field := range result.Fields {
		if field.StructName == "User" {
			userFields = append(userFields, field)
		}
	}
	c.Assert(len(userFields), qt.Equals, 3)

	fieldNames := []string{}
	for _, field := range userFields {
		fieldNames = append(fieldNames, field.Name)
	}
	c.Assert(fieldNames, qt.Contains, "id")
	c.Assert(fieldNames, qt.Contains, "name")
	c.Assert(fieldNames, qt.Contains, "email")
}
