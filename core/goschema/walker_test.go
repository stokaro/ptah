package goschema_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

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

	// Verify extensions are merged correctly (sorted alphabetically)
	c.Assert(len(result.Extensions), qt.Equals, 2)
	c.Assert(result.Extensions[0].Name, qt.Equals, "btree_gin")
	c.Assert(result.Extensions[0].IfNotExists, qt.Equals, true)
	c.Assert(result.Extensions[0].Comment, qt.Equals, "Enable GIN indexes on btree types")
	c.Assert(result.Extensions[1].Name, qt.Equals, "pg_trgm")
	c.Assert(result.Extensions[1].IfNotExists, qt.Equals, true)
	c.Assert(result.Extensions[1].Comment, qt.Equals, "Enable trigram similarity search")

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

// createTestFS creates an in-memory test filesystem with the given files.
//
// This helper function creates a fstest.MapFS that can be used for testing
// ParseFS without requiring actual files on disk. This provides fast,
// isolated testing that doesn't depend on the host filesystem.
//
// Parameters:
//   - files: A map of file paths to file contents
//
// Returns:
//   - fs.FS: An in-memory filesystem containing the specified files
func createTestFS(files map[string]string) fs.FS {
	fsys := make(fstest.MapFS)
	for path, content := range files {
		fsys[path] = &fstest.MapFile{
			Data: []byte(content),
		}
	}
	return fsys
}

func TestParseFS_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		files          map[string]string
		rootDir        string
		expectedTables int
		expectedFields int
		expectedEnums  int
		checkTables    []string
		checkFields    []string
	}{
		{
			name: "single file with basic table",
			files: map[string]string{
				"user.go": `package models

//migrator:schema:table name="users" comment="User accounts"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string

	//migrator:schema:field name="name" type="VARCHAR(100)" not_null="true"
	Name string
}`,
			},
			rootDir:        ".",
			expectedTables: 1,
			expectedFields: 3,
			expectedEnums:  0,
			checkTables:    []string{"users"},
			checkFields:    []string{"id", "email", "name"},
		},
		{
			name: "multiple files with dependencies",
			files: map[string]string{
				"user.go": `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string
}`,
				"article.go": `package models

//migrator:schema:table name="articles"
type Article struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string

	//migrator:schema:field name="user_id" type="INT" not_null="true" foreign="users(id)"
	UserID int64
}`,
			},
			rootDir:        ".",
			expectedTables: 2,
			expectedFields: 5,
			expectedEnums:  0,
			checkTables:    []string{"users", "articles"},
			checkFields:    []string{"id", "email", "title", "user_id"},
		},
		{
			name: "nested directory structure",
			files: map[string]string{
				"models/user.go": `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
				"models/auth/session.go": `package auth

//migrator:schema:table name="sessions"
type Session struct {
	//migrator:schema:field name="id" type="VARCHAR(255)" primary="true"
	ID string

	//migrator:schema:field name="user_id" type="INT" foreign="users(id)"
	UserID int64
}`,
			},
			rootDir:        ".",
			expectedTables: 2,
			expectedFields: 3,
			expectedEnums:  0,
			checkTables:    []string{"users", "sessions"},
			checkFields:    []string{"id", "user_id"},
		},
		{
			name: "with enums and extensions",
			files: map[string]string{
				"models.go": `package models

//migrator:schema:extension name="uuid-ossp" comment="UUID generation functions"

//migrator:schema:table name="products"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="status" type="ENUM" enum="active,inactive,discontinued" not_null="true"
	Status string
}`,
			},
			rootDir:        ".",
			expectedTables: 1,
			expectedFields: 2,
			expectedEnums:  1,
			checkTables:    []string{"products"},
			checkFields:    []string{"id", "status"},
		},
		{
			name: "empty directory",
			files: map[string]string{
				"README.md": "# Empty project",
			},
			rootDir:        ".",
			expectedTables: 0,
			expectedFields: 0,
			expectedEnums:  0,
			checkTables:    []string{},
			checkFields:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create test filesystem
			fsys := createTestFS(tt.files)

			// Parse filesystem
			result, err := goschema.ParseFS(fsys, tt.rootDir)
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.IsNotNil)

			// Check counts
			c.Assert(len(result.Tables), qt.Equals, tt.expectedTables)
			c.Assert(len(result.Fields), qt.Equals, tt.expectedFields)
			c.Assert(len(result.Enums), qt.Equals, tt.expectedEnums)

			// Check specific tables exist
			tableNames := make(map[string]bool)
			for _, table := range result.Tables {
				tableNames[table.Name] = true
			}
			for _, expectedTable := range tt.checkTables {
				c.Assert(tableNames[expectedTable], qt.IsTrue, qt.Commentf("Expected table %s not found", expectedTable))
			}

			// Check specific fields exist
			fieldNames := make(map[string]bool)
			for _, field := range result.Fields {
				fieldNames[field.Name] = true
			}
			for _, expectedField := range tt.checkFields {
				c.Assert(fieldNames[expectedField], qt.IsTrue, qt.Commentf("Expected field %s not found", expectedField))
			}

			// Verify result structure is properly initialized
			c.Assert(result.Dependencies, qt.IsNotNil)
			c.Assert(result.FunctionDependencies, qt.IsNotNil)
			c.Assert(result.SelfReferencingForeignKeys, qt.IsNotNil)
		})
	}
}

func TestParseFS_FileFiltering(t *testing.T) {
	tests := []struct {
		name           string
		files          map[string]string
		expectedTables int
		expectedFields int
	}{
		{
			name: "excludes test files",
			files: map[string]string{
				"user.go": `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
				"user_test.go": `package models

//migrator:schema:table name="test_users"
type TestUser struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
			},
			expectedTables: 1, // Only user.go should be parsed
			expectedFields: 1,
		},
		{
			name: "excludes vendor directories",
			files: map[string]string{
				"user.go": `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
				"vendor/github.com/example/lib/model.go": `package lib

//migrator:schema:table name="vendor_table"
type VendorModel struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
			},
			expectedTables: 1, // Only user.go should be parsed
			expectedFields: 1,
		},
		{
			name: "excludes non-go files",
			files: map[string]string{
				"user.go": `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
				"README.md":   "# Documentation",
				"config.json": `{"database": "postgres"}`,
				"script.sh":   "#!/bin/bash\necho 'hello'",
				"data.txt":    "some data",
			},
			expectedTables: 1, // Only user.go should be parsed
			expectedFields: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create test filesystem
			fsys := createTestFS(tt.files)

			// Parse filesystem
			result, err := goschema.ParseFS(fsys, ".")
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.IsNotNil)

			// Check counts
			c.Assert(len(result.Tables), qt.Equals, tt.expectedTables)
			c.Assert(len(result.Fields), qt.Equals, tt.expectedFields)
		})
	}
}

func TestParseFS_ErrorCases(t *testing.T) {
	tests := []struct {
		name        string
		fsys        fs.FS
		rootDir     string
		expectError bool
		errorCheck  func(error) bool
	}{
		{
			name:        "non-existent root directory",
			fsys:        createTestFS(map[string]string{}),
			rootDir:     "non-existent",
			expectError: true,
			errorCheck:  func(err error) bool { return err != nil },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := goschema.ParseFS(tt.fsys, tt.rootDir)

			if tt.expectError {
				c.Assert(err, qt.IsNotNil)
				c.Assert(tt.errorCheck(err), qt.IsTrue, qt.Commentf("Error check failed for error: %v", err))
				c.Assert(result, qt.IsNil)
			} else {
				c.Assert(err, qt.IsNil)
				c.Assert(result, qt.IsNotNil)
			}
		})
	}
}

func TestParseFS_PanicCases(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
	}{
		{
			name: "invalid go syntax causes panic",
			files: map[string]string{
				"invalid.go": `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
	// Missing closing brace
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create test filesystem
			fsys := createTestFS(tt.files)

			// Expect panic
			c.Assert(func() {
				_, _ = goschema.ParseFS(fsys, ".")
			}, qt.PanicMatches, "Failed to parse file")
		})
	}
}

func TestParseFS_DependencyResolution(t *testing.T) {
	tests := []struct {
		name                    string
		files                   map[string]string
		expectedDependencies    map[string][]string
		expectedSelfReferencing map[string]int
	}{
		{
			name: "simple foreign key dependency",
			files: map[string]string{
				"models.go": `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}

//migrator:schema:table name="articles"
type Article struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="user_id" type="INT" foreign="users(id)"
	UserID int64
}`,
			},
			expectedDependencies: map[string][]string{
				"articles": {"users"},
				"users":    {},
			},
			expectedSelfReferencing: map[string]int{},
		},
		{
			name: "self-referencing foreign key",
			files: map[string]string{
				"models.go": `package models

//migrator:schema:table name="categories"
type Category struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="parent_id" type="INT" foreign="categories(id)"
	ParentID *int64
}`,
			},
			expectedDependencies: map[string][]string{
				"categories": {},
			},
			expectedSelfReferencing: map[string]int{
				"categories": 1,
			},
		},
		{
			name: "complex dependency chain",
			files: map[string]string{
				"models.go": `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}

//migrator:schema:table name="categories"
type Category struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}

//migrator:schema:table name="products"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="category_id" type="INT" foreign="categories(id)"
	CategoryID int64
}

//migrator:schema:table name="orders"
type Order struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="user_id" type="INT" foreign="users(id)"
	UserID int64

	//migrator:schema:field name="product_id" type="INT" foreign="products(id)"
	ProductID int64
}`,
			},
			expectedDependencies: map[string][]string{
				"users":      {},
				"categories": {},
				"products":   {"categories"},
				"orders":     {"users", "products"},
			},
			expectedSelfReferencing: map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create test filesystem
			fsys := createTestFS(tt.files)

			// Parse filesystem
			result, err := goschema.ParseFS(fsys, ".")
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.IsNotNil)

			// Check dependencies
			for table, expectedDeps := range tt.expectedDependencies {
				actualDeps := result.Dependencies[table]
				c.Assert(actualDeps, qt.DeepEquals, expectedDeps, qt.Commentf("Dependencies for table %s", table))
			}

			// Check self-referencing foreign keys
			for table, expectedCount := range tt.expectedSelfReferencing {
				actualSelfRefs := result.SelfReferencingForeignKeys[table]
				c.Assert(len(actualSelfRefs), qt.Equals, expectedCount, qt.Commentf("Self-referencing FKs for table %s", table))
			}
		})
	}
}

func TestParseFS_Deduplication(t *testing.T) {
	tests := []struct {
		name           string
		files          map[string]string
		expectedTables int
		expectedFields int
	}{
		{
			name: "duplicate table definitions",
			files: map[string]string{
				"user1.go": `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string
}`,
				"user2.go": `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string
}`,
			},
			expectedTables: 1, // Should be deduplicated
			expectedFields: 2, // Should be deduplicated
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create test filesystem
			fsys := createTestFS(tt.files)

			// Parse filesystem
			result, err := goschema.ParseFS(fsys, ".")
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.IsNotNil)

			// Check counts after deduplication
			c.Assert(len(result.Tables), qt.Equals, tt.expectedTables)
			c.Assert(len(result.Fields), qt.Equals, tt.expectedFields)

			// Verify no duplicate table names
			tableNames := make(map[string]int)
			for _, table := range result.Tables {
				tableNames[table.Name]++
			}
			for name, count := range tableNames {
				c.Assert(count, qt.Equals, 1, qt.Commentf("Table %s should appear only once", name))
			}
		})
	}
}

func TestParseFS_EmbeddedFields(t *testing.T) {
	tests := []struct {
		name                    string
		files                   map[string]string
		expectedEmbeddedFields  int
		expectedProcessedFields int
	}{
		{
			name: "embedded struct fields",
			files: map[string]string{
				"models.go": `package models

//migrator:schema:embed
type BaseModel struct {
	//migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true"
	CreatedAt string

	//migrator:schema:field name="updated_at" type="TIMESTAMP"
	UpdatedAt string
}

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string

	//migrator:embedded mode="inline"
	BaseModel
}`,
			},
			expectedEmbeddedFields:  1,
			expectedProcessedFields: 6, // BaseModel.created_at, BaseModel.updated_at, User.id, User.email, User.created_at, User.updated_at
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create test filesystem
			fsys := createTestFS(tt.files)

			// Parse filesystem
			result, err := goschema.ParseFS(fsys, ".")
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.IsNotNil)

			// Check embedded fields
			c.Assert(len(result.EmbeddedFields), qt.Equals, tt.expectedEmbeddedFields)

			// Check that embedded fields are processed into regular fields
			c.Assert(len(result.Fields), qt.Equals, tt.expectedProcessedFields)
		})
	}
}

func TestParseFS_RealFilesystem(t *testing.T) {
	c := qt.New(t)

	// Test using the existing stubs directory which we know works
	result, err := goschema.ParseFS(os.DirFS("../../stubs"), ".")
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.IsNotNil)

	// Should find the same number of tables as ParseDir
	c.Assert(len(result.Tables) > 0, qt.IsTrue)
	c.Assert(len(result.Fields) > 0, qt.IsTrue)

	// Verify dependency resolution works
	c.Assert(result.Dependencies, qt.IsNotNil)
}

func TestParseFS_PostgreSQLFeatures(t *testing.T) {
	tests := []struct {
		name                string
		files               map[string]string
		expectedExtensions  int
		expectedFunctions   int
		expectedRLSPolicies int
		expectedRoles       int
	}{
		{
			name: "postgresql extensions and functions",
			files: map[string]string{
				"models.go": `package models

//migrator:schema:extension name="uuid-ossp" comment="UUID generation functions"
//migrator:schema:extension name="pg_trgm" comment="Trigram matching"
type DatabaseExtensions struct{}

//migrator:schema:function name="update_timestamp" params="" returns="TRIGGER" language="plpgsql" body="BEGIN NEW.updated_at = NOW(); RETURN NEW; END;"
//migrator:schema:role name="app_user" login="true" password="encrypted_password"
//migrator:schema:rls:enable table="users" comment="Enable RLS for users"
//migrator:schema:rls:policy name="user_policy" table="users" for="ALL" to="app_user" using="user_id = current_user_id()"
//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string
}`,
			},
			expectedExtensions:  2,
			expectedFunctions:   1,
			expectedRLSPolicies: 1,
			expectedRoles:       1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create test filesystem
			fsys := createTestFS(tt.files)

			// Parse filesystem
			result, err := goschema.ParseFS(fsys, ".")
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.IsNotNil)

			// Check PostgreSQL features
			c.Assert(len(result.Extensions), qt.Equals, tt.expectedExtensions)
			c.Assert(len(result.Functions), qt.Equals, tt.expectedFunctions)
			c.Assert(len(result.RLSPolicies), qt.Equals, tt.expectedRLSPolicies)
			c.Assert(len(result.Roles), qt.Equals, tt.expectedRoles)

			// Verify specific content
			if tt.expectedExtensions > 0 {
				extensionNames := make(map[string]bool)
				for _, ext := range result.Extensions {
					extensionNames[ext.Name] = true
				}
				c.Assert(extensionNames["uuid-ossp"], qt.IsTrue)
				c.Assert(extensionNames["pg_trgm"], qt.IsTrue)
			}

			if tt.expectedFunctions > 0 {
				c.Assert(result.Functions[0].Name, qt.Equals, "update_timestamp")
				c.Assert(result.Functions[0].Language, qt.Equals, "plpgsql")
			}

			if tt.expectedRoles > 0 {
				c.Assert(result.Roles[0].Name, qt.Equals, "app_user")
				c.Assert(result.Roles[0].Login, qt.IsTrue)
			}
		})
	}
}

func TestParseFS_EdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		files          map[string]string
		rootDir        string
		expectedTables int
		expectedFields int
	}{
		{
			name: "empty go files",
			files: map[string]string{
				"empty.go": `package models`,
			},
			rootDir:        ".",
			expectedTables: 0,
			expectedFields: 0,
		},
		{
			name: "go files without schema annotations",
			files: map[string]string{
				"regular.go": `package models

type RegularStruct struct {
	ID int64
	Name string
}

func SomeFunction() {
	// Regular Go code
}`,
			},
			rootDir:        ".",
			expectedTables: 0,
			expectedFields: 0,
		},
		{
			name: "mixed content with and without annotations",
			files: map[string]string{
				"mixed.go": `package models

// Regular struct without annotations
type RegularStruct struct {
	ID int64
	Name string
}

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}

// Another regular struct
type AnotherStruct struct {
	Value string
}`,
			},
			rootDir:        ".",
			expectedTables: 1,
			expectedFields: 1,
		},
		{
			name: "deeply nested directory structure",
			files: map[string]string{
				"a/b/c/d/e/deep.go": `package deep

//migrator:schema:table name="deep_table"
type DeepTable struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
			},
			rootDir:        ".",
			expectedTables: 1,
			expectedFields: 1,
		},
		{
			name: "parsing from subdirectory",
			files: map[string]string{
				"models/user.go": `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
				"other/other.go": `package other

//migrator:schema:table name="other_table"
type OtherTable struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}`,
			},
			rootDir:        "models",
			expectedTables: 1, // Only models/user.go should be parsed
			expectedFields: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create test filesystem
			fsys := createTestFS(tt.files)

			// Parse filesystem
			result, err := goschema.ParseFS(fsys, tt.rootDir)
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.IsNotNil)

			// Check counts
			c.Assert(len(result.Tables), qt.Equals, tt.expectedTables)
			c.Assert(len(result.Fields), qt.Equals, tt.expectedFields)
		})
	}
}

// TestParseFS_CompareWithParseDir verifies that ParseFS produces the same results as ParseDir
func TestParseFS_CompareWithParseDir(t *testing.T) {
	c := qt.New(t)

	// Parse using ParseDir (existing functionality)
	resultDir, err := goschema.ParseDir("../../stubs")
	c.Assert(err, qt.IsNil)

	// Parse using ParseFS with os.DirFS
	resultFS, err := goschema.ParseFS(os.DirFS("../../stubs"), ".")
	c.Assert(err, qt.IsNil)

	// Results should be identical
	c.Assert(len(resultFS.Tables), qt.Equals, len(resultDir.Tables))
	c.Assert(len(resultFS.Fields), qt.Equals, len(resultDir.Fields))
	c.Assert(len(resultFS.Indexes), qt.Equals, len(resultDir.Indexes))
	c.Assert(len(resultFS.Enums), qt.Equals, len(resultDir.Enums))
	c.Assert(len(resultFS.EmbeddedFields), qt.Equals, len(resultDir.EmbeddedFields))

	// Compare table names
	tableDirNames := make(map[string]bool)
	for _, table := range resultDir.Tables {
		tableDirNames[table.Name] = true
	}

	tableFSNames := make(map[string]bool)
	for _, table := range resultFS.Tables {
		tableFSNames[table.Name] = true
	}

	c.Assert(tableFSNames, qt.DeepEquals, tableDirNames)
}

// BenchmarkParseFS_SmallProject benchmarks ParseFS performance on a small project
func BenchmarkParseFS_SmallProject(b *testing.B) {
	files := map[string]string{
		"user.go": `package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string
}`,
		"product.go": `package models

//migrator:schema:table name="products"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//migrator:schema:field name="user_id" type="INT" foreign="users(id)"
	UserID int64
}`,
	}

	fsys := createTestFS(files)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := goschema.ParseFS(fsys, ".")
		if err != nil {
			b.Fatalf("ParseFS failed: %v", err)
		}
	}
}

// BenchmarkParseFS_LargeProject benchmarks ParseFS performance on a larger project
func BenchmarkParseFS_LargeProject(b *testing.B) {
	files := make(map[string]string)

	// Generate 50 model files with dependencies
	for i := 0; i < 50; i++ {
		filename := fmt.Sprintf("model%d.go", i)
		tableName := fmt.Sprintf("table%d", i)

		content := fmt.Sprintf(`package models

//migrator:schema:table name="%s"
type Model%d struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	//migrator:schema:field name="description" type="TEXT"
	Description string

	//migrator:schema:field name="status" type="ENUM" enum="active,inactive" not_null="true"
	Status string
}`, tableName, i)

		files[filename] = content
	}

	fsys := createTestFS(files)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := goschema.ParseFS(fsys, ".")
		if err != nil {
			b.Fatalf("ParseFS failed: %v", err)
		}
	}
}
