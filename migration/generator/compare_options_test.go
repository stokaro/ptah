package generator_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/generator"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// TestGenerateMigrationOptions_CompareOptions_Initialization tests that CompareOptions
// can be properly set and retrieved from the generator struct
func TestGenerateMigrationOptions_CompareOptions_Initialization(t *testing.T) {
	tests := []struct {
		name           string
		compareOptions *config.CompareOptions
		expected       *config.CompareOptions
	}{
		{
			name:           "nil compare options",
			compareOptions: nil,
			expected:       nil,
		},
		{
			name:           "default compare options",
			compareOptions: config.DefaultCompareOptions(),
			expected:       config.DefaultCompareOptions(),
		},
		{
			name:           "custom compare options with ignored extensions",
			compareOptions: config.WithIgnoredExtensions("plpgsql", "adminpack"),
			expected:       config.WithIgnoredExtensions("plpgsql", "adminpack"),
		},
		{
			name:           "empty ignored extensions list",
			compareOptions: config.WithIgnoredExtensions(),
			expected:       config.WithIgnoredExtensions(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			opts := generator.GenerateMigrationOptions{
				GoEntitiesDir:  "./testdata",
				DatabaseURL:    "memory://test",
				MigrationName:  "test_migration",
				OutputDir:      "/tmp/migrations",
				CompareOptions: tt.compareOptions,
			}

			// Verify that CompareOptions field is properly set
			c.Assert(opts.CompareOptions, qt.DeepEquals, tt.expected)

			// If both are nil, they should be equal
			if tt.compareOptions == nil && tt.expected == nil {
				c.Assert(opts.CompareOptions, qt.IsNil)
				return
			}

			// If not nil, verify the IgnoredExtensions field
			if tt.expected != nil {
				c.Assert(opts.CompareOptions.IgnoredExtensions, qt.DeepEquals, tt.expected.IgnoredExtensions)
			}
		})
	}
}

// TestGenerateMigrationOptions_CompareOptions_NilHandling_DefaultBehavior tests behavior when
// CompareOptions is nil and should use default behavior
func TestGenerateMigrationOptions_CompareOptions_NilHandling_DefaultBehavior(t *testing.T) {
	c := qt.New(t)

	// Create test schemas
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}

	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
		},
	}

	// Test schema comparison with nil options (should use defaults)
	diff := schemadiff.CompareWithOptions(generated, database, nil)

	// With nil options, should use defaults (ignore plpgsql)
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(len(diff.ExtensionsRemoved), qt.Equals, 1) // adminpack should be removed
	c.Assert(diff.ExtensionsRemoved, qt.Contains, "adminpack")
}

// TestGenerateMigrationOptions_CompareOptions_NilHandling_CustomOptions tests behavior when
// CompareOptions contains valid configuration
func TestGenerateMigrationOptions_CompareOptions_NilHandling_CustomOptions(t *testing.T) {
	c := qt.New(t)

	// Create test schemas
	generated := &goschema.Database{
		Extensions: []goschema.Extension{
			{Name: "pg_trgm", IfNotExists: true},
		},
	}

	database := &types.DBSchema{
		Extensions: []types.DBExtension{
			{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			{Name: "adminpack", Version: "2.1", Schema: "public"},
		},
	}

	// Test schema comparison with custom options
	compareOptions := config.WithIgnoredExtensions("plpgsql", "adminpack")
	diff := schemadiff.CompareWithOptions(generated, database, compareOptions)

	// With custom options ignoring both plpgsql and adminpack
	c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(len(diff.ExtensionsRemoved), qt.Equals, 0) // both should be ignored
}

// TestGenerateMigrationOptions_CompareOptions_ConfigurationValidation tests that
// different CompareOptions settings are properly applied during schema comparison
func TestGenerateMigrationOptions_CompareOptions_ConfigurationValidation(t *testing.T) {
	tests := []struct {
		name                 string
		compareOptions       *config.CompareOptions
		expectedIgnored      []string
		expectedNotIgnored   []string
		expectedAddedCount   int
		expectedRemovedCount int
	}{
		{
			name:                 "default options ignore plpgsql only",
			compareOptions:       config.DefaultCompareOptions(),
			expectedIgnored:      []string{"plpgsql"},
			expectedNotIgnored:   []string{"adminpack", "pg_trgm"},
			expectedAddedCount:   1, // pg_trgm
			expectedRemovedCount: 1, // adminpack
		},
		{
			name:                 "custom options ignore multiple extensions",
			compareOptions:       config.WithIgnoredExtensions("plpgsql", "adminpack", "pg_stat_statements"),
			expectedIgnored:      []string{"plpgsql", "adminpack", "pg_stat_statements"},
			expectedNotIgnored:   []string{"pg_trgm", "uuid-ossp"},
			expectedAddedCount:   1, // pg_trgm
			expectedRemovedCount: 0, // all database extensions ignored
		},
		{
			name:                 "no ignored extensions",
			compareOptions:       config.WithIgnoredExtensions(),
			expectedIgnored:      []string{},
			expectedNotIgnored:   []string{"plpgsql", "adminpack", "pg_trgm"},
			expectedAddedCount:   1, // pg_trgm
			expectedRemovedCount: 2, // plpgsql, adminpack
		},
		{
			name:                 "additional ignored extensions",
			compareOptions:       config.WithAdditionalIgnoredExtensions("adminpack"),
			expectedIgnored:      []string{"plpgsql", "adminpack"},
			expectedNotIgnored:   []string{"pg_trgm", "uuid-ossp"},
			expectedAddedCount:   1, // pg_trgm
			expectedRemovedCount: 0, // both database extensions ignored
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Test configuration validation
			for _, ext := range tt.expectedIgnored {
				c.Assert(tt.compareOptions.IsExtensionIgnored(ext), qt.IsTrue,
					qt.Commentf("Extension %s should be ignored", ext))
			}

			for _, ext := range tt.expectedNotIgnored {
				c.Assert(tt.compareOptions.IsExtensionIgnored(ext), qt.IsFalse,
					qt.Commentf("Extension %s should not be ignored", ext))
			}

			// Test schema comparison with these options
			generated := &goschema.Database{
				Extensions: []goschema.Extension{
					{Name: "pg_trgm", IfNotExists: true},
				},
			}

			database := &types.DBSchema{
				Extensions: []types.DBExtension{
					{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
					{Name: "adminpack", Version: "2.1", Schema: "public"},
				},
			}

			diff := schemadiff.CompareWithOptions(generated, database, tt.compareOptions)

			c.Assert(len(diff.ExtensionsAdded), qt.Equals, tt.expectedAddedCount,
				qt.Commentf("Expected %d extensions to be added", tt.expectedAddedCount))
			c.Assert(len(diff.ExtensionsRemoved), qt.Equals, tt.expectedRemovedCount,
				qt.Commentf("Expected %d extensions to be removed", tt.expectedRemovedCount))
		})
	}
}

// TestGenerateMigration_CompareOptions_Integration_NilOptions tests that nil CompareOptions
// use default behavior and ignore plpgsql
func TestGenerateMigration_CompareOptions_Integration_NilOptions(t *testing.T) {
	c := qt.New(t)

	// Skip if no database URL is available
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("Skipping integration test: TEST_DATABASE_URL environment variable is not set")
	}

	// Create temporary directory structure
	tempDir := c.TempDir()
	entitiesDir := filepath.Join(tempDir, "entities")
	err := os.MkdirAll(entitiesDir, 0755)
	c.Assert(err, qt.IsNil)

	migrationsDir := filepath.Join(tempDir, "migrations")
	err = os.MkdirAll(migrationsDir, 0755)
	c.Assert(err, qt.IsNil)

	// Create schema file with no extensions
	schemaContent := createSchemaContent([]goschema.Extension{})
	schemaPath := filepath.Join(entitiesDir, "schema.go")
	err = os.WriteFile(schemaPath, []byte(schemaContent), 0600)
	c.Assert(err, qt.IsNil)

	// Create database connection and set up test extensions
	conn, err := dbschema.ConnectToDatabase(dbURL)
	c.Assert(err, qt.IsNil, qt.Commentf("Failed to connect to database: %v", err))
	defer conn.Close()

	// Clean up any existing test extensions
	cleanupTestExtensions(c, conn)

	// Set up plpgsql extension in database
	setupTestExtensions(c, conn, []types.DBExtension{
		{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
	})

	// Test migration generation with nil CompareOptions
	opts := generator.GenerateMigrationOptions{
		GoEntitiesDir:  entitiesDir,
		DBConn:         conn,
		MigrationName:  "test_nil_options",
		OutputDir:      migrationsDir,
		CompareOptions: nil,
	}

	files, err := generator.GenerateMigration(opts)
	c.Assert(err, qt.IsNil, qt.Commentf("Migration generation failed: %v", err))

	// With nil options, plpgsql should be ignored, so no migration should be generated
	c.Assert(files, qt.IsNil, qt.Commentf("Expected no migration to be generated when plpgsql is ignored by default"))

	// Clean up test extensions
	cleanupTestExtensions(c, conn)
}

// TestGenerateMigration_CompareOptions_Integration_CustomIgnoreList tests that custom
// CompareOptions properly ignore specified extensions
func TestGenerateMigration_CompareOptions_Integration_CustomIgnoreList(t *testing.T) {
	c := qt.New(t)

	// Skip if no database URL is available
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("Skipping integration test: TEST_DATABASE_URL environment variable is not set")
	}

	// Create temporary directory structure
	tempDir := c.TempDir()
	entitiesDir := filepath.Join(tempDir, "entities")
	err := os.MkdirAll(entitiesDir, 0755)
	c.Assert(err, qt.IsNil)

	migrationsDir := filepath.Join(tempDir, "migrations")
	err = os.MkdirAll(migrationsDir, 0755)
	c.Assert(err, qt.IsNil)

	// Create schema file with no extensions
	schemaContent := createSchemaContent([]goschema.Extension{})
	schemaPath := filepath.Join(entitiesDir, "schema.go")
	err = os.WriteFile(schemaPath, []byte(schemaContent), 0600)
	c.Assert(err, qt.IsNil)

	// Create database connection
	conn, err := dbschema.ConnectToDatabase(dbURL)
	c.Assert(err, qt.IsNil, qt.Commentf("Failed to connect to database: %v", err))
	defer conn.Close()

	// Clean up any existing test extensions
	cleanupTestExtensions(c, conn)

	// Test migration generation with custom ignore list that includes adminpack
	opts := generator.GenerateMigrationOptions{
		GoEntitiesDir:  entitiesDir,
		DBConn:         conn,
		MigrationName:  "test_custom_ignore",
		OutputDir:      migrationsDir,
		CompareOptions: config.WithIgnoredExtensions("plpgsql", "adminpack"),
	}

	files, err := generator.GenerateMigration(opts)
	c.Assert(err, qt.IsNil, qt.Commentf("Migration generation failed: %v", err))

	// With custom options ignoring adminpack, no migration should be generated
	c.Assert(files, qt.IsNil, qt.Commentf("Expected no migration to be generated when adminpack is ignored"))

	// Clean up test extensions
	cleanupTestExtensions(c, conn)
}

// TestGenerateMigration_CompareOptions_Integration_NoIgnoredExtensions tests that when
// no extensions are ignored, all extensions are managed
func TestGenerateMigration_CompareOptions_Integration_NoIgnoredExtensions(t *testing.T) {
	c := qt.New(t)

	// Skip if no database URL is available
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("Skipping integration test: TEST_DATABASE_URL environment variable is not set")
	}

	// Create temporary directory structure
	tempDir := c.TempDir()
	entitiesDir := filepath.Join(tempDir, "entities")
	err := os.MkdirAll(entitiesDir, 0755)
	c.Assert(err, qt.IsNil)

	migrationsDir := filepath.Join(tempDir, "migrations")
	err = os.MkdirAll(migrationsDir, 0755)
	c.Assert(err, qt.IsNil)

	// Create schema file with no extensions
	schemaContent := createSchemaContent([]goschema.Extension{})
	schemaPath := filepath.Join(entitiesDir, "schema.go")
	err = os.WriteFile(schemaPath, []byte(schemaContent), 0600)
	c.Assert(err, qt.IsNil)

	// Create database connection and set up test extensions
	conn, err := dbschema.ConnectToDatabase(dbURL)
	c.Assert(err, qt.IsNil, qt.Commentf("Failed to connect to database: %v", err))
	defer conn.Close()

	// Clean up any existing test extensions
	cleanupTestExtensions(c, conn)

	// Set up plpgsql extension in database
	setupTestExtensions(c, conn, []types.DBExtension{
		{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
	})

	// Test migration generation with no ignored extensions
	opts := generator.GenerateMigrationOptions{
		GoEntitiesDir:  entitiesDir,
		DBConn:         conn,
		MigrationName:  "test_no_ignored",
		OutputDir:      migrationsDir,
		CompareOptions: config.WithIgnoredExtensions(),
	}

	files, err := generator.GenerateMigration(opts)
	c.Assert(err, qt.IsNil, qt.Commentf("Migration generation failed: %v", err))

	// With no ignored extensions, plpgsql should be managed and migration should be generated
	c.Assert(files, qt.IsNotNil, qt.Commentf("Expected migration to be generated when no extensions are ignored"))
	c.Assert(files.UpFile, qt.Not(qt.Equals), "")
	c.Assert(files.DownFile, qt.Not(qt.Equals), "")

	// Verify UP migration content doesn't contain CREATE EXTENSION (since we're removing, not adding)
	upContent, err := os.ReadFile(files.UpFile)
	c.Assert(err, qt.IsNil)
	upSQL := string(upContent)
	c.Assert(strings.Contains(upSQL, "CREATE EXTENSION"), qt.IsFalse,
		qt.Commentf("UP SQL should not contain CREATE EXTENSION when removing extensions"))

	// Clean up test extensions
	cleanupTestExtensions(c, conn)
}

// TestGenerateMigration_CompareOptions_Integration_AddExtension tests that extensions
// are properly added while respecting ignore list
func TestGenerateMigration_CompareOptions_Integration_AddExtension(t *testing.T) {
	c := qt.New(t)

	// Skip if no database URL is available
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("Skipping integration test: TEST_DATABASE_URL environment variable is not set")
	}

	// Create temporary directory structure
	tempDir := c.TempDir()
	entitiesDir := filepath.Join(tempDir, "entities")
	err := os.MkdirAll(entitiesDir, 0755)
	c.Assert(err, qt.IsNil)

	migrationsDir := filepath.Join(tempDir, "migrations")
	err = os.MkdirAll(migrationsDir, 0755)
	c.Assert(err, qt.IsNil)

	// Create schema file with pg_trgm extension
	schemaContent := createSchemaContent([]goschema.Extension{
		{Name: "pg_trgm", IfNotExists: true},
	})
	schemaPath := filepath.Join(entitiesDir, "schema.go")
	err = os.WriteFile(schemaPath, []byte(schemaContent), 0600)
	c.Assert(err, qt.IsNil)

	// Create database connection and set up test extensions
	conn, err := dbschema.ConnectToDatabase(dbURL)
	c.Assert(err, qt.IsNil, qt.Commentf("Failed to connect to database: %v", err))
	defer conn.Close()

	// Clean up any existing test extensions
	cleanupTestExtensions(c, conn)

	// Set up plpgsql extension in database (should be ignored)
	setupTestExtensions(c, conn, []types.DBExtension{
		{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
	})

	// Test migration generation with custom ignore list
	opts := generator.GenerateMigrationOptions{
		GoEntitiesDir:  entitiesDir,
		DBConn:         conn,
		MigrationName:  "test_add_extension",
		OutputDir:      migrationsDir,
		CompareOptions: config.WithIgnoredExtensions("plpgsql"),
	}

	files, err := generator.GenerateMigration(opts)
	c.Assert(err, qt.IsNil, qt.Commentf("Migration generation failed: %v", err))

	// Migration should be generated to add pg_trgm
	c.Assert(files, qt.IsNotNil, qt.Commentf("Expected migration to be generated to add pg_trgm"))
	c.Assert(files.UpFile, qt.Not(qt.Equals), "")
	c.Assert(files.DownFile, qt.Not(qt.Equals), "")

	// Verify UP migration content contains CREATE EXTENSION for pg_trgm
	upContent, err := os.ReadFile(files.UpFile)
	c.Assert(err, qt.IsNil)
	upSQL := string(upContent)
	c.Assert(strings.Contains(upSQL, "CREATE EXTENSION IF NOT EXISTS pg_trgm"), qt.IsTrue,
		qt.Commentf("UP SQL should contain CREATE EXTENSION for pg_trgm"))
	c.Assert(strings.Contains(upSQL, "DROP EXTENSION"), qt.IsFalse,
		qt.Commentf("UP SQL should not contain DROP EXTENSION"))

	// Clean up test extensions
	cleanupTestExtensions(c, conn)
}

// createSchemaContent generates Go schema content with the specified extensions.
// This helper function creates a valid Go package with extension annotations
// and a simple table definition for testing purposes.
func createSchemaContent(extensions []goschema.Extension) string {
	var content strings.Builder
	content.WriteString("package entities\n\n")

	// Add extension definitions
	for _, ext := range extensions {
		content.WriteString("//migrator:schema:extension name=\"")
		content.WriteString(ext.Name)
		content.WriteString("\" if_not_exists=\"true\"\n")
	}

	// Add a simple table to make the schema valid
	content.WriteString(`
//migrator:schema:table name="test_table_compare_options"
type TestTable struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`)

	return content.String()
}

// setupTestExtensions creates the specified extensions in the test database.
// This helper function ensures that test extensions exist in the database
// for comparison testing. It only creates commonly available extensions.
func setupTestExtensions(c *qt.C, conn *dbschema.DatabaseConnection, extensions []types.DBExtension) {
	for _, ext := range extensions {
		// Only create extensions that are commonly available
		if ext.Name == "plpgsql" {
			// plpgsql is usually already installed, just ensure it exists
			err := conn.Writer().ExecuteSQL("CREATE EXTENSION IF NOT EXISTS plpgsql")
			c.Assert(err, qt.IsNil, qt.Commentf("Failed to ensure plpgsql extension exists"))
		}
		// Skip other extensions as they may not be available in test environment
	}
}

// cleanupTestExtensions removes test extensions and tables from the database.
// This helper function ensures a clean state before and after each test.
func cleanupTestExtensions(c *qt.C, conn *dbschema.DatabaseConnection) {
	// Clean up any test tables first
	_ = conn.Writer().ExecuteSQL("DROP TABLE IF EXISTS test_table_compare_options")

	// Note: We don't drop plpgsql as it's a core extension and may be needed by other tests
}

// TestGenerateMigration_CompareOptions_UnhappyPath_EmptyIgnoredList tests that
// empty ignored extensions list works correctly
func TestGenerateMigration_CompareOptions_UnhappyPath_EmptyIgnoredList(t *testing.T) {
	c := qt.New(t)

	// Create temporary directory structure
	tempDir := c.TempDir()
	entitiesDir := filepath.Join(tempDir, "entities")
	err := os.MkdirAll(entitiesDir, 0755)
	c.Assert(err, qt.IsNil)

	migrationsDir := filepath.Join(tempDir, "migrations")
	err = os.MkdirAll(migrationsDir, 0755)
	c.Assert(err, qt.IsNil)

	// Create minimal schema file
	schemaContent := `package entities

//migrator:schema:table name="test_table_empty_ignored"
type TestTable struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`
	schemaPath := filepath.Join(entitiesDir, "schema.go")
	err = os.WriteFile(schemaPath, []byte(schemaContent), 0600)
	c.Assert(err, qt.IsNil)

	// Test with memory database and empty ignored extensions list
	opts := generator.GenerateMigrationOptions{
		GoEntitiesDir:  entitiesDir,
		DatabaseURL:    "memory://test",
		MigrationName:  "test_empty_ignored",
		OutputDir:      migrationsDir,
		CompareOptions: config.WithIgnoredExtensions(),
	}

	// This should fail due to memory database limitations, but not due to CompareOptions
	_, err = generator.GenerateMigration(opts)
	c.Assert(err, qt.IsNotNil)

	// Verify that the error is not related to CompareOptions
	errMsg := err.Error()
	c.Assert(strings.Contains(errMsg, "CompareOptions"), qt.IsFalse,
		qt.Commentf("Error should not be related to CompareOptions: %s", errMsg))
}

// TestGenerateMigration_CompareOptions_UnhappyPath_DuplicateIgnoredExtensions tests that
// duplicate ignored extensions do not cause issues
func TestGenerateMigration_CompareOptions_UnhappyPath_DuplicateIgnoredExtensions(t *testing.T) {
	c := qt.New(t)

	// Create temporary directory structure
	tempDir := c.TempDir()
	entitiesDir := filepath.Join(tempDir, "entities")
	err := os.MkdirAll(entitiesDir, 0755)
	c.Assert(err, qt.IsNil)

	migrationsDir := filepath.Join(tempDir, "migrations")
	err = os.MkdirAll(migrationsDir, 0755)
	c.Assert(err, qt.IsNil)

	// Create minimal schema file
	schemaContent := `package entities

//migrator:schema:table name="test_table_duplicate_ignored"
type TestTable struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`
	schemaPath := filepath.Join(entitiesDir, "schema.go")
	err = os.WriteFile(schemaPath, []byte(schemaContent), 0600)
	c.Assert(err, qt.IsNil)

	// Test with memory database and duplicate ignored extensions
	opts := generator.GenerateMigrationOptions{
		GoEntitiesDir:  entitiesDir,
		DatabaseURL:    "memory://test",
		MigrationName:  "test_duplicate_ignored",
		OutputDir:      migrationsDir,
		CompareOptions: config.WithIgnoredExtensions("plpgsql", "plpgsql", "adminpack"),
	}

	// This should fail due to memory database limitations, but not due to CompareOptions
	_, err = generator.GenerateMigration(opts)
	c.Assert(err, qt.IsNotNil)

	// Verify that the error is not related to CompareOptions
	errMsg := err.Error()
	c.Assert(strings.Contains(errMsg, "CompareOptions"), qt.IsFalse,
		qt.Commentf("Error should not be related to CompareOptions: %s", errMsg))
}

// TestGenerateMigration_CompareOptions_FilterIgnoredExtensions tests the FilterIgnoredExtensions method
func TestGenerateMigration_CompareOptions_FilterIgnoredExtensions(t *testing.T) {
	tests := []struct {
		name            string
		compareOptions  *config.CompareOptions
		inputExtensions []string
		expectedOutput  []string
	}{
		{
			name:            "filter with default options",
			compareOptions:  config.DefaultCompareOptions(),
			inputExtensions: []string{"plpgsql", "pg_trgm", "adminpack"},
			expectedOutput:  []string{"pg_trgm", "adminpack"},
		},
		{
			name:            "filter with custom ignored list",
			compareOptions:  config.WithIgnoredExtensions("plpgsql", "adminpack"),
			inputExtensions: []string{"plpgsql", "pg_trgm", "adminpack", "uuid-ossp"},
			expectedOutput:  []string{"pg_trgm", "uuid-ossp"},
		},
		{
			name:            "filter with no ignored extensions",
			compareOptions:  config.WithIgnoredExtensions(),
			inputExtensions: []string{"plpgsql", "pg_trgm", "adminpack"},
			expectedOutput:  []string{"plpgsql", "pg_trgm", "adminpack"},
		},
		{
			name:            "filter empty input list",
			compareOptions:  config.DefaultCompareOptions(),
			inputExtensions: []string{},
			expectedOutput:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := tt.compareOptions.FilterIgnoredExtensions(tt.inputExtensions)
			c.Assert(result, qt.DeepEquals, tt.expectedOutput)
		})
	}
}
