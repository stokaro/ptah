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

// TestGenerateMigrationOptions_CompareOptions_NilHandling tests behavior when
// CompareOptions is nil vs when it contains valid configuration
func TestGenerateMigrationOptions_CompareOptions_NilHandling(t *testing.T) {
	tests := []struct {
		name           string
		compareOptions *config.CompareOptions
		description    string
	}{
		{
			name:           "nil options should use defaults in schema comparison",
			compareOptions: nil,
			description:    "When CompareOptions is nil, schemadiff.CompareWithOptions should use default behavior",
		},
		{
			name:           "valid options should be used in schema comparison",
			compareOptions: config.WithIgnoredExtensions("plpgsql", "adminpack"),
			description:    "When CompareOptions is provided, it should be passed to schema comparison",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			// Test schema comparison directly with the options
			diff := schemadiff.CompareWithOptions(generated, database, tt.compareOptions)

			// Verify that the comparison behaves correctly based on options
			if tt.compareOptions == nil {
				// With nil options, should use defaults (ignore plpgsql)
				c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
				c.Assert(len(diff.ExtensionsRemoved), qt.Equals, 1) // adminpack should be removed
				c.Assert(diff.ExtensionsRemoved, qt.Contains, "adminpack")
			} else {
				// With custom options ignoring both plpgsql and adminpack
				c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pg_trgm"})
				c.Assert(len(diff.ExtensionsRemoved), qt.Equals, 0) // both should be ignored
			}
		})
	}
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

// TestGenerateMigration_CompareOptions_Integration tests that CompareOptions are
// actually used when the generator performs schema comparisons
func TestGenerateMigration_CompareOptions_Integration(t *testing.T) {
	tests := []struct {
		name                     string
		compareOptions           *config.CompareOptions
		databaseExtensions       []types.DBExtension
		generatedExtensions      []goschema.Extension
		expectMigrationGenerated bool
		expectedUpSQLContains    []string
		expectedUpSQLNotContains []string
		description              string
	}{
		{
			name:           "nil options use defaults - ignore plpgsql",
			compareOptions: nil,
			databaseExtensions: []types.DBExtension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			},
			generatedExtensions:      []goschema.Extension{},
			expectMigrationGenerated: false, // plpgsql ignored, no changes
			description:              "When CompareOptions is nil, plpgsql should be ignored by default",
		},
		{
			name:           "custom options ignore adminpack",
			compareOptions: config.WithIgnoredExtensions("plpgsql", "adminpack"),
			databaseExtensions: []types.DBExtension{
				{Name: "adminpack", Version: "2.1", Schema: "public"},
			},
			generatedExtensions:      []goschema.Extension{},
			expectMigrationGenerated: false, // adminpack ignored, no changes
			description:              "When CompareOptions ignores adminpack, it should not generate migration",
		},
		{
			name:           "no ignored extensions - manage all",
			compareOptions: config.WithIgnoredExtensions(),
			databaseExtensions: []types.DBExtension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			},
			generatedExtensions:      []goschema.Extension{},
			expectMigrationGenerated: true, // plpgsql not ignored, should remove it
			expectedUpSQLContains:    []string{},
			expectedUpSQLNotContains: []string{"CREATE EXTENSION"},
			description:              "When no extensions are ignored, plpgsql should be managed",
		},
		{
			name:           "add extension with custom ignore list",
			compareOptions: config.WithIgnoredExtensions("plpgsql"),
			databaseExtensions: []types.DBExtension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			},
			generatedExtensions: []goschema.Extension{
				{Name: "pg_trgm", IfNotExists: true},
			},
			expectMigrationGenerated: true, // pg_trgm should be added
			expectedUpSQLContains:    []string{"CREATE EXTENSION IF NOT EXISTS pg_trgm"},
			expectedUpSQLNotContains: []string{"DROP EXTENSION"},
			description:              "Should add pg_trgm while ignoring plpgsql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			// Create schema file with extensions
			schemaContent := createSchemaContent(tt.generatedExtensions)
			schemaPath := filepath.Join(entitiesDir, "schema.go")
			err = os.WriteFile(schemaPath, []byte(schemaContent), 0600)
			c.Assert(err, qt.IsNil)

			// Create database connection and set up test extensions
			conn, err := dbschema.ConnectToDatabase(dbURL)
			if err != nil {
				t.Skipf("Skipping test due to database connection error: %v", err)
				return
			}
			defer conn.Close()

			// Clean up any existing test extensions
			cleanupTestExtensions(c, conn)

			// Set up database extensions for test
			setupTestExtensions(c, conn, tt.databaseExtensions)

			// Test migration generation with CompareOptions
			opts := generator.GenerateMigrationOptions{
				GoEntitiesDir:  entitiesDir,
				DBConn:         conn,
				MigrationName:  "test_compare_options",
				OutputDir:      migrationsDir,
				CompareOptions: tt.compareOptions,
			}

			files, err := generator.GenerateMigration(opts)
			c.Assert(err, qt.IsNil, qt.Commentf("Migration generation failed: %v", err))

			if tt.expectMigrationGenerated {
				c.Assert(files, qt.IsNotNil, qt.Commentf("Expected migration to be generated"))
				c.Assert(files.UpFile, qt.Not(qt.Equals), "")
				c.Assert(files.DownFile, qt.Not(qt.Equals), "")

				// Verify UP migration content
				upContent, err := os.ReadFile(files.UpFile)
				c.Assert(err, qt.IsNil)
				upSQL := string(upContent)

				for _, expected := range tt.expectedUpSQLContains {
					c.Assert(strings.Contains(upSQL, expected), qt.IsTrue,
						qt.Commentf("UP SQL should contain: %s\nActual SQL:\n%s", expected, upSQL))
				}

				for _, notExpected := range tt.expectedUpSQLNotContains {
					c.Assert(strings.Contains(upSQL, notExpected), qt.IsFalse,
						qt.Commentf("UP SQL should not contain: %s\nActual SQL:\n%s", notExpected, upSQL))
				}
			} else {
				c.Assert(files, qt.IsNil, qt.Commentf("Expected no migration to be generated"))
			}

			// Clean up test extensions
			cleanupTestExtensions(c, conn)
		})
	}
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

// TestGenerateMigration_CompareOptions_UnhappyPath tests error conditions and edge cases
func TestGenerateMigration_CompareOptions_UnhappyPath(t *testing.T) {
	tests := []struct {
		name           string
		compareOptions *config.CompareOptions
		setupError     bool
		description    string
	}{
		{
			name:           "valid compare options with empty ignored list",
			compareOptions: config.WithIgnoredExtensions(),
			setupError:     false,
			description:    "Empty ignored extensions list should work correctly",
		},
		{
			name:           "compare options with duplicate ignored extensions",
			compareOptions: config.WithIgnoredExtensions("plpgsql", "plpgsql", "adminpack"),
			setupError:     false,
			description:    "Duplicate ignored extensions should not cause issues",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

//migrator:schema:table name="test_table_unhappy"
type TestTable struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`
			schemaPath := filepath.Join(entitiesDir, "schema.go")
			err = os.WriteFile(schemaPath, []byte(schemaContent), 0600)
			c.Assert(err, qt.IsNil)

			// Test with memory database (should fail gracefully)
			opts := generator.GenerateMigrationOptions{
				GoEntitiesDir:  entitiesDir,
				DatabaseURL:    "memory://test",
				MigrationName:  "test_unhappy",
				OutputDir:      migrationsDir,
				CompareOptions: tt.compareOptions,
			}

			// This should fail due to memory database limitations, but not due to CompareOptions
			_, err = generator.GenerateMigration(opts)
			c.Assert(err, qt.IsNotNil)

			// Verify that the error is not related to CompareOptions
			errMsg := err.Error()
			c.Assert(strings.Contains(errMsg, "CompareOptions"), qt.IsFalse,
				qt.Commentf("Error should not be related to CompareOptions: %s", errMsg))
		})
	}
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
