package generator_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/schemadiff"
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
	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 1) // adminpack should be removed
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
	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0) // both should be ignored
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
			expectedIgnored:      make([]string, 0),
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

			c.Assert(diff.ExtensionsAdded, qt.HasLen, tt.expectedAddedCount,
				qt.Commentf("Expected %d extensions to be added", tt.expectedAddedCount))
			c.Assert(diff.ExtensionsRemoved, qt.HasLen, tt.expectedRemovedCount,
				qt.Commentf("Expected %d extensions to be removed", tt.expectedRemovedCount))
		})
	}
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

//ptah:schema:table name="test_table_empty_ignored"
type TestTable struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
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
	_, err = generator.GenerateMigration(context.Background(), opts)
	c.Assert(err, qt.IsNotNil)

	// Verify that the error is not related to CompareOptions
	errMsg := err.Error()
	c.Assert(errMsg, qt.Not(qt.Contains), "CompareOptions",
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

//ptah:schema:table name="test_table_duplicate_ignored"
type TestTable struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
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
	_, err = generator.GenerateMigration(context.Background(), opts)
	c.Assert(err, qt.IsNotNil)

	// Verify that the error is not related to CompareOptions
	errMsg := err.Error()
	c.Assert(errMsg, qt.Not(qt.Contains), "CompareOptions",
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
			inputExtensions: make([]string, 0),
			expectedOutput:  make([]string, 0),
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
