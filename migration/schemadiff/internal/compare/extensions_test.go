package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestExtensions(t *testing.T) {
	tests := []struct {
		name                string
		generatedExtensions []goschema.Extension
		databaseExtensions  []types.DBExtension
		expectedAdded       []string
		expectedRemoved     []string
	}{
		{
			name:                "no extensions in either schema",
			generatedExtensions: []goschema.Extension{},
			databaseExtensions:  []types.DBExtension{},
			expectedAdded:       []string{},
			expectedRemoved:     []string{},
		},
		{
			name: "extension needs to be added",
			generatedExtensions: []goschema.Extension{
				{Name: "uuid-ossp", IfNotExists: true},
			},
			databaseExtensions: []types.DBExtension{},
			expectedAdded:      []string{"uuid-ossp"},
			expectedRemoved:    []string{},
		},
		{
			name:                "extension needs to be removed",
			generatedExtensions: []goschema.Extension{},
			databaseExtensions: []types.DBExtension{
				{Name: "hstore", Version: "1.8", Schema: "public"},
			},
			expectedAdded:   []string{},
			expectedRemoved: []string{"hstore"},
		},
		{
			name: "extension already exists - no changes",
			generatedExtensions: []goschema.Extension{
				{Name: "uuid-ossp", IfNotExists: true},
			},
			databaseExtensions: []types.DBExtension{
				{Name: "uuid-ossp", Version: "1.1", Schema: "public"},
			},
			expectedAdded:   []string{},
			expectedRemoved: []string{},
		},
		{
			name: "multiple extensions - mixed operations",
			generatedExtensions: []goschema.Extension{
				{Name: "uuid-ossp", IfNotExists: true},
				{Name: "hstore", IfNotExists: true},
				{Name: "postgis", Version: "3.0"},
			},
			databaseExtensions: []types.DBExtension{
				{Name: "uuid-ossp", Version: "1.1", Schema: "public"},
				{Name: "citext", Version: "1.6", Schema: "public"},
			},
			expectedAdded:   []string{"hstore", "postgis"},
			expectedRemoved: []string{"citext"},
		},
		{
			name: "extensions with different versions - no version comparison",
			generatedExtensions: []goschema.Extension{
				{Name: "postgis", Version: "3.1"},
			},
			databaseExtensions: []types.DBExtension{
				{Name: "postgis", Version: "3.0", Schema: "public"},
			},
			expectedAdded:   []string{},
			expectedRemoved: []string{},
		},
		{
			name: "sorted output verification",
			generatedExtensions: []goschema.Extension{
				{Name: "z_extension", IfNotExists: true},
				{Name: "a_extension", IfNotExists: true},
				{Name: "m_extension", IfNotExists: true},
			},
			databaseExtensions: []types.DBExtension{
				{Name: "z_old_ext", Version: "1.0", Schema: "public"},
				{Name: "a_old_ext", Version: "1.0", Schema: "public"},
			},
			expectedAdded:   []string{"a_extension", "m_extension", "z_extension"},
			expectedRemoved: []string{"a_old_ext", "z_old_ext"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create test schemas
			generated := &goschema.Database{
				Extensions: tt.generatedExtensions,
			}

			database := &types.DBSchema{
				Extensions: tt.databaseExtensions,
			}

			// Create empty diff to populate
			diff := &difftypes.SchemaDiff{}

			// Run the comparison
			compare.Extensions(generated, database, diff, nil)

			// Verify results
			c.Assert(diff.ExtensionsAdded, qt.DeepEquals, tt.expectedAdded)
			c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, tt.expectedRemoved)
		})
	}
}

func TestExtensions_RealWorldScenarios(t *testing.T) {
	tests := []struct {
		name        string
		description string
		setup       func() (*goschema.Database, *types.DBSchema)
		verify      func(c *qt.C, diff *difftypes.SchemaDiff)
	}{
		{
			name:        "fresh database setup",
			description: "Setting up PostgreSQL extensions on a fresh database",
			setup: func() (*goschema.Database, *types.DBSchema) {
				generated := &goschema.Database{
					Extensions: []goschema.Extension{
						{Name: "uuid-ossp", IfNotExists: true, Comment: "UUID generation"},
						{Name: "hstore", IfNotExists: true, Comment: "Key-value store"},
						{Name: "postgis", Version: "3.0", Comment: "Geographic data"},
					},
				}
				database := &types.DBSchema{
					Extensions: []types.DBExtension{
						{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},    // ignored by default
						{Name: "btree_gin", Version: "1.3", Schema: "public"},     // ignored by default
						{Name: "pg_trgm", Version: "1.6", Schema: "public"},       // ignored by default
					},
				}
				return generated, database
			},
			verify: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(len(diff.ExtensionsAdded), qt.Equals, 3)
				c.Assert(diff.ExtensionsAdded, qt.Contains, "uuid-ossp")
				c.Assert(diff.ExtensionsAdded, qt.Contains, "hstore")
				c.Assert(diff.ExtensionsAdded, qt.Contains, "postgis")
				c.Assert(len(diff.ExtensionsRemoved), qt.Equals, 0) // ignored extensions not removed
			},
		},
		{
			name:        "production database cleanup",
			description: "Removing unused extensions from production database",
			setup: func() (*goschema.Database, *types.DBSchema) {
				generated := &goschema.Database{
					Extensions: []goschema.Extension{
						{Name: "uuid-ossp", IfNotExists: true},
					},
				}
				database := &types.DBSchema{
					Extensions: []types.DBExtension{
						{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},    // ignored by default
						{Name: "btree_gin", Version: "1.3", Schema: "public"},     // ignored by default
						{Name: "pg_trgm", Version: "1.6", Schema: "public"},       // ignored by default
						{Name: "uuid-ossp", Version: "1.1", Schema: "public"},
						{Name: "postgis", Version: "3.0", Schema: "public"},
						{Name: "hstore", Version: "1.8", Schema: "public"},
					},
				}
				return generated, database
			},
			verify: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(len(diff.ExtensionsAdded), qt.Equals, 0)
				c.Assert(len(diff.ExtensionsRemoved), qt.Equals, 2) // only non-ignored extensions removed
				c.Assert(diff.ExtensionsRemoved, qt.Contains, "postgis")
				c.Assert(diff.ExtensionsRemoved, qt.Contains, "hstore")
				// btree_gin is ignored by default, so it should not be removed
			},
		},
		{
			name:        "incremental extension addition",
			description: "Adding new extensions to existing setup",
			setup: func() (*goschema.Database, *types.DBSchema) {
				generated := &goschema.Database{
					Extensions: []goschema.Extension{
						{Name: "pg_trgm", IfNotExists: true},
						{Name: "btree_gin", IfNotExists: true},
						{Name: "postgis", Version: "3.1"},
						{Name: "uuid-ossp", IfNotExists: true},
					},
				}
				database := &types.DBSchema{
					Extensions: []types.DBExtension{
						{Name: "pg_trgm", Version: "1.6", Schema: "public"},
						{Name: "btree_gin", Version: "1.3", Schema: "public"},
					},
				}
				return generated, database
			},
			verify: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(len(diff.ExtensionsAdded), qt.Equals, 2)
				c.Assert(diff.ExtensionsAdded, qt.Contains, "postgis")
				c.Assert(diff.ExtensionsAdded, qt.Contains, "uuid-ossp")
				c.Assert(len(diff.ExtensionsRemoved), qt.Equals, 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Setup test scenario
			generated, database := tt.setup()

			// Create empty diff to populate
			diff := &difftypes.SchemaDiff{}

			// Run the comparison
			compare.Extensions(generated, database, diff, nil)

			// Verify results using custom verification function
			tt.verify(c, diff)
		})
	}
}

func TestExtensions_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		description string
		generated   *goschema.Database
		database    *types.DBSchema
		expectPanic bool
	}{
		{
			name:        "nil generated extensions",
			description: "Handle nil extensions slice in generated schema",
			generated: &goschema.Database{
				Extensions: nil,
			},
			database: &types.DBSchema{
				Extensions: []types.DBExtension{
					{Name: "pg_trgm", Version: "1.6", Schema: "public"},
				},
			},
			expectPanic: false,
		},
		{
			name:        "nil database extensions",
			description: "Handle nil extensions slice in database schema",
			generated: &goschema.Database{
				Extensions: []goschema.Extension{
					{Name: "pg_trgm", IfNotExists: true},
				},
			},
			database: &types.DBSchema{
				Extensions: nil,
			},
			expectPanic: false,
		},
		{
			name:        "empty extension names",
			description: "Handle extensions with empty names gracefully",
			generated: &goschema.Database{
				Extensions: []goschema.Extension{
					{Name: "", IfNotExists: true},
					{Name: "pg_trgm", IfNotExists: true},
				},
			},
			database: &types.DBSchema{
				Extensions: []types.DBExtension{
					{Name: "", Version: "1.0", Schema: "public"},
				},
			},
			expectPanic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Create empty diff to populate
			diff := &difftypes.SchemaDiff{}

			// Run the comparison and check for panics
			if tt.expectPanic {
				c.Assert(func() {
					compare.Extensions(tt.generated, tt.database, diff, nil)
				}, qt.PanicMatches, ".*")
			} else {
				// Should not panic
				compare.Extensions(tt.generated, tt.database, diff, nil)
				// Basic sanity check - diff should be populated
				c.Assert(diff, qt.IsNotNil)
			}
		})
	}
}

func TestExtensions_WithIgnoreConfiguration(t *testing.T) {
	tests := []struct {
		name                string
		generatedExtensions []goschema.Extension
		databaseExtensions  []types.DBExtension
		options             *config.CompareOptions
		expectedAdded       []string
		expectedRemoved     []string
	}{
		{
			name: "default ignore extensions - ignored extensions in database not removed",
			generatedExtensions: []goschema.Extension{
				{Name: "uuid-ossp", IfNotExists: true},
			},
			databaseExtensions: []types.DBExtension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
				{Name: "btree_gin", Version: "1.3", Schema: "public"},
				{Name: "pg_trgm", Version: "1.6", Schema: "public"},
				{Name: "uuid-ossp", Version: "1.1", Schema: "public"},
			},
			options:         nil, // Use defaults (ignores plpgsql, btree_gin, pg_trgm)
			expectedAdded:   []string{},
			expectedRemoved: []string{},
		},
		{
			name: "default ignore extensions - non-ignored extension added",
			generatedExtensions: []goschema.Extension{
				{Name: "uuid-ossp", IfNotExists: true},
			},
			databaseExtensions: []types.DBExtension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
				{Name: "btree_gin", Version: "1.3", Schema: "public"},
				{Name: "pg_trgm", Version: "1.6", Schema: "public"},
			},
			options:         nil, // Use defaults (ignores plpgsql, btree_gin, pg_trgm)
			expectedAdded:   []string{"uuid-ossp"},
			expectedRemoved: []string{}, // ignored extensions should not be removed
		},
		{
			name: "custom ignore list - ignore adminpack only",
			generatedExtensions: []goschema.Extension{
				{Name: "uuid-ossp", IfNotExists: true},
			},
			databaseExtensions: []types.DBExtension{
				{Name: "adminpack", Version: "2.1", Schema: "public"},
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			},
			options:         config.WithIgnoredExtensions("adminpack"),
			expectedAdded:   []string{"uuid-ossp"},
			expectedRemoved: []string{"plpgsql"}, // plpgsql not ignored in custom list
		},
		{
			name: "ignore multiple extensions",
			generatedExtensions: []goschema.Extension{
				{Name: "uuid-ossp", IfNotExists: true},
			},
			databaseExtensions: []types.DBExtension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
				{Name: "adminpack", Version: "2.1", Schema: "public"},
				{Name: "pg_stat_statements", Version: "1.9", Schema: "public"},
			},
			options:         config.WithIgnoredExtensions("plpgsql", "adminpack"),
			expectedAdded:   []string{"uuid-ossp"},
			expectedRemoved: []string{"pg_stat_statements"}, // Only non-ignored extension should be removed
		},
		{
			name: "no ignored extensions - manage all",
			generatedExtensions: []goschema.Extension{
				{Name: "uuid-ossp", IfNotExists: true},
			},
			databaseExtensions: []types.DBExtension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
				{Name: "adminpack", Version: "2.1", Schema: "public"},
			},
			options:         config.WithIgnoredExtensions(), // Empty ignore list
			expectedAdded:   []string{"uuid-ossp"},
			expectedRemoved: []string{"adminpack", "plpgsql"}, // All extensions should be managed
		},
		{
			name: "additional ignored extensions",
			generatedExtensions: []goschema.Extension{
				{Name: "uuid-ossp", IfNotExists: true},
			},
			databaseExtensions: []types.DBExtension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
				{Name: "btree_gin", Version: "1.3", Schema: "public"},
				{Name: "pg_trgm", Version: "1.6", Schema: "public"},
				{Name: "adminpack", Version: "2.1", Schema: "public"},
				{Name: "pg_stat_statements", Version: "1.9", Schema: "public"},
			},
			options:         config.WithAdditionalIgnoredExtensions("adminpack"),
			expectedAdded:   []string{"uuid-ossp"},
			expectedRemoved: []string{"pg_stat_statements"}, // plpgsql, btree_gin, pg_trgm, and adminpack ignored
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Setup test data
			generated := &goschema.Database{
				Extensions: tt.generatedExtensions,
			}
			database := &types.DBSchema{
				Extensions: tt.databaseExtensions,
			}

			// Create empty diff to populate
			diff := &difftypes.SchemaDiff{}

			// Run the comparison with options
			compare.Extensions(generated, database, diff, tt.options)

			// Verify results
			c.Assert(diff.ExtensionsAdded, qt.DeepEquals, tt.expectedAdded)
			c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, tt.expectedRemoved)
		})
	}
}
