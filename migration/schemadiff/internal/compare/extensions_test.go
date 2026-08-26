package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

func TestExtensions(t *testing.T) {
	tests := []struct {
		name                string
		generatedExtensions []schemamodel.Extension
		databaseExtensions  []catalog.Extension
		expectedAdded       []string
		expectedRemoved     []string
	}{
		{
			name:                "no extensions in either schema",
			generatedExtensions: make([]schemamodel.Extension, 0),
			databaseExtensions:  make([]catalog.Extension, 0),
			expectedAdded:       make([]string, 0),
			expectedRemoved:     make([]string, 0),
		},
		{
			name: "extension needs to be added",
			generatedExtensions: []schemamodel.Extension{
				{Name: "pg_trgm", IfNotExists: true},
			},
			databaseExtensions: make([]catalog.Extension, 0),
			expectedAdded:      []string{"pg_trgm"},
			expectedRemoved:    make([]string, 0),
		},
		{
			name:                "extension needs to be removed",
			generatedExtensions: make([]schemamodel.Extension, 0),
			databaseExtensions: []catalog.Extension{
				{Name: "btree_gin", Version: "1.3", Schema: "public"},
			},
			expectedAdded:   make([]string, 0),
			expectedRemoved: []string{"btree_gin"},
		},
		{
			name: "extension already exists - no changes",
			generatedExtensions: []schemamodel.Extension{
				{Name: "pg_trgm", IfNotExists: true},
			},
			databaseExtensions: []catalog.Extension{
				{Name: "pg_trgm", Version: "1.6", Schema: "public"},
			},
			expectedAdded:   make([]string, 0),
			expectedRemoved: make([]string, 0),
		},
		{
			name: "multiple extensions - mixed operations",
			generatedExtensions: []schemamodel.Extension{
				{Name: "pg_trgm", IfNotExists: true},
				{Name: "btree_gin", IfNotExists: true},
				{Name: "postgis", Version: "3.0"},
			},
			databaseExtensions: []catalog.Extension{
				{Name: "pg_trgm", Version: "1.6", Schema: "public"},
				{Name: "uuid-ossp", Version: "1.1", Schema: "public"},
			},
			expectedAdded:   []string{"btree_gin", "postgis"},
			expectedRemoved: []string{"uuid-ossp"},
		},
		{
			name: "extensions with different versions - no version comparison",
			generatedExtensions: []schemamodel.Extension{
				{Name: "postgis", Version: "3.1"},
			},
			databaseExtensions: []catalog.Extension{
				{Name: "postgis", Version: "3.0", Schema: "public"},
			},
			expectedAdded:   make([]string, 0),
			expectedRemoved: make([]string, 0),
		},
		{
			name: "sorted output verification",
			generatedExtensions: []schemamodel.Extension{
				{Name: "z_extension", IfNotExists: true},
				{Name: "a_extension", IfNotExists: true},
				{Name: "m_extension", IfNotExists: true},
			},
			databaseExtensions: []catalog.Extension{
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
			desired := &schemamodel.Database{
				Extensions: tt.generatedExtensions,
			}

			database := &catalog.Database{
				Extensions: tt.databaseExtensions,
			}

			// Create empty diff to populate
			diff := &difftypes.SchemaDiff{}

			// Run the comparison
			compare.Extensions(desired, database, diff, nil, compare.CoverageOf(desired, database))

			// Verify results
			c.Assert(diff.ExtensionsAdded, qt.DeepEquals, tt.expectedAdded)
			c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, tt.expectedRemoved)
		})
	}
}

func TestExtensionsWithSemanticsComparesInstallationSchema(t *testing.T) {
	tests := []struct {
		name     string
		desired  schemamodel.Extension
		database catalog.Extension
		want     []difftypes.ExtensionDiff
	}{
		{
			name:     "implicit default matches explicit public",
			desired:  schemamodel.Extension{Name: "pgcrypto"},
			database: catalog.Extension{Name: "pgcrypto", Schema: "public"},
			want:     make([]difftypes.ExtensionDiff, 0),
		},
		{
			name:     "identical nondefault schema is synced",
			desired:  schemamodel.Extension{Name: "pgcrypto", Schema: "extensions"},
			database: catalog.Extension{Name: "pgcrypto", Schema: "extensions"},
			want:     make([]difftypes.ExtensionDiff, 0),
		},
		{
			name:     "placement change is not synced",
			desired:  schemamodel.Extension{Name: "pgcrypto", Schema: "extensions"},
			database: catalog.Extension{Name: "pgcrypto", Schema: "public"},
			want: []difftypes.ExtensionDiff{{
				Name:       "pgcrypto",
				FromSchema: "public",
				ToSchema:   "extensions",
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			desired := &schemamodel.Database{Extensions: []schemamodel.Extension{test.desired}}
			database := &catalog.Database{Extensions: []catalog.Extension{test.database}}
			compare.ExtensionsWithSemantics(
				desired,
				database,
				diff,
				nil,
				compare.CoverageOf(desired, database),
				identifier.Semantics{
					DefaultSchema:  "public",
					IndexNamespace: identifier.IndexNamespaceSchema,
					IndexNames:     identifier.ComparisonExact,
					TableNames:     identifier.ComparisonExact,
					ColumnNames:    identifier.ComparisonExact,
				},
			)
			c.Assert(diff.ExtensionsModified, qt.DeepEquals, test.want)
		})
	}
}

func TestExtensionsWrapperUsesPostgreSQLDefaultSchema(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "pgcrypto"}}}
	database := &catalog.Database{Extensions: []catalog.Extension{{Name: "pgcrypto", Schema: "public"}}}
	diff := &difftypes.SchemaDiff{}

	compare.Extensions(desired, database, diff, nil, compare.CoverageOf(desired, database))

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestExtensions_RealWorldScenarios(t *testing.T) {
	tests := []struct {
		name        string
		description string
		setup       func() (*schemamodel.Database, *catalog.Database)
		// The whole sorted set is expected, not a membership sample: an
		// extension the comparison invents on top of the ones a row names is a
		// CREATE EXTENSION nobody asked for, and a "contains" claim cannot see
		// it.
		wantAdded   []string
		wantRemoved []string
	}{
		{
			name:        "fresh database setup",
			description: "Setting up PostgreSQL extensions on a fresh database",
			setup: func() (*schemamodel.Database, *catalog.Database) {
				desired := &schemamodel.Database{
					Extensions: []schemamodel.Extension{
						{Name: "pg_trgm", IfNotExists: true, Comment: "Trigram similarity"},
						{Name: "btree_gin", IfNotExists: true, Comment: "GIN indexes for btree"},
						{Name: "postgis", Version: "3.0", Comment: "Geographic data"},
					},
				}
				database := &catalog.Database{
					Extensions: make([]catalog.Extension, 0), // Fresh database
				}
				return desired, database
			},
			wantAdded:   []string{"btree_gin", "pg_trgm", "postgis"},
			wantRemoved: make([]string, 0),
		},
		{
			name:        "production database cleanup",
			description: "Removing unused extensions from production database",
			setup: func() (*schemamodel.Database, *catalog.Database) {
				desired := &schemamodel.Database{
					Extensions: []schemamodel.Extension{
						{Name: "pg_trgm", IfNotExists: true},
					},
				}
				database := &catalog.Database{
					Extensions: []catalog.Extension{
						{Name: "pg_trgm", Version: "1.6", Schema: "public"},
						{Name: "uuid-ossp", Version: "1.1", Schema: "public"},
						{Name: "postgis", Version: "3.0", Schema: "public"},
						{Name: "btree_gin", Version: "1.3", Schema: "public"},
					},
				}
				return desired, database
			},
			wantAdded:   make([]string, 0),
			wantRemoved: []string{"btree_gin", "postgis", "uuid-ossp"},
		},
		{
			name:        "incremental extension addition",
			description: "Adding new extensions to existing setup",
			setup: func() (*schemamodel.Database, *catalog.Database) {
				desired := &schemamodel.Database{
					Extensions: []schemamodel.Extension{
						{Name: "pg_trgm", IfNotExists: true},
						{Name: "btree_gin", IfNotExists: true},
						{Name: "postgis", Version: "3.1"},
						{Name: "uuid-ossp", IfNotExists: true},
					},
				}
				database := &catalog.Database{
					Extensions: []catalog.Extension{
						{Name: "pg_trgm", Version: "1.6", Schema: "public"},
						{Name: "btree_gin", Version: "1.3", Schema: "public"},
					},
				}
				return desired, database
			},
			wantAdded:   []string{"postgis", "uuid-ossp"},
			wantRemoved: make([]string, 0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			desired, database := tt.setup()
			diff := &difftypes.SchemaDiff{}

			compare.Extensions(desired, database, diff, nil, compare.CoverageOf(desired, database))

			c.Assert(diff.ExtensionsAdded, qt.DeepEquals, tt.wantAdded)
			c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, tt.wantRemoved)
		})
	}
}

func TestExtensions_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		description string
		desired     *schemamodel.Database
		database    *catalog.Database
		expectPanic bool
	}{
		{
			name:        "nil generated extensions",
			description: "Handle nil extensions slice in generated schema",
			desired: &schemamodel.Database{
				Extensions: nil,
			},
			database: &catalog.Database{
				Extensions: []catalog.Extension{
					{Name: "pg_trgm", Version: "1.6", Schema: "public"},
				},
			},
			expectPanic: false,
		},
		{
			name:        "nil database extensions",
			description: "Handle nil extensions slice in database schema",
			desired: &schemamodel.Database{
				Extensions: []schemamodel.Extension{
					{Name: "pg_trgm", IfNotExists: true},
				},
			},
			database: &catalog.Database{
				Extensions: nil,
			},
			expectPanic: false,
		},
		{
			name:        "empty extension names",
			description: "Handle extensions with empty names gracefully",
			desired: &schemamodel.Database{
				Extensions: []schemamodel.Extension{
					{Name: "", IfNotExists: true},
					{Name: "pg_trgm", IfNotExists: true},
				},
			},
			database: &catalog.Database{
				Extensions: []catalog.Extension{
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
					compare.Extensions(tt.desired, tt.database, diff, nil, compare.CoverageOf(tt.desired, tt.database))
				}, qt.PanicMatches, ".*")
			} else {
				// Should not panic
				compare.Extensions(tt.desired, tt.database, diff, nil, compare.CoverageOf(tt.desired, tt.database))
				// Basic sanity check - diff should be populated
				c.Assert(diff, qt.IsNotNil)
			}
		})
	}
}

func TestExtensions_WithIgnoreConfiguration(t *testing.T) {
	tests := []struct {
		name                string
		generatedExtensions []schemamodel.Extension
		databaseExtensions  []catalog.Extension
		options             *config.CompareOptions
		expectedAdded       []string
		expectedRemoved     []string
	}{
		{
			name: "default ignore plpgsql - plpgsql in database not removed",
			generatedExtensions: []schemamodel.Extension{
				{Name: "pg_trgm", IfNotExists: true},
			},
			databaseExtensions: []catalog.Extension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
				{Name: "pg_trgm", Version: "1.6", Schema: "public"},
			},
			options:         nil, // Use defaults (ignores plpgsql)
			expectedAdded:   make([]string, 0),
			expectedRemoved: make([]string, 0),
		},
		{
			name: "default ignore plpgsql - plpgsql not in generated schema",
			generatedExtensions: []schemamodel.Extension{
				{Name: "pg_trgm", IfNotExists: true},
			},
			databaseExtensions: []catalog.Extension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			},
			options:         nil, // Use defaults (ignores plpgsql)
			expectedAdded:   []string{"pg_trgm"},
			expectedRemoved: make([]string, 0), // plpgsql should not be removed
		},
		{
			name: "custom ignore list - ignore adminpack",
			generatedExtensions: []schemamodel.Extension{
				{Name: "pg_trgm", IfNotExists: true},
			},
			databaseExtensions: []catalog.Extension{
				{Name: "adminpack", Version: "2.1", Schema: "public"},
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
			},
			options:         config.WithIgnoredExtensions("adminpack"),
			expectedAdded:   []string{"pg_trgm"},
			expectedRemoved: []string{"plpgsql"}, // plpgsql not ignored in custom list
		},
		{
			name: "ignore multiple extensions",
			generatedExtensions: []schemamodel.Extension{
				{Name: "pg_trgm", IfNotExists: true},
			},
			databaseExtensions: []catalog.Extension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
				{Name: "adminpack", Version: "2.1", Schema: "public"},
				{Name: "pg_stat_statements", Version: "1.9", Schema: "public"},
			},
			options:         config.WithIgnoredExtensions("plpgsql", "adminpack"),
			expectedAdded:   []string{"pg_trgm"},
			expectedRemoved: []string{"pg_stat_statements"}, // Only non-ignored extension should be removed
		},
		{
			name: "no ignored extensions - manage all",
			generatedExtensions: []schemamodel.Extension{
				{Name: "pg_trgm", IfNotExists: true},
			},
			databaseExtensions: []catalog.Extension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
				{Name: "adminpack", Version: "2.1", Schema: "public"},
			},
			options:         config.WithIgnoredExtensions(), // Empty ignore list
			expectedAdded:   []string{"pg_trgm"},
			expectedRemoved: []string{"adminpack", "plpgsql"}, // All extensions should be managed
		},
		{
			name: "additional ignored extensions",
			generatedExtensions: []schemamodel.Extension{
				{Name: "pg_trgm", IfNotExists: true},
			},
			databaseExtensions: []catalog.Extension{
				{Name: "plpgsql", Version: "1.0", Schema: "pg_catalog"},
				{Name: "adminpack", Version: "2.1", Schema: "public"},
				{Name: "pg_stat_statements", Version: "1.9", Schema: "public"},
			},
			options:         config.WithAdditionalIgnoredExtensions("adminpack"),
			expectedAdded:   []string{"pg_trgm"},
			expectedRemoved: []string{"pg_stat_statements"}, // plpgsql and adminpack ignored
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Setup test data
			desired := &schemamodel.Database{
				Extensions: tt.generatedExtensions,
			}
			database := &catalog.Database{
				Extensions: tt.databaseExtensions,
			}

			// Create empty diff to populate
			diff := &difftypes.SchemaDiff{}

			// Run the comparison with options
			compare.Extensions(desired, database, diff, tt.options, compare.CoverageOf(desired, database))

			// Verify results
			c.Assert(diff.ExtensionsAdded, qt.DeepEquals, tt.expectedAdded)
			c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, tt.expectedRemoved)
		})
	}
}
