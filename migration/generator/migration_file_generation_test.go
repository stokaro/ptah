package generator

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/migration/schemadiff"
)

func TestMigrationFileGeneration_ExtensionSQL(t *testing.T) {
	tests := []struct {
		name              string
		generatedSchema   *schemamodel.Database
		databaseSchema    *catalog.Database
		expectedUpSQL     []string
		expectedDownSQL   []string
		unexpectedUpSQL   []string
		unexpectedDownSQL []string
	}{
		{
			name: "extension addition generates correct up and down SQL",
			generatedSchema: ownerDeclaring(
				ownerExtension{name: "pg_trgm", comment: "Enable trigram similarity search"},
				ownerExtension{name: "btree_gin", comment: "Enable GIN indexes on btree types"},
			),
			databaseSchema: ownerHolding(),
			expectedUpSQL: []string{
				"-- Direction: UP",
				"CREATE EXTENSION IF NOT EXISTS pg_trgm;",
				"COMMENT ON EXTENSION pg_trgm IS 'Enable trigram similarity search';",
				"CREATE EXTENSION IF NOT EXISTS btree_gin;",
				"COMMENT ON EXTENSION btree_gin IS 'Enable GIN indexes on btree types';",
			},
			expectedDownSQL: []string{
				"-- Direction: DOWN",
				"WARNING: Removing extension 'pg_trgm' may break existing functionality",
				"DROP EXTENSION IF EXISTS pg_trgm;",
				"WARNING: Removing extension 'btree_gin' may break existing functionality",
				"DROP EXTENSION IF EXISTS btree_gin;",
			},
			unexpectedUpSQL: []string{
				"DROP EXTENSION",
			},
			unexpectedDownSQL: []string{
				"CREATE EXTENSION",
			},
		},
		{
			name:            "extension removal generates correct up and down SQL",
			generatedSchema: ownerDeclaring(),
			databaseSchema:  ownerHolding(ownerExtension{name: "pg_trgm", version: "1.6", schema: "public"}),
			expectedUpSQL: []string{
				"-- Direction: UP",
				"WARNING: Removing extension 'pg_trgm' may break existing functionality",
				"DROP EXTENSION IF EXISTS pg_trgm;",
			},
			expectedDownSQL: []string{
				"-- Direction: DOWN",
				"CREATE EXTENSION IF NOT EXISTS pg_trgm",
			},
			unexpectedUpSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS pg_trgm;",
			},
			unexpectedDownSQL: []string{
				"DROP EXTENSION",
			},
		},
		{
			name:            "nondefault extension removal restores its schema on rollback",
			generatedSchema: ownerDeclaring(),
			databaseSchema:  ownerHolding(ownerExtension{name: "pgcrypto", version: "1.3", schema: "Extension Store"}),
			expectedUpSQL: []string{
				"DROP EXTENSION IF EXISTS pgcrypto;",
			},
			expectedDownSQL: []string{
				`CREATE SCHEMA IF NOT EXISTS "Extension Store";`,
				`CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA "Extension Store" VERSION '1.3';`,
			},
			unexpectedDownSQL: []string{
				"CREATE EXTENSION IF NOT EXISTS pgcrypto VERSION",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// 1. Calculate schema diff
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(),
				tt.generatedSchema, tt.databaseSchema, "postgres", must.Must(builtin.New()),
			))
			c.Assert(diff.HasChanges(), qt.IsTrue)

			// 2. Generate up migration SQL
			upSQL, err := upMigrationSQL(
				context.Background(), must.Must(builtin.New()),
				diff, tt.generatedSchema, "postgres",
			)
			c.Assert(err, qt.IsNil)
			upSQL = legacyRenderedSQL(upSQL)

			// 3. Generate down migration SQL using the fixed reverseSchemaDiff function
			downSQL, err := downMigrationSQL(t.Context(), must.Must(builtin.New()),
				diff, tt.generatedSchema, tt.databaseSchema, "postgres")
			c.Assert(err, qt.IsNil)
			downSQL = legacyRenderedSQL(downSQL)

			// 4. Verify up migration SQL contains expected patterns
			for _, expected := range tt.expectedUpSQL {
				c.Assert(upSQL, qt.Contains, expected,
					qt.Commentf("Expected up SQL to contain: %s\nActual up SQL:\n%s", expected, upSQL))
			}

			for _, unexpected := range tt.unexpectedUpSQL {
				c.Assert(upSQL, qt.Not(qt.Contains), unexpected,
					qt.Commentf("Expected up SQL to NOT contain: %s\nActual up SQL:\n%s", unexpected, upSQL))
			}

			// 5. Verify down migration SQL contains expected patterns
			for _, expected := range tt.expectedDownSQL {
				c.Assert(downSQL, qt.Contains, expected,
					qt.Commentf("Expected down SQL to contain: %s\nActual down SQL:\n%s", expected, downSQL))
			}

			for _, unexpected := range tt.unexpectedDownSQL {
				c.Assert(downSQL, qt.Not(qt.Contains), unexpected,
					qt.Commentf("Expected down SQL to NOT contain: %s\nActual down SQL:\n%s", unexpected, downSQL))
			}
		})
	}
}

func TestExtensionMigrationSQL_CompleteFlow(t *testing.T) {
	c := qt.New(t)

	// Test the complete flow: schema with extensions -> empty database -> up migration -> down migration
	generatedSchema := ownerDeclaring(ownerExtension{name: "pg_trgm", comment: "Enable trigram similarity search"})
	emptyDatabase := ownerHolding()

	// 1. Generate up migration (should create extension)
	upDiff := must.Must(schemadiff.CompareWithDialect(t.Context(),
		generatedSchema, emptyDatabase, "postgres", must.Must(builtin.New()),
	))
	upSQL, err := upMigrationSQL(
		context.Background(), must.Must(builtin.New()),
		upDiff, generatedSchema, "postgres",
	)
	c.Assert(err, qt.IsNil)
	upSQL = legacyRenderedSQL(upSQL)
	c.Assert(upSQL, qt.Contains, "CREATE EXTENSION IF NOT EXISTS pg_trgm;")

	// 2. Simulate database state after up migration
	databaseAfterUp := ownerHolding(ownerExtension{name: "pg_trgm", version: "1.6", schema: "public"})

	// 3. Generate down migration (should drop extension)
	// For down migration, we use the original upDiff and reverse it
	downSQL, err := downMigrationSQL(t.Context(), must.Must(builtin.New()),
		upDiff, generatedSchema, databaseAfterUp, "postgres")
	c.Assert(err, qt.IsNil)
	downSQL = legacyRenderedSQL(downSQL)
	c.Assert(downSQL, qt.Contains, "DROP EXTENSION IF EXISTS pg_trgm;")

	// 4. Verify the cycle is complete
	added, removed := ownerChanges(upDiff)
	c.Assert(added, qt.DeepEquals, []string{"pg_trgm"})
	c.Assert(removed, qt.HasLen, 0)
}

// ownerExtension is one extension a fixture declares or holds.
type ownerExtension struct{ name, schema, version, comment string }

// ownerDeclaring is a desired schema that declares each extension to the
// extension owner and claims that it declares every extension.
func ownerDeclaring(extensions ...ownerExtension) *schemamodel.Database {
	objects := make([]schemaext.Object, 0, len(extensions))
	for _, extension := range extensions {
		objects = append(objects, must.Must(pgextension.DesiredObject(pgextension.Ref(extension.name),
			pgextension.DesiredExtension{IfNotExists: true, Schema: extension.schema, Version: extension.version, Comment: extension.comment})))
	}
	return &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired)),
	}
}

// ownerHolding is a read that holds each extension, as the PostgreSQL reader
// hands them to the extension owner, and describes every extension.
func ownerHolding(extensions ...ownerExtension) *catalog.Database {
	objects := make([]schemaext.Object, 0, len(extensions))
	for _, extension := range extensions {
		objects = append(objects, must.Must(pgextension.ObservedObject(pgextension.Ref(extension.name),
			pgextension.ObservedExtension{Schema: extension.schema, Version: extension.version})))
	}
	return &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}
}

// ownerChanges names the extensions a diff installs and drops, from the
// extension owner's changes.
func ownerChanges(diff *difftypes.SchemaDiff) (added, removed []string) {
	for _, record := range diff.FeatureChanges {
		change, ok := record.Value.(*pgextension.Change)
		switch {
		case !ok:
		case change.Before == nil:
			added = append(added, pgextension.Name(record.Subject))
		case change.After == nil:
			removed = append(removed, pgextension.Name(record.Subject))
		}
	}
	return added, removed
}

// TestMigrationFileGeneration_EmptyDiffPrevention tests the fix for issue #36
// where empty UP migrations with dangerous DOWN migrations were generated.
// Updated to verify that empty diffs are treated as successful no-op operations.
func TestMigrationFileGeneration_EmptyDiffPrevention(t *testing.T) {
	tests := []struct {
		name            string
		generatedSchema *schemamodel.Database
		databaseSchema  *catalog.Database
		diff            *difftypes.SchemaDiff
		description     string
	}{
		{
			// A column the generated schema does not define is not a reachable
			// state: a column travels WITH its change, so a diff that names one
			// carries its definition and there is nothing to be missing
			// (stokaro/ptah#2315).
			//
			// What this row asserts is the property underneath: a table diff
			// that changes nothing writes no migration.
			name: "table modification that changes nothing should not generate migration",
			generatedSchema: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "users", StructName: "User"},
				},
				Fields: make([]schemamodel.Field, 0),
			},
			databaseSchema: &catalog.Database{
				Tables: []catalog.Table{
					{
						Name: "users",
						Columns: []catalog.Column{
							{Name: "id", DataType: "integer", IsPrimaryKey: true},
						},
					},
				},
			},
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{
					{TableName: "users"},
				},
			},
			description: "A table diff carrying no change writes no migration",
		},
		{
			name: "completely empty diff should not generate migration",
			generatedSchema: &schemamodel.Database{
				Tables: make([]schemamodel.Table, 0),
				Fields: make([]schemamodel.Field, 0),
			},
			databaseSchema: &catalog.Database{
				Tables: make([]catalog.Table, 0),
			},
			diff: &difftypes.SchemaDiff{
				// Completely empty diff
			},
			description: "Empty diffs should be treated as successful no-op operations",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Generate up migration SQL - should return success with empty SQL for no changes
			upSQL, err := upMigrationSQL(
				context.Background(), must.Must(builtin.New()),
				tt.diff, tt.generatedSchema, "postgres",
			)
			c.Assert(err, qt.IsNil, qt.Commentf("Expected success for empty migration, but got error: %v", err))
			c.Assert(upSQL, qt.Equals, "", qt.Commentf("Expected empty SQL for no changes, but got: %s", upSQL))
		})
	}
}

// TestGenerateUpMigrationSQL_NoChangesSuccess tests that the up file renders empty, as a success,
// when no actual SQL statements are generated, treating it as a successful no-op operation
func TestGenerateUpMigrationSQL_NoChangesSuccess(t *testing.T) {
	c := qt.New(t)

	// Test case: empty diff should return success with empty SQL
	emptyDiff := &difftypes.SchemaDiff{
		// Completely empty diff
	}

	emptySchema := &schemamodel.Database{
		Tables: make([]schemamodel.Table, 0),
		Fields: make([]schemamodel.Field, 0),
	}

	// Generate up migration SQL - should return success with empty SQL
	upSQL, err := upMigrationSQL(
		context.Background(), must.Must(builtin.New()),
		emptyDiff, emptySchema, "postgres",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("Expected success for empty diff, but got error: %v", err))
	c.Assert(upSQL, qt.Equals, "", qt.Commentf("Expected empty SQL for no changes, but got: %s", upSQL))
}

// TestHasActualSQLStatements tests the helper function that detects comment-only statements
func TestHasActualSQLStatements(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		expected   bool
	}{
		{
			name:       "empty statements",
			statements: make([]string, 0),
			expected:   false,
		},
		{
			name:       "only comments",
			statements: []string{"-- Add/modify columns for table: users"},
			expected:   false,
		},
		{
			name:       "comments with whitespace",
			statements: []string{"  -- Add/modify columns for table: users  "},
			expected:   false,
		},
		{
			name:       "multiple comments only",
			statements: []string{"-- Comment 1", "-- Comment 2", "/* Block comment */"},
			expected:   false,
		},
		{
			name:       "actual SQL statement",
			statements: []string{"ALTER TABLE users ADD COLUMN email VARCHAR(255)"},
			expected:   true,
		},
		{
			name:       "mix of comments and SQL",
			statements: []string{"-- Add column", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"},
			expected:   true,
		},
		{
			name:       "SQL with inline comments",
			statements: []string{"ALTER TABLE users ADD COLUMN email VARCHAR(255) -- Add email column"},
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			result := hasActualSQLStatements(tt.statements)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestWithGeneratedTimeoutDirectives(t *testing.T) {
	c := qt.New(t)

	sql := "-- Migration generated from schema differences\n-- Direction: UP\n\nALTER TABLE users ADD COLUMN email TEXT;"
	got := withGeneratedTimeoutDirectives(sql, "postgres")

	c.Assert(got, qt.Contains, "-- +ptah lock_timeout=3s")
	c.Assert(got, qt.Contains, "-- +ptah statement_timeout=30s")
	c.Assert(got, qt.Matches, "(?s).*-- Direction: UP\n-- \\+ptah lock_timeout=3s\n-- \\+ptah statement_timeout=30s\n\nALTER TABLE.*")
}

func TestWithGeneratedTimeoutDirectivesForOptions_SkipsNoTransactionPair(t *testing.T) {
	c := qt.New(t)

	sql := "-- Migration rollback\n-- Direction: DOWN\n\nALTER TABLE users DROP COLUMN email;"
	got := withGeneratedTimeoutDirectivesForOptions(sql, "postgres", generatedDirectiveOptions{skipTimeouts: true})

	c.Assert(got, qt.Equals, sql)
	c.Assert(got, qt.Not(qt.Contains), "-- +ptah lock_timeout")
	c.Assert(got, qt.Not(qt.Contains), "-- +ptah statement_timeout")
}

// TestWithGeneratedTimeoutDirectives_TriggerChange covers a file whose
// statements wait for a lock on an existing table without an ALTER TABLE among
// them: a trigger swap under LOCK TABLES, and a MariaDB replacement in one
// statement (stokaro/ptah#4012).
func TestWithGeneratedTimeoutDirectives_TriggerChange(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		body    string
	}{
		{
			name:    "a trigger swap under LOCK TABLES",
			dialect: "mysql",
			body: "LOCK TABLES `mytable` WRITE;\nDROP TRIGGER IF EXISTS `audit`;\n" +
				"CREATE TRIGGER `audit` AFTER INSERT ON `mytable` FOR EACH ROW SET @x = 1;\nUNLOCK TABLES;",
		},
		{
			name:    "a replacement in one statement",
			dialect: "mariadb",
			body:    "CREATE OR REPLACE TRIGGER `audit` AFTER INSERT ON `mytable` FOR EACH ROW SET @x = 1;",
		},
		{
			name:    "a trigger dropped on its own",
			dialect: "postgres",
			body:    `DROP TRIGGER IF EXISTS "audit" ON "mytable";`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := withGeneratedTimeoutDirectives("-- Direction: UP\n\n"+test.body, test.dialect)

			c.Assert(got, qt.Equals, "-- Direction: UP\n-- +ptah lock_timeout=3s\n-- +ptah statement_timeout=30s\n\n"+test.body)
		})
	}
}

func TestWithGeneratedTimeoutDirectives_NoAlterTable(t *testing.T) {
	c := qt.New(t)

	// A trigger named in a column comment is not a trigger change.
	sql := "-- Direction: UP\n\nCREATE TABLE users (id INTEGER PRIMARY KEY, note TEXT); -- DROP TRIGGER audit"
	got := withGeneratedTimeoutDirectives(sql, "postgres")

	c.Assert(got, qt.Equals, sql)
}

func TestWithGeneratedTimeoutDirectives_UnsupportedDialect(t *testing.T) {
	c := qt.New(t)

	sql := "-- Direction: UP\n\nALTER TABLE events ADD COLUMN payload String;"
	got := withGeneratedTimeoutDirectives(sql, "clickhouse")

	c.Assert(got, qt.Equals, sql)

	got = withGeneratedTimeoutDirectives(sql, "spanner")
	c.Assert(got, qt.Equals, sql)
}
