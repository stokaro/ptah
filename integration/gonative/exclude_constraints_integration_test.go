//go:build integration

package gonative_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

func TestExcludeConstraints_EndToEnd_PostgreSQL(t *testing.T) {
	tests := []struct {
		name        string
		desired     *schemamodel.Database
		database    *catalog.Database
		expectedSQL []string
	}{
		{
			name: "EXCLUDE constraint with WHERE clause",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "user_sessions", StructName: "UserSession"},
				},
				Fields: []schemamodel.Field{
					{Name: "user_id", Type: "BIGINT", StructName: "UserSession", Nullable: false},
					{Name: "is_active", Type: "BOOLEAN", StructName: "UserSession", Nullable: false},
				},
				Constraints: []schemamodel.Constraint{
					{
						StructName:      "UserSession",
						Name:            "one_active_session_per_user",
						Type:            "EXCLUDE",
						Table:           "user_sessions",
						UsingMethod:     "gist",
						ExcludeElements: "user_id WITH =",
						WhereCondition:  "is_active = true",
					},
				},
			},
			database: &catalog.Database{
				Tables: []catalog.Table{
					{
						Name: "user_sessions",
						Columns: []catalog.Column{
							{Name: "user_id", DataType: "BIGINT", IsNullable: "NO"},
							{Name: "is_active", DataType: "BOOLEAN", IsNullable: "NO"},
						},
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE user_sessions ADD CONSTRAINT one_active_session_per_user EXCLUDE USING gist (user_id WITH =) WHERE (is_active = true);",
			},
		},
		{
			name: "EXCLUDE constraint without WHERE clause",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "bookings", StructName: "Booking"},
				},
				Fields: []schemamodel.Field{
					{Name: "room_id", Type: "INTEGER", StructName: "Booking", Nullable: false},
					{Name: "during", Type: "TSRANGE", StructName: "Booking", Nullable: false},
				},
				Constraints: []schemamodel.Constraint{
					{
						StructName:      "Booking",
						Name:            "no_overlapping_bookings",
						Type:            "EXCLUDE",
						Table:           "bookings",
						UsingMethod:     "gist",
						ExcludeElements: "room_id WITH =, during WITH &&",
					},
				},
			},
			database: &catalog.Database{
				Tables: []catalog.Table{
					{
						Name: "bookings",
						Columns: []catalog.Column{
							{Name: "room_id", DataType: "INTEGER", IsNullable: "NO"},
							{Name: "during", DataType: "TSRANGE", IsNullable: "NO"},
						},
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE bookings ADD CONSTRAINT no_overlapping_bookings EXCLUDE USING gist (room_id WITH =, during WITH &&);",
			},
		},
		{
			name: "Multiple constraint types",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{
					{Name: "products", StructName: "Product"},
					{Name: "users", StructName: "User"},
				},
				Fields: []schemamodel.Field{
					{Name: "price", Type: "DECIMAL", StructName: "Product", Nullable: false},
					{Name: "user_id", Type: "BIGINT", StructName: "User", Nullable: false},
					{Name: "email", Type: "VARCHAR(255)", StructName: "User", Nullable: false},
				},
				Constraints: []schemamodel.Constraint{
					{
						StructName:      "Product",
						Name:            "positive_price",
						Type:            "CHECK",
						Table:           "products",
						CheckExpression: "price > 0",
					},
					{
						StructName: "User",
						Name:       "unique_user_email",
						Type:       "UNIQUE",
						Table:      "users",
						Columns:    []string{"user_id", "email"},
					},
				},
			},
			database: &catalog.Database{
				Tables: []catalog.Table{
					{
						Name: "products",
						Columns: []catalog.Column{
							{Name: "price", DataType: "DECIMAL", IsNullable: "NO"},
						},
					},
					{
						Name: "users",
						Columns: []catalog.Column{
							{Name: "user_id", DataType: "BIGINT", IsNullable: "NO"},
							{Name: "email", DataType: "VARCHAR(255)", IsNullable: "NO"},
						},
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE products ADD CONSTRAINT positive_price CHECK (price > 0);",
				"ALTER TABLE users ADD CONSTRAINT unique_user_email UNIQUE (user_id, email);",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// Step 1: Compare schemas to detect differences
			diff := schemadiff.Compare(tt.desired, tt.database)

			// Step 2: Verify that constraints are detected as added
			c.Assert(diff.ConstraintsAdded, qt.HasLen, len(tt.expectedSQL))

			// Step 3: Generate migration AST using PostgreSQL planner
			nodes, err := planner.GenerateSchemaDiffAST(diff, "postgres")
			c.Assert(err, qt.IsNil)

			// Step 4: Render AST to SQL
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)

			// Step 5: Extract and verify SQL statements
			lines := strings.Split(legacyRenderedSQL(sql), "\n")
			var actualSQL []string
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" && !strings.HasPrefix(line, "--") {
					actualSQL = append(actualSQL, line)
				}
			}

			// Debug: Print actual SQL for troubleshooting
			if len(actualSQL) != len(tt.expectedSQL) {
				t.Logf("Generated SQL:\n%s", sql)
				t.Logf("Actual SQL statements: %v", actualSQL)
				t.Logf("Expected SQL statements: %v", tt.expectedSQL)
			}

			// Step 6: Verify generated SQL matches expected (order-independent)
			c.Assert(actualSQL, qt.HasLen, len(tt.expectedSQL))
			for _, expected := range tt.expectedSQL {
				c.Assert(actualSQL, qt.Contains, expected)
			}
		})
	}
}

func TestExcludeConstraints_EndToEnd_MySQL(t *testing.T) {
	c := qt.New(t)

	// Test that EXCLUDE constraints generate warnings for MySQL
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "user_sessions", StructName: "UserSession"},
		},
		Fields: []schemamodel.Field{
			{Name: "user_id", Type: "BIGINT", StructName: "UserSession", Nullable: false},
		},
		Constraints: []schemamodel.Constraint{
			{
				StructName:      "UserSession",
				Name:            "one_active_session_per_user",
				Type:            "EXCLUDE",
				Table:           "user_sessions",
				UsingMethod:     "gist",
				ExcludeElements: "user_id WITH =",
			},
		},
	}

	database := &catalog.Database{
		Tables: []catalog.Table{
			{
				Name: "user_sessions",
				Columns: []catalog.Column{
					{Name: "user_id", DataType: "BIGINT", IsNullable: "NO"},
				},
			},
		},
	}

	// Step 1: Compare schemas
	diff := schemadiff.Compare(desired, database)

	// Step 2: Generate migration AST using MySQL planner
	nodes, err := planner.GenerateSchemaDiffAST(diff, "mysql")
	c.Assert(err, qt.IsNil)

	// Step 3: Render AST to SQL
	sql, err := renderer.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)

	// Step 4: Verify that a warning is generated for EXCLUDE constraints
	c.Assert(sql, qt.Contains, "WARNING")
	c.Assert(sql, qt.Contains, "EXCLUDE constraint")
	c.Assert(sql, qt.Contains, "not supported in MySQL")
}

func TestExcludeConstraints_SchemaComparison(t *testing.T) {
	c := qt.New(t)

	// Test that schema comparison correctly identifies constraint changes
	desired := &schemamodel.Database{
		Constraints: []schemamodel.Constraint{
			{
				StructName:      "Booking",
				Name:            "no_overlapping_bookings",
				Type:            "EXCLUDE",
				Table:           "bookings",
				UsingMethod:     "gist",
				ExcludeElements: "room_id WITH =, during WITH &&",
			},
			{
				StructName:      "Product",
				Name:            "positive_price",
				Type:            "CHECK",
				Table:           "products",
				CheckExpression: "price > 0",
			},
		},
	}

	database := &catalog.Database{
		// Empty database - no existing constraints
	}

	diff := schemadiff.Compare(desired, database)

	// Verify that both constraints are detected as additions
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 2)
	c.Assert(diff.ConstraintsAdded.Names(), qt.Contains, "no_overlapping_bookings")
	c.Assert(diff.ConstraintsAdded.Names(), qt.Contains, "positive_price")

	// Verify that HasChanges returns true
	c.Assert(diff.HasChanges(), qt.IsTrue)
}

func TestExcludeConstraints_EmptySchema(t *testing.T) {
	c := qt.New(t)

	// Test with empty schemas
	desired := &schemamodel.Database{
		Constraints: make([]schemamodel.Constraint, 0),
	}

	database := &catalog.Database{}

	diff := schemadiff.Compare(desired, database)

	// Verify no changes detected
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
	c.Assert(diff.HasChanges(), qt.IsFalse)

	// Generate migration AST - should be empty
	nodes, err := planner.GenerateSchemaDiffAST(diff, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 0)
}
