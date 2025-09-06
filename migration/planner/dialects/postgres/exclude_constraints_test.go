package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_GenerateMigrationAST_ConstraintsAdded(t *testing.T) {
	tests := []struct {
		name        string
		diff        *types.SchemaDiff
		generated   *goschema.Database
		expectedSQL []string
	}{
		{
			name: "EXCLUDE constraint added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"no_overlapping_bookings"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Booking",
						Name:            "no_overlapping_bookings",
						Type:            "EXCLUDE",
						Table:           "bookings",
						UsingMethod:     "gist",
						ExcludeElements: "room_id WITH =, during WITH &&",
						WhereCondition:  "is_active = true",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE bookings ADD CONSTRAINT no_overlapping_bookings EXCLUDE USING gist (room_id WITH =, during WITH &&) WHERE (is_active = true);",
			},
		},
		{
			name: "EXCLUDE constraint without WHERE clause",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"unique_locations"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Location",
						Name:            "unique_locations",
						Type:            "EXCLUDE",
						Table:           "locations",
						UsingMethod:     "gist",
						ExcludeElements: "location WITH &&",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE locations ADD CONSTRAINT unique_locations EXCLUDE USING gist (location WITH &&);",
			},
		},
		{
			name: "CHECK constraint added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"positive_price"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Product",
						Name:            "positive_price",
						Type:            "CHECK",
						Table:           "products",
						CheckExpression: "price > 0",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE products ADD CONSTRAINT positive_price CHECK (price > 0);",
			},
		},
		{
			name: "UNIQUE constraint added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"unique_user_email"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName: "User",
						Name:       "unique_user_email",
						Type:       "UNIQUE",
						Table:      "users",
						Columns:    []string{"user_id", "email"},
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE users ADD CONSTRAINT unique_user_email UNIQUE (user_id, email);",
			},
		},
		{
			name: "FOREIGN KEY constraint added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"fk_user"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:    "Order",
						Name:          "fk_user",
						Type:          "FOREIGN KEY",
						Table:         "orders",
						Columns:       []string{"user_id"},
						ForeignTable:  "users",
						ForeignColumn: "id",
						OnDelete:      "CASCADE",
					},
				},
			},
			expectedSQL: []string{
				"ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;",
			},
		},
		{
			name: "Multiple constraints added",
			diff: &types.SchemaDiff{
				ConstraintsAdded: []string{"no_overlapping_bookings", "positive_price"},
			},
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
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
			},
			expectedSQL: []string{
				"ALTER TABLE bookings ADD CONSTRAINT no_overlapping_bookings EXCLUDE USING gist (room_id WITH =, during WITH &&);",
				"ALTER TABLE products ADD CONSTRAINT positive_price CHECK (price > 0);",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			planner := postgres.New()
			nodes := planner.GenerateMigrationAST(tt.diff, tt.generated)

			// Convert AST nodes to SQL for verification
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)

			// Remove header comments and split into statements
			lines := strings.Split(sql, "\n")
			var sqlStatements []string
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" && !strings.HasPrefix(line, "--") {
					sqlStatements = append(sqlStatements, line)
				}
			}

			c.Assert(len(sqlStatements), qt.Equals, len(tt.expectedSQL))
			for i, expected := range tt.expectedSQL {
				c.Assert(sqlStatements[i], qt.Equals, expected)
			}
		})
	}
}

func TestPlanner_GenerateMigrationAST_ConstraintsRemoved(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		ConstraintsRemoved: []string{"old_constraint"},
	}
	generated := &goschema.Database{}

	planner := postgres.New()
	nodes := planner.GenerateMigrationAST(diff, generated)

	// Should generate 4 nodes: create function, execute function, drop main function, drop exec function
	c.Assert(len(nodes), qt.Equals, 4)

	// Check the first node (create function)
	sql1, err := renderer.RenderSQL("postgres", nodes[0])
	c.Assert(err, qt.IsNil)
	c.Assert(sql1, qt.Contains, "CREATE OR REPLACE FUNCTION ptah_drop_constraint_old_constraint()")
	c.Assert(sql1, qt.Contains, "information_schema.table_constraints")
	c.Assert(sql1, qt.Contains, "WHERE constraint_name = 'old_constraint'")

	// Check the second node (execute function)
	sql2, err := renderer.RenderSQL("postgres", nodes[1])
	c.Assert(err, qt.IsNil)
	c.Assert(sql2, qt.Contains, "SELECT ptah_drop_constraint_old_constraint();")

	// Check the third node (drop main function)
	sql3, err := renderer.RenderSQL("postgres", nodes[2])
	c.Assert(err, qt.IsNil)
	c.Assert(sql3, qt.Contains, "DROP FUNCTION IF EXISTS ptah_drop_constraint_old_constraint")

	// Check the fourth node (drop exec function)
	sql4, err := renderer.RenderSQL("postgres", nodes[3])
	c.Assert(err, qt.IsNil)
	c.Assert(sql4, qt.Contains, "DROP FUNCTION IF EXISTS ptah_exec_ptah_drop_constraint_old_constraint")
}
