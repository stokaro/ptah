package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestConstraints(t *testing.T) {
	tests := []struct {
		name      string
		generated *goschema.Database
		database  *types.DBSchema
		expected  *difftypes.SchemaDiff
	}{
		{
			name: "new EXCLUDE constraint added",
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
			database: &types.DBSchema{
				// Empty database - no existing constraints
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"no_overlapping_bookings"},
			},
		},
		{
			name: "multiple constraints added",
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
					{
						StructName: "User",
						Name:       "unique_user_email",
						Type:       "UNIQUE",
						Table:      "users",
						Columns:    []string{"user_id", "email"},
					},
				},
			},
			database: &types.DBSchema{
				// Empty database - no existing constraints
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"no_overlapping_bookings", "positive_price", "unique_user_email"},
			},
		},
		{
			name: "no constraints in either schema",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{},
			},
			database: &types.DBSchema{
				// Empty database - no existing constraints
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{},
				ConstraintsRemoved: []string{},
			},
		},
		{
			name: "CHECK constraint added",
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
			database: &types.DBSchema{
				// Empty database - no existing constraints
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"positive_price"},
			},
		},
		{
			name: "UNIQUE constraint added",
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
			database: &types.DBSchema{
				// Empty database - no existing constraints
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"unique_user_email"},
			},
		},
		{
			name: "FOREIGN KEY constraint added",
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
			database: &types.DBSchema{
				// Empty database - no existing constraints
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"fk_user"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.Constraints(tt.generated, tt.database, diff)

			// Check constraints added
			c.Assert(len(diff.ConstraintsAdded), qt.Equals, len(tt.expected.ConstraintsAdded))
			// Check that all expected constraints are present (order doesn't matter)
			for _, expected := range tt.expected.ConstraintsAdded {
				c.Assert(diff.ConstraintsAdded, qt.Contains, expected)
			}

			// Check constraints removed
			c.Assert(len(diff.ConstraintsRemoved), qt.Equals, len(tt.expected.ConstraintsRemoved))
			// Check that all expected constraints are present (order doesn't matter)
			for _, expected := range tt.expected.ConstraintsRemoved {
				c.Assert(diff.ConstraintsRemoved, qt.Contains, expected)
			}
		})
	}
}

func TestConstraints_HasChanges(t *testing.T) {
	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		expected bool
	}{
		{
			name: "has constraint additions",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"no_overlapping_bookings"},
			},
			expected: true,
		},
		{
			name: "has constraint removals",
			diff: &difftypes.SchemaDiff{
				ConstraintsRemoved: []string{"old_constraint"},
			},
			expected: true,
		},
		{
			name: "has both constraint additions and removals",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"new_constraint"},
				ConstraintsRemoved: []string{"old_constraint"},
			},
			expected: true,
		},
		{
			name: "no constraint changes",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{},
				ConstraintsRemoved: []string{},
			},
			expected: false,
		},
		{
			name: "empty diff",
			diff: &difftypes.SchemaDiff{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			result := tt.diff.HasChanges()
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

func TestConstraints_EdgeCases(t *testing.T) {
	c := qt.New(t)

	// Test with nil slices
	generated := &goschema.Database{
		Constraints: nil,
	}
	database := &types.DBSchema{}
	diff := &difftypes.SchemaDiff{}

	compare.Constraints(generated, database, diff)

	c.Assert(len(diff.ConstraintsAdded), qt.Equals, 0)
	c.Assert(len(diff.ConstraintsRemoved), qt.Equals, 0)
}
