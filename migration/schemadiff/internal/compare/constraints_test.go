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
			compare.Constraints(tt.generated, tt.database, diff, nil)

			// Check constraints added
			c.Assert(diff.ConstraintsAdded, qt.HasLen, len(tt.expected.ConstraintsAdded))
			// Check that all expected constraints are present (order doesn't matter)
			for _, expected := range tt.expected.ConstraintsAdded {
				c.Assert(diff.ConstraintsAdded, qt.Contains, expected)
			}

			// Check constraints removed
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, len(tt.expected.ConstraintsRemoved))
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
			name:     "empty diff",
			diff:     &difftypes.SchemaDiff{},
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

	compare.Constraints(generated, database, diff, nil)

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

func TestConstraints_ExcludeConstraintComparison(t *testing.T) {
	tests := []struct {
		name      string
		generated *goschema.Database
		database  *types.DBSchema
		expected  *difftypes.SchemaDiff
	}{
		{
			name: "EXCLUDE constraint added",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
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
				Constraints: []types.DBConstraint{},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"no_overlapping_bookings"},
			},
		},
		{
			name: "EXCLUDE constraint removed",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{},
			},
			database: &types.DBSchema{
				Constraints: []types.DBConstraint{
					{
						Name:            "no_overlapping_bookings",
						TableName:       "bookings",
						Type:            "EXCLUDE",
						UsingMethod:     new("gist"),
						ExcludeElements: new("room_id WITH =, during WITH &&"),
						WhereCondition:  new("is_active = true"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsRemoved: []string{"no_overlapping_bookings"},
			},
		},
		{
			name: "EXCLUDE constraint modified - using method changed",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						Name:            "no_overlapping_bookings",
						Type:            "EXCLUDE",
						Table:           "bookings",
						UsingMethod:     "btree",
						ExcludeElements: "room_id WITH =, during WITH &&",
						WhereCondition:  "is_active = true",
					},
				},
			},
			database: &types.DBSchema{
				Constraints: []types.DBConstraint{
					{
						Name:            "no_overlapping_bookings",
						TableName:       "bookings",
						Type:            "EXCLUDE",
						UsingMethod:     new("gist"),
						ExcludeElements: new("room_id WITH =, during WITH &&"),
						WhereCondition:  new("is_active = true"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsRemoved: []string{"no_overlapping_bookings"},
				ConstraintsAdded:   []string{"no_overlapping_bookings"},
			},
		},
		{
			name: "EXCLUDE constraint modified - elements changed",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						Name:            "no_overlapping_bookings",
						Type:            "EXCLUDE",
						Table:           "bookings",
						UsingMethod:     "gist",
						ExcludeElements: "room_id WITH =, time_range WITH &&",
						WhereCondition:  "is_active = true",
					},
				},
			},
			database: &types.DBSchema{
				Constraints: []types.DBConstraint{
					{
						Name:            "no_overlapping_bookings",
						TableName:       "bookings",
						Type:            "EXCLUDE",
						UsingMethod:     new("gist"),
						ExcludeElements: new("room_id WITH =, during WITH &&"),
						WhereCondition:  new("is_active = true"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsRemoved: []string{"no_overlapping_bookings"},
				ConstraintsAdded:   []string{"no_overlapping_bookings"},
			},
		},
		{
			name: "EXCLUDE constraint modified - WHERE condition changed",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						Name:            "no_overlapping_bookings",
						Type:            "EXCLUDE",
						Table:           "bookings",
						UsingMethod:     "gist",
						ExcludeElements: "room_id WITH =, during WITH &&",
						WhereCondition:  "status = 'confirmed'",
					},
				},
			},
			database: &types.DBSchema{
				Constraints: []types.DBConstraint{
					{
						Name:            "no_overlapping_bookings",
						TableName:       "bookings",
						Type:            "EXCLUDE",
						UsingMethod:     new("gist"),
						ExcludeElements: new("room_id WITH =, during WITH &&"),
						WhereCondition:  new("is_active = true"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsRemoved: []string{"no_overlapping_bookings"},
				ConstraintsAdded:   []string{"no_overlapping_bookings"},
			},
		},
		{
			name: "EXCLUDE constraint unchanged",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
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
				Constraints: []types.DBConstraint{
					{
						Name:            "no_overlapping_bookings",
						TableName:       "bookings",
						Type:            "EXCLUDE",
						UsingMethod:     new("gist"),
						ExcludeElements: new("room_id WITH =, during WITH &&"),
						WhereCondition:  new("is_active = true"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{},
				ConstraintsRemoved: []string{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.Constraints(tt.generated, tt.database, diff, nil)

			// Check constraints added
			c.Assert(diff.ConstraintsAdded, qt.HasLen, len(tt.expected.ConstraintsAdded))
			for _, expected := range tt.expected.ConstraintsAdded {
				c.Assert(diff.ConstraintsAdded, qt.Contains, expected)
			}

			// Check constraints removed
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, len(tt.expected.ConstraintsRemoved))
			for _, expected := range tt.expected.ConstraintsRemoved {
				c.Assert(diff.ConstraintsRemoved, qt.Contains, expected)
			}
		})
	}
}

// TestConstraints_FieldLevelCheck covers issue #112 — column-level `check=`
// annotations need to participate in drift detection. The compare layer
// synthesizes goschema.Constraint entries from field.Check for columns that
// already exist in the introspected database, so add/remove/modify all run
// through the standard Constraints() diff path.
func TestConstraints_FieldLevelCheck(t *testing.T) {
	// Shared setup: a "files" table with one existing column "category".
	filesTable := types.DBTable{
		Name:    "files",
		Columns: []types.DBColumn{{Name: "category"}},
	}

	tests := []struct {
		name      string
		generated *goschema.Database
		database  *types.DBSchema
		expected  *difftypes.SchemaDiff
	}{
		{
			name: "field-level CHECK added on existing column",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "File", Name: "files"}},
				Fields: []goschema.Field{
					{
						StructName: "File",
						Name:       "category",
						Type:       "TEXT",
						Check:      "category IN ('a','b')",
					},
				},
			},
			database: &types.DBSchema{
				Tables: []types.DBTable{filesTable},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"files_category_check"},
			},
		},
		{
			// Realistic introspection shape: PostgreSQL stores the clause
			// as the parser/rewriter produced it, NOT as the user wrote
			// it. The user authored `category IN ('a','b')`; what comes
			// back is roughly `((category)::text = ANY ((ARRAY['a'::text,
			// 'b'::text])::text[]))`. The diff must STILL be empty —
			// that's the whole point of the trust-name v1 contract. If a
			// future regression reintroduces an expression compare here,
			// this case will start failing.
			name: "field-level CHECK matches existing — no diff (idempotency, Postgres-normalized clause)",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "File", Name: "files"}},
				Fields: []goschema.Field{
					{
						StructName: "File",
						Name:       "category",
						Type:       "TEXT",
						Check:      "category IN ('a','b')",
					},
				},
			},
			database: &types.DBSchema{
				Tables: []types.DBTable{filesTable},
				Constraints: []types.DBConstraint{
					{
						Name:        "files_category_check",
						TableName:   "files",
						Type:        "CHECK",
						CheckClause: new("((category)::text = ANY ((ARRAY['a'::text, 'b'::text])::text[]))"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			// v1 trade-off documented on checkConstraintChanged: an
			// expression-only change with the same constraint name does
			// NOT trigger a migration, because PostgreSQL normalizes the
			// stored clause (parens / casts / `IN (...)` → `= ANY
			// (ARRAY[...])`) and a literal string compare would otherwise
			// produce a permanent drift loop on every run. Users who need
			// to force a regen should change `check_name=` alongside the
			// expression — covered by the next case below.
			name: "field-level CHECK expression-only change is intentionally not surfaced (v1)",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "File", Name: "files"}},
				Fields: []goschema.Field{
					{
						StructName: "File",
						Name:       "category",
						Type:       "TEXT",
						Check:      "category IN ('a','b','c')",
					},
				},
			},
			database: &types.DBSchema{
				Tables: []types.DBTable{filesTable},
				Constraints: []types.DBConstraint{
					{
						Name:        "files_category_check",
						TableName:   "files",
						Type:        "CHECK",
						CheckClause: new("category IN ('a','b')"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			// Renaming the constraint via `check_name=` while keeping the
			// expression IS observable: the diff drops the old-named DB
			// constraint and adds the renamed synthesized one. This is
			// the documented escape hatch for forcing an expression change.
			name: "field-level CHECK rename via check_name → drop + add",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "File", Name: "files"}},
				Fields: []goschema.Field{
					{
						StructName: "File",
						Name:       "category",
						Type:       "TEXT",
						Check:      "category IN ('a','b','c')",
						CheckName:  "files_category_v2",
					},
				},
			},
			database: &types.DBSchema{
				Tables: []types.DBTable{filesTable},
				Constraints: []types.DBConstraint{
					{
						Name:        "files_category_check",
						TableName:   "files",
						Type:        "CHECK",
						CheckClause: new("category IN ('a','b')"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"files_category_v2"},
				ConstraintsRemoved: []string{"files_category_check"},
			},
		},
		{
			name: "field-level CHECK removed from annotation → drop existing",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "File", Name: "files"}},
				Fields: []goschema.Field{
					{StructName: "File", Name: "category", Type: "TEXT"},
				},
			},
			database: &types.DBSchema{
				Tables: []types.DBTable{filesTable},
				Constraints: []types.DBConstraint{
					{
						Name:        "files_category_check",
						TableName:   "files",
						Type:        "CHECK",
						CheckClause: new("category IN ('a','b')"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsRemoved: []string{"files_category_check"},
			},
		},
		{
			name: "explicit check_name overrides deterministic name",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "File", Name: "files"}},
				Fields: []goschema.Field{
					{
						StructName: "File",
						Name:       "category",
						Type:       "TEXT",
						Check:      "category IN ('a','b')",
						CheckName:  "files_category_valid",
					},
				},
			},
			database: &types.DBSchema{
				Tables: []types.DBTable{filesTable},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"files_category_valid"},
			},
		},
		{
			name: "field-level CHECK on column not yet in DB → no synthesized constraint (handled inline by CREATE/ADD COLUMN)",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "File", Name: "files"}},
				Fields: []goschema.Field{
					{
						StructName: "File",
						Name:       "new_column",
						Type:       "TEXT",
						Check:      "new_column IN ('x','y')",
					},
				},
			},
			database: &types.DBSchema{
				Tables: []types.DBTable{filesTable},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "NOT NULL CHECK (internal Postgres representation) is not touched by field-level CHECK synthesis",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "File", Name: "files"}},
				Fields: []goschema.Field{
					{StructName: "File", Name: "category", Type: "TEXT", Nullable: false},
				},
			},
			database: &types.DBSchema{
				Tables: []types.DBTable{filesTable},
				Constraints: []types.DBConstraint{
					{
						Name:      "2200_files_category_not_null",
						TableName: "files",
						Type:      "CHECK",
					},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.Constraints(tt.generated, tt.database, diff, nil)

			c.Assert(diff.ConstraintsAdded, qt.HasLen, len(tt.expected.ConstraintsAdded),
				qt.Commentf("ConstraintsAdded=%v", diff.ConstraintsAdded))
			for _, expected := range tt.expected.ConstraintsAdded {
				c.Assert(diff.ConstraintsAdded, qt.Contains, expected)
			}

			c.Assert(diff.ConstraintsRemoved, qt.HasLen, len(tt.expected.ConstraintsRemoved),
				qt.Commentf("ConstraintsRemoved=%v", diff.ConstraintsRemoved))
			for _, expected := range tt.expected.ConstraintsRemoved {
				c.Assert(diff.ConstraintsRemoved, qt.Contains, expected)
			}
		})
	}
}
