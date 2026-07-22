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
			name: "CHECK constraint modified",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Product",
						Name:            "positive_price",
						Type:            "CHECK",
						Table:           "products",
						CheckExpression: "price >= 0",
					},
				},
			},
			database: &types.DBSchema{
				Constraints: []types.DBConstraint{
					{
						Name:        "positive_price",
						TableName:   "products",
						Type:        "CHECK",
						CheckClause: new("price > 0"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"positive_price"},
				ConstraintsRemoved: []string{"positive_price"},
			},
		},
		{
			name: "CHECK constraint semantic parentheses are preserved",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Invoice",
						Name:            "positive_balance",
						Type:            "CHECK",
						Table:           "invoices",
						CheckExpression: "amount - (discount - fee) > 0",
					},
				},
			},
			database: &types.DBSchema{
				Constraints: []types.DBConstraint{
					{
						Name:        "positive_balance",
						TableName:   "invoices",
						Type:        "CHECK",
						CheckClause: new("amount - discount - fee > 0"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"positive_balance"},
				ConstraintsRemoved: []string{"positive_balance"},
			},
		},
		{
			name: "CHECK constraint explicit column cast is preserved",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Invoice",
						Name:            "positive_amount",
						Type:            "CHECK",
						Table:           "invoices",
						CheckExpression: "amount::numeric > 0",
					},
				},
			},
			database: &types.DBSchema{
				Constraints: []types.DBConstraint{
					{
						Name:        "positive_amount",
						TableName:   "invoices",
						Type:        "CHECK",
						CheckClause: new("amount > 0"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"positive_amount"},
				ConstraintsRemoved: []string{"positive_amount"},
			},
		},
		{
			name: "CHECK constraint MySQL quoted identifier is equivalent",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName:      "Product",
						Name:            "products_quantity_check",
						Type:            "CHECK",
						Table:           "products",
						CheckExpression: "quantity > 0",
					},
				},
			},
			database: &types.DBSchema{
				Constraints: []types.DBConstraint{
					{
						Name:        "products_quantity_check",
						TableName:   "products",
						Type:        "CHECK",
						CheckClause: new("(`quantity` > 0)"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{},
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
			name: "UNIQUE constraint column set changed",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{
					{
						StructName: "User",
						Name:       "unique_user_email",
						Type:       "UNIQUE",
						Table:      "users",
						Columns:    []string{"user_id", "email", "tenant_id"},
					},
				},
			},
			database: &types.DBSchema{
				Constraints: []types.DBConstraint{
					{
						Name:        "unique_user_email",
						TableName:   "users",
						Type:        "UNIQUE",
						ColumnNames: []string{"user_id", "email"},
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"unique_user_email"},
				ConstraintsRemoved: []string{"unique_user_email"},
			},
		},
		{
			name: "UNIQUE constraint column order changed",
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
				Constraints: []types.DBConstraint{
					{
						Name:        "unique_user_email",
						TableName:   "users",
						Type:        "UNIQUE",
						ColumnNames: []string{"email", "user_id"},
					},
				},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "UNIQUE constraint nulls distinct changed",
			generated: func() *goschema.Database {
				nullsDistinct := false
				return &goschema.Database{
					Constraints: []goschema.Constraint{
						{
							StructName:    "User",
							Name:          "users_c_key",
							Type:          "UNIQUE",
							Table:         "users",
							Columns:       []string{"c"},
							NullsDistinct: &nullsDistinct,
						},
					},
				}
			}(),
			database: &types.DBSchema{
				Constraints: []types.DBConstraint{
					{
						Name:        "users_c_key",
						TableName:   "users",
						Type:        "UNIQUE",
						ColumnNames: []string{"c"},
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"users_c_key"},
				ConstraintsRemoved: []string{"users_c_key"},
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

func TestConstraints_SameNameTypeDriftCarriesAdditionBody(t *testing.T) {
	tests := []struct {
		name      string
		generated *goschema.Database
		database  *types.DBSchema
		expected  []difftypes.ConstraintAdditionInfo
	}{
		{
			name: "CHECK to UNIQUE",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{{
					StructName: "Account",
					Name:       "accounts_identity",
					Type:       "UNIQUE",
					Table:      "accounts",
					Columns:    []string{"email", "region"},
				}},
			},
			database: &types.DBSchema{
				Constraints: []types.DBConstraint{{
					Name:        "accounts_identity",
					TableName:   "accounts",
					Type:        "CHECK",
					CheckClause: new("email <> ''"),
				}},
			},
			expected: []difftypes.ConstraintAdditionInfo{{
				Name:      "accounts_identity",
				TableName: "accounts",
				Type:      "UNIQUE",
				Columns:   []string{"email", "region"},
			}},
		},
		{
			name: "UNIQUE to CHECK",
			generated: &goschema.Database{
				Constraints: []goschema.Constraint{{
					StructName:      "Product",
					Name:            "products_quantity_guard",
					Type:            "CHECK",
					Table:           "products",
					CheckExpression: "quantity > 10",
				}},
			},
			database: &types.DBSchema{
				Constraints: []types.DBConstraint{{
					Name:        "products_quantity_guard",
					TableName:   "products",
					Type:        "UNIQUE",
					ColumnNames: []string{"quantity"},
				}},
			},
			expected: []difftypes.ConstraintAdditionInfo{{
				Name:            "products_quantity_guard",
				TableName:       "products",
				Type:            "CHECK",
				CheckExpression: "quantity > 10",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.Constraints(tt.generated, tt.database, diff, nil)

			c.Assert(diff.ConstraintsAdded, qt.DeepEquals, []string{tt.expected[0].Name})
			c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, []string{tt.expected[0].Name})
			c.Assert(diff.ConstraintsAddedWithTables, qt.DeepEquals, tt.expected)
			c.Assert(diff.ConstraintsRemovedWithTables, qt.HasLen, 1)
			c.Assert(diff.ConstraintsRemovedWithTables[0].Name, qt.Equals, tt.expected[0].Name)
			c.Assert(diff.ConstraintsRemovedWithTables[0].TableName, qt.Equals, tt.expected[0].TableName)
		})
	}
}

func TestConstraints_UniqueIncludeDrift(t *testing.T) {
	tests := []struct {
		name      string
		generated goschema.Constraint
		database  types.DBConstraint
	}{
		{
			name: "missing include column",
			generated: goschema.Constraint{
				StructName:     "Account",
				Name:           "accounts_email_unique",
				Type:           "UNIQUE",
				Table:          "accounts",
				Columns:        []string{"email"},
				IncludeColumns: []string{"updated_at"},
			},
			database: types.DBConstraint{
				Name:        "accounts_email_unique",
				TableName:   "accounts",
				Type:        "UNIQUE",
				ColumnNames: []string{"email"},
			},
		},
		{
			name: "changed include column",
			generated: goschema.Constraint{
				StructName:     "Account",
				Name:           "accounts_email_unique",
				Type:           "UNIQUE",
				Table:          "accounts",
				Columns:        []string{"email"},
				IncludeColumns: []string{"updated_at"},
			},
			database: types.DBConstraint{
				Name:           "accounts_email_unique",
				TableName:      "accounts",
				Type:           "UNIQUE",
				ColumnNames:    []string{"email"},
				IncludeColumns: []string{"deleted_at"},
			},
		},
		{
			name: "extra include column",
			generated: goschema.Constraint{
				StructName: "Account",
				Name:       "accounts_email_unique",
				Type:       "UNIQUE",
				Table:      "accounts",
				Columns:    []string{"email"},
			},
			database: types.DBConstraint{
				Name:           "accounts_email_unique",
				TableName:      "accounts",
				Type:           "UNIQUE",
				ColumnNames:    []string{"email"},
				IncludeColumns: []string{"updated_at"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{Constraints: []goschema.Constraint{tt.generated}}
			database := &types.DBSchema{Constraints: []types.DBConstraint{tt.database}}

			diff := &difftypes.SchemaDiff{}
			compare.Constraints(generated, database, diff, nil)

			c.Assert(diff.ConstraintsRemovedWithTables, qt.DeepEquals, []difftypes.ConstraintRemovalInfo{{
				Name:      tt.generated.Name,
				TableName: tt.generated.Table,
				Type:      "UNIQUE",
			}})
			c.Assert(diff.ConstraintsAddedWithTables, qt.DeepEquals, []difftypes.ConstraintAdditionInfo{{
				Name:           tt.generated.Name,
				TableName:      tt.generated.Table,
				Type:           "UNIQUE",
				Columns:        append([]string(nil), tt.generated.Columns...),
				IncludeColumns: append([]string(nil), tt.generated.IncludeColumns...),
			}})
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

func TestConstraints_CompositeForeignKeyAdditionCarriesReferencedColumns(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Constraints: []goschema.Constraint{
			{
				StructName:     "Order",
				Name:           "fk_orders_accounts",
				Type:           "FOREIGN KEY",
				Table:          "orders",
				Columns:        []string{"tenant_id", "owner_id"},
				ForeignTable:   "accounts",
				ForeignColumn:  "tenant_id",
				ForeignColumns: []string{"tenant_id", "id"},
				OnDelete:       "CASCADE",
			},
		},
	}

	diff := &difftypes.SchemaDiff{}
	compare.Constraints(generated, &types.DBSchema{}, diff, nil)

	c.Assert(diff.ConstraintsAdded, qt.DeepEquals, []string{"fk_orders_accounts"})
	c.Assert(diff.ConstraintsAddedWithTables, qt.DeepEquals, []difftypes.ConstraintAdditionInfo{
		{
			Name:           "fk_orders_accounts",
			TableName:      "orders",
			Type:           "FOREIGN KEY",
			Columns:        []string{"tenant_id", "owner_id"},
			ForeignTable:   "accounts",
			ForeignColumn:  "tenant_id",
			ForeignColumns: []string{"tenant_id", "id"},
			OnDelete:       "CASCADE",
		},
	})
}

func TestConstraints_CompositeForeignKeyReferencedColumnDrift(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Constraints: []goschema.Constraint{
			{
				StructName:     "Order",
				Name:           "fk_orders_accounts",
				Type:           "FOREIGN KEY",
				Table:          "orders",
				Columns:        []string{"tenant_id", "owner_id"},
				ForeignTable:   "accounts",
				ForeignColumn:  "tenant_id",
				ForeignColumns: []string{"tenant_id", "id"},
			},
		},
	}
	database := &types.DBSchema{
		Constraints: []types.DBConstraint{
			{
				Name:           "fk_orders_accounts",
				TableName:      "orders",
				Type:           "FOREIGN KEY",
				ColumnName:     "tenant_id",
				ColumnNames:    []string{"tenant_id", "owner_id"},
				ForeignTable:   new("accounts"),
				ForeignColumn:  new("tenant_id"),
				ForeignColumns: []string{"tenant_id", "account_id"},
				DeleteRule:     new("NO ACTION"),
				UpdateRule:     new("NO ACTION"),
			},
		},
	}

	diff := &difftypes.SchemaDiff{}
	compare.Constraints(generated, database, diff, nil)

	c.Assert(diff.ConstraintsRemovedWithTables, qt.DeepEquals, []difftypes.ConstraintRemovalInfo{
		{Name: "fk_orders_accounts", TableName: "orders", Type: "FOREIGN KEY"},
	})
	c.Assert(diff.ConstraintsAddedWithTables, qt.DeepEquals, []difftypes.ConstraintAdditionInfo{
		{
			Name:           "fk_orders_accounts",
			TableName:      "orders",
			Type:           "FOREIGN KEY",
			Columns:        []string{"tenant_id", "owner_id"},
			ForeignTable:   "accounts",
			ForeignColumn:  "tenant_id",
			ForeignColumns: []string{"tenant_id", "id"},
		},
	})
}

func TestConstraints_CompositeForeignKeyLocalColumnDrift(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Constraints: []goschema.Constraint{
			{
				StructName:     "Order",
				Name:           "fk_orders_accounts",
				Type:           "FOREIGN KEY",
				Table:          "orders",
				Columns:        []string{"tenant_id", "owner_id"},
				ForeignTable:   "accounts",
				ForeignColumn:  "tenant_id",
				ForeignColumns: []string{"tenant_id", "id"},
			},
		},
	}
	database := &types.DBSchema{
		Constraints: []types.DBConstraint{
			{
				Name:           "fk_orders_accounts",
				TableName:      "orders",
				Type:           "FOREIGN KEY",
				ColumnName:     "tenant_id",
				ColumnNames:    []string{"tenant_id", "account_owner_id"},
				ForeignTable:   new("accounts"),
				ForeignColumn:  new("tenant_id"),
				ForeignColumns: []string{"tenant_id", "id"},
				DeleteRule:     new("NO ACTION"),
				UpdateRule:     new("NO ACTION"),
			},
		},
	}

	diff := &difftypes.SchemaDiff{}
	compare.Constraints(generated, database, diff, nil)

	c.Assert(diff.ConstraintsRemovedWithTables, qt.DeepEquals, []difftypes.ConstraintRemovalInfo{
		{Name: "fk_orders_accounts", TableName: "orders", Type: "FOREIGN KEY"},
	})
	c.Assert(diff.ConstraintsAddedWithTables, qt.DeepEquals, []difftypes.ConstraintAdditionInfo{
		{
			Name:           "fk_orders_accounts",
			TableName:      "orders",
			Type:           "FOREIGN KEY",
			Columns:        []string{"tenant_id", "owner_id"},
			ForeignTable:   "accounts",
			ForeignColumn:  "tenant_id",
			ForeignColumns: []string{"tenant_id", "id"},
		},
	})
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
			// it. The user authored `category IN('a','b')`; what comes
			// back is roughly `((category)::text = ANY ((ARRAY['a'::text,
			// 'b'::text])::text[]))`. The comparator deliberately treats
			// that `IN (...)` to `ANY (ARRAY[...])` form as an unsupported
			// rewrite rather than emitting a perpetual drop+add loop.
			name: "field-level CHECK matches existing — no diff (idempotency, Postgres-normalized clause)",
			generated: &goschema.Database{
				Tables: []goschema.Table{{StructName: "File", Name: "files"}},
				Fields: []goschema.Field{
					{
						StructName: "File",
						Name:       "category",
						Type:       "TEXT",
						Check:      "category IN('a','b')",
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
			name: "field-level CHECK expression-only change surfaces as drop + add",
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
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   []string{"files_category_check"},
				ConstraintsRemoved: []string{"files_category_check"},
			},
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
