package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/constraintscope"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

func TestConstraints(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
		expected *difftypes.SchemaDiff
	}{
		{
			name: "new EXCLUDE constraint added",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
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
			database: &catalog.Database{
				// Empty database - no existing constraints
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "no_overlapping_bookings"}},
			},
		},
		{
			name: "multiple constraints added",
			desired: &schemamodel.Database{
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
				// Empty database - no existing constraints
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "no_overlapping_bookings"}, {Name: "positive_price"}, {Name: "unique_user_email"}},
			},
		},
		{
			name: "no constraints in either schema",
			desired: &schemamodel.Database{
				Constraints: make([]schemamodel.Constraint, 0),
			},
			database: &catalog.Database{
				// Empty database - no existing constraints
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   difftypes.ConstraintAdditions{},
				ConstraintsRemoved: difftypes.ConstraintRemovals{},
			},
		},
		{
			name: "CHECK constraint added",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
					{
						StructName:      "Product",
						Name:            "positive_price",
						Type:            "CHECK",
						Table:           "products",
						CheckExpression: "price > 0",
					},
				},
			},
			database: &catalog.Database{
				// Empty database - no existing constraints
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "positive_price"}},
			},
		},
		{
			name: "CHECK constraint modified",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
					{
						StructName:      "Product",
						Name:            "positive_price",
						Type:            "CHECK",
						Table:           "products",
						CheckExpression: "price >= 0",
					},
				},
			},
			database: &catalog.Database{
				Constraints: []catalog.Constraint{
					{
						Name:        "positive_price",
						TableName:   "products",
						Type:        "CHECK",
						CheckClause: new("price > 0"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "positive_price"}},
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "positive_price"}},
			},
		},
		{
			name: "CHECK constraint semantic parentheses are preserved",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
					{
						StructName:      "Invoice",
						Name:            "positive_balance",
						Type:            "CHECK",
						Table:           "invoices",
						CheckExpression: "amount - (discount - fee) > 0",
					},
				},
			},
			database: &catalog.Database{
				Constraints: []catalog.Constraint{
					{
						Name:        "positive_balance",
						TableName:   "invoices",
						Type:        "CHECK",
						CheckClause: new("amount - discount - fee > 0"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "positive_balance"}},
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "positive_balance"}},
			},
		},
		{
			name: "CHECK constraint explicit column cast is preserved",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
					{
						StructName:      "Invoice",
						Name:            "positive_amount",
						Type:            "CHECK",
						Table:           "invoices",
						CheckExpression: "amount::numeric > 0",
					},
				},
			},
			database: &catalog.Database{
				Constraints: []catalog.Constraint{
					{
						Name:        "positive_amount",
						TableName:   "invoices",
						Type:        "CHECK",
						CheckClause: new("amount > 0"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "positive_amount"}},
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "positive_amount"}},
			},
		},
		{
			name: "CHECK constraint MySQL quoted identifier is equivalent",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
					{
						StructName:      "Product",
						Name:            "products_quantity_check",
						Type:            "CHECK",
						Table:           "products",
						CheckExpression: "quantity > 0",
					},
				},
			},
			database: &catalog.Database{
				Constraints: []catalog.Constraint{
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
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
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
				// Empty database - no existing constraints
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "unique_user_email"}},
			},
		},
		{
			name: "UNIQUE constraint column set changed",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
					{
						StructName: "User",
						Name:       "unique_user_email",
						Type:       "UNIQUE",
						Table:      "users",
						Columns:    []string{"user_id", "email", "tenant_id"},
					},
				},
			},
			database: &catalog.Database{
				Constraints: []catalog.Constraint{
					{
						Name:        "unique_user_email",
						TableName:   "users",
						Type:        "UNIQUE",
						ColumnNames: []string{"user_id", "email"},
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "unique_user_email"}},
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "unique_user_email"}},
			},
		},
		{
			name: "UNIQUE constraint column order changed",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
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
				Constraints: []catalog.Constraint{
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
			desired: func() *schemamodel.Database {
				nullsDistinct := false
				return &schemamodel.Database{
					Constraints: []schemamodel.Constraint{
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
			database: &catalog.Database{
				Constraints: []catalog.Constraint{
					{
						Name:        "users_c_key",
						TableName:   "users",
						Type:        "UNIQUE",
						ColumnNames: []string{"c"},
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "users_c_key"}},
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "users_c_key"}},
			},
		},
		{
			name: "FOREIGN KEY constraint added",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
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
			database: &catalog.Database{
				// Empty database - no existing constraints
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "fk_user"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.Constraints(tt.desired, tt.database, diff, nil)

			// The rows state the constraints by name; the records they resolve
			// to are asserted by the tests that are about the body.
			c.Assert(diff.ConstraintsAdded, qt.HasLen, len(tt.expected.ConstraintsAdded))
			for _, expected := range tt.expected.ConstraintsAdded {
				c.Assert(diff.ConstraintsAdded.Names(), qt.Contains, expected.Name)
			}

			c.Assert(diff.ConstraintsRemoved, qt.HasLen, len(tt.expected.ConstraintsRemoved))
			for _, expected := range tt.expected.ConstraintsRemoved {
				c.Assert(diff.ConstraintsRemoved.Names(), qt.Contains, expected.Name)
			}
		})
	}
}

func TestConstraints_SameNameTypeDriftCarriesAdditionBody(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
		expected difftypes.ConstraintAdditions
	}{
		{
			name: "CHECK to UNIQUE",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{{
					StructName: "Account",
					Name:       "accounts_identity",
					Type:       "UNIQUE",
					Table:      "accounts",
					Columns:    []string{"email", "region"},
				}},
			},
			database: &catalog.Database{
				Constraints: []catalog.Constraint{{
					Name:        "accounts_identity",
					TableName:   "accounts",
					Type:        "CHECK",
					CheckClause: new("email <> ''"),
				}},
			},
			expected: []difftypes.ConstraintAdditionInfo{{
				Name:      "accounts_identity",
				TableName: "accounts",
				Identity:  difftypes.ConstraintIdentity{Table: "accounts", Name: "accounts_identity"},
				Type:      "UNIQUE",
				Columns:   []string{"email", "region"},
			}},
		},
		{
			name: "UNIQUE to CHECK",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{{
					StructName:      "Product",
					Name:            "products_quantity_guard",
					Type:            "CHECK",
					Table:           "products",
					CheckExpression: "quantity > 10",
				}},
			},
			database: &catalog.Database{
				Constraints: []catalog.Constraint{{
					Name:        "products_quantity_guard",
					TableName:   "products",
					Type:        "UNIQUE",
					ColumnNames: []string{"quantity"},
				}},
			},
			expected: []difftypes.ConstraintAdditionInfo{{
				Name:            "products_quantity_guard",
				TableName:       "products",
				Identity:        difftypes.ConstraintIdentity{Table: "products", Name: "products_quantity_guard"},
				Type:            "CHECK",
				CheckExpression: "quantity > 10",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.Constraints(tt.desired, tt.database, diff, nil)

			c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, []string{tt.expected[0].Name})
			c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, []string{tt.expected[0].Name})
			c.Assert(diff.ConstraintsAdded, qt.DeepEquals, tt.expected)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 1)
			c.Assert(diff.ConstraintsRemoved[0].Name, qt.Equals, tt.expected[0].Name)
			c.Assert(diff.ConstraintsRemoved[0].TableName, qt.Equals, tt.expected[0].TableName)
		})
	}
}

func TestConstraints_UniqueIncludeDrift(t *testing.T) {
	tests := []struct {
		name     string
		desired  schemamodel.Constraint
		database catalog.Constraint
	}{
		{
			name: "missing include column",
			desired: schemamodel.Constraint{
				StructName:     "Account",
				Name:           "accounts_email_unique",
				Type:           "UNIQUE",
				Table:          "accounts",
				Columns:        []string{"email"},
				IncludeColumns: []string{"updated_at"},
			},
			database: catalog.Constraint{
				Name:        "accounts_email_unique",
				TableName:   "accounts",
				Type:        "UNIQUE",
				ColumnNames: []string{"email"},
			},
		},
		{
			name: "changed include column",
			desired: schemamodel.Constraint{
				StructName:     "Account",
				Name:           "accounts_email_unique",
				Type:           "UNIQUE",
				Table:          "accounts",
				Columns:        []string{"email"},
				IncludeColumns: []string{"updated_at"},
			},
			database: catalog.Constraint{
				Name:           "accounts_email_unique",
				TableName:      "accounts",
				Type:           "UNIQUE",
				ColumnNames:    []string{"email"},
				IncludeColumns: []string{"deleted_at"},
			},
		},
		{
			name: "extra include column",
			desired: schemamodel.Constraint{
				StructName: "Account",
				Name:       "accounts_email_unique",
				Type:       "UNIQUE",
				Table:      "accounts",
				Columns:    []string{"email"},
			},
			database: catalog.Constraint{
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
			desired := &schemamodel.Database{Constraints: []schemamodel.Constraint{tt.desired}}
			database := &catalog.Database{Constraints: []catalog.Constraint{tt.database}}

			diff := &difftypes.SchemaDiff{}
			compare.Constraints(desired, database, diff, nil)

			c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, difftypes.ConstraintRemovals{{
				Name:      tt.desired.Name,
				TableName: tt.desired.Table,
				Identity:  difftypes.ConstraintIdentity{Table: tt.desired.Table, Name: tt.desired.Name},
				Type:      "UNIQUE",
			}})
			c.Assert(diff.ConstraintsAdded, qt.DeepEquals, difftypes.ConstraintAdditions{{
				Name:           tt.desired.Name,
				TableName:      tt.desired.Table,
				Identity:       difftypes.ConstraintIdentity{Table: tt.desired.Table, Name: tt.desired.Name},
				Type:           "UNIQUE",
				Columns:        append([]string(nil), tt.desired.Columns...),
				IncludeColumns: append([]string(nil), tt.desired.IncludeColumns...),
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
				ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "no_overlapping_bookings"}},
			},
			expected: true,
		},
		{
			name: "has constraint removals",
			diff: &difftypes.SchemaDiff{
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "old_constraint"}},
			},
			expected: true,
		},
		{
			name: "has both constraint additions and removals",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "new_constraint"}},
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "old_constraint"}},
			},
			expected: true,
		},
		{
			name: "no constraint changes",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded:   difftypes.ConstraintAdditions{},
				ConstraintsRemoved: difftypes.ConstraintRemovals{},
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

	desired := &schemamodel.Database{
		Constraints: []schemamodel.Constraint{
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
	compare.Constraints(desired, &catalog.Database{}, diff, nil)

	c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, []string{"fk_orders_accounts"})
	c.Assert(diff.ConstraintsAdded, qt.DeepEquals, difftypes.ConstraintAdditions{
		{
			Name:           "fk_orders_accounts",
			TableName:      "orders",
			Identity:       difftypes.ConstraintIdentity{Table: "orders", Name: "fk_orders_accounts"},
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

	desired := &schemamodel.Database{
		Constraints: []schemamodel.Constraint{
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
	database := &catalog.Database{
		Constraints: []catalog.Constraint{
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
	compare.Constraints(desired, database, diff, nil)

	c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, difftypes.ConstraintRemovals{
		{Name: "fk_orders_accounts", TableName: "orders", Type: "FOREIGN KEY", Identity: difftypes.ConstraintIdentity{Table: "orders", Name: "fk_orders_accounts"}},
	})
	c.Assert(diff.ForeignKeysRemovedWithTables, qt.DeepEquals, []difftypes.ForeignKeyRemovalInfo{
		{
			Name: "fk_orders_accounts", TableName: "orders",
			Identity: constraintscope.Identity(identifier.Semantics{}, "orders", "fk_orders_accounts"),
			Columns:  []string{"tenant_id", "owner_id"}, ForeignTable: "accounts",
			ForeignColumns: []string{"tenant_id", "account_id"},
		},
	})
	c.Assert(diff.ConstraintsAdded, qt.DeepEquals, difftypes.ConstraintAdditions{
		{
			Name:           "fk_orders_accounts",
			TableName:      "orders",
			Identity:       difftypes.ConstraintIdentity{Table: "orders", Name: "fk_orders_accounts"},
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

	desired := &schemamodel.Database{
		Constraints: []schemamodel.Constraint{
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
	database := &catalog.Database{
		Constraints: []catalog.Constraint{
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
	compare.Constraints(desired, database, diff, nil)

	c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, difftypes.ConstraintRemovals{
		{Name: "fk_orders_accounts", TableName: "orders", Type: "FOREIGN KEY", Identity: difftypes.ConstraintIdentity{Table: "orders", Name: "fk_orders_accounts"}},
	})
	c.Assert(diff.ForeignKeysRemovedWithTables, qt.DeepEquals, []difftypes.ForeignKeyRemovalInfo{
		{
			Name: "fk_orders_accounts", TableName: "orders",
			Identity: constraintscope.Identity(identifier.Semantics{}, "orders", "fk_orders_accounts"),
			Columns:  []string{"tenant_id", "account_owner_id"}, ForeignTable: "accounts",
			ForeignColumns: []string{"tenant_id", "id"},
		},
	})
	c.Assert(diff.ConstraintsAdded, qt.DeepEquals, difftypes.ConstraintAdditions{
		{
			Name:           "fk_orders_accounts",
			TableName:      "orders",
			Identity:       difftypes.ConstraintIdentity{Table: "orders", Name: "fk_orders_accounts"},
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
	desired := &schemamodel.Database{
		Constraints: nil,
	}
	database := &catalog.Database{}
	diff := &difftypes.SchemaDiff{}

	compare.Constraints(desired, database, diff, nil)

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

func TestConstraints_ExcludeConstraintComparison(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
		expected *difftypes.SchemaDiff
	}{
		{
			name: "EXCLUDE constraint added",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
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
			database: &catalog.Database{
				Constraints: make([]catalog.Constraint, 0),
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "no_overlapping_bookings"}},
			},
		},
		{
			name: "EXCLUDE constraint removed",
			desired: &schemamodel.Database{
				Constraints: make([]schemamodel.Constraint, 0),
			},
			database: &catalog.Database{
				Constraints: []catalog.Constraint{
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
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "no_overlapping_bookings"}},
			},
		},
		{
			name: "EXCLUDE constraint modified - using method changed",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
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
			database: &catalog.Database{
				Constraints: []catalog.Constraint{
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
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "no_overlapping_bookings"}},
				ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "no_overlapping_bookings"}},
			},
		},
		{
			name: "EXCLUDE constraint modified - elements changed",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
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
			database: &catalog.Database{
				Constraints: []catalog.Constraint{
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
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "no_overlapping_bookings"}},
				ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "no_overlapping_bookings"}},
			},
		},
		{
			name: "EXCLUDE constraint modified - WHERE condition changed",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
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
			database: &catalog.Database{
				Constraints: []catalog.Constraint{
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
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "no_overlapping_bookings"}},
				ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "no_overlapping_bookings"}},
			},
		},
		{
			name: "EXCLUDE constraint unchanged",
			desired: &schemamodel.Database{
				Constraints: []schemamodel.Constraint{
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
			database: &catalog.Database{
				Constraints: []catalog.Constraint{
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
				ConstraintsAdded:   difftypes.ConstraintAdditions{},
				ConstraintsRemoved: difftypes.ConstraintRemovals{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := &difftypes.SchemaDiff{}
			compare.Constraints(tt.desired, tt.database, diff, nil)

			// Check constraints added
			c.Assert(diff.ConstraintsAdded, qt.HasLen, len(tt.expected.ConstraintsAdded))
			for _, expected := range tt.expected.ConstraintsAdded {
				c.Assert(diff.ConstraintsAdded.Names(), qt.Contains, expected.Name)
			}

			// Check constraints removed
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, len(tt.expected.ConstraintsRemoved))
			for _, expected := range tt.expected.ConstraintsRemoved {
				c.Assert(diff.ConstraintsRemoved.Names(), qt.Contains, expected.Name)
			}
		})
	}
}

// TestConstraints_FieldLevelCheck covers issue #112 — column-level `check=`
// annotations need to participate in drift detection. The compare layer
// synthesizes schemamodel.Constraint entries from field.Check for columns that
// already exist in the introspected database, so add/remove/modify all run
// through the standard Constraints() diff path.
func TestConstraints_FieldLevelCheck(t *testing.T) {
	// Shared setup: a "files" table with one existing column "category".
	filesTable := catalog.Table{
		Name:    "files",
		Columns: []catalog.Column{{Name: "category"}},
	}

	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
		expected *difftypes.SchemaDiff
	}{
		{
			name: "field-level CHECK added on existing column",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "File", Name: "files"}},
				Fields: []schemamodel.Field{
					{
						StructName: "File",
						Name:       "category",
						Type:       "TEXT",
						Check:      "category IN ('a','b')",
					},
				},
			},
			database: &catalog.Database{
				Tables: []catalog.Table{filesTable},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "files_category_check"}},
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
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "File", Name: "files"}},
				Fields: []schemamodel.Field{
					{
						StructName: "File",
						Name:       "category",
						Type:       "TEXT",
						Check:      "category IN('a','b')",
					},
				},
			},
			database: &catalog.Database{
				Tables: []catalog.Table{filesTable},
				Constraints: []catalog.Constraint{
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
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "File", Name: "files"}},
				Fields: []schemamodel.Field{
					{
						StructName: "File",
						Name:       "category",
						Type:       "TEXT",
						Check:      "category IN ('a','b','c')",
					},
				},
			},
			database: &catalog.Database{
				Tables: []catalog.Table{filesTable},
				Constraints: []catalog.Constraint{
					{
						Name:        "files_category_check",
						TableName:   "files",
						Type:        "CHECK",
						CheckClause: new("category IN ('a','b')"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "files_category_check"}},
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "files_category_check"}},
			},
		},
		{
			// Renaming the constraint via `check_name=` while keeping the
			// expression IS observable: the diff drops the old-named DB
			// constraint and adds the renamed synthesized one. This is
			// the documented escape hatch for forcing an expression change.
			name: "field-level CHECK rename via check_name → drop + add",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "File", Name: "files"}},
				Fields: []schemamodel.Field{
					{
						StructName: "File",
						Name:       "category",
						Type:       "TEXT",
						Check:      "category IN ('a','b','c')",
						CheckName:  "files_category_v2",
					},
				},
			},
			database: &catalog.Database{
				Tables: []catalog.Table{filesTable},
				Constraints: []catalog.Constraint{
					{
						Name:        "files_category_check",
						TableName:   "files",
						Type:        "CHECK",
						CheckClause: new("category IN ('a','b')"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded:   difftypes.ConstraintAdditions{{Name: "files_category_v2"}},
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "files_category_check"}},
			},
		},
		{
			name: "field-level CHECK removed from annotation → drop existing",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "File", Name: "files"}},
				Fields: []schemamodel.Field{
					{StructName: "File", Name: "category", Type: "TEXT"},
				},
			},
			database: &catalog.Database{
				Tables: []catalog.Table{filesTable},
				Constraints: []catalog.Constraint{
					{
						Name:        "files_category_check",
						TableName:   "files",
						Type:        "CHECK",
						CheckClause: new("category IN ('a','b')"),
					},
				},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: "files_category_check"}},
			},
		},
		{
			name: "explicit check_name overrides deterministic name",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "File", Name: "files"}},
				Fields: []schemamodel.Field{
					{
						StructName: "File",
						Name:       "category",
						Type:       "TEXT",
						Check:      "category IN ('a','b')",
						CheckName:  "files_category_valid",
					},
				},
			},
			database: &catalog.Database{
				Tables: []catalog.Table{filesTable},
			},
			expected: &difftypes.SchemaDiff{
				ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "files_category_valid"}},
			},
		},
		{
			name: "field-level CHECK on column not yet in DB → no synthesized constraint (handled inline by CREATE/ADD COLUMN)",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "File", Name: "files"}},
				Fields: []schemamodel.Field{
					{
						StructName: "File",
						Name:       "new_column",
						Type:       "TEXT",
						Check:      "new_column IN ('x','y')",
					},
				},
			},
			database: &catalog.Database{
				Tables: []catalog.Table{filesTable},
			},
			expected: &difftypes.SchemaDiff{},
		},
		{
			name: "NOT NULL CHECK (internal Postgres representation) is not touched by field-level CHECK synthesis",
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "File", Name: "files"}},
				Fields: []schemamodel.Field{
					{StructName: "File", Name: "category", Type: "TEXT", Nullable: false},
				},
			},
			database: &catalog.Database{
				Tables: []catalog.Table{filesTable},
				Constraints: []catalog.Constraint{
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
			compare.Constraints(tt.desired, tt.database, diff, nil)

			c.Assert(diff.ConstraintsAdded, qt.HasLen, len(tt.expected.ConstraintsAdded),
				qt.Commentf("ConstraintsAdded=%v", diff.ConstraintsAdded))
			for _, expected := range tt.expected.ConstraintsAdded {
				c.Assert(diff.ConstraintsAdded.Names(), qt.Contains, expected.Name)
			}

			c.Assert(diff.ConstraintsRemoved, qt.HasLen, len(tt.expected.ConstraintsRemoved),
				qt.Commentf("ConstraintsRemoved=%v", diff.ConstraintsRemoved))
			for _, expected := range tt.expected.ConstraintsRemoved {
				c.Assert(diff.ConstraintsRemoved.Names(), qt.Contains, expected.Name)
			}
		})
	}
}
