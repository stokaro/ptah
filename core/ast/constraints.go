package ast

// NewPrimaryKeyConstraint creates a table-level primary key constraint.
//
// This function creates a primary key constraint that spans one or more columns.
// For single-column primary keys, you can also use the SetPrimary() method on
// the column itself.
//
// Example:
//
//	// Single column primary key
//	pk := NewPrimaryKeyConstraint("id")
//	// Composite primary key
//	pk := NewPrimaryKeyConstraint("user_id", "role_id")
func NewPrimaryKeyConstraint(columns ...string) *ConstraintNode {
	return &ConstraintNode{
		Type:    PrimaryKeyConstraint,
		Columns: columns,
	}
}

// NewUniqueConstraint creates a table-level unique constraint with a name.
//
// This function creates a named unique constraint that can span multiple columns.
// The constraint name is useful for referencing the constraint later (e.g., for
// dropping it in migrations).
//
// Example:
//
//	// Single column unique constraint
//	unique := NewUniqueConstraint("uk_users_email", "email")
//	// Multi-column unique constraint
//	unique := NewUniqueConstraint("uk_users_name_company", "name", "company_id")
func NewUniqueConstraint(name string, columns ...string) *ConstraintNode {
	return &ConstraintNode{
		Type:    UniqueConstraint,
		Name:    name,
		Columns: columns,
	}
}

// NewForeignKeyConstraint creates a table-level foreign key constraint.
//
// This function creates a named foreign key constraint that references another
// table. The constraint can span multiple columns if both the source and target
// have composite keys.
//
// Example:
//
//	ref := &ForeignKeyRef{
//		Table:    "users",
//		Column:   "id",
//		OnDelete: "CASCADE",
//		Name:     "fk_orders_user",
//	}
//	fk := NewForeignKeyConstraint("fk_orders_user", []string{"user_id"}, ref)
func NewForeignKeyConstraint(name string, columns []string, ref *ForeignKeyRef) *ConstraintNode {
	return &ConstraintNode{
		Type:      ForeignKeyConstraint,
		Name:      name,
		Columns:   columns,
		Reference: ref,
	}
}

// NewExcludeConstraint creates a table-level exclude constraint with a name, using method, and elements.
//
// This function creates a named exclude constraint that prevents conflicts based on
// specified operators and conditions. EXCLUDE constraints are PostgreSQL-specific
// and are particularly useful for temporal data, spatial relationships, or complex
// business rules.
//
// Example:
//
//	// Prevent overlapping active sessions per user
//	exclude := NewExcludeConstraint("one_active_session_per_user", "gist", "user_id WITH =").
//		SetWhereCondition("is_active = true")
//	// Prevent overlapping time ranges
//	exclude := NewExcludeConstraint("no_overlapping_bookings", "gist", "room_id WITH =, during WITH &&")
func NewExcludeConstraint(name, usingMethod, elements string) *ConstraintNode {
	return &ConstraintNode{
		Type:            ExcludeConstraint,
		Name:            name,
		UsingMethod:     usingMethod,
		ExcludeElements: elements,
	}
}

// SetWhereCondition sets the optional WHERE clause for an EXCLUDE constraint and returns the constraint for chaining.
//
// The WHERE clause allows the constraint to apply only to rows that satisfy the condition.
//
// Example:
//
//	exclude := NewExcludeConstraint("one_active_session_per_user", "gist", "user_id WITH =").
//		SetWhereCondition("is_active = true")
func (c *ConstraintNode) SetWhereCondition(condition string) *ConstraintNode {
	c.WhereCondition = condition
	return c
}
