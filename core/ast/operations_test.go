package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// TestAddColumnOperation_Accept pins that the visitor receives the operation.
// The column it carries stays reachable through the operation and is not
// visited on its own.
func TestAddColumnOperation_Accept(t *testing.T) {
	c := qt.New(t)

	column := &ast.ColumnNode{Name: "new_column"}
	op := &ast.AddColumnOperation{Column: column}

	call := singleCall(c, op)

	c.Assert(call.node, qt.Equals, any(op))
	c.Assert(call.node, qt.Not(qt.Equals), any(column))
	c.Assert(op.Column, qt.Equals, column)
}

func TestAddColumnOperation_AcceptError(t *testing.T) {
	c := qt.New(t)

	visitor := &MockVisitor{ReturnError: true}
	column := &ast.ColumnNode{Name: "new_column"}
	op := &ast.AddColumnOperation{Column: column}

	err := op.Accept(visitor)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "mock error")
}

func TestDropColumnOperation_Accept(t *testing.T) {
	c := qt.New(t)

	visitor := &MockVisitor{}
	op := &ast.DropColumnOperation{ColumnName: "old_column"}

	err := op.Accept(visitor)

	c.Assert(err, qt.IsNil)
	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{"DropColumnOperation:old_column"})
}

func TestDropColumnOperation_AcceptError(t *testing.T) {
	c := qt.New(t)

	visitor := &MockVisitor{ReturnError: true}
	op := &ast.DropColumnOperation{ColumnName: "old_column"}

	err := op.Accept(visitor)

	c.Assert(err, qt.ErrorMatches, "mock error")
	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{"DropColumnOperation:old_column"})
}

// TestModifyColumnOperation_Accept pins that the visitor receives the
// operation, so a renderer learns the column is being altered rather than
// added.
func TestModifyColumnOperation_Accept(t *testing.T) {
	c := qt.New(t)

	column := &ast.ColumnNode{Name: "modified_column"}
	op := &ast.ModifyColumnOperation{Column: column}

	call := singleCall(c, op)

	c.Assert(call.node, qt.Equals, any(op))
	c.Assert(call.node, qt.Not(qt.Equals), any(column))
	c.Assert(op.Column, qt.Equals, column)
}

func TestModifyColumnOperation_AcceptError(t *testing.T) {
	c := qt.New(t)

	visitor := &MockVisitor{ReturnError: true}
	column := &ast.ColumnNode{Name: "modified_column"}
	op := &ast.ModifyColumnOperation{Column: column}

	err := op.Accept(visitor)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, "mock error")
}

// Test that alter operations implement the AlterOperation interface
func TestAlterOperations_ImplementInterface(t *testing.T) {
	c := qt.New(t)

	var ops []ast.AlterOperation

	// Test that all alter operations can be assigned to AlterOperation interface
	// This is a compile-time check - if they don't implement the interface, this won't compile
	ops = append(ops, &ast.AddColumnOperation{Column: ast.NewColumn("test", "INTEGER")})
	ops = append(ops, &ast.DropColumnOperation{ColumnName: "test"})
	ops = append(ops, &ast.ModifyColumnOperation{Column: ast.NewColumn("test", "INTEGER")})
	ops = append(ops, &ast.AddConstraintOperation{Constraint: ast.NewUniqueConstraint("uk_test", "test")})
	ops = append(ops, &ast.DropConstraintOperation{ConstraintName: "test_constraint"})
	ops = append(ops, &ast.RenameColumnOperation{OldName: "old_test", NewName: "test"})
	ops = append(ops, &ast.RenameTableOperation{NewName: "renamed_test"})

	c.Assert(ops, qt.HasLen, 7)

	// Test that they all implement the Node interface as well (since AlterOperation embeds Node)
	for _, op := range ops {
		// This tests that Accept method exists and can be called
		visitor := &MockVisitor{}
		err := op.Accept(visitor)
		c.Assert(err, qt.IsNil)
	}
}

// Test that operations implement the AlterOperation interface (compile-time check)
func TestAlterOperations_InterfaceCompliance(t *testing.T) {
	c := qt.New(t)

	// Test that types implement the interface - this is a compile-time check
	var _ ast.AlterOperation = &ast.AddColumnOperation{}
	var _ ast.AlterOperation = &ast.DropColumnOperation{}
	var _ ast.AlterOperation = &ast.ModifyColumnOperation{}
	var _ ast.AlterOperation = &ast.AddConstraintOperation{}
	var _ ast.AlterOperation = &ast.DropConstraintOperation{}
	var _ ast.AlterOperation = &ast.RenameColumnOperation{}
	var _ ast.AlterOperation = &ast.RenameTableOperation{}

	// Test that they also implement Node interface
	var _ ast.Node = &ast.AddColumnOperation{}
	var _ ast.Node = &ast.DropColumnOperation{}
	var _ ast.Node = &ast.ModifyColumnOperation{}
	var _ ast.Node = &ast.AddConstraintOperation{}
	var _ ast.Node = &ast.DropConstraintOperation{}
	var _ ast.Node = &ast.RenameColumnOperation{}
	var _ ast.Node = &ast.RenameTableOperation{}

	// If we get here, all interfaces are implemented correctly
	c.Assert(true, qt.IsTrue)
}

// Test AddConstraintOperation with foreign key constraint
func TestAddConstraintOperation_ForeignKey(t *testing.T) {
	c := qt.New(t)

	fkRef := &ast.ForeignKeyRef{
		Table:  "users",
		Column: "id",
		Name:   "fk_posts_user",
	}
	constraint := ast.NewForeignKeyConstraint("fk_posts_user", []string{"user_id"}, fkRef)
	op := &ast.AddConstraintOperation{Constraint: constraint}

	call := singleCall(c, op)

	c.Assert(call.node, qt.Equals, any(op))
	c.Assert(call.node, qt.Not(qt.Equals), any(constraint))

	// Verify the constraint properties are preserved
	c.Assert(op.Constraint.Name, qt.Equals, "fk_posts_user")
	c.Assert(op.Constraint.Type, qt.Equals, ast.ForeignKeyConstraint)
	c.Assert(op.Constraint.Columns, qt.DeepEquals, []string{"user_id"})
	c.Assert(op.Constraint.Reference, qt.Equals, fkRef)
}

func TestDropConstraintOperation_Accept(t *testing.T) {
	c := qt.New(t)

	visitor := &MockVisitor{}
	op := &ast.DropConstraintOperation{
		ConstraintName: "test_constraint",
		IfExists:       true,
	}

	err := op.Accept(visitor)

	c.Assert(err, qt.IsNil)
	c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{"DropConstraintOperation:test_constraint"})

	// Verify the operation properties are preserved
	c.Assert(op.ConstraintName, qt.Equals, "test_constraint")
	c.Assert(op.IfExists, qt.IsTrue)
}

// Test AddColumnOperation with complex column
func TestAddColumnOperation_ComplexColumn(t *testing.T) {
	c := qt.New(t)

	column := ast.NewColumn("user_id", "INTEGER").
		SetNotNull().
		SetForeignKey("users", "id", "fk_user").
		SetComment("Foreign key to users table")

	op := &ast.AddColumnOperation{Column: column}

	call := singleCall(c, op)

	c.Assert(call.node, qt.Equals, any(op))

	// Verify the column properties are preserved
	c.Assert(op.Column.Name, qt.Equals, "user_id")
	c.Assert(op.Column.Type, qt.Equals, "INTEGER")
	c.Assert(op.Column.Nullable, qt.IsFalse)
	c.Assert(op.Column.ForeignKey, qt.IsNotNil)
	c.Assert(op.Column.Comment, qt.Equals, "Foreign key to users table")
}

// Test ModifyColumnOperation with complex column
func TestModifyColumnOperation_ComplexColumn(t *testing.T) {
	c := qt.New(t)

	column := ast.NewColumn("status", "VARCHAR(20)").
		SetNotNull().
		SetDefault("'active'").
		SetCheck("status IN ('active', 'inactive', 'pending')").
		SetComment("User status")

	op := &ast.ModifyColumnOperation{Column: column}

	call := singleCall(c, op)

	c.Assert(call.node, qt.Equals, any(op))

	// Verify the column properties are preserved
	c.Assert(op.Column.Name, qt.Equals, "status")
	c.Assert(op.Column.Type, qt.Equals, "VARCHAR(20)")
	c.Assert(op.Column.Nullable, qt.IsFalse)
	c.Assert(op.Column.Default, qt.IsNotNil)
	c.Assert(op.Column.Default.Value, qt.Equals, "'active'")
	c.Assert(op.Column.Check, qt.Equals, "status IN ('active', 'inactive', 'pending')")
	c.Assert(op.Column.Comment, qt.Equals, "User status")
}

// Test DropColumnOperation with different column names
func TestDropColumnOperation_VariousNames(t *testing.T) {
	tests := []struct {
		name        string
		columnName  string
		wantVisited string
	}{
		{
			name:        "SimpleColumn",
			columnName:  "id",
			wantVisited: "DropColumnOperation:id",
		},
		{
			name:        "UnderscoreColumn",
			columnName:  "user_id",
			wantVisited: "DropColumnOperation:user_id",
		},
		{
			name:        "LongColumn",
			columnName:  "very_long_column_name_with_many_underscores",
			wantVisited: "DropColumnOperation:very_long_column_name_with_many_underscores",
		},
		{
			name:        "EmptyColumn",
			columnName:  "",
			wantVisited: "DropColumnOperation:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			visitor := &MockVisitor{}
			op := &ast.DropColumnOperation{ColumnName: tt.columnName}

			err := op.Accept(visitor)

			c.Assert(err, qt.IsNil)
			c.Assert(visitor.VisitedNodes, qt.DeepEquals, []string{tt.wantVisited})
			c.Assert(op.ColumnName, qt.Equals, tt.columnName)
		})
	}
}

// TestAlterOperations_NilColumn pins that Accept reads nothing off the
// operation it hands over. An operation carrying no column reaches the visitor
// intact, and what a missing column means is the visitor's answer to give.
func TestAlterOperations_NilColumn(t *testing.T) {
	c := qt.New(t)

	addOp := &ast.AddColumnOperation{Column: nil}
	modifyOp := &ast.ModifyColumnOperation{Column: nil}

	c.Assert(singleCall(c, addOp).node, qt.Equals, any(addOp))
	c.Assert(singleCall(c, modifyOp).node, qt.Equals, any(modifyOp))
}

// Test that operations can be used in AlterTableNode
func TestAlterOperations_InAlterTable(t *testing.T) {
	c := qt.New(t)

	addOp := &ast.AddColumnOperation{
		Column: ast.NewColumn("new_col", "VARCHAR(255)"),
	}
	dropOp := &ast.DropColumnOperation{
		ColumnName: "old_col",
	}
	modifyOp := &ast.ModifyColumnOperation{
		Column: ast.NewColumn("existing_col", "TEXT"),
	}

	alterTable := &ast.AlterTableNode{
		Name:       "users",
		Operations: []ast.AlterOperation{addOp, dropOp, modifyOp},
	}

	c.Assert(alterTable.Name, qt.Equals, "users")
	c.Assert(alterTable.Operations, qt.HasLen, 3)

	// Verify operations are correctly stored
	c.Assert(alterTable.Operations[0], qt.Equals, addOp)
	c.Assert(alterTable.Operations[1], qt.Equals, dropOp)
	c.Assert(alterTable.Operations[2], qt.Equals, modifyOp)
}
