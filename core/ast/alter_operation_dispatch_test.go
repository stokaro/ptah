package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// TestAlterOperationAccept_HandsTheVisitorTheOperation pins the three ALTER
// TABLE operations that hold a node of their own. The visitor is handed the
// operation, by pointer, and the node it holds never reaches the visitor on its
// own.
func TestAlterOperationAccept_HandsTheVisitorTheOperation(t *testing.T) {
	column := &ast.ColumnNode{Name: "email"}
	constraint := &ast.ConstraintNode{Name: "users_email_key"}

	tests := []struct {
		name       string
		operation  ast.AlterOperation
		wantMethod string
		heldNode   any
	}{
		{
			name:       "add column",
			operation:  &ast.AddColumnOperation{Column: column},
			wantMethod: "VisitAddColumnOperation",
			heldNode:   column,
		},
		{
			name:       "modify column",
			operation:  &ast.ModifyColumnOperation{Column: column},
			wantMethod: "VisitModifyColumnOperation",
			heldNode:   column,
		},
		{
			name:       "add constraint",
			operation:  &ast.AddConstraintOperation{Constraint: constraint},
			wantMethod: "VisitAddConstraintOperation",
			heldNode:   constraint,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			call := singleCall(c, tt.operation)

			c.Assert(call.method, qt.Equals, tt.wantMethod)
			c.Assert(call.node, qt.Equals, any(tt.operation))
			c.Assert(call.node, qt.Not(qt.Equals), tt.heldNode)
		})
	}
}

// TestAlterOperationAccept_AddAndModifyColumnAreDistinguishable pins what a
// renderer reading an ALTER TABLE depends on: the two operations carry the same
// column and still arrive as different nodes, so a visitor driven by Accept
// knows whether the column is being added or altered. ADD COLUMN and ALTER
// COLUMN are different statements on every dialect, and a visitor handed only
// the column would have to guess which one the author asked for.
func TestAlterOperationAccept_AddAndModifyColumnAreDistinguishable(t *testing.T) {
	c := qt.New(t)

	column := &ast.ColumnNode{Name: "email"}
	add := &ast.AddColumnOperation{Column: column}
	modify := &ast.ModifyColumnOperation{Column: column}

	addCall := singleCall(c, add)
	modifyCall := singleCall(c, modify)

	c.Assert(addCall.method, qt.Not(qt.Equals), modifyCall.method)
	c.Assert(addCall.node, qt.Not(qt.Equals), modifyCall.node)
	c.Assert(addCall.node, qt.Equals, any(add))
	c.Assert(modifyCall.node, qt.Equals, any(modify))
	c.Assert(addCall.node, qt.Not(qt.Equals), any(column))
	c.Assert(modifyCall.node, qt.Not(qt.Equals), any(column))
}
