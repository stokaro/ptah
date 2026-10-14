package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// TestCreateTypeNode_Constructor tests the NewCreateType constructor
func TestCreateTypeNode_Constructor(t *testing.T) {
	c := qt.New(t)

	enumDef := ast.NewEnumTypeDef("active", "inactive")
	createType := ast.NewCreateType("status", enumDef)

	c.Assert(createType.Name, qt.Equals, "status")
	c.Assert(createType.TypeDef, qt.Equals, enumDef)
	c.Assert(createType.Comment, qt.Equals, "")
}

// TestCreateTypeNode_FluentAPI tests the fluent API methods
func TestCreateTypeNode_FluentAPI(t *testing.T) {
	c := qt.New(t)

	enumDef := ast.NewEnumTypeDef("active", "inactive")
	createType := ast.NewCreateType("status", enumDef).
		SetComment("User status enumeration")

	c.Assert(createType.Name, qt.Equals, "status")
	c.Assert(createType.TypeDef, qt.Equals, enumDef)
	c.Assert(createType.Comment, qt.Equals, "User status enumeration")
}

// TestCreateTypeNode_Accept tests the visitor pattern
func TestCreateTypeNode_Accept(t *testing.T) {
	c := qt.New(t)

	visitor := &MockVisitor{}
	enumDef := ast.NewEnumTypeDef("active", "inactive")
	createType := ast.NewCreateType("status", enumDef)

	err := createType.Accept(visitor)

	c.Assert(err, qt.IsNil)
	c.Assert(visitor.VisitedNodes, qt.HasLen, 1)
	c.Assert(visitor.VisitedNodes[0], qt.Equals, "CreateType:status")
}

// TestAlterTypeNode_Constructor tests the NewAlterType constructor
func TestAlterTypeNode_Constructor(t *testing.T) {
	c := qt.New(t)

	alterType := ast.NewAlterType("status")

	c.Assert(alterType.Name, qt.Equals, "status")
	c.Assert(alterType.Operations, qt.HasLen, 0)
}

// TestAlterTypeNode_FluentAPI tests the fluent API methods
func TestAlterTypeNode_FluentAPI(t *testing.T) {
	c := qt.New(t)

	addOp := ast.NewAddEnumValueOperation("pending")
	renameOp := ast.NewRenameEnumValueOperation("old", "new")

	alterType := ast.NewAlterType("status").
		AddOperation(addOp).
		AddOperation(renameOp)

	c.Assert(alterType.Name, qt.Equals, "status")
	c.Assert(alterType.Operations, qt.HasLen, 2)
	c.Assert(alterType.Operations[0], qt.Equals, addOp)
	c.Assert(alterType.Operations[1], qt.Equals, renameOp)
}

// TestAlterTypeNode_Accept tests the visitor pattern
func TestAlterTypeNode_Accept(t *testing.T) {
	c := qt.New(t)

	visitor := &MockVisitor{}
	alterType := ast.NewAlterType("status")

	err := alterType.Accept(visitor)

	c.Assert(err, qt.IsNil)
	c.Assert(visitor.VisitedNodes, qt.HasLen, 1)
	c.Assert(visitor.VisitedNodes[0], qt.Equals, "AlterType:status")
}

// TestEnumTypeDef_Constructor tests the NewEnumTypeDef constructor
func TestEnumTypeDef_Constructor(t *testing.T) {
	c := qt.New(t)

	enumDef := ast.NewEnumTypeDef("active", "inactive", "pending")

	c.Assert(enumDef.Values, qt.DeepEquals, []string{"active", "inactive", "pending"})
}

// TestEnumTypeDef_EmptyValues tests enum with no values
func TestEnumTypeDef_EmptyValues(t *testing.T) {
	c := qt.New(t)

	enumDef := ast.NewEnumTypeDef()

	c.Assert(enumDef.Values, qt.HasLen, 0)
}

// TestAddEnumValueOperation_Constructor tests the NewAddEnumValueOperation constructor
func TestAddEnumValueOperation_Constructor(t *testing.T) {
	c := qt.New(t)

	op := ast.NewAddEnumValueOperation("pending")

	c.Assert(op.Value, qt.Equals, "pending")
	c.Assert(op.Before, qt.Equals, "")
	c.Assert(op.After, qt.Equals, "")
}

// TestAddEnumValueOperation_FluentAPI tests the fluent API methods
func TestAddEnumValueOperation_FluentAPI(t *testing.T) {
	c := qt.New(t)

	// Test BEFORE clause
	opBefore := ast.NewAddEnumValueOperation("pending").SetBefore("inactive")
	c.Assert(opBefore.Value, qt.Equals, "pending")
	c.Assert(opBefore.Before, qt.Equals, "inactive")
	c.Assert(opBefore.After, qt.Equals, "")

	// Test AFTER clause
	opAfter := ast.NewAddEnumValueOperation("pending").SetAfter("active")
	c.Assert(opAfter.Value, qt.Equals, "pending")
	c.Assert(opAfter.Before, qt.Equals, "")
	c.Assert(opAfter.After, qt.Equals, "active")

	// Test that setting BEFORE clears AFTER
	opBeforeAfter := ast.NewAddEnumValueOperation("pending").
		SetAfter("active").
		SetBefore("inactive")
	c.Assert(opBeforeAfter.Before, qt.Equals, "inactive")
	c.Assert(opBeforeAfter.After, qt.Equals, "")
}

// TestRenameEnumValueOperation_Constructor tests the NewRenameEnumValueOperation constructor
func TestRenameEnumValueOperation_Constructor(t *testing.T) {
	c := qt.New(t)

	op := ast.NewRenameEnumValueOperation("old_value", "new_value")

	c.Assert(op.OldValue, qt.Equals, "old_value")
	c.Assert(op.NewValue, qt.Equals, "new_value")
}

// TestRenameTypeOperation_Constructor tests the NewRenameTypeOperation constructor
func TestRenameTypeOperation_Constructor(t *testing.T) {
	c := qt.New(t)

	op := ast.NewRenameTypeOperation("new_type_name")

	c.Assert(op.NewName, qt.Equals, "new_type_name")
}

// TestTypeDefinition_Interface tests that type definitions implement the interface
func TestTypeDefinition_Interface(t *testing.T) {
	c := qt.New(t)

	var _ ast.TypeDefinition = (*ast.EnumTypeDef)(nil)

	// Test that they also implement Node
	var _ ast.Node = (*ast.EnumTypeDef)(nil)

	c.Assert(true, qt.IsTrue) // Just to have an assertion
}

// TestTypeOperation_Interface tests that type operations implement the interface
func TestTypeOperation_Interface(t *testing.T) {
	c := qt.New(t)

	var _ ast.TypeOperation = (*ast.AddEnumValueOperation)(nil)
	var _ ast.TypeOperation = (*ast.RenameEnumValueOperation)(nil)
	var _ ast.TypeOperation = (*ast.RenameTypeOperation)(nil)

	// Test that they also implement Node
	var _ ast.Node = (*ast.AddEnumValueOperation)(nil)
	var _ ast.Node = (*ast.RenameEnumValueOperation)(nil)
	var _ ast.Node = (*ast.RenameTypeOperation)(nil)

	c.Assert(true, qt.IsTrue) // Just to have an assertion
}
