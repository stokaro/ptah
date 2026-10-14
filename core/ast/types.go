package ast

// TypeDefinition represents a type definition a CREATE TYPE statement carries.
//
// The common model defines enums. Domains, composite types and range types
// belong to their feature owners, whose operations render their own
// statements.
type TypeDefinition interface {
	Node
	typeDefinition() // marker method to ensure type safety
}

// TypeOperation represents different types of operations that can be performed in ALTER TYPE statements.
//
// This interface extends the Node interface and includes a marker method to ensure
// type safety. All ALTER TYPE operations must implement both the visitor pattern
// and the marker method.
type TypeOperation interface {
	Node
	typeOperation() // marker method to ensure type safety
}

// EnumTypeDef represents an enum type definition for CREATE TYPE ... AS ENUM statements.
//
// This is used within CreateTypeNode to define enum types with a list of values.
type EnumTypeDef struct {
	// Values contains the list of allowed enum values
	Values []string
}

// NewEnumTypeDef creates a new enum type definition with the specified values.
//
// Example:
//
//	enumDef := NewEnumTypeDef("active", "inactive", "pending")
func NewEnumTypeDef(values ...string) *EnumTypeDef {
	return &EnumTypeDef{
		Values: values,
	}
}

// Accept implements the Node interface for EnumTypeDef.
func (td *EnumTypeDef) Accept(visitor Visitor) error {
	return visitor.
		// This is typically handled by the parent CreateTypeNode's visitor
		VisitNode(td)
}

// typeDefinition implements the marker method for type safety.
func (td *EnumTypeDef) typeDefinition() {}

// AddEnumValueOperation represents an ADD VALUE operation for ALTER TYPE statements.
//
// This operation adds a new value to an existing enum type. The position
// can be specified using BEFORE or AFTER clauses.
type AddEnumValueOperation struct {
	// Value is the new enum value to add
	Value string
	// Before specifies the existing value before which to insert (optional)
	Before string
	// After specifies the existing value after which to insert (optional)
	After string
}

// NewAddEnumValueOperation creates a new add enum value operation.
//
// Example:
//
//	op := NewAddEnumValueOperation("pending")
//	op := NewAddEnumValueOperation("archived").SetAfter("inactive")
func NewAddEnumValueOperation(value string) *AddEnumValueOperation {
	return &AddEnumValueOperation{
		Value: value,
	}
}

// SetBefore sets the BEFORE clause and returns the operation for chaining.
func (op *AddEnumValueOperation) SetBefore(value string) *AddEnumValueOperation {
	op.Before = value
	op.After = "" // Clear After if Before is set
	return op
}

// SetAfter sets the AFTER clause and returns the operation for chaining.
func (op *AddEnumValueOperation) SetAfter(value string) *AddEnumValueOperation {
	op.After = value
	op.Before = "" // Clear Before if After is set
	return op
}

// Accept implements the Node interface for AddEnumValueOperation.
func (op *AddEnumValueOperation) Accept(visitor Visitor) error {
	return visitor.
		// This is typically handled by the parent AlterTypeNode's visitor
		VisitNode(op)
}

// typeOperation implements the marker method for type safety.
func (op *AddEnumValueOperation) typeOperation() {}

// RenameEnumValueOperation represents a RENAME VALUE operation for ALTER TYPE statements.
//
// This operation renames an existing enum value to a new name.
type RenameEnumValueOperation struct {
	// OldValue is the current enum value name
	OldValue string
	// NewValue is the new enum value name
	NewValue string
}

// NewRenameEnumValueOperation creates a new rename enum value operation.
//
// Example:
//
//	op := NewRenameEnumValueOperation("old_status", "new_status")
func NewRenameEnumValueOperation(oldValue, newValue string) *RenameEnumValueOperation {
	return &RenameEnumValueOperation{
		OldValue: oldValue,
		NewValue: newValue,
	}
}

// Accept implements the Node interface for RenameEnumValueOperation.
func (op *RenameEnumValueOperation) Accept(visitor Visitor) error {
	return visitor.
		// This is typically handled by the parent AlterTypeNode's visitor
		VisitNode(op)
}

// typeOperation implements the marker method for type safety.
func (op *RenameEnumValueOperation) typeOperation() {}

// RenameTypeOperation represents a RENAME TO operation for ALTER TYPE statements.
//
// This operation renames the type itself to a new name.
type RenameTypeOperation struct {
	// NewName is the new type name
	NewName string
}

// NewRenameTypeOperation creates a new rename type operation.
//
// Example:
//
//	op := NewRenameTypeOperation("new_type_name")
func NewRenameTypeOperation(newName string) *RenameTypeOperation {
	return &RenameTypeOperation{
		NewName: newName,
	}
}

// Accept implements the Node interface for RenameTypeOperation.
func (op *RenameTypeOperation) Accept(visitor Visitor) error {
	return visitor.
		// This is typically handled by the parent AlterTypeNode's visitor
		VisitNode(op)
}

// typeOperation implements the marker method for type safety.
func (op *RenameTypeOperation) typeOperation() {}
