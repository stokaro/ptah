package ast

// AlterOperation represents different types of operations that can be performed in ALTER TABLE statements.
//
// This interface extends the Node interface and includes a marker method to ensure
// type safety. All ALTER operations must implement both the visitor pattern
// and the marker method.
type AlterOperation interface {
	Node
	alterOperation() // marker method to ensure type safety
}

// AddColumnOperation represents an ADD COLUMN operation in ALTER TABLE statements.
//
// This operation adds a new column to an existing table with all the specified
// column attributes (type, constraints, defaults, etc.).
type AddColumnOperation struct {
	// Column contains the complete column definition to add
	Column *ColumnNode
}

// Accept implements the Node interface for AddColumnOperation.
//
// The visitor typically handles this by delegating to the column's Accept method
// or by processing it within the VisitAlterTable method.
func (op *AddColumnOperation) Accept(visitor Visitor) error {
	return op.Column.Accept(visitor)
}

// alterOperation implements the marker method for type safety.
func (op *AddColumnOperation) alterOperation() {}

// DropColumnOperation represents a DROP COLUMN operation in ALTER TABLE statements.
//
// This operation removes an existing column from a table. Note that dropping
// columns may have cascading effects on indexes, constraints, and foreign keys.
type DropColumnOperation struct {
	// ColumnName is the name of the column to drop
	ColumnName string
	// Cascade indicates whether to automatically drop dependent objects
	Cascade bool
}

// Accept implements the Node interface for DropColumnOperation.
//
// The actual rendering is typically handled by the visitor's VisitAlterTable method
// rather than delegating to a separate visitor method.
func (op *DropColumnOperation) Accept(_visitor Visitor) error {
	// This would be handled by the visitor's VisitAlterTable method
	return nil
}

// alterOperation implements the marker method for type safety.
func (op *DropColumnOperation) alterOperation() {}

// ModifyColumnOperation represents an ALTER COLUMN/MODIFY COLUMN operation in ALTER TABLE statements.
//
// This operation changes the definition of an existing column. The exact syntax
// varies between database systems (ALTER COLUMN vs MODIFY COLUMN).
type ModifyColumnOperation struct {
	// Column contains the new column definition
	Column *ColumnNode
}

// Accept implements the Node interface for ModifyColumnOperation.
//
// The visitor typically handles this by delegating to the column's Accept method
// or by processing it within the VisitAlterTable method.
func (op *ModifyColumnOperation) Accept(visitor Visitor) error {
	return op.Column.Accept(visitor)
}

// alterOperation implements the marker method for type safety.
func (op *ModifyColumnOperation) alterOperation() {}

// AddConstraintOperation represents an ADD CONSTRAINT operation in ALTER TABLE statements.
//
// This operation adds a new constraint to an existing table. The constraint can be
// a primary key, unique, foreign key, or check constraint.
type AddConstraintOperation struct {
	// Constraint contains the complete constraint definition to add
	Constraint *ConstraintNode
}

// Accept implements the Node interface for AddConstraintOperation.
//
// The visitor typically handles this by delegating to the constraint's Accept method
// or by processing it within the VisitAlterTable method.
func (op *AddConstraintOperation) Accept(visitor Visitor) error {
	return op.Constraint.Accept(visitor)
}

// alterOperation implements the marker method for type safety.
func (op *AddConstraintOperation) alterOperation() {}

// DropConstraintOperation represents a DROP CONSTRAINT operation in ALTER TABLE statements.
//
// This operation removes an existing constraint from a table. The constraint can be
// a primary key, unique, foreign key, check, or exclude constraint.
type DropConstraintOperation struct {
	// ConstraintName is the name of the constraint to drop
	ConstraintName string
	// IfExists indicates whether to use IF EXISTS clause to avoid errors if constraint doesn't exist
	IfExists bool
	// ForeignKey marks the constraint as a FOREIGN KEY. MySQL/MariaDB require
	// the dedicated `DROP FOREIGN KEY <name>` syntax for foreign keys rather
	// than the generic `DROP CONSTRAINT <name>`; renderers that distinguish the
	// two read this flag. Renderers where DROP CONSTRAINT already covers foreign
	// keys (PostgreSQL) may ignore it.
	ForeignKey bool
}

// Accept implements the Node interface for DropConstraintOperation.
//
// The actual rendering is typically handled by the visitor's VisitAlterTable method
// rather than delegating to a separate visitor method.
func (op *DropConstraintOperation) Accept(_visitor Visitor) error {
	// This would be handled by the visitor's VisitAlterTable method
	return nil
}

// alterOperation implements the marker method for type safety.
func (op *DropConstraintOperation) alterOperation() {}

// RenameColumnOperation represents a RENAME COLUMN operation in ALTER TABLE statements.
//
// Both PostgreSQL and MySQL 8.0+/MariaDB 10.5.2+ natively support
// `ALTER TABLE x RENAME COLUMN old TO new`. ClickHouse 22.6+ also supports
// the same syntax on MergeTree-family engines. The runtime DB version is the
// caller's responsibility; the renderer always emits the canonical spelling.
type RenameColumnOperation struct {
	// OldName is the current column name.
	OldName string
	// NewName is the new column name.
	NewName string
}

// Accept implements the Node interface for RenameColumnOperation.
//
// The actual rendering is handled by the dialect's VisitAlterTable method;
// this stub exists to satisfy the Node interface.
func (op *RenameColumnOperation) Accept(_visitor Visitor) error { return nil }

// alterOperation implements the marker method for type safety.
func (op *RenameColumnOperation) alterOperation() {}

// AddSkippingIndexOperation represents ClickHouse's
// `ALTER TABLE x ADD INDEX name expression TYPE indexType GRANULARITY n`.
//
// Skipping indexes are a ClickHouse-only concept; other dialects emit a
// `-- <DIALECT>: data-skipping indexes are ClickHouse-specific; ignored.`
// comment and otherwise treat the operation as a no-op.
type AddSkippingIndexOperation struct {
	// Name is the index name.
	Name string
	// Expression is the indexed expression (column name, function call, tuple,
	// etc.). Empty Expression is rejected by the ClickHouse renderer.
	Expression string
	// IndexType is the ClickHouse skipping-index type. Examples:
	// "minmax", "set(N)", "bloom_filter(0.01)", "tokenbf_v1(...)". The empty
	// string defaults to "minmax" at render time.
	IndexType string
	// Granularity is the GRANULARITY value. Zero defaults to 8192 at render
	// time, matching ClickHouse's documented default for skipping indexes.
	Granularity int
}

// Accept implements the Node interface for AddSkippingIndexOperation.
//
// The actual rendering is handled by the dialect's VisitAlterTable method.
func (op *AddSkippingIndexOperation) Accept(_visitor Visitor) error { return nil }

// alterOperation implements the marker method for type safety.
func (op *AddSkippingIndexOperation) alterOperation() {}

// ModifyTTLOperation represents ClickHouse's `ALTER TABLE x MODIFY TTL ...`
// and `ALTER TABLE x REMOVE TTL`.
//
// Empty Expression instructs the ClickHouse renderer to emit `REMOVE TTL`;
// any non-empty value is emitted verbatim as `MODIFY TTL <expression>`.
//
// Table TTL is a ClickHouse-only concept; other dialects emit a
// `-- <DIALECT>: table TTL is ClickHouse-specific; ignored.` comment and
// otherwise treat the operation as a no-op.
type ModifyTTLOperation struct {
	// Expression is the new TTL clause. Empty clears the TTL.
	Expression string
}

// Accept implements the Node interface for ModifyTTLOperation.
//
// The actual rendering is handled by the dialect's VisitAlterTable method.
func (op *ModifyTTLOperation) Accept(_visitor Visitor) error { return nil }

// alterOperation implements the marker method for type safety.
func (op *ModifyTTLOperation) alterOperation() {}
