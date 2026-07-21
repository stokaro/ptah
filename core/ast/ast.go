package ast

// Visitor defines the interface for visiting AST nodes using the visitor pattern.
//
// The visitor pattern allows for dialect-specific rendering of SQL statements
// without modifying the AST node structures. Each visitor method corresponds
// to a specific node type and is responsible for generating the appropriate
// SQL representation for that node.
//
// Implementations of this interface should handle the rendering logic for
// their specific database dialect (PostgreSQL, MySQL, MariaDB, etc.).
type Visitor interface {
	// VisitCreateTable renders a CREATE TABLE statement
	VisitCreateTable(*CreateTableNode) error
	// VisitCreateSchema renders a CREATE SCHEMA statement
	VisitCreateSchema(*CreateSchemaNode) error
	// VisitCreateDatabase renders a CREATE DATABASE statement
	VisitCreateDatabase(*CreateDatabaseNode) error
	// VisitAlterTable renders an ALTER TABLE statement
	VisitAlterTable(*AlterTableNode) error
	// VisitColumn renders a column definition (typically called from other visitors)
	VisitColumn(*ColumnNode) error
	// VisitConstraint renders a constraint definition (typically called from other visitors)
	VisitConstraint(*ConstraintNode) error
	// VisitIndex renders a CREATE INDEX statement
	VisitIndex(*IndexNode) error
	// VisitDropIndex renders a DROP INDEX statement
	VisitDropIndex(*DropIndexNode) error
	// VisitEnum renders an enum type definition (PostgreSQL-specific, legacy)
	VisitEnum(*EnumNode) error
	// VisitCreateType renders a CREATE TYPE statement with various type definitions
	VisitCreateType(*CreateTypeNode) error
	// VisitAlterType renders an ALTER TYPE statement with various operations
	VisitAlterType(*AlterTypeNode) error
	// VisitComment renders a SQL comment
	VisitComment(*CommentNode) error
	// VisitDropTable renders a DROP TABLE statement
	VisitDropTable(*DropTableNode) error
	// VisitDropType renders a DROP TYPE statement (PostgreSQL-specific)
	VisitDropType(*DropTypeNode) error
	// VisitExtension renders a CREATE EXTENSION statement (PostgreSQL-specific)
	VisitExtension(*ExtensionNode) error
	// VisitDropExtension renders a DROP EXTENSION statement (PostgreSQL-specific)
	VisitDropExtension(*DropExtensionNode) error
	// VisitCreateFunction renders a CREATE FUNCTION statement (PostgreSQL-specific)
	VisitCreateFunction(*CreateFunctionNode) error
	// VisitDropFunction renders a DROP FUNCTION statement (PostgreSQL-specific)
	VisitDropFunction(*DropFunctionNode) error
	// VisitCreateView renders a CREATE VIEW statement
	VisitCreateView(*CreateViewNode) error
	// VisitDropView renders a DROP VIEW statement
	VisitDropView(*DropViewNode) error
	// VisitCreateMaterializedView renders a CREATE MATERIALIZED VIEW statement
	VisitCreateMaterializedView(*CreateMaterializedViewNode) error
	// VisitDropMaterializedView renders a DROP MATERIALIZED VIEW statement
	VisitDropMaterializedView(*DropMaterializedViewNode) error
	// VisitRefreshMaterializedView renders a REFRESH MATERIALIZED VIEW statement
	VisitRefreshMaterializedView(*RefreshMaterializedViewNode) error
	// VisitCreateTrigger renders a CREATE TRIGGER statement
	VisitCreateTrigger(*CreateTriggerNode) error
	// VisitDropTrigger renders a DROP TRIGGER statement
	VisitDropTrigger(*DropTriggerNode) error
	// VisitCreatePolicy renders a CREATE POLICY statement for RLS (PostgreSQL-specific)
	VisitCreatePolicy(*CreatePolicyNode) error
	// VisitDropPolicy renders a DROP POLICY statement for RLS (PostgreSQL-specific)
	VisitDropPolicy(*DropPolicyNode) error
	// VisitAlterTableEnableRLS renders an ALTER TABLE ENABLE ROW LEVEL SECURITY statement (PostgreSQL-specific)
	VisitAlterTableEnableRLS(*AlterTableEnableRLSNode) error
	// VisitAlterTableDisableRLS renders an ALTER TABLE DISABLE ROW LEVEL SECURITY statement (PostgreSQL-specific)
	VisitAlterTableDisableRLS(*AlterTableDisableRLSNode) error
	// VisitCreateRole renders a CREATE ROLE statement (PostgreSQL-specific)
	VisitCreateRole(*CreateRoleNode) error
	// VisitDropRole renders a DROP ROLE statement (PostgreSQL-specific)
	VisitDropRole(*DropRoleNode) error
	// VisitAlterRole renders an ALTER ROLE statement (PostgreSQL-specific)
	VisitAlterRole(*AlterRoleNode) error
	// VisitGrantPrivilege renders a GRANT statement (PostgreSQL-specific)
	VisitGrantPrivilege(*GrantPrivilegeNode) error
	// VisitRevokePrivilege renders a REVOKE statement (PostgreSQL-specific)
	VisitRevokePrivilege(*RevokePrivilegeNode) error
	// VisitRawSQL renders a literal SQL fragment verbatim. Use sparingly —
	// reach for structured nodes first.
	VisitRawSQL(*RawSQLNode) error
	// VisitUpsert renders a dialect-independent upsert statement.
	VisitUpsert(*UpsertNode) error
}

// DefaultValue represents different types of default values for table columns.
//
// A default value can be either a literal value (like 'active', 42, true) or
// a function call (like NOW(), CURRENT_TIMESTAMP, UUID()). Only one of Value
// or Function should be set.
type DefaultValue struct {
	// Value contains literal default values like 'default_value', '42', 'true'
	Value string
	// ValueSet distinguishes an explicitly empty literal from no literal.
	ValueSet bool
	// Function contains function calls like NOW(), UUID()
	Expression string
}

// HasLiteral reports whether the default is a literal value, including an
// explicitly empty string. Non-empty Value is accepted for compatibility with
// existing struct literals.
func (d *DefaultValue) HasLiteral() bool {
	return d != nil && (d.ValueSet || d.Value != "")
}

// ForeignKeyRef represents a foreign key reference with optional referential actions.
//
// This structure defines the target table and columns for a foreign key constraint,
// along with optional ON DELETE and ON UPDATE actions that specify what should
// happen when the referenced row is deleted or updated.
type ForeignKeyRef struct {
	// Table is the name of the referenced table
	Table string
	// Column is the name of the referenced column. It is kept for single-column
	// foreign keys and compatibility with existing struct literals.
	Column string
	// Columns contains the referenced columns for composite foreign keys. When
	// empty, Column remains the source of truth.
	Columns []string
	// OnDelete specifies the action when the referenced row is deleted (CASCADE, SET NULL, etc.)
	OnDelete string
	// OnUpdate specifies the action when the referenced row is updated (CASCADE, SET NULL, etc.)
	OnUpdate string
	// Name is the constraint name for the foreign key
	Name string
}

// ReferencedColumns returns the referenced column list, falling back to Column
// for legacy single-column foreign key references.
func (f *ForeignKeyRef) ReferencedColumns() []string {
	if f == nil {
		return nil
	}
	if len(f.Columns) > 0 {
		return f.Columns
	}
	if f.Column != "" {
		return []string{f.Column}
	}
	return nil
}

// ConstraintType represents the different types of table constraints.
//
// This enumeration covers the standard SQL constraint types that can be
// applied at the table level, including primary keys, unique constraints,
// foreign keys, and check constraints.
type ConstraintType int

const (
	// PrimaryKeyConstraint represents a PRIMARY KEY constraint
	PrimaryKeyConstraint ConstraintType = iota
	// UniqueConstraint represents a UNIQUE constraint
	UniqueConstraint
	// ForeignKeyConstraint represents a FOREIGN KEY constraint
	ForeignKeyConstraint
	// CheckConstraint represents a CHECK constraint
	CheckConstraint
	// ExcludeConstraint represents an EXCLUDE constraint (PostgreSQL-specific)
	ExcludeConstraint
)

// String returns the SQL representation of the constraint type.
//
// This method converts the ConstraintType enumeration value to its
// corresponding SQL keyword that would appear in DDL statements.
func (ct ConstraintType) String() string {
	switch ct {
	case PrimaryKeyConstraint:
		return "PRIMARY KEY"
	case UniqueConstraint:
		return "UNIQUE"
	case ForeignKeyConstraint:
		return "FOREIGN KEY"
	case CheckConstraint:
		return "CHECK"
	default:
		return "UNKNOWN"
	}
}
