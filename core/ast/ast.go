package ast

// Visitor renders one AST node.
//
// A dialect implements this by dispatching on the node's concrete type and
// calling its own handler for that type. The interface carries one method
// because a node kind is not a method: adding a kind would otherwise change the
// contract every renderer and every embedder implements, and the forwarding
// that costs is what a type switch inside one implementation does for free.
//
// What the single method gives up is the compiler's answer to "did this
// renderer decide about that kind". [ptah.run/internal/astrouteguard] asks it
// instead, over every concrete node type against every renderer, and a kind no
// renderer routes fails the build there.
//
// An implementation must not answer an unrecognized node with a nil error. A
// node that produces no output and no error is indistinguishable from one a
// renderer deliberately skips, and that is the failure the routing gate and
// this sentence exist to prevent.
type Visitor interface {
	// VisitNode renders node, or reports why it cannot.
	VisitNode(node Node) error
}

// VisitorFunc adapts a function to [Visitor].
//
// It is what a caller that only wants to look at nodes writes instead of a type
// with one method.
type VisitorFunc func(node Node) error

// VisitNode calls f.
func (f VisitorFunc) VisitNode(node Node) error { return f(node) }

// DefaultValue represents different types of default values for table columns.
//
// A default value can be either a literal value (like 'active', 42, true) or
// an expression (like NOW(), CURRENT_TIMESTAMP, UUID()). Only one of Value
// or Expression should be set; HasLiteral reports whether the literal half is
// in use.
type DefaultValue struct {
	// Value contains literal default values like 'default_value', '42', 'true'
	Value string
	// ValueSet distinguishes an explicitly empty literal from no literal.
	ValueSet bool
	// Expression contains function calls and other SQL expressions like NOW(),
	// UUID(). It is emitted as written and is never quoted, so it has to be
	// valid SQL on the target.
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
	// Deferrable marks a foreign key whose check may be postponed to the end of
	// a transaction, which is the standard answer to a circular reference and
	// to a bulk load that transiently violates the constraint.
	//
	// It is a separate field from Initially because the two are separate
	// clauses: `DEFERRABLE` alone is legal and means the check CAN be deferred
	// while still running immediately by default (stokaro/ptah#1624).
	Deferrable bool
	// Initially is the default timing of a deferrable check: "deferred" or
	// "immediate". Empty means the clause was not written, which is the same
	// thing the engine defaults to -- IMMEDIATE -- spelled as "the author did
	// not say".
	Initially string
	// OnDeleteColumns limits ON DELETE SET NULL or SET DEFAULT to these
	// referencing columns (PostgreSQL 15 and later); the other referencing
	// columns keep their values. Empty means every referencing column, which
	// is also what a list naming all of them means. A renderer for a target
	// without the clause refuses a non-empty list rather than widen the
	// action to every column.
	OnDeleteColumns []string
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
	case ExcludeConstraint:
		return "EXCLUDE"
	default:
		return "UNKNOWN"
	}
}
