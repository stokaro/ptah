package ast

// This file defines the Phase 1 DML query AST: a single-table SELECT statement
// and the composable boolean expression tree used by its WHERE clause. These
// nodes live alongside the DDL nodes but do not participate in the DDL Visitor
// interface, because DML rendering must return bound arguments in addition to a
// SQL string. Rendering is handled by renderer.RenderSelect.
//
// Phase 1 deliberately models only SELECT / WHERE / ORDER BY / LIMIT / OFFSET.
// JOIN, GROUP BY, HAVING, DISTINCT, subqueries, functions, and arithmetic are
// intentionally absent; they are follow-up phases. The types below are shaped
// so those extensions slot in without breaking callers (for example, Comparison
// takes Expression operands on both sides rather than a bare column string).

// Expression is a boolean or scalar expression used in DML statements such as
// the WHERE clause of a SELECT.
//
// Expression is a sealed sum type: only the node types declared in this package
// implement it. Sealing serves the query builder's core safety property — a
// caller cannot smuggle a raw SQL string in as an expression, and every value
// reaches the SQL through a BoundValue node (a placeholder), never inlined.
type Expression interface {
	// expressionNode is an unexported marker method that seals the interface.
	expressionNode()
}

// ColumnRef is a reference to a column by name.
//
// A ColumnRef always renders as a dialect-quoted identifier and never as literal
// SQL text, so an attacker-shaped name cannot terminate the identifier.
type ColumnRef struct {
	// Name is the unqualified column name.
	Name string
}

func (*ColumnRef) expressionNode() {}

// BoundValue is a parameter value.
//
// The renderer emits a dialect-specific placeholder ($1, $2, … for PostgreSQL;
// ? for MySQL, MariaDB, and SQLite) and appends Value to the returned argument
// slice. The value is never interpolated into the SQL string.
type BoundValue struct {
	// Value is the parameter value, bound positionally at render time.
	Value any
}

func (*BoundValue) expressionNode() {}

// ComparisonOperator enumerates the binary comparison operators supported in
// Phase 1.
type ComparisonOperator int

const (
	// OpEqual is the = operator.
	OpEqual ComparisonOperator = iota
	// OpNotEqual is the <> operator.
	OpNotEqual
	// OpLessThan is the < operator.
	OpLessThan
	// OpLessThanOrEqual is the <= operator.
	OpLessThanOrEqual
	// OpGreaterThan is the > operator.
	OpGreaterThan
	// OpGreaterThanOrEqual is the >= operator.
	OpGreaterThanOrEqual
)

// String returns the SQL token for the operator, or an empty string when the
// operator is outside the defined range. Renderers treat an empty string as an
// error rather than emitting invalid SQL.
func (op ComparisonOperator) String() string {
	switch op {
	case OpEqual:
		return "="
	case OpNotEqual:
		return "<>"
	case OpLessThan:
		return "<"
	case OpLessThanOrEqual:
		return "<="
	case OpGreaterThan:
		return ">"
	case OpGreaterThanOrEqual:
		return ">="
	default:
		return ""
	}
}

// Comparison is a binary comparison of the form "Left <Operator> Right".
//
// In Phase 1 the query builder produces a ColumnRef on the left and a BoundValue
// on the right, but the node accepts any Expression on either side so later
// phases can compare columns or expressions directly.
type Comparison struct {
	// Left is the left-hand operand.
	Left Expression
	// Operator is the comparison operator.
	Operator ComparisonOperator
	// Right is the right-hand operand.
	Right Expression
}

func (*Comparison) expressionNode() {}

// InExpr is a membership test of the form "Operand IN (Values...)".
//
// Values must be non-empty; the renderer rejects an empty list rather than
// emitting the invalid "IN ()". Each value renders as its own placeholder.
type InExpr struct {
	// Operand is the expression tested for membership, typically a ColumnRef.
	Operand Expression
	// Values are the candidate values, each bound as a placeholder.
	Values []Expression
}

func (*InExpr) expressionNode() {}

// NullTest is a null check of the form "Operand IS NULL" or, when Negated,
// "Operand IS NOT NULL".
type NullTest struct {
	// Operand is the expression tested for null, typically a ColumnRef.
	Operand Expression
	// Negated selects IS NOT NULL instead of IS NULL.
	Negated bool
}

func (*NullTest) expressionNode() {}

// LogicalOperator enumerates the boolean combinators used by LogicalExpr.
type LogicalOperator int

const (
	// LogicalAnd is the AND combinator.
	LogicalAnd LogicalOperator = iota
	// LogicalOr is the OR combinator.
	LogicalOr
)

// String returns the SQL keyword for the operator, or an empty string when the
// operator is outside the defined range.
func (op LogicalOperator) String() string {
	switch op {
	case LogicalAnd:
		return "AND"
	case LogicalOr:
		return "OR"
	default:
		return ""
	}
}

// LogicalExpr combines two or more operands with a single boolean operator, as
// in "Operands[0] AND Operands[1] AND …". The renderer parenthesizes the whole
// expression so precedence is explicit regardless of nesting.
type LogicalExpr struct {
	// Operator is the boolean combinator applied between operands.
	Operator LogicalOperator
	// Operands are the sub-expressions being combined; at least one is required.
	Operands []Expression
}

func (*LogicalExpr) expressionNode() {}

// NotExpr negates its operand, rendering as "NOT (Operand)".
type NotExpr struct {
	// Operand is the expression being negated.
	Operand Expression
}

func (*NotExpr) expressionNode() {}

// SortDirection is the direction of an ORDER BY term.
type SortDirection int

const (
	// SortAscending sorts in ascending order (ASC).
	SortAscending SortDirection = iota
	// SortDescending sorts in descending order (DESC).
	SortDescending
)

// String returns the SQL keyword for the direction, or an empty string when the
// direction is outside the defined range.
func (d SortDirection) String() string {
	switch d {
	case SortAscending:
		return "ASC"
	case SortDescending:
		return "DESC"
	default:
		return ""
	}
}

// OrderByClause is a single ORDER BY term: a column and a sort direction. The
// direction is always rendered explicitly so output is deterministic.
type OrderByClause struct {
	// Column is the column name to order by, rendered as a quoted identifier.
	Column string
	// Direction is the sort direction.
	Direction SortDirection
}

// ResultColumn is one entry in a SELECT projection.
//
// When Star is true the entry renders as "*" (select all columns) and Name is
// ignored; otherwise Name renders as a dialect-quoted identifier.
type ResultColumn struct {
	// Name is the column name, used when Star is false.
	Name string
	// Star selects all columns (SELECT *).
	Star bool
}

// SelectStatement is a single-table SELECT with an optional WHERE clause,
// ORDER BY terms, and LIMIT/OFFSET bounds.
//
// It is the Phase 1 DML node. JOIN, GROUP BY, HAVING, DISTINCT, subqueries,
// functions, and arithmetic are intentionally not modeled yet. Render it with
// renderer.RenderSelect, which returns the SQL string and its positional
// arguments. Build one fluently with the core/query package.
type SelectStatement struct {
	// Columns is the projection. An empty slice renders as "*".
	Columns []ResultColumn
	// From is the single source table, rendered as a quoted identifier. Required.
	From string
	// Where is the optional filter expression tree; nil means no WHERE clause.
	Where Expression
	// OrderBy is the optional list of sort terms, applied in order.
	OrderBy []OrderByClause
	// Limit is the optional row limit; nil means no LIMIT. The value is bound.
	Limit *int64
	// Offset is the optional row offset; nil means no OFFSET. The value is bound.
	Offset *int64
}
