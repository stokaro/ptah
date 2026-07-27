package ast

// This file defines the DML query AST: a SELECT statement and the composable
// boolean expression tree used by its WHERE and JOIN ON clauses. These nodes
// live alongside the DDL nodes but do not participate in the DDL Visitor
// interface, because DML rendering must return bound arguments in addition to a
// SQL string. Rendering is handled by renderer.RenderSelect.
//
// Phase 1 modeled SELECT / WHERE / ORDER BY / LIMIT / OFFSET over a single
// table. Phase 2 adds JOINs: a table alias on the FROM clause, an optional
// qualifier on ColumnRef / ResultColumn / OrderByClause so a column can render
// as "alias"."col", and a JoinClause list on SelectStatement. GROUP BY, HAVING,
// DISTINCT, subqueries, functions, and arithmetic remain follow-up phases. The
// types are shaped so those extensions slot in without breaking callers (for
// example, Comparison takes Expression operands on both sides rather than a bare
// column string, so a JOIN ON can compare two columns directly).

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

// ColumnRef is a reference to a column, optionally qualified by a table name or
// alias.
//
// A ColumnRef always renders as a dialect-quoted identifier and never as literal
// SQL text, so an attacker-shaped name cannot terminate the identifier. When
// Qualifier is set the column renders as "Qualifier"."Name", each part quoted
// independently.
type ColumnRef struct {
	// Qualifier is the optional table name or alias qualifying the column. An
	// empty Qualifier renders a bare column name. It is quoted independently of
	// Name so neither part can break out of its quotes.
	Qualifier string
	// Name is the column name.
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
	// Qualifier is the optional table name or alias qualifying the column,
	// quoted independently. An empty Qualifier renders a bare column name.
	Qualifier string
	// Column is the column name to order by, rendered as a quoted identifier.
	Column string
	// Direction is the sort direction.
	Direction SortDirection
}

// ResultColumn is one entry in a SELECT projection.
//
// When Star is true the entry renders as "*" (select all columns) and Name and
// Qualifier are ignored; otherwise Name renders as a dialect-quoted identifier,
// prefixed by "Qualifier". when Qualifier is set.
type ResultColumn struct {
	// Qualifier is the optional table name or alias qualifying the column,
	// quoted independently. An empty Qualifier renders a bare column name. It is
	// ignored when Star is true.
	Qualifier string
	// Name is the column name, used when Star is false.
	Name string
	// Star selects all columns (SELECT *).
	Star bool
}

// JoinType enumerates the join kinds supported in Phase 2.
type JoinType int

const (
	// JoinInner is an INNER JOIN: only rows matching the ON condition.
	JoinInner JoinType = iota
	// JoinLeft is a LEFT [OUTER] JOIN: all left rows, right columns NULL when
	// unmatched.
	JoinLeft
	// JoinRight is a RIGHT [OUTER] JOIN: all right rows, left columns NULL when
	// unmatched.
	JoinRight
	// JoinFull is a FULL OUTER JOIN: all rows from both sides.
	JoinFull
)

// String returns the SQL keyword for the join type, or an empty string when the
// type is outside the defined range. Renderers treat an empty string as an error
// rather than emitting invalid SQL.
func (t JoinType) String() string {
	switch t {
	case JoinInner:
		return "INNER JOIN"
	case JoinLeft:
		return "LEFT JOIN"
	case JoinRight:
		return "RIGHT JOIN"
	case JoinFull:
		return "FULL OUTER JOIN"
	default:
		return ""
	}
}

// JoinClause is a single join applied to the SELECT's FROM clause: a join type,
// a target table with an optional alias, and an ON condition.
//
// The ON condition reuses the Expression tree, so an equi-join is a Comparison
// of two ColumnRef operands and richer predicates compose with LogicalExpr. The
// ON expression is rendered through the same bound-parameter and identifier
// quoting path as WHERE.
type JoinClause struct {
	// Type is the join type (INNER, LEFT, RIGHT, or FULL OUTER).
	Type JoinType
	// Table is the joined table name, rendered as a quoted identifier. Required.
	Table string
	// Alias is the optional alias for the joined table; empty means no alias.
	Alias string
	// On is the join condition; nil is rejected by the renderer.
	On Expression
}

// SelectStatement is a SELECT over a source table and zero or more joins, with
// an optional WHERE clause, ORDER BY terms, and LIMIT/OFFSET bounds.
//
// GROUP BY, HAVING, DISTINCT, subqueries, functions, and arithmetic are not
// modeled yet. Render it with renderer.RenderSelect, which returns the SQL
// string and its positional arguments. Build one fluently with the core/query
// package.
type SelectStatement struct {
	// Columns is the projection. An empty slice renders as "*".
	Columns []ResultColumn
	// From is the source table, rendered as a quoted identifier. Required.
	From string
	// FromAlias is the optional alias for the source table; empty means no alias.
	// When set, the FROM clause renders as "From" "FromAlias".
	FromAlias string
	// Joins is the optional list of joins, rendered after FROM in order. Their ON
	// conditions are bound before the WHERE clause.
	Joins []JoinClause
	// Where is the optional filter expression tree; nil means no WHERE clause.
	Where Expression
	// OrderBy is the optional list of sort terms, applied in order.
	OrderBy []OrderByClause
	// Limit is the optional row limit; nil means no LIMIT. The value is bound.
	Limit *int64
	// Offset is the optional row offset; nil means no OFFSET. The value is bound.
	Offset *int64
}
