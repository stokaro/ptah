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
// as "alias"."col", and a JoinClause list on SelectStatement. Phase 3 adds
// DISTINCT, GROUP BY, HAVING, and aggregate functions: a Distinct flag, a
// GroupBy column list, and a Having expression on SelectStatement; a general
// FuncCall expression node for COUNT / SUM / AVG / MIN / MAX; and an optional
// Expr (and Alias) on ResultColumn so a projection entry can be an expression
// rather than a plain column. Non-aggregate functions, arithmetic,
// subqueries, and window functions remain follow-up phases. The types are shaped
// so those extensions slot in without breaking callers (for example, Comparison
// takes Expression operands on both sides rather than a bare column string, so a
// HAVING can compare an aggregate against a bound value directly).

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
	// OpLike is the LIKE operator.
	//
	// Its CASE SENSITIVITY belongs to the server, not to Ptah: PostgreSQL's
	// LIKE is case-sensitive, MySQL's follows the column's collation and is
	// usually not, and SQLite's folds ASCII. Ptah emits the operator and lets
	// each engine mean what it means -- inventing a portable spelling would
	// claim a uniformity none of them share (stokaro/ptah#941).
	OpLike
	// OpNotLike is the NOT LIKE operator. See OpLike on case sensitivity.
	OpNotLike
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
	case OpLike:
		return "LIKE"
	case OpNotLike:
		return "NOT LIKE"
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

// FuncCall is a function-call expression, such as an aggregate: COUNT(*),
// COUNT("col"), COUNT(DISTINCT "col"), or SUM("col"). It is shaped as a general
// function call so non-aggregate functions can reuse it in a later phase.
//
// The function is usable anywhere an Expression is: in a projection (via
// ResultColumn.Expr) and in a HAVING comparison (as an operand of Comparison).
// Name is a bare SQL keyword emitted verbatim, never quoted and never bound — the
// renderer rejects a Name that is not a simple identifier so a caller cannot
// smuggle SQL through it. Args are ordinary expressions, so a column argument is
// quoted through the identifier path and a value argument is bound as a
// placeholder.
type FuncCall struct {
	// Name is the function name, emitted verbatim as a keyword. Required. It must
	// be a simple identifier — a letter or underscore followed by letters, digits,
	// or underscores — and the renderer rejects anything else rather than quote a
	// function name.
	Name string
	// Args are the function arguments, each an expression. A column argument is
	// quoted; a value argument is bound. Ignored when Star is true.
	Args []Expression
	// Star renders the argument list as a single "*", as in COUNT(*). When Star is
	// true, Args must be empty and Distinct must be false; the renderer rejects
	// either combination rather than emit invalid SQL.
	Star bool
	// Distinct emits the DISTINCT keyword before the arguments, as in
	// COUNT(DISTINCT "col").
	Distinct bool
}

func (*FuncCall) expressionNode() {}

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
// When Expr is set the entry renders that expression (for example an aggregate)
// and Qualifier, Name, and Star are ignored. Otherwise, when Star is true the
// entry renders as "*" (select all columns) and Name and Qualifier are ignored;
// otherwise Name renders as a dialect-quoted identifier, prefixed by
// "Qualifier". when Qualifier is set. When Alias is set the entry is followed by
// AS and the quoted alias. A zero ResultColumn leaves Expr nil and Alias empty,
// so the Phase 1 and Phase 2 rendering is preserved unchanged.
type ResultColumn struct {
	// Expr is an optional projected expression, such as a FuncCall aggregate. When
	// non-nil it is rendered instead of Qualifier, Name, and Star.
	Expr Expression
	// Qualifier is the optional table name or alias qualifying the column,
	// quoted independently. An empty Qualifier renders a bare column name. It is
	// ignored when Star is true or Expr is set.
	Qualifier string
	// Name is the column name, used when Star is false and Expr is nil.
	Name string
	// Star selects all columns (SELECT *). Ignored when Expr is set.
	Star bool
	// Alias is an optional output-column alias, rendered as `AS "alias"` with the
	// alias quoted independently. An empty Alias renders no alias.
	Alias string
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
// an optional DISTINCT, WHERE clause, GROUP BY / HAVING, ORDER BY terms, and
// LIMIT/OFFSET bounds.
//
// Subqueries, non-aggregate functions, arithmetic, and window functions are not
// modeled yet. Render it with renderer.RenderSelect, which returns the SQL string
// and its positional arguments. Build one fluently with the core/query package.
// CommonTableExpression is one named subquery of a WITH clause.
//
// Name is emitted as a quoted identifier and referenced from the outer query's
// From (or a join) as a plain table name. Query is rendered in parentheses, and
// its bound values are appended before the outer query's, which is what keeps
// positional placeholders in the order the driver will read them.
type CommonTableExpression struct {
	Name  string
	Query *SelectStatement
}

type SelectStatement struct {
	// Distinct renders SELECT DISTINCT, deduplicating result rows. It defaults to
	// false, so a zero statement renders a plain SELECT unchanged.
	Distinct bool
	// Columns is the projection. An empty slice renders as "*".
	Columns []ResultColumn
	// With is the optional WITH clause: named subqueries evaluated before the
	// main query and referenced from it by name. An empty slice emits no WITH.
	//
	// Only non-recursive CTEs are modeled. RECURSIVE needs a UNION ALL body and
	// a termination argument the builder has no way to state, and emitting the
	// keyword without one would produce a query that runs forever rather than
	// one that fails to parse.
	With []CommonTableExpression
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
	// GroupBy is the optional list of GROUP BY columns, rendered after WHERE. Each
	// column may be qualified. An empty slice renders no GROUP BY. Columns carry no
	// bound values, so they do not affect placeholder numbering.
	GroupBy []ColumnRef
	// Having is the optional filter over grouped rows, rendered after GROUP BY; nil
	// means no HAVING. Its bound values are numbered after the WHERE clause and
	// before LIMIT/OFFSET.
	Having Expression
	// OrderBy is the optional list of sort terms, applied in order.
	OrderBy []OrderByClause
	// Limit is the optional row limit; nil means no LIMIT. The value is bound.
	Limit *int64
	// Offset is the optional row offset; nil means no OFFSET. The value is bound.
	Offset *int64
}
