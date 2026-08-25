package query

import (
	"slices"
	"strings"
)

// SelectBuilder builds a *SelectStatement through a fluent, chainable API.
//
// A zero SelectBuilder is not meant to be used directly; start with Select. Each
// method mutates and returns the same builder for chaining. Build produces the
// statement. Builders are not safe for concurrent use.
type SelectBuilder struct {
	with      []CommonTableExpression
	distinct  bool
	columns   []ResultColumn
	from      string
	fromAlias string
	joins     []JoinClause
	where     Expression
	groupBy   []ColumnRef
	having    Expression
	orderBy   []OrderByClause
	limit     *int64
	offset    *int64
}

// Select starts a SELECT with the given projection columns.
//
// Each name is rendered as a dialect-quoted identifier, except the special
// column "*", which selects all columns. Calling Select with no arguments (or a
// single "*") selects all columns.
func Select(columns ...string) *SelectBuilder {
	b := &SelectBuilder{}
	for _, name := range columns {
		if name == "*" {
			b.columns = append(b.columns, ResultColumn{Star: true})
			continue
		}
		b.columns = append(b.columns, ResultColumn{Name: name})
	}
	return b
}

// With prepends a named subquery to the WITH clause, referenced from the outer
// query by name as though it were a table.
//
// Calls accumulate in order, and the order matters twice: a later CTE may read
// an earlier one, and the bound values of all of them precede the outer query's
// in the returned args.
//
// Only non-recursive CTEs are modeled; see [SelectStatement].With.
func (b *SelectBuilder) With(name string, query *SelectBuilder) *SelectBuilder {
	if query == nil {
		return b
	}
	b.with = append(b.with, CommonTableExpression{Name: name, Query: query.Build()})
	return b
}

// InQuery builds "column IN (SELECT …)", reading the candidate values from a
// query rather than from a value list.
func InQuery(column string, sub *SelectBuilder) Expression {
	if sub == nil {
		return nil
	}
	return &InExpr{Operand: &ColumnRef{Name: column}, Subquery: sub.Build()}
}

// Exists builds "EXISTS (SELECT …)".
//
// The subquery's projection does not affect the test; EXISTS stops at the first
// row whatever it selects.
func Exists(sub *SelectBuilder) Expression {
	if sub == nil {
		return nil
	}
	return &ExistsExpr{Query: sub.Build()}
}

// NotExists builds "NOT EXISTS (SELECT …)".
func NotExists(sub *SelectBuilder) Expression {
	if sub == nil {
		return nil
	}
	return &ExistsExpr{Query: sub.Build(), Negated: true}
}

// From sets the source table for the query. It is required; rendering a
// statement without a table returns an error. Calling From clears any alias set
// by a previous FromAs.
func (b *SelectBuilder) From(table string) *SelectBuilder {
	b.from = table
	b.fromAlias = ""
	return b
}

// FromAs sets the source table together with an alias, rendered as
// "table" "alias". Use the alias to qualify columns across joins with Col. An
// empty alias behaves like From.
func (b *SelectBuilder) FromAs(table, alias string) *SelectBuilder {
	b.from = table
	b.fromAlias = alias
	return b
}

// Columns appends result columns to the projection, allowing qualified columns
// built with Col to be mixed with any columns already selected. Appending to an
// otherwise empty projection replaces the implicit "*".
func (b *SelectBuilder) Columns(columns ...Column) *SelectBuilder {
	for _, col := range columns {
		b.columns = append(b.columns, col.resultColumn())
	}
	return b
}

// Distinct renders the query as SELECT DISTINCT, deduplicating result rows.
func (b *SelectBuilder) Distinct() *SelectBuilder {
	b.distinct = true
	return b
}

// Exprs appends expression projection entries — typically aggregates built with
// Count, Sum, and friends — to the SELECT list, in the order given across calls.
// Use ExprAs to attach an output-column alias.
func (b *SelectBuilder) Exprs(exprs ...Expression) *SelectBuilder {
	for _, expr := range exprs {
		b.columns = append(b.columns, ResultColumn{Expr: expr})
	}
	return b
}

// ExprAs appends a single expression projection entry with an output-column
// alias, rendering as `<expr> AS "alias"`. It is the aliased counterpart to
// Exprs, for example ExprAs(query.CountStar(), "n") to project COUNT(*) AS "n".
func (b *SelectBuilder) ExprAs(expr Expression, alias string) *SelectBuilder {
	b.columns = append(b.columns, ResultColumn{Expr: expr, Alias: alias})
	return b
}

// InnerJoin appends an INNER JOIN of table (with an optional alias) on the given
// condition. Build the condition from qualified columns, for example
// Col("o", "user_id").EqCol(Col("u", "id")). An empty alias joins the bare table.
func (b *SelectBuilder) InnerJoin(table, alias string, on Expression) *SelectBuilder {
	return b.join(JoinInner, table, alias, on)
}

// LeftJoin appends a LEFT OUTER JOIN of table (with an optional alias) on the
// given condition.
func (b *SelectBuilder) LeftJoin(table, alias string, on Expression) *SelectBuilder {
	return b.join(JoinLeft, table, alias, on)
}

// RightJoin appends a RIGHT OUTER JOIN of table (with an optional alias) on the
// given condition. RenderSelect rejects a RIGHT JOIN on SQLite.
func (b *SelectBuilder) RightJoin(table, alias string, on Expression) *SelectBuilder {
	return b.join(JoinRight, table, alias, on)
}

// FullJoin appends a FULL OUTER JOIN of table (with an optional alias) on the
// given condition. RenderSelect rejects a FULL OUTER JOIN on SQLite.
func (b *SelectBuilder) FullJoin(table, alias string, on Expression) *SelectBuilder {
	return b.join(JoinFull, table, alias, on)
}

// join appends a join clause of the given type. It is shared by the exported
// per-type join methods.
func (b *SelectBuilder) join(joinType JoinType, table, alias string, on Expression) *SelectBuilder {
	b.joins = append(b.joins, JoinClause{
		Type:  joinType,
		Table: table,
		Alias: alias,
		On:    on,
	})
	return b
}

// Where sets the filter expression. Calling Where again replaces the previous
// expression; compose multiple conditions with And, Or, and Not.
func (b *SelectBuilder) Where(expr Expression) *SelectBuilder {
	b.where = expr
	return b
}

// GroupBy appends GROUP BY columns, in the order given across calls. Build each
// column with Col: Col(table, name) for a qualified column across joins, or
// Col("", name) for a bare column. GROUP BY carries no bound values.
func (b *SelectBuilder) GroupBy(columns ...Column) *SelectBuilder {
	for _, col := range columns {
		b.groupBy = append(b.groupBy, *col.columnRef())
	}
	return b
}

// Having sets the HAVING filter over grouped rows. Its bound values are numbered
// after the WHERE clause. Calling Having again replaces the previous expression;
// compare an aggregate against a bound value with Expr, for example
// Expr(query.CountStar()).Gt(int64(5)), and compose with And, Or, and Not.
func (b *SelectBuilder) Having(expr Expression) *SelectBuilder {
	b.having = expr
	return b
}

// OrderBy appends sort terms, applied in the order given across calls. Use Asc
// and Desc to build terms.
func (b *SelectBuilder) OrderBy(terms ...OrderByClause) *SelectBuilder {
	b.orderBy = append(b.orderBy, terms...)
	return b
}

// Limit sets the maximum number of rows. The value is bound as a parameter, not
// inlined. Setting Limit to 0 emits LIMIT 0 (no rows); omit the call for no
// limit at all.
func (b *SelectBuilder) Limit(n int64) *SelectBuilder {
	b.limit = &n
	return b
}

// Offset sets the number of rows to skip. The value is bound as a parameter.
func (b *SelectBuilder) Offset(n int64) *SelectBuilder {
	b.offset = &n
	return b
}

// Build returns the assembled *SelectStatement. Render it with
// RenderSelect.
func (b *SelectBuilder) Build() *SelectStatement {
	return &SelectStatement{
		With:      b.with,
		Distinct:  b.distinct,
		Columns:   b.columns,
		From:      b.from,
		FromAlias: b.fromAlias,
		Joins:     b.joins,
		Where:     b.where,
		GroupBy:   b.groupBy,
		Having:    b.having,
		OrderBy:   b.orderBy,
		Limit:     b.limit,
		Offset:    b.offset,
	}
}

// comparison builds a "column <op> value" expression with the column as a quoted
// identifier and the value bound as a parameter.
func comparison(column string, op ComparisonOperator, value any) Expression {
	return &Comparison{
		Left:     &ColumnRef{Name: column},
		Operator: op,
		Right:    &BoundValue{Value: value},
	}
}

// Eq builds "column = value".
func Eq(column string, value any) Expression {
	return comparison(column, OpEqual, value)
}

// Ne builds "column <> value".
func Ne(column string, value any) Expression {
	return comparison(column, OpNotEqual, value)
}

// Lt builds "column < value".
func Lt(column string, value any) Expression {
	return comparison(column, OpLessThan, value)
}

// Le builds "column <= value".
func Le(column string, value any) Expression {
	return comparison(column, OpLessThanOrEqual, value)
}

// Gt builds "column > value".
func Gt(column string, value any) Expression {
	return comparison(column, OpGreaterThan, value)
}

// Ge builds "column >= value".
func Ge(column string, value any) Expression {
	return comparison(column, OpGreaterThanOrEqual, value)
}

// Like builds "column LIKE pattern", binding the pattern as a parameter.
//
// The pattern is a VALUE, so it is bound rather than interpolated and cannot
// carry SQL into the statement. Its wildcards are the caller's to write: `%`
// matches any run of characters and `_` any single one, and a caller matching a
// literal `%` or `_` has to escape it themselves -- Ptah cannot tell an intended
// wildcard from an accidental one, and escaping on the caller's behalf would
// break every pattern that meant them.
//
// Case sensitivity is the SERVER's. PostgreSQL matches case-sensitively, MySQL
// follows the column's collation and usually does not, and SQLite folds ASCII.
// A query relying on one behavior is relying on that engine, not on this
// builder (stokaro/ptah#941).
func Like(column, pattern string) Expression {
	return comparison(column, OpLike, pattern)
}

// NotLike builds "column NOT LIKE pattern". See [Like] on wildcards and on
// case sensitivity.
func NotLike(column, pattern string) Expression {
	return comparison(column, OpNotLike, pattern)
}

// Func builds a call to name with args, for the functions this package has no
// named helper for.
//
// The name is a KEYWORD, emitted verbatim and never quoted, and the renderer
// refuses anything that is not a simple identifier -- so a name cannot carry
// SQL. Arguments are ordinary expressions: Col produces a quoted identifier and
// Value a bound placeholder, which is what keeps a caller's data out of the
// statement text (stokaro/ptah#941).
//
//	query.Func("COALESCE", query.ColExpr("nick"), query.Value("anon"))
//	// COALESCE("nick", $1)
func Func(name string, args ...Expression) Expression {
	return &FuncCall{Name: name, Args: slices.Clone(args)}
}

// ColExpr is a column reference usable as a function argument or an arithmetic
// operand.
//
// It is spelled apart from [Col], which builds the Column value a projection or
// GROUP BY term takes. The two are different positions in the grammar and
// giving them one name would let a caller pass the wrong one and find out at
// the type checker rather than at the point of confusion.
func ColExpr(name string) Expression { return &ColumnRef{Name: name} }

// Value is a bound value usable as a function argument or an arithmetic
// operand. It is never interpolated into the statement text.
func Value(v any) Expression { return &BoundValue{Value: v} }

// Add, Sub, Mul, Div and Mod build binary arithmetic expressions.
//
// Each renders parenthesized, so the tree the caller built is the expression
// the server evaluates rather than one its precedence rules recover. See
// [Arithmetic].
func Add(left, right Expression) Expression { return arith(left, OpAdd, right) }

// Sub builds "(left - right)". See [Add].
func Sub(left, right Expression) Expression { return arith(left, OpSubtract, right) }

// Mul builds "(left * right)". See [Add].
func Mul(left, right Expression) Expression { return arith(left, OpMultiply, right) }

// Div builds "(left / right)". See [Add].
func Div(left, right Expression) Expression { return arith(left, OpDivide, right) }

// Mod builds "(left %% right)". Its spelling is portable and its result for a
// negative operand is not; see [OpModulo].
func Mod(left, right Expression) Expression { return arith(left, OpModulo, right) }

func arith(left Expression, op ArithmeticOperator, right Expression) Expression {
	return &Arithmetic{Left: left, Operator: op, Right: right}
}

// Over turns a function call into a window function.
//
// The call is whatever this package builds -- an aggregate such as Sum, or a
// ranking function through Func -- and the spec says which rows it is computed
// over:
//
//	query.Over(query.Sum("total"), query.Partition("user_id"), query.OrderAsc("day"))
//	// SUM("total") OVER (PARTITION BY "user_id" ORDER BY "day" ASC)
//
// A window function belongs in a projection and not in WHERE, because the
// window is computed after the rows are filtered. The engines say so clearly in
// their own terms, so this package does not add a rule of its own
// (stokaro/ptah#941).
//
// No frame clause is emitted. Without one the engine applies its default, which
// is what an unframed window means everywhere; guessing a frame would change
// results.
func Over(call Expression, options ...WindowOption) Expression {
	fn, ok := call.(*FuncCall)
	if !ok {
		return call
	}
	spec := &WindowSpec{}
	for _, option := range options {
		option(spec)
	}
	fn.Over = spec
	return fn
}

// WindowOption configures the OVER clause built by [Over].
type WindowOption func(*WindowSpec)

// Partition adds PARTITION BY columns to a window.
func Partition(columns ...string) WindowOption {
	return func(spec *WindowSpec) {
		for _, name := range columns {
			spec.PartitionBy = append(spec.PartitionBy, ColumnRef{Name: name})
		}
	}
}

// OrderAsc adds ascending ORDER BY terms to a window.
func OrderAsc(columns ...string) WindowOption {
	return windowOrder(SortAscending, columns)
}

// OrderDesc adds descending ORDER BY terms to a window.
func OrderDesc(columns ...string) WindowOption {
	return windowOrder(SortDescending, columns)
}

func windowOrder(direction SortDirection, columns []string) WindowOption {
	return func(spec *WindowSpec) {
		for _, name := range columns {
			spec.OrderBy = append(spec.OrderBy, OrderByClause{Column: name, Direction: direction})
		}
	}
}

// In builds "column IN (values...)", binding each value as a parameter. The
// values slice must be non-empty; rendering an empty IN returns an error. The
// generic element type lets callers pass, for example, []string or []int64
// without converting to []any.
func In[T any](column string, values []T) Expression {
	bound := make([]Expression, len(values))
	for i, v := range values {
		bound[i] = &BoundValue{Value: v}
	}
	return &InExpr{
		Operand: &ColumnRef{Name: column},
		Values:  bound,
	}
}

// IsNull builds "column IS NULL".
func IsNull(column string) Expression {
	return &NullTest{Operand: &ColumnRef{Name: column}}
}

// IsNotNull builds "column IS NOT NULL".
func IsNotNull(column string) Expression {
	return &NullTest{Operand: &ColumnRef{Name: column}, Negated: true}
}

// And combines expressions with AND. The renderer parenthesizes the result, so
// it composes safely inside Or and Not.
func And(exprs ...Expression) Expression {
	return &LogicalExpr{Operator: LogicalAnd, Operands: exprs}
}

// Or combines expressions with OR. The renderer parenthesizes the result, so it
// composes safely inside And and Not.
func Or(exprs ...Expression) Expression {
	return &LogicalExpr{Operator: LogicalOr, Operands: exprs}
}

// Not negates an expression, rendering as "NOT (expr)".
func Not(expr Expression) Expression {
	return &NotExpr{Operand: expr}
}

// aggregate builds a single-argument aggregate function call over a bare column,
// as in SUM("column"). It backs the bare-column aggregate constructors.
func aggregate(name, column string) Expression {
	return &FuncCall{Name: name, Args: []Expression{&ColumnRef{Name: column}}}
}

// CountStar builds the COUNT(*) aggregate, counting all rows. It is usable in a
// projection (via Exprs or ExprAs) and in a HAVING comparison (via Expr).
func CountStar() Expression {
	return &FuncCall{Name: "COUNT", Star: true}
}

// Count builds COUNT("column"), counting the non-null values of a bare column.
// As a convenience, Count("*") is COUNT(*) — identical to CountStar — rather than
// the invalid COUNT("*") over a column literally named "*". Use
// Col(table, name).Count for a qualified column, and CountStar as the primary way
// to count all rows.
func Count(column string) Expression {
	if strings.TrimSpace(column) == "*" {
		return CountStar()
	}
	return aggregate("COUNT", column)
}

// CountDistinct builds COUNT(DISTINCT "column") over a bare column. Use
// Col(table, name).CountDistinct for a qualified column.
func CountDistinct(column string) Expression {
	return &FuncCall{Name: "COUNT", Args: []Expression{&ColumnRef{Name: column}}, Distinct: true}
}

// Sum builds SUM("column") over a bare column.
func Sum(column string) Expression { return aggregate("SUM", column) }

// Avg builds AVG("column") over a bare column.
func Avg(column string) Expression { return aggregate("AVG", column) }

// Min builds MIN("column") over a bare column.
func Min(column string) Expression { return aggregate("MIN", column) }

// Max builds MAX("column") over a bare column.
func Max(column string) Expression { return aggregate("MAX", column) }

// ExprCompare adapts an expression — typically an aggregate produced by Count,
// Sum, and friends — so it can be compared against a bound value, the usual shape
// of a HAVING predicate. Build one with Expr. It mirrors the comparison helpers
// on Column, but its left operand is any expression rather than a column.
type ExprCompare struct {
	left Expression
}

// Expr wraps an expression so it can be compared against a bound value with the
// methods on ExprCompare, for example Expr(query.CountStar()).Gt(int64(5)) as a
// HAVING predicate. The result composes with And, Or, and Not like any other
// expression.
func Expr(left Expression) ExprCompare {
	return ExprCompare{left: left}
}

// compare builds "expr <op> value" with the value bound as a parameter.
func (e ExprCompare) compare(op ComparisonOperator, value any) Expression {
	return &Comparison{Left: e.left, Operator: op, Right: &BoundValue{Value: value}}
}

// Eq builds "expr = value", binding value as a parameter.
func (e ExprCompare) Eq(value any) Expression { return e.compare(OpEqual, value) }

// Ne builds "expr <> value", binding value as a parameter.
func (e ExprCompare) Ne(value any) Expression { return e.compare(OpNotEqual, value) }

// Lt builds "expr < value", binding value as a parameter.
func (e ExprCompare) Lt(value any) Expression { return e.compare(OpLessThan, value) }

// Le builds "expr <= value", binding value as a parameter.
func (e ExprCompare) Le(value any) Expression { return e.compare(OpLessThanOrEqual, value) }

// Gt builds "expr > value", binding value as a parameter.
func (e ExprCompare) Gt(value any) Expression { return e.compare(OpGreaterThan, value) }

// Ge builds "expr >= value", binding value as a parameter.
func (e ExprCompare) Ge(value any) Expression { return e.compare(OpGreaterThanOrEqual, value) }

// Asc builds an ascending ORDER BY term for column.
func Asc(column string) OrderByClause {
	return OrderByClause{Column: column, Direction: SortAscending}
}

// Desc builds a descending ORDER BY term for column.
func Desc(column string) OrderByClause {
	return OrderByClause{Column: column, Direction: SortDescending}
}

// Column is a reference to a column, optionally qualified by a table name or
// alias. It is the entry point for the qualified-column API used with joins:
// build one with Col, then turn it into a projection entry (via the builder's
// Columns method), a comparison, a null test, or an ORDER BY term.
//
// The value-oriented Phase 1 helpers (Eq, In, Asc, and so on) that take a bare
// column name still work unchanged for unqualified columns; Column adds
// qualified-column support alongside them, it does not replace them. A qualifier
// and name are always emitted as separately quoted identifiers, never inlined.
type Column struct {
	qualifier string
	name      string
}

// Col returns a column qualified by a table name or alias, rendering as
// "qualifier"."name". Use it to disambiguate columns across joined tables. An
// empty qualifier yields a bare, unqualified column.
func Col(qualifier, name string) Column {
	return Column{qualifier: qualifier, name: name}
}

// columnRef returns the AST node for the column, used as a comparison operand.
func (c Column) columnRef() *ColumnRef {
	return &ColumnRef{Qualifier: c.qualifier, Name: c.name}
}

// resultColumn returns the projection entry for the column.
func (c Column) resultColumn() ResultColumn {
	return ResultColumn{Qualifier: c.qualifier, Name: c.name}
}

// compareValue builds "column <op> value" with the value bound as a parameter.
func (c Column) compareValue(op ComparisonOperator, value any) Expression {
	return &Comparison{
		Left:     c.columnRef(),
		Operator: op,
		Right:    &BoundValue{Value: value},
	}
}

// Eq builds "column = value", binding value as a parameter.
func (c Column) Eq(value any) Expression { return c.compareValue(OpEqual, value) }

// Ne builds "column <> value", binding value as a parameter.
func (c Column) Ne(value any) Expression { return c.compareValue(OpNotEqual, value) }

// Lt builds "column < value", binding value as a parameter.
func (c Column) Lt(value any) Expression { return c.compareValue(OpLessThan, value) }

// Le builds "column <= value", binding value as a parameter.
func (c Column) Le(value any) Expression { return c.compareValue(OpLessThanOrEqual, value) }

// Gt builds "column > value", binding value as a parameter.
func (c Column) Gt(value any) Expression { return c.compareValue(OpGreaterThan, value) }

// Ge builds "column >= value", binding value as a parameter.
func (c Column) Ge(value any) Expression { return c.compareValue(OpGreaterThanOrEqual, value) }

// EqCol builds "column = other", comparing two columns rather than a column and
// a bound value. It is the common equi-join predicate for a join ON condition.
func (c Column) EqCol(other Column) Expression {
	return &Comparison{
		Left:     c.columnRef(),
		Operator: OpEqual,
		Right:    other.columnRef(),
	}
}

// IsNull builds "column IS NULL".
func (c Column) IsNull() Expression {
	return &NullTest{Operand: c.columnRef()}
}

// IsNotNull builds "column IS NOT NULL".
func (c Column) IsNotNull() Expression {
	return &NullTest{Operand: c.columnRef(), Negated: true}
}

// Asc builds an ascending ORDER BY term for the column.
func (c Column) Asc() OrderByClause {
	return OrderByClause{Qualifier: c.qualifier, Column: c.name, Direction: SortAscending}
}

// Desc builds a descending ORDER BY term for the column.
func (c Column) Desc() OrderByClause {
	return OrderByClause{Qualifier: c.qualifier, Column: c.name, Direction: SortDescending}
}

// aggregate builds a single-argument aggregate function call over the qualified
// column, as in SUM("u"."total"). It backs the qualified-column aggregate
// methods.
func (c Column) aggregate(name string) Expression {
	return &FuncCall{Name: name, Args: []Expression{c.columnRef()}}
}

// Count builds COUNT over the qualified column, as in COUNT("u"."id").
func (c Column) Count() Expression { return c.aggregate("COUNT") }

// CountDistinct builds COUNT(DISTINCT …) over the qualified column, as in
// COUNT(DISTINCT "u"."id").
func (c Column) CountDistinct() Expression {
	return &FuncCall{Name: "COUNT", Args: []Expression{c.columnRef()}, Distinct: true}
}

// Sum builds SUM over the qualified column, as in SUM("o"."total").
func (c Column) Sum() Expression { return c.aggregate("SUM") }

// Avg builds AVG over the qualified column.
func (c Column) Avg() Expression { return c.aggregate("AVG") }

// Min builds MIN over the qualified column.
func (c Column) Min() Expression { return c.aggregate("MIN") }

// Max builds MAX over the qualified column.
func (c Column) Max() Expression { return c.aggregate("MAX") }
