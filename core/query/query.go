package query

import (
	"strings"

	"github.com/stokaro/ptah/core/ast"
)

// SelectBuilder builds an *ast.SelectStatement through a fluent, chainable API.
//
// A zero SelectBuilder is not meant to be used directly; start with Select. Each
// method mutates and returns the same builder for chaining. Build produces the
// statement. Builders are not safe for concurrent use.
type SelectBuilder struct {
	distinct  bool
	columns   []ast.ResultColumn
	from      string
	fromAlias string
	joins     []ast.JoinClause
	where     ast.Expression
	groupBy   []ast.ColumnRef
	having    ast.Expression
	orderBy   []ast.OrderByClause
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
			b.columns = append(b.columns, ast.ResultColumn{Star: true})
			continue
		}
		b.columns = append(b.columns, ast.ResultColumn{Name: name})
	}
	return b
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
func (b *SelectBuilder) Exprs(exprs ...ast.Expression) *SelectBuilder {
	for _, expr := range exprs {
		b.columns = append(b.columns, ast.ResultColumn{Expr: expr})
	}
	return b
}

// ExprAs appends a single expression projection entry with an output-column
// alias, rendering as `<expr> AS "alias"`. It is the aliased counterpart to
// Exprs, for example ExprAs(query.CountStar(), "n") to project COUNT(*) AS "n".
func (b *SelectBuilder) ExprAs(expr ast.Expression, alias string) *SelectBuilder {
	b.columns = append(b.columns, ast.ResultColumn{Expr: expr, Alias: alias})
	return b
}

// InnerJoin appends an INNER JOIN of table (with an optional alias) on the given
// condition. Build the condition from qualified columns, for example
// Col("o", "user_id").EqCol(Col("u", "id")). An empty alias joins the bare table.
func (b *SelectBuilder) InnerJoin(table, alias string, on ast.Expression) *SelectBuilder {
	return b.join(ast.JoinInner, table, alias, on)
}

// LeftJoin appends a LEFT OUTER JOIN of table (with an optional alias) on the
// given condition.
func (b *SelectBuilder) LeftJoin(table, alias string, on ast.Expression) *SelectBuilder {
	return b.join(ast.JoinLeft, table, alias, on)
}

// RightJoin appends a RIGHT OUTER JOIN of table (with an optional alias) on the
// given condition. RenderSelect rejects a RIGHT JOIN on SQLite.
func (b *SelectBuilder) RightJoin(table, alias string, on ast.Expression) *SelectBuilder {
	return b.join(ast.JoinRight, table, alias, on)
}

// FullJoin appends a FULL OUTER JOIN of table (with an optional alias) on the
// given condition. RenderSelect rejects a FULL OUTER JOIN on SQLite.
func (b *SelectBuilder) FullJoin(table, alias string, on ast.Expression) *SelectBuilder {
	return b.join(ast.JoinFull, table, alias, on)
}

// join appends a join clause of the given type. It is shared by the exported
// per-type join methods.
func (b *SelectBuilder) join(joinType ast.JoinType, table, alias string, on ast.Expression) *SelectBuilder {
	b.joins = append(b.joins, ast.JoinClause{
		Type:  joinType,
		Table: table,
		Alias: alias,
		On:    on,
	})
	return b
}

// Where sets the filter expression. Calling Where again replaces the previous
// expression; compose multiple conditions with And, Or, and Not.
func (b *SelectBuilder) Where(expr ast.Expression) *SelectBuilder {
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
func (b *SelectBuilder) Having(expr ast.Expression) *SelectBuilder {
	b.having = expr
	return b
}

// OrderBy appends sort terms, applied in the order given across calls. Use Asc
// and Desc to build terms.
func (b *SelectBuilder) OrderBy(terms ...ast.OrderByClause) *SelectBuilder {
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

// Build returns the assembled *ast.SelectStatement. Render it with
// renderer.RenderSelect.
func (b *SelectBuilder) Build() *ast.SelectStatement {
	return &ast.SelectStatement{
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
func comparison(column string, op ast.ComparisonOperator, value any) ast.Expression {
	return &ast.Comparison{
		Left:     &ast.ColumnRef{Name: column},
		Operator: op,
		Right:    &ast.BoundValue{Value: value},
	}
}

// Eq builds "column = value".
func Eq(column string, value any) ast.Expression {
	return comparison(column, ast.OpEqual, value)
}

// Ne builds "column <> value".
func Ne(column string, value any) ast.Expression {
	return comparison(column, ast.OpNotEqual, value)
}

// Lt builds "column < value".
func Lt(column string, value any) ast.Expression {
	return comparison(column, ast.OpLessThan, value)
}

// Le builds "column <= value".
func Le(column string, value any) ast.Expression {
	return comparison(column, ast.OpLessThanOrEqual, value)
}

// Gt builds "column > value".
func Gt(column string, value any) ast.Expression {
	return comparison(column, ast.OpGreaterThan, value)
}

// Ge builds "column >= value".
func Ge(column string, value any) ast.Expression {
	return comparison(column, ast.OpGreaterThanOrEqual, value)
}

// In builds "column IN (values...)", binding each value as a parameter. The
// values slice must be non-empty; rendering an empty IN returns an error. The
// generic element type lets callers pass, for example, []string or []int64
// without converting to []any.
func In[T any](column string, values []T) ast.Expression {
	bound := make([]ast.Expression, len(values))
	for i, v := range values {
		bound[i] = &ast.BoundValue{Value: v}
	}
	return &ast.InExpr{
		Operand: &ast.ColumnRef{Name: column},
		Values:  bound,
	}
}

// IsNull builds "column IS NULL".
func IsNull(column string) ast.Expression {
	return &ast.NullTest{Operand: &ast.ColumnRef{Name: column}}
}

// IsNotNull builds "column IS NOT NULL".
func IsNotNull(column string) ast.Expression {
	return &ast.NullTest{Operand: &ast.ColumnRef{Name: column}, Negated: true}
}

// And combines expressions with AND. The renderer parenthesizes the result, so
// it composes safely inside Or and Not.
func And(exprs ...ast.Expression) ast.Expression {
	return &ast.LogicalExpr{Operator: ast.LogicalAnd, Operands: exprs}
}

// Or combines expressions with OR. The renderer parenthesizes the result, so it
// composes safely inside And and Not.
func Or(exprs ...ast.Expression) ast.Expression {
	return &ast.LogicalExpr{Operator: ast.LogicalOr, Operands: exprs}
}

// Not negates an expression, rendering as "NOT (expr)".
func Not(expr ast.Expression) ast.Expression {
	return &ast.NotExpr{Operand: expr}
}

// aggregate builds a single-argument aggregate function call over a bare column,
// as in SUM("column"). It backs the bare-column aggregate constructors.
func aggregate(name, column string) ast.Expression {
	return &ast.FuncCall{Name: name, Args: []ast.Expression{&ast.ColumnRef{Name: column}}}
}

// CountStar builds the COUNT(*) aggregate, counting all rows. It is usable in a
// projection (via Exprs or ExprAs) and in a HAVING comparison (via Expr).
func CountStar() ast.Expression {
	return &ast.FuncCall{Name: "COUNT", Star: true}
}

// Count builds COUNT("column"), counting the non-null values of a bare column.
// As a convenience, Count("*") is COUNT(*) — identical to CountStar — rather than
// the invalid COUNT("*") over a column literally named "*". Use
// Col(table, name).Count for a qualified column, and CountStar as the primary way
// to count all rows.
func Count(column string) ast.Expression {
	if strings.TrimSpace(column) == "*" {
		return CountStar()
	}
	return aggregate("COUNT", column)
}

// CountDistinct builds COUNT(DISTINCT "column") over a bare column. Use
// Col(table, name).CountDistinct for a qualified column.
func CountDistinct(column string) ast.Expression {
	return &ast.FuncCall{Name: "COUNT", Args: []ast.Expression{&ast.ColumnRef{Name: column}}, Distinct: true}
}

// Sum builds SUM("column") over a bare column.
func Sum(column string) ast.Expression { return aggregate("SUM", column) }

// Avg builds AVG("column") over a bare column.
func Avg(column string) ast.Expression { return aggregate("AVG", column) }

// Min builds MIN("column") over a bare column.
func Min(column string) ast.Expression { return aggregate("MIN", column) }

// Max builds MAX("column") over a bare column.
func Max(column string) ast.Expression { return aggregate("MAX", column) }

// ExprCompare adapts an expression — typically an aggregate produced by Count,
// Sum, and friends — so it can be compared against a bound value, the usual shape
// of a HAVING predicate. Build one with Expr. It mirrors the comparison helpers
// on Column, but its left operand is any expression rather than a column.
type ExprCompare struct {
	left ast.Expression
}

// Expr wraps an expression so it can be compared against a bound value with the
// methods on ExprCompare, for example Expr(query.CountStar()).Gt(int64(5)) as a
// HAVING predicate. The result composes with And, Or, and Not like any other
// expression.
func Expr(left ast.Expression) ExprCompare {
	return ExprCompare{left: left}
}

// compare builds "expr <op> value" with the value bound as a parameter.
func (e ExprCompare) compare(op ast.ComparisonOperator, value any) ast.Expression {
	return &ast.Comparison{Left: e.left, Operator: op, Right: &ast.BoundValue{Value: value}}
}

// Eq builds "expr = value", binding value as a parameter.
func (e ExprCompare) Eq(value any) ast.Expression { return e.compare(ast.OpEqual, value) }

// Ne builds "expr <> value", binding value as a parameter.
func (e ExprCompare) Ne(value any) ast.Expression { return e.compare(ast.OpNotEqual, value) }

// Lt builds "expr < value", binding value as a parameter.
func (e ExprCompare) Lt(value any) ast.Expression { return e.compare(ast.OpLessThan, value) }

// Le builds "expr <= value", binding value as a parameter.
func (e ExprCompare) Le(value any) ast.Expression { return e.compare(ast.OpLessThanOrEqual, value) }

// Gt builds "expr > value", binding value as a parameter.
func (e ExprCompare) Gt(value any) ast.Expression { return e.compare(ast.OpGreaterThan, value) }

// Ge builds "expr >= value", binding value as a parameter.
func (e ExprCompare) Ge(value any) ast.Expression { return e.compare(ast.OpGreaterThanOrEqual, value) }

// Asc builds an ascending ORDER BY term for column.
func Asc(column string) ast.OrderByClause {
	return ast.OrderByClause{Column: column, Direction: ast.SortAscending}
}

// Desc builds a descending ORDER BY term for column.
func Desc(column string) ast.OrderByClause {
	return ast.OrderByClause{Column: column, Direction: ast.SortDescending}
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
func (c Column) columnRef() *ast.ColumnRef {
	return &ast.ColumnRef{Qualifier: c.qualifier, Name: c.name}
}

// resultColumn returns the projection entry for the column.
func (c Column) resultColumn() ast.ResultColumn {
	return ast.ResultColumn{Qualifier: c.qualifier, Name: c.name}
}

// compareValue builds "column <op> value" with the value bound as a parameter.
func (c Column) compareValue(op ast.ComparisonOperator, value any) ast.Expression {
	return &ast.Comparison{
		Left:     c.columnRef(),
		Operator: op,
		Right:    &ast.BoundValue{Value: value},
	}
}

// Eq builds "column = value", binding value as a parameter.
func (c Column) Eq(value any) ast.Expression { return c.compareValue(ast.OpEqual, value) }

// Ne builds "column <> value", binding value as a parameter.
func (c Column) Ne(value any) ast.Expression { return c.compareValue(ast.OpNotEqual, value) }

// Lt builds "column < value", binding value as a parameter.
func (c Column) Lt(value any) ast.Expression { return c.compareValue(ast.OpLessThan, value) }

// Le builds "column <= value", binding value as a parameter.
func (c Column) Le(value any) ast.Expression { return c.compareValue(ast.OpLessThanOrEqual, value) }

// Gt builds "column > value", binding value as a parameter.
func (c Column) Gt(value any) ast.Expression { return c.compareValue(ast.OpGreaterThan, value) }

// Ge builds "column >= value", binding value as a parameter.
func (c Column) Ge(value any) ast.Expression { return c.compareValue(ast.OpGreaterThanOrEqual, value) }

// EqCol builds "column = other", comparing two columns rather than a column and
// a bound value. It is the common equi-join predicate for a join ON condition.
func (c Column) EqCol(other Column) ast.Expression {
	return &ast.Comparison{
		Left:     c.columnRef(),
		Operator: ast.OpEqual,
		Right:    other.columnRef(),
	}
}

// IsNull builds "column IS NULL".
func (c Column) IsNull() ast.Expression {
	return &ast.NullTest{Operand: c.columnRef()}
}

// IsNotNull builds "column IS NOT NULL".
func (c Column) IsNotNull() ast.Expression {
	return &ast.NullTest{Operand: c.columnRef(), Negated: true}
}

// Asc builds an ascending ORDER BY term for the column.
func (c Column) Asc() ast.OrderByClause {
	return ast.OrderByClause{Qualifier: c.qualifier, Column: c.name, Direction: ast.SortAscending}
}

// Desc builds a descending ORDER BY term for the column.
func (c Column) Desc() ast.OrderByClause {
	return ast.OrderByClause{Qualifier: c.qualifier, Column: c.name, Direction: ast.SortDescending}
}

// aggregate builds a single-argument aggregate function call over the qualified
// column, as in SUM("u"."total"). It backs the qualified-column aggregate
// methods.
func (c Column) aggregate(name string) ast.Expression {
	return &ast.FuncCall{Name: name, Args: []ast.Expression{c.columnRef()}}
}

// Count builds COUNT over the qualified column, as in COUNT("u"."id").
func (c Column) Count() ast.Expression { return c.aggregate("COUNT") }

// CountDistinct builds COUNT(DISTINCT …) over the qualified column, as in
// COUNT(DISTINCT "u"."id").
func (c Column) CountDistinct() ast.Expression {
	return &ast.FuncCall{Name: "COUNT", Args: []ast.Expression{c.columnRef()}, Distinct: true}
}

// Sum builds SUM over the qualified column, as in SUM("o"."total").
func (c Column) Sum() ast.Expression { return c.aggregate("SUM") }

// Avg builds AVG over the qualified column.
func (c Column) Avg() ast.Expression { return c.aggregate("AVG") }

// Min builds MIN over the qualified column.
func (c Column) Min() ast.Expression { return c.aggregate("MIN") }

// Max builds MAX over the qualified column.
func (c Column) Max() ast.Expression { return c.aggregate("MAX") }
