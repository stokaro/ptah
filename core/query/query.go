package query

import (
	"github.com/stokaro/ptah/core/ast"
)

// SelectBuilder builds an *ast.SelectStatement through a fluent, chainable API.
//
// A zero SelectBuilder is not meant to be used directly; start with Select. Each
// method mutates and returns the same builder for chaining. Build produces the
// statement. Builders are not safe for concurrent use.
type SelectBuilder struct {
	columns []ast.ResultColumn
	from    string
	where   ast.Expression
	orderBy []ast.OrderByClause
	limit   *int64
	offset  *int64
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

// From sets the single source table for the query. It is required; rendering a
// statement without a table returns an error.
func (b *SelectBuilder) From(table string) *SelectBuilder {
	b.from = table
	return b
}

// Where sets the filter expression. Calling Where again replaces the previous
// expression; compose multiple conditions with And, Or, and Not.
func (b *SelectBuilder) Where(expr ast.Expression) *SelectBuilder {
	b.where = expr
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
		Columns: b.columns,
		From:    b.from,
		Where:   b.where,
		OrderBy: b.orderBy,
		Limit:   b.limit,
		Offset:  b.offset,
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

// Asc builds an ascending ORDER BY term for column.
func Asc(column string) ast.OrderByClause {
	return ast.OrderByClause{Column: column, Direction: ast.SortAscending}
}

// Desc builds a descending ORDER BY term for column.
func Desc(column string) ast.OrderByClause {
	return ast.OrderByClause{Column: column, Direction: ast.SortDescending}
}
