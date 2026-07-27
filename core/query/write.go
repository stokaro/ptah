package query

import "github.com/stokaro/ptah/core/ast"

// This file adds the write-side builders — INSERT, UPDATE, and DELETE — alongside
// the SELECT builder. They share the SELECT builder's conventions: each method
// mutates and returns the same builder for chaining, values passed as any are
// wrapped as bound parameters (never inlined), identifiers stay identifiers, and
// Build produces a plain *ast node that renderer.RenderInsert / RenderUpdate /
// RenderDelete turns into dialect SQL plus its positional arguments. Validation
// of degenerate input (no rows, ragged rows, empty SET, a WHERE-less whole-table
// mutation) happens at render time, mirroring the SELECT builder, so Build never
// fails.

// InsertBuilder builds an *ast.InsertStatement through a fluent, chainable API.
// Start with InsertInto. A builder is not safe for concurrent use.
type InsertBuilder struct {
	table     string
	columns   []string
	rows      [][]ast.Expression
	returning []ast.ColumnRef
}

// InsertInto starts an INSERT INTO the given table. Follow it with Columns to
// declare the column list and one or more Values calls to add rows.
func InsertInto(table string) *InsertBuilder {
	return &InsertBuilder{table: table}
}

// Columns declares (or extends) the inserted column list. Each name is rendered
// as a dialect-quoted identifier. Every row added with Values must supply exactly
// one value per column; a mismatch is rejected at render time.
func (b *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	b.columns = append(b.columns, columns...)
	return b
}

// Values appends one row of values, each bound as a parameter. Call Values once
// per row for a multi-row INSERT; rows render in call order and their values are
// numbered left to right, row by row. Passing nil as a value binds SQL NULL.
func (b *InsertBuilder) Values(values ...any) *InsertBuilder {
	row := make([]ast.Expression, len(values))
	for i, v := range values {
		row[i] = &ast.BoundValue{Value: v}
	}
	b.rows = append(b.rows, row)
	return b
}

// Returning adds columns to the RETURNING clause, projecting them from the
// inserted rows. RETURNING renders only on the PostgreSQL family and SQLite;
// RenderInsert rejects it on MySQL and MariaDB.
func (b *InsertBuilder) Returning(columns ...string) *InsertBuilder {
	b.returning = appendReturning(b.returning, columns)
	return b
}

// Build returns the assembled *ast.InsertStatement. Render it with
// renderer.RenderInsert.
func (b *InsertBuilder) Build() *ast.InsertStatement {
	return &ast.InsertStatement{
		Table:     b.table,
		Columns:   b.columns,
		Rows:      b.rows,
		Returning: b.returning,
	}
}

// UpdateBuilder builds an *ast.UpdateStatement through a fluent, chainable API.
// Start with Update. A builder is not safe for concurrent use.
type UpdateBuilder struct {
	table         string
	set           []ast.Assignment
	where         ast.Expression
	unconditional bool
	returning     []ast.ColumnRef
}

// Update starts an UPDATE of the given table. Add assignments with Set and a
// filter with Where.
func Update(table string) *UpdateBuilder {
	return &UpdateBuilder{table: table}
}

// Set appends a "column = value" assignment, binding value as a parameter.
// Assignments render in call order, and their values are numbered before any
// WHERE value. Passing nil sets the column to SQL NULL.
func (b *UpdateBuilder) Set(column string, value any) *UpdateBuilder {
	b.set = append(b.set, ast.Assignment{Column: column, Value: &ast.BoundValue{Value: value}})
	return b
}

// Where sets the filter expression. Calling Where again replaces the previous
// expression; compose multiple conditions with And, Or, and Not.
func (b *UpdateBuilder) Where(expr ast.Expression) *UpdateBuilder {
	b.where = expr
	return b
}

// Unconditional opts in to updating every row. It is required when no Where is
// set: RenderUpdate rejects a WHERE-less UPDATE unless the statement is marked
// unconditional, so a whole-table update is always deliberate. It has no effect
// when a Where is present.
func (b *UpdateBuilder) Unconditional() *UpdateBuilder {
	b.unconditional = true
	return b
}

// Returning adds columns to the RETURNING clause. See InsertBuilder.Returning for
// the dialect rules.
func (b *UpdateBuilder) Returning(columns ...string) *UpdateBuilder {
	b.returning = appendReturning(b.returning, columns)
	return b
}

// Build returns the assembled *ast.UpdateStatement. Render it with
// renderer.RenderUpdate.
func (b *UpdateBuilder) Build() *ast.UpdateStatement {
	return &ast.UpdateStatement{
		Table:         b.table,
		Set:           b.set,
		Where:         b.where,
		Unconditional: b.unconditional,
		Returning:     b.returning,
	}
}

// DeleteBuilder builds an *ast.DeleteStatement through a fluent, chainable API.
// Start with DeleteFrom. A builder is not safe for concurrent use.
type DeleteBuilder struct {
	table         string
	where         ast.Expression
	unconditional bool
	returning     []ast.ColumnRef
}

// DeleteFrom starts a DELETE FROM the given table. Add a filter with Where.
func DeleteFrom(table string) *DeleteBuilder {
	return &DeleteBuilder{table: table}
}

// Where sets the filter expression. Calling Where again replaces the previous
// expression; compose multiple conditions with And, Or, and Not.
func (b *DeleteBuilder) Where(expr ast.Expression) *DeleteBuilder {
	b.where = expr
	return b
}

// Unconditional opts in to deleting every row. It is required when no Where is
// set, exactly as on UpdateBuilder.
func (b *DeleteBuilder) Unconditional() *DeleteBuilder {
	b.unconditional = true
	return b
}

// Returning adds columns to the RETURNING clause. See InsertBuilder.Returning for
// the dialect rules.
func (b *DeleteBuilder) Returning(columns ...string) *DeleteBuilder {
	b.returning = appendReturning(b.returning, columns)
	return b
}

// Build returns the assembled *ast.DeleteStatement. Render it with
// renderer.RenderDelete.
func (b *DeleteBuilder) Build() *ast.DeleteStatement {
	return &ast.DeleteStatement{
		Table:         b.table,
		Where:         b.where,
		Unconditional: b.unconditional,
		Returning:     b.returning,
	}
}

// appendReturning appends bare column names as RETURNING projection entries. It
// backs the Returning method on all three write builders.
func appendReturning(dst []ast.ColumnRef, columns []string) []ast.ColumnRef {
	for _, name := range columns {
		dst = append(dst, ast.ColumnRef{Name: name})
	}
	return dst
}
