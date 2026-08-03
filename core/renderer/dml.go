package renderer

import (
	"errors"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
)

// This file renders the write-side DML statements — INSERT, UPDATE, and DELETE —
// to parameterized SQL. It reuses the SELECT renderer's parameter machinery
// (selectRenderer in select.go) so that the whole family shares one placeholder
// numbering pass, one identifier-quoting path, and one expression renderer. A
// value can therefore never become an identifier, an identifier can never break
// out of its quotes, and placeholder numbering is a single left-to-right pass —
// INSERT binds row by row; UPDATE binds every SET value before any WHERE value;
// DELETE binds its WHERE values — so callers never manage indices by hand.

// RenderInsert renders an INSERT statement to parameterized SQL for the given
// dialect, returning the SQL text and its positional arguments.
//
// Every value in every row is emitted as a placeholder and returned in args,
// never interpolated; placeholders are numbered row by row, left to right, in the
// dialect's style ($1, $2, … for the PostgreSQL family; ? for MySQL, MariaDB, and
// SQLite). The table and column names are quoted for the dialect. An optional
// RETURNING clause is emitted only where the dialect supports it (see
// renderReturning); on MySQL and MariaDB a non-empty RETURNING is an error.
//
// It returns an error for a nil statement, an unsupported dialect, a missing
// table, an empty column list, an empty row list, or a row whose length does not
// match the column count.
func RenderInsert(stmt *ast.InsertStatement, dialect string) (string, []any, error) {
	if stmt == nil {
		return "", nil, errors.New("renderer: nil insert statement")
	}
	r, err := newWriteRenderer(dialect, "INSERT")
	if err != nil {
		return "", nil, err
	}
	if err := r.renderInsert(stmt); err != nil {
		return "", nil, err
	}
	return r.buf.String(), r.args, nil
}

// RenderUpdate renders an UPDATE statement to parameterized SQL for the given
// dialect, returning the SQL text and its positional arguments.
//
// SET values are bound before WHERE values, matching emission order, so
// placeholder numbering is a single left-to-right pass. Table and column names
// are quoted; every value is bound. An optional RETURNING clause is emitted only
// where the dialect supports it.
//
// A whole-table UPDATE is a footgun, so a statement with no WHERE clause is
// rejected unless it is explicitly marked Unconditional. It also returns an error
// for a nil statement, an unsupported dialect, a missing table, an empty SET
// list, an assignment with an empty column or nil value, or a RETURNING clause on
// a dialect that cannot execute one.
func RenderUpdate(stmt *ast.UpdateStatement, dialect string) (string, []any, error) {
	if stmt == nil {
		return "", nil, errors.New("renderer: nil update statement")
	}
	r, err := newWriteRenderer(dialect, "UPDATE")
	if err != nil {
		return "", nil, err
	}
	if err := r.renderUpdate(stmt); err != nil {
		return "", nil, err
	}
	return r.buf.String(), r.args, nil
}

// RenderDelete renders a DELETE statement to parameterized SQL for the given
// dialect, returning the SQL text and its positional arguments.
//
// The table name is quoted and every WHERE value is bound. An optional RETURNING
// clause is emitted only where the dialect supports it.
//
// As with UPDATE, a WHERE-less whole-table DELETE is rejected unless the
// statement is explicitly marked Unconditional. It also returns an error for a
// nil statement, an unsupported dialect, a missing table, or a RETURNING clause
// on a dialect that cannot execute one.
func RenderDelete(stmt *ast.DeleteStatement, dialect string) (string, []any, error) {
	if stmt == nil {
		return "", nil, errors.New("renderer: nil delete statement")
	}
	r, err := newWriteRenderer(dialect, "DELETE")
	if err != nil {
		return "", nil, err
	}
	if err := r.renderDelete(stmt); err != nil {
		return "", nil, err
	}
	return r.buf.String(), r.args, nil
}

// newWriteRenderer builds a parameter renderer for a write statement, reusing the
// SELECT renderer's dialect resolution and placeholder numbering (bind),
// identifier quoting (quote / writeQualifiedIdent / writeTableRef), and
// expression rendering (renderExpr). kind names the statement in the
// unsupported-dialect error.
func newWriteRenderer(dialect, kind string) (*selectRenderer, error) {
	normalized := platform.NormalizeDialect(dialect)
	style, ok := selectPlaceholderStyle(normalized)
	if !ok {
		return nil, fmt.Errorf("renderer: %s rendering is not supported for dialect %q", kind, dialect)
	}
	return &selectRenderer{dialect: normalized, placeholder: style}, nil
}

func (r *selectRenderer) renderInsert(stmt *ast.InsertStatement) error {
	if strings.TrimSpace(stmt.Table) == "" {
		return errors.New("renderer: insert statement requires a table")
	}
	if len(stmt.Columns) == 0 {
		return errors.New("renderer: insert statement requires at least one column")
	}
	if len(stmt.Rows) == 0 {
		return errors.New("renderer: insert statement requires at least one row")
	}

	r.buf.WriteString("INSERT INTO ")
	r.writeTableRef(stmt.Table, "")
	r.buf.WriteString(" (")
	if err := r.writeInsertColumns(stmt.Columns); err != nil {
		return err
	}
	r.buf.WriteString(") VALUES ")
	if err := r.writeInsertRows(stmt.Columns, stmt.Rows); err != nil {
		return err
	}
	return r.renderReturning(stmt.Returning)
}

// writeInsertColumns writes the parenthesized column list. Each name is quoted
// for the dialect; a blank name is rejected. Names are quoted verbatim, matching
// how the SELECT projection quotes a column, so the empty check trims but the
// output does not.
func (r *selectRenderer) writeInsertColumns(columns []string) error {
	for i, col := range columns {
		if strings.TrimSpace(col) == "" {
			return errors.New("renderer: insert column has an empty name")
		}
		if i > 0 {
			r.buf.WriteString(", ")
		}
		r.buf.WriteString(r.quote(col))
	}
	return nil
}

// writeInsertRows writes the comma-separated VALUES rows, binding each value in
// order so args match placeholder numbering row by row. A row whose length does
// not match the column count is rejected rather than emitted, so ragged input
// fails cleanly instead of producing a mismatched INSERT.
func (r *selectRenderer) writeInsertRows(columns []string, rows [][]ast.Expression) error {
	for i, row := range rows {
		if len(row) != len(columns) {
			return fmt.Errorf("renderer: insert row %d has %d values but there are %d columns", i+1, len(row), len(columns))
		}
		if i > 0 {
			r.buf.WriteString(", ")
		}
		r.buf.WriteString("(")
		for j, value := range row {
			if j > 0 {
				r.buf.WriteString(", ")
			}
			if err := r.renderExpr(value); err != nil {
				return err
			}
		}
		r.buf.WriteString(")")
	}
	return nil
}

func (r *selectRenderer) renderUpdate(stmt *ast.UpdateStatement) error {
	if strings.TrimSpace(stmt.Table) == "" {
		return errors.New("renderer: update statement requires a table")
	}
	if len(stmt.Set) == 0 {
		return errors.New("renderer: update statement requires at least one assignment")
	}
	if stmt.Where == nil && !stmt.Unconditional {
		return errors.New("renderer: update without a WHERE clause must be marked unconditional")
	}

	r.buf.WriteString("UPDATE ")
	r.writeTableRef(stmt.Table, "")
	r.buf.WriteString(" SET ")
	if err := r.writeAssignments(stmt.Set); err != nil {
		return err
	}
	if stmt.Where != nil {
		r.buf.WriteString(" WHERE ")
		if err := r.renderExpr(stmt.Where); err != nil {
			return err
		}
	}
	return r.renderReturning(stmt.Returning)
}

// writeAssignments writes the SET list, binding each value after its quoted
// column. Because SET renders before WHERE, these values are numbered first.
func (r *selectRenderer) writeAssignments(assignments []ast.Assignment) error {
	for i := range assignments {
		if strings.TrimSpace(assignments[i].Column) == "" {
			return errors.New("renderer: assignment has an empty column")
		}
		if assignments[i].Value == nil {
			return errors.New("renderer: assignment has a nil value")
		}
		if i > 0 {
			r.buf.WriteString(", ")
		}
		r.buf.WriteString(r.quote(assignments[i].Column))
		r.buf.WriteString(" = ")
		if err := r.renderExpr(assignments[i].Value); err != nil {
			return err
		}
	}
	return nil
}

func (r *selectRenderer) renderDelete(stmt *ast.DeleteStatement) error {
	if strings.TrimSpace(stmt.Table) == "" {
		return errors.New("renderer: delete statement requires a table")
	}
	if stmt.Where == nil && !stmt.Unconditional {
		return errors.New("renderer: delete without a WHERE clause must be marked unconditional")
	}

	r.buf.WriteString("DELETE FROM ")
	r.writeTableRef(stmt.Table, "")
	if stmt.Where != nil {
		r.buf.WriteString(" WHERE ")
		if err := r.renderExpr(stmt.Where); err != nil {
			return err
		}
	}
	return r.renderReturning(stmt.Returning)
}

// renderReturning appends a RETURNING clause when cols is non-empty, and only for
// a dialect that can execute one. Each column is quoted like any other
// identifier; a blank or "*" column is rejected because RETURNING here projects
// named columns, not a star.
//
// RETURNING is supported on the PostgreSQL family (always) and SQLite (since
// 3.35, 2021). MySQL has no RETURNING at all, and MariaDB supports it only for
// INSERT and DELETE, not UPDATE — so, to keep one rule across all three write
// statements, both MySQL and MariaDB are treated as unsupported and a non-empty
// RETURNING is rejected there rather than emitted as SQL the engine cannot run.
//
// "The PostgreSQL family" includes Cloud Spanner's PostgreSQL interface, which
// is what Ptah targets when the dialect is spanner: RETURNING is emitted there
// exactly as it is for PostgreSQL, rather than singled out for a refusal that
// would make this the one place in the repository where Spanner is not
// PostgreSQL. Spanner has no live coverage in this repository (stokaro/ptah#942)
// and the databases support matrix already asks callers to review generated
// Spanner SQL before relying on it; that caveat covers the whole statement, not
// this clause in particular.
func (r *selectRenderer) renderReturning(cols []ast.ColumnRef) error {
	if len(cols) == 0 {
		return nil
	}
	if !r.supportsReturning() {
		return fmt.Errorf("renderer: %s does not support RETURNING", r.dialect)
	}
	r.buf.WriteString(" RETURNING ")
	for i := range cols {
		if strings.TrimSpace(cols[i].Name) == "" {
			return errors.New("renderer: RETURNING column has an empty name")
		}
		if strings.TrimSpace(cols[i].Name) == "*" {
			return errors.New("renderer: RETURNING does not support a star column")
		}
		if i > 0 {
			r.buf.WriteString(", ")
		}
		r.writeQualifiedIdent(cols[i].Qualifier, cols[i].Name)
	}
	return nil
}

// supportsReturning reports whether the renderer's dialect can execute a
// RETURNING clause. See renderReturning for the per-dialect rationale.
//
// Family membership is asked of platform.IsPostgresFamily rather than spelled
// out, for the same reason as in selectPlaceholderStyle: a list of names here is
// a list that drifts, and it had already drifted — it omitted Spanner.
func (r *selectRenderer) supportsReturning() bool {
	return platform.IsPostgresFamily(r.dialect) || r.dialect == platform.SQLite
}
