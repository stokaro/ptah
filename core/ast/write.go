package ast

// This file defines the write-side DML statement AST: INSERT, UPDATE, and
// DELETE. Like SelectStatement, these are statements rather than expressions —
// they do not implement the sealed Expression interface — because rendering a
// statement returns bound arguments alongside a SQL string. Their value and
// filter positions reuse the Expression tree from select.go (BoundValue,
// ColumnRef, Comparison, and the boolean combinators), so every value still
// reaches SQL as a placeholder and every identifier through dialect quoting.
// Render them with renderer.RenderInsert, renderer.RenderUpdate, and
// renderer.RenderDelete; build them fluently with the core/query package.
//
// Deliberately out of scope for now, and tracked as follow-ups on issue #98:
// ON CONFLICT / upsert (the DDL-side UpsertNode in dml.go is a separate,
// unrelated node), INSERT … SELECT, common table expressions, subqueries in a
// VALUES row or a SET value, and multi-table UPDATE / DELETE.

// Assignment is a single "column = value" pair in an UPDATE SET clause.
//
// Column is rendered as a dialect-quoted identifier; Value is an ordinary
// expression, so the query builder supplies a BoundValue and the value travels
// to the database as a placeholder rather than inline SQL.
type Assignment struct {
	// Column is the target column name, rendered as a quoted identifier.
	Column string
	// Value is the assigned value expression, typically a BoundValue.
	Value Expression
}

// InsertStatement is an INSERT INTO … VALUES … over a single table, with one or
// more value rows and an optional RETURNING projection.
//
// Every element of every row is an expression — the query builder supplies a
// BoundValue — so values are bound positionally at render time, row by row and
// left to right, and never interpolated. The renderer rejects a statement with
// no columns, no rows, or a row whose length does not match Columns, rather than
// emit invalid SQL or panic on ragged input.
type InsertStatement struct {
	// Table is the target table, rendered as a quoted identifier. Required.
	Table string
	// Columns is the inserted column list, each a quoted identifier. Required and
	// non-empty; every row must supply exactly one value per column.
	Columns []string
	// Rows is the list of value rows. At least one row is required, and each row
	// must have the same length as Columns. Values are bound row by row, in order.
	Rows [][]Expression
	// Returning is the optional RETURNING projection: the columns to return from
	// the inserted rows. An empty slice emits no RETURNING. RETURNING is supported
	// only on the PostgreSQL family and SQLite; the renderer rejects it on MySQL
	// and MariaDB rather than emit SQL those engines cannot run.
	Returning []ColumnRef
}

// UpdateStatement is an UPDATE … SET … over a single table, with an optional
// WHERE clause and RETURNING projection.
//
// SET values are bound before WHERE values, matching left-to-right emission
// order, so placeholder numbering is deterministic. An UPDATE with no WHERE
// clause rewrites every row; because that is rarely intended, the renderer
// rejects it unless Unconditional is set, turning an accidental whole-table
// update into a render-time error rather than silent data loss.
type UpdateStatement struct {
	// Table is the target table, rendered as a quoted identifier. Required.
	Table string
	// Set is the list of column assignments. Required and non-empty; the renderer
	// rejects an empty SET.
	Set []Assignment
	// Where is the optional filter expression tree; nil means no WHERE clause. A
	// nil Where is allowed only together with Unconditional.
	Where Expression
	// Unconditional opts in to a whole-table UPDATE when Where is nil. It is the
	// explicit acknowledgment that every row is to be updated; without it, a nil
	// Where is an error. It has no effect when Where is set.
	Unconditional bool
	// Returning is the optional RETURNING projection. See InsertStatement.Returning
	// for the dialect support rules.
	Returning []ColumnRef
}

// DeleteStatement is a DELETE FROM … over a single table, with an optional WHERE
// clause and RETURNING projection.
//
// Like UpdateStatement, a DELETE with no WHERE clause removes every row, so the
// renderer rejects it unless Unconditional is set.
type DeleteStatement struct {
	// Table is the target table, rendered as a quoted identifier. Required.
	Table string
	// Where is the optional filter expression tree; nil means no WHERE clause. A
	// nil Where is allowed only together with Unconditional.
	Where Expression
	// Unconditional opts in to a whole-table DELETE when Where is nil, exactly as
	// on UpdateStatement.
	Unconditional bool
	// Returning is the optional RETURNING projection. See InsertStatement.Returning
	// for the dialect support rules.
	Returning []ColumnRef
}
