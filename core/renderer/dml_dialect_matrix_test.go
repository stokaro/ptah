package renderer_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/internal/sqlident"
)

// This file is the dialect-coverage guard for the query builder's four render
// entry points. It exists because the previous shape of the suite let the
// builder acquire a dialect silently: only three cells anywhere pinned a
// refusal, all three on ClickHouse, so adding a dialect to
// selectPlaceholderStyle and deleting those three cells produced a green suite
// and SQL nobody had ever looked at.
//
// Two independent things are pinned here:
//
//  1. TestDMLDialectMatrix walks renderer.SupportedDialects() x the four render
//     functions and pins every one of the 48 cells to an exact SQL string with
//     exact args, or to an exact error string. The table must list exactly the
//     names SupportedDialects() returns, so a new dialect name cannot enter the
//     builder without a row being written for it.
//  2. TestDMLGenericRefusalCensus observes which cells still answer with the
//     generic "rendering is not supported for dialect" refusal and pins that
//     observed set against a hand-written quarantine list. The census reads
//     behavior, not the table above, so the two cannot be edited into agreement
//     one at a time.
//
// A third test, TestDMLPlaceholderAgreesWithRebind, pins the DML renderer's
// placeholder for the first bound value against sqlutil.Rebind for the same
// dialect. The expectation is derived from Rebind rather than written out, so
// the two placeholder tables in this repository cannot drift apart.

// genericRefusalMarker is the substring newSelectRenderer and newWriteRenderer
// emit for a dialect that has no entry in selectPlaceholderStyle. A cell that
// still carries it is a dialect the builder has never been taught, as opposed to
// one it refuses for a stated engine reason.
const genericRefusalMarker = "rendering is not supported for dialect"

// dmlCell is the pinned outcome of one (dialect, verb) cell: either sql plus
// args, or err. The two are mutually exclusive — a refusing render returns an
// empty string and nil args, which is what the zero values here express.
type dmlCell struct {
	sql  string
	args []any
	err  string
}

// dmlVerb is one of the four render entry points, bound to the fixture this
// guard renders through it. Every verb uses the same table, the same column
// names, and the same single bound id, so the cells differ only where the
// dialects differ.
type dmlVerb struct {
	name   string
	render func(dialect string) (string, []any, error)
}

func dmlVerbs() []dmlVerb {
	return []dmlVerb{
		{
			name: "SELECT",
			render: func(dialect string) (string, []any, error) {
				return renderer.RenderSelect(&ast.SelectStatement{
					Columns: []ast.ResultColumn{{Name: "id"}, {Name: "name"}},
					From:    "users",
					Where:   matrixWhereID(),
					Limit:   matrixInt64(10),
				}, dialect)
			},
		},
		{
			name: "INSERT",
			render: func(dialect string) (string, []any, error) {
				return renderer.RenderInsert(&ast.InsertStatement{
					Table:   "users",
					Columns: []string{"id", "name"},
					Rows:    [][]ast.Expression{{&ast.BoundValue{Value: int64(1)}, &ast.BoundValue{Value: "a"}}},
				}, dialect)
			},
		},
		{
			name: "UPDATE",
			render: func(dialect string) (string, []any, error) {
				return renderer.RenderUpdate(&ast.UpdateStatement{
					Table: "users",
					Set:   []ast.Assignment{{Column: "name", Value: &ast.BoundValue{Value: "a"}}},
					Where: matrixWhereID(),
				}, dialect)
			},
		},
		{
			name: "DELETE",
			render: func(dialect string) (string, []any, error) {
				return renderer.RenderDelete(&ast.DeleteStatement{
					Table: "users",
					Where: matrixWhereID(),
				}, dialect)
			},
		},
	}
}

func matrixWhereID() ast.Expression {
	return &ast.Comparison{
		Left:     &ast.ColumnRef{Name: "id"},
		Operator: ast.OpEqual,
		Right:    &ast.BoundValue{Value: int64(1)},
	}
}

func matrixInt64(v int64) *int64 { return new(v) }

// dmlMatrixRow pins all four cells for one dialect name. The cells are listed in
// the order dmlVerbs() returns.
type dmlMatrixRow struct {
	dialect string
	sel     dmlCell
	ins     dmlCell
	upd     dmlCell
	del     dmlCell
}

func (r dmlMatrixRow) cells() []dmlCell { return []dmlCell{r.sel, r.ins, r.upd, r.del} }

// dollarCells is the pinned rendering for a PostgreSQL-family dialect: double
// quoted identifiers and $n placeholders numbered left to right.
func dollarCells(dialect string) dmlMatrixRow {
	return dmlMatrixRow{
		dialect: dialect,
		sel:     dmlCell{sql: `SELECT "id", "name" FROM "users" WHERE "id" = $1 LIMIT $2`, args: []any{int64(1), int64(10)}},
		ins:     dmlCell{sql: `INSERT INTO "users" ("id", "name") VALUES ($1, $2)`, args: []any{int64(1), "a"}},
		upd:     dmlCell{sql: `UPDATE "users" SET "name" = $1 WHERE "id" = $2`, args: []any{"a", int64(1)}},
		del:     dmlCell{sql: `DELETE FROM "users" WHERE "id" = $1`, args: []any{int64(1)}},
	}
}

// backtickQuestionCells is the pinned rendering for MySQL and MariaDB.
func backtickQuestionCells(dialect string) dmlMatrixRow {
	return dmlMatrixRow{
		dialect: dialect,
		sel:     dmlCell{sql: "SELECT `id`, `name` FROM `users` WHERE `id` = ? LIMIT ?", args: []any{int64(1), int64(10)}},
		ins:     dmlCell{sql: "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)", args: []any{int64(1), "a"}},
		upd:     dmlCell{sql: "UPDATE `users` SET `name` = ? WHERE `id` = ?", args: []any{"a", int64(1)}},
		del:     dmlCell{sql: "DELETE FROM `users` WHERE `id` = ?", args: []any{int64(1)}},
	}
}

// quotedQuestionCells is the pinned rendering for SQLite: PostgreSQL-style
// identifier quoting with ? placeholders.
func quotedQuestionCells(dialect string) dmlMatrixRow {
	return dmlMatrixRow{
		dialect: dialect,
		sel:     dmlCell{sql: `SELECT "id", "name" FROM "users" WHERE "id" = ? LIMIT ?`, args: []any{int64(1), int64(10)}},
		ins:     dmlCell{sql: `INSERT INTO "users" ("id", "name") VALUES (?, ?)`, args: []any{int64(1), "a"}},
		upd:     dmlCell{sql: `UPDATE "users" SET "name" = ? WHERE "id" = ?`, args: []any{"a", int64(1)}},
		del:     dmlCell{sql: `DELETE FROM "users" WHERE "id" = ?`, args: []any{int64(1)}},
	}
}

// genericRefusalCells is the row for a dialect the builder has not been taught:
// all four verbs answer with the generic unsupported-dialect error, quoting the
// caller's spelling of the dialect rather than the normalized name.
func genericRefusalCells(dialect string) dmlMatrixRow {
	refusal := func(verb string) dmlCell {
		return dmlCell{err: fmt.Sprintf("renderer: %s %s %q", verb, genericRefusalMarker, dialect)}
	}
	return dmlMatrixRow{
		dialect: dialect,
		sel:     refusal("SELECT"),
		ins:     refusal("INSERT"),
		upd:     refusal("UPDATE"),
		del:     refusal("DELETE"),
	}
}

// dmlMatrixRows lists one row per name in renderer.SupportedDialects(), in the
// same order. TestDMLDialectMatrix compares the two lists, so this table cannot
// fall behind the set of names the renderer accepts.
func dmlMatrixRows() []dmlMatrixRow {
	return []dmlMatrixRow{
		dollarCells("postgresql"),
		dollarCells("postgres"),
		backtickQuestionCells("mysql"),
		backtickQuestionCells("mariadb"),
		genericRefusalCells("clickhouse"),
		quotedQuestionCells("sqlite"),
		quotedQuestionCells("sqlite3"),
		genericRefusalCells("sqlserver"),
		genericRefusalCells("mssql"),
		dollarCells("cockroachdb"),
		dollarCells("yugabytedb"),
		dollarCells("spanner"),
	}
}

// dmlGenericRefusalQuarantine is the hand-written list of cells that still
// answer with the generic unsupported-dialect refusal. Every entry is a dialect
// the query builder has never been taught, tracked on stokaro/ptah#941:
// SQL Server (workstream W2) and ClickHouse (workstream W3).
//
// Adding a dialect to selectPlaceholderStyle without editing this list makes
// TestDMLGenericRefusalCensus red, and the list is compared against observed
// behavior rather than against dmlMatrixRows, so shrinking it without also
// teaching the renderer is red too.
func dmlGenericRefusalQuarantine() []string {
	return []string{
		"clickhouse/SELECT", "clickhouse/INSERT", "clickhouse/UPDATE", "clickhouse/DELETE",
		"sqlserver/SELECT", "sqlserver/INSERT", "sqlserver/UPDATE", "sqlserver/DELETE",
		"mssql/SELECT", "mssql/INSERT", "mssql/UPDATE", "mssql/DELETE",
	}
}

// TestDMLDialectMatrix pins all 48 (dialect, verb) cells.
//
// Revert the Spanner placeholder entry and the four spanner cells stop at the
// first line of the cell check, printing
//
//	got:  `renderer: SELECT rendering is not supported for dialect "spanner"`
//	want: ""
//
// Change a rendering instead of removing one — a quote style, a placeholder, a
// keyword — and the cell reaches the second line and prints the two SQL strings.
//
// Drop a name from renderer.SupportedDialects(), or add one, and the
// "table covers exactly SupportedDialects" subtest prints the two lists side by
// side with the difference marked.
func TestDMLDialectMatrix(t *testing.T) {
	rows := dmlMatrixRows()
	verbs := dmlVerbs()

	t.Run("table covers exactly SupportedDialects", func(t *testing.T) {
		c := qt.New(t)
		names := make([]string, 0, len(rows))
		for _, row := range rows {
			names = append(names, row.dialect)
		}
		c.Assert(names, qt.DeepEquals, renderer.SupportedDialects())
	})

	for _, row := range rows {
		cells := row.cells()
		for i, verb := range verbs {
			t.Run(row.dialect+"/"+verb.name, func(t *testing.T) {
				c := qt.New(t)
				sql, args, err := verb.render(row.dialect)
				want := cells[i]
				c.Assert(errorText(err), qt.Equals, want.err)
				c.Assert(sql, qt.Equals, want.sql)
				c.Assert(args, qt.DeepEquals, want.args)
			})
		}
	}
}

// TestDMLGenericRefusalCensus pins which cells are still untaught dialects
// rather than deliberate refusals.
//
// Revert the Spanner placeholder entry and this prints the observed list with
// the four spanner entries back in it, against a want list without them —
// quicktest marks the four extra elements. Teach the renderer a dialect without
// striking it from dmlGenericRefusalQuarantine and the diff runs the other way:
// its cells are marked missing from the observed list, one per verb.
func TestDMLGenericRefusalCensus(t *testing.T) {
	c := qt.New(t)

	var observed []string
	for _, dialect := range renderer.SupportedDialects() {
		for _, verb := range dmlVerbs() {
			_, _, err := verb.render(dialect)
			observed = append(observed, genericRefusalLabel(dialect, verb.name, err)...)
		}
	}

	c.Assert(observed, qt.DeepEquals, dmlGenericRefusalQuarantine())
}

// genericRefusalLabel returns a one-element slice naming the cell when err is
// the generic unsupported-dialect refusal, and an empty slice otherwise.
func genericRefusalLabel(dialect, verb string, err error) []string {
	if err == nil || !strings.Contains(err.Error(), genericRefusalMarker) {
		return nil
	}
	return []string{dialect + "/" + verb}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// placeholderRow pins how one dialect names its first bound parameter. The check
// is a func field rather than a flag so the two outcomes — renders, or refuses —
// each state their own expectation.
type placeholderRow struct {
	dialect string
	check   func(c *qt.C, dialect string)
}

// onePlaceholderInsert is the smallest statement with exactly one bound value,
// so the placeholder it renders is unambiguously the first one.
func onePlaceholderInsert() *ast.InsertStatement {
	return &ast.InsertStatement{
		Table:   "users",
		Columns: []string{"id"},
		Rows:    [][]ast.Expression{{&ast.BoundValue{Value: int64(1)}}},
	}
}

// agreesWithRebind renders the one-placeholder INSERT and compares it against a
// string built from sqlutil.Rebind for the same dialect. The expectation is
// derived, not written out: there is no constant here to edit into agreement, so
// a placeholder style added to selectPlaceholderStyle that disagrees with Rebind
// fails on this line.
func agreesWithRebind(c *qt.C, dialect string) {
	sql, args, err := renderer.RenderInsert(onePlaceholderInsert(), dialect)
	c.Assert(err, qt.IsNil)
	c.Assert(args, qt.DeepEquals, []any{int64(1)})
	c.Assert(sql, qt.Equals, fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		sqlident.Quote(dialect, "users"),
		sqlident.Quote(dialect, "id"),
		sqlutil.Rebind(dialect, "?"),
	))
}

// refusesButRebindKnows pins the pair for a dialect the builder has not been
// taught: the render refuses, while sqlutil.Rebind already has an answer for the
// same name. wantRebind records that answer so whoever teaches the renderer this
// dialect has the placeholder style in front of them; for SQL Server it is @p1,
// not the ? a naive entry in selectPlaceholderStyle would emit.
func refusesButRebindKnows(wantRebind string) func(c *qt.C, dialect string) {
	return func(c *qt.C, dialect string) {
		_, _, err := renderer.RenderInsert(onePlaceholderInsert(), dialect)
		c.Assert(errorText(err), qt.Equals,
			fmt.Sprintf("renderer: INSERT %s %q", genericRefusalMarker, dialect))
		c.Assert(sqlutil.Rebind(dialect, "?"), qt.Equals, wantRebind)
	}
}

// TestDMLPlaceholderAgreesWithRebind pins the DML renderer's placeholder for the
// first bound value against sqlutil.Rebind, the repository's other per-dialect
// placeholder table.
//
// Revert the Spanner placeholder entry and the spanner row prints
//
//	error: got non-nil error, want nil
//	error message: renderer: INSERT rendering is not supported for dialect "spanner"
//
// Give a dialect the wrong style — sqlserver with ?, say — and its row prints the
// rendered SQL against the Rebind-derived string, `VALUES (?)` against
// `VALUES (@p1)`.
func TestDMLPlaceholderAgreesWithRebind(t *testing.T) {
	c := qt.New(t)

	rows := []placeholderRow{
		{dialect: "postgresql", check: agreesWithRebind},
		{dialect: "postgres", check: agreesWithRebind},
		{dialect: "mysql", check: agreesWithRebind},
		{dialect: "mariadb", check: agreesWithRebind},
		{dialect: "clickhouse", check: refusesButRebindKnows("?")},
		{dialect: "sqlite", check: agreesWithRebind},
		{dialect: "sqlite3", check: agreesWithRebind},
		{dialect: "sqlserver", check: refusesButRebindKnows("@p1")},
		{dialect: "mssql", check: refusesButRebindKnows("@p1")},
		{dialect: "cockroachdb", check: agreesWithRebind},
		{dialect: "yugabytedb", check: agreesWithRebind},
		{dialect: "spanner", check: agreesWithRebind},
	}

	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, row.dialect)
	}
	c.Assert(names, qt.DeepEquals, renderer.SupportedDialects())

	for _, row := range rows {
		c.Run(row.dialect, func(c *qt.C) {
			row.check(c, row.dialect)
		})
	}
}
