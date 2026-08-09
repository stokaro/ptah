package sqlitekey

// White-box testing required: rendersAsSQLiteInteger is a copy of a decision
// that belongs to the SQLite renderer -- which declared types it writes as
// exactly `INTEGER`, since that spelling and no other makes a single-column key
// the rowid alias. The list is unexported because nothing outside this package
// should ask the question, but a copy that drifts from the renderer is exactly
// how this rule would go wrong silently, so the guard has to reach the
// unexported function.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
)

// TestRendersAsSQLiteIntegerMatchesTheRenderer renders a one-column table for
// each declared type and asserts that rendersAsSQLiteInteger agrees with whether
// the SQLite renderer wrote the type as `INTEGER`.
//
// Change mapColumnType in core/renderer/internal/dialects/sqlite -- add a type
// that now renders as INTEGER, or stop mapping one -- and the row for that type
// reddens here rather than quietly turning a rowid alias into a NOT NULL key
// column, or the reverse.
func TestRendersAsSQLiteIntegerMatchesTheRenderer(t *testing.T) {
	tests := []struct {
		name    string
		rawType string
	}{
		{name: "integer", rawType: "INTEGER"},
		{name: "lower case integer", rawType: "integer"},
		{name: "int", rawType: "INT"},
		{name: "bigint", rawType: "BIGINT"},
		{name: "smallint", rawType: "SMALLINT"},
		{name: "serial", rawType: "SERIAL"},
		{name: "bigserial", rawType: "BIGSERIAL"},
		{name: "smallserial", rawType: "SMALLSERIAL"},
		{name: "boolean", rawType: "BOOLEAN"},
		{name: "bool", rawType: "BOOL"},
		{name: "text", rawType: "TEXT"},
		{name: "varchar", rawType: "VARCHAR(255)"},
		{name: "blob", rawType: "BLOB"},
		{name: "real", rawType: "REAL"},
		{name: "numeric", rawType: "NUMERIC(10,2)"},
		{name: "any", rawType: "ANY"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := &ast.CreateTableNode{
				Name:    "t",
				Columns: []*ast.ColumnNode{ast.NewColumn("id", test.rawType)},
			}
			sql, err := renderer.RenderSQL("sqlite", table)
			c.Assert(err, qt.IsNil)

			renderedType := renderedColumnType(sql, `"id"`)
			c.Assert(renderedType, qt.Not(qt.Equals), "",
				qt.Commentf("no column line found in: %s", sql))
			// Case-insensitively, because SQLite reads the declared type that
			// way: measured, `id integer PRIMARY KEY` and `id InTeGeR PRIMARY
			// KEY` in a STRICT table both report notnull=0, so both are the
			// rowid alias.
			c.Assert(rendersAsSQLiteInteger(test.rawType), qt.Equals,
				strings.EqualFold(renderedType, "INTEGER"),
				qt.Commentf("rendered SQL: %s", sql))
		})
	}
}

// renderedColumnType returns the type the renderer wrote for the column named by
// quotedName, read out of the rendered CREATE TABLE text, or "" when no such
// column line is there.
func renderedColumnType(sql, quotedName string) string {
	for line := range strings.SplitSeq(sql, "\n") {
		parts := strings.Fields(strings.TrimSuffix(strings.TrimSpace(line), ","))
		if len(parts) >= 2 && parts[0] == quotedName {
			return parts[1]
		}
	}
	return ""
}
