package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// setNotNull is the plan for a column that becomes NOT NULL and declares no
// default, whatever its type: a comment saying the statement fails on a NULL
// row, then the statement.
const setNotNull = "-- ALTER statements: --\n" +
	"-- POSTGRES: SET NOT NULL fails if any row of \"t\" holds NULL in \"c\"; the column declares no default to fill it with.\n" +
	"ALTER TABLE \"t\" ALTER COLUMN \"c\" SET NOT NULL;\n\n"

// A column that becomes NOT NULL and declares no default is not filled with a
// value its type suggests. Every row but the last is a type that has one: 0,
// 0.0, the empty string, false, CURRENT_TIMESTAMP, CURRENT_DATE or
// CURRENT_TIME. Written into every NULL row, it makes a change the server
// refuses report success. Atlas CE v1.3.0 writes SET NOT NULL alone and the
// server fails it with SQLSTATE 23502 (stokaro/ptah#3648).
func TestPostgres_SetNotNull_InventsNoBackfillValue(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
	}{
		{name: "integer", columnType: "INTEGER"},
		{name: "bigint", columnType: "BIGINT"},
		{name: "numeric", columnType: "NUMERIC(10,2)"},
		{name: "text", columnType: "TEXT"},
		{name: "varchar", columnType: "VARCHAR(20)"},
		{name: "boolean", columnType: "BOOLEAN"},
		{name: "timestamp", columnType: "TIMESTAMPTZ"},
		{name: "date", columnType: "DATE"},
		{name: "time", columnType: "TIME"},
		{name: "a type with no suggested value", columnType: "JSONB"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := ast.NewColumn("c", test.columnType).SetNotNull()

			got := renderPostgres(c, modifyColumn(column, ast.ColumnProperties{Nullability: true}, true))

			c.Assert(got, qt.Equals, setNotNull)
		})
	}
}

// A column that declares a default is still filled with it before SET NOT
// NULL: that value is one the author wrote. The rows are the control for the
// test above, and they fail if the backfill is dropped altogether.
func TestPostgres_SetNotNull_BackfillsWithTheDeclaredDefault(t *testing.T) {
	tests := []struct {
		name   string
		column *ast.ColumnNode
		value  string
	}{
		{name: "a literal", column: ast.NewColumn("c", "INTEGER").SetNotNull().SetDefault("9"), value: "'9'"},
		{name: "an expression", column: ast.NewColumn("c", "TIMESTAMPTZ").SetNotNull().SetDefaultExpression("now()"), value: "now()"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := renderPostgres(c, modifyColumn(test.column, ast.ColumnProperties{Nullability: true}, true))

			c.Assert(got, qt.Equals, "-- ALTER statements: --\n"+
				"DO $$\nBEGIN\n"+
				"    IF EXISTS (SELECT 1 FROM \"t\" WHERE \"c\" IS NULL LIMIT 1) THEN\n"+
				"        UPDATE \"t\" SET \"c\" = "+test.value+" WHERE \"c\" IS NULL;\n"+
				"    END IF;\nEND\n$$;\n"+
				"ALTER TABLE \"t\" ALTER COLUMN \"c\" SET NOT NULL;\n\n")
		})
	}
}

// A modification that does not say what changed restates the column, and
// restating a NOT NULL column with no default invents nothing either.
func TestPostgres_RestatedNotNullColumn_InventsNoBackfillValue(t *testing.T) {
	c := qt.New(t)
	column := ast.NewColumn("c", "INTEGER").SetNotNull()

	got := renderPostgres(c, modifyColumn(column, ast.ColumnProperties{}, false))

	c.Assert(got, qt.Equals, "-- ALTER statements: --\n"+
		"ALTER TABLE \"t\" ALTER COLUMN \"c\" TYPE INTEGER;\n"+
		"-- POSTGRES: SET NOT NULL fails if any row of \"t\" holds NULL in \"c\"; the column declares no default to fill it with.\n"+
		"ALTER TABLE \"t\" ALTER COLUMN \"c\" SET NOT NULL;\n"+
		"ALTER TABLE \"t\" ALTER COLUMN \"c\" DROP DEFAULT;\n\n")
}
