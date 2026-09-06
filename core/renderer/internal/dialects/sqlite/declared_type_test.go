package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer/internal/dialects/sqlite"
)

// TestRender_ATypeTheCatalogStoredIsWrittenAsItStands pins the two halves of
// stokaro/ptah#2040 that meet in the renderer.
//
// SQLite keeps the declaration and derives an affinity from it at use time, so
// a description of a `VARCHAR(80)` column that rendered `TEXT` would replay as
// a different table. A declaration a person wrote has no such text to protect
// and still goes through the canonical spelling, which is what gives a Go or
// HCL schema one type per affinity.
func TestRender_ATypeTheCatalogStoredIsWrittenAsItStands(t *testing.T) {
	tests := []struct {
		name         string
		declaredText bool
		columnType   string
		want         string
	}{
		{
			name: "a varchar the catalog stored", declaredText: true,
			columnType: "VARCHAR(80)", want: `"c" VARCHAR(80)`,
		},
		{
			name: "a type nothing models", declaredText: true,
			columnType: "MY_OWN_TYPE", want: `"c" MY_OWN_TYPE`,
		},
		{
			name: "a boolean the catalog stored", declaredText: true,
			columnType: "BOOLEAN", want: `"c" BOOLEAN`,
		},
		{
			// The control, and requirement the fix must not break: a
			// declaration still renders canonically.
			name: "a varchar somebody declared", columnType: "VARCHAR(80)", want: `"c" TEXT`,
		},
		{
			name: "a boolean somebody declared", columnType: "BOOLEAN", want: `"c" INTEGER`,
		},
		{
			// A column with no declared type at all is written with none, both
			// ways. SQLite accepts that and gives it BLOB affinity, which is
			// what the reader reports for such a column; nothing here may
			// invent a type the author did not write.
			name: "no type at all", declaredText: true, columnType: "", want: "\"c\" \n",
		},
		{
			name: "no type at all, undeclared", columnType: "", want: "\"c\" \n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := ast.NewColumn("c", test.columnType)
			column.TypeIsDeclaredText = test.declaredText
			table := ast.NewCreateTable("t").AddColumn(column)

			out, err := sqlite.New().Render(table)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, test.want)
		})
	}
}
