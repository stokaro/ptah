package diffreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/internal/cli/internal/diffreport"
)

// spelledDiff is a diff that adds and modifies tables whose qualified names
// quote a spelled name a second time, as a comparison writes a table stored
// as orders on Oracle, beside a table whose name needs no quotes.
func spelledDiff() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesAdded:    difftypes.TableChanges{{Name: `QRICH."""Items"""`}, {Name: "QRICH.KEEP_ME"}},
		TablesModified: []difftypes.TableDiff{{TableName: `QRICH."""orders"""`, ColumnsAdded: difftypes.ColumnChanges{{Name: `"New"`}}}},
	}
}

// TestCategoriesFor_StoredSpelling reports the same diff for each dialect. On
// Oracle, which allows no quote character in a name, a name quoted twice is
// written with one pair of quotes, as a refusal and a DROP write it. On
// PostgreSQL a stored name may hold quote characters, and the doubled quote is
// how SQL writes one, so the names stay as the diff carries them.
func TestCategoriesFor_StoredSpelling(t *testing.T) {
	tests := []struct {
		dialect  string
		added    []string
		modified []string
	}{
		{dialect: "oracle", added: []string{`QRICH."Items"`, "QRICH.KEEP_ME"}, modified: []string{`QRICH."orders"`}},
		{dialect: "postgres", added: []string{`QRICH."""Items"""`, "QRICH.KEEP_ME"}, modified: []string{`QRICH."""orders"""`}},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			categories := diffreport.CategoriesFor(spelledDiff(), test.dialect)

			c.Assert(diffreport.Names(categories), qt.DeepEquals, []string{"tables_added", "tables_modified"})
			c.Assert(categories[0].Objects, qt.DeepEquals, test.added)
			c.Assert(categories[1].Objects, qt.DeepEquals, test.modified)
		})
	}
}
