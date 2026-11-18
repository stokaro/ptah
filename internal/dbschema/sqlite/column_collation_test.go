package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/sqlite"
)

// TestReader_ReportsTheCollationAColumnDeclares reads each column's collation
// out of the table's text, the only place SQLite keeps it. Each want is the
// coll sqlite3 3.51.0 reports in pragma_index_xinfo for an index on that
// column, with BINARY, the collation a column that declares none takes, read
// as no collation: a column that restates it is the same column.
func TestReader_ReportsTheCollationAColumnDeclares(t *testing.T) {
	c := qt.New(t)
	db := openMemoryDB(t)
	execSQL(t, db, `CREATE TABLE t (
		inherited TEXT,
		declared TEXT COLLATE NOCASE,
		"quoted name" TEXT NOT NULL COLLATE "RTRIM",
		restated TEXT COLLATE binary,
		twice TEXT COLLATE nocase COLLATE RTRIM,
		after_default TEXT DEFAULT 'x' COLLATE NoCase,
		in_check TEXT CHECK (in_check COLLATE NOCASE <> 'a'),
		single_quoted TEXT COLLATE 'NOCASE',
		counted INTEGER COLLATE NOCASE,
		UNIQUE (inherited COLLATE NOCASE)
	)`)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 1)
	got := make(map[string]string)
	for _, column := range schema.Tables[0].Columns {
		got[column.Name] = column.Collate
		c.Assert(column.EffectiveCollate, qt.Equals, "", qt.Commentf("column %s", column.Name))
	}
	c.Assert(got, qt.DeepEquals, map[string]string{
		"inherited":     "",
		"declared":      "NOCASE",
		"quoted name":   "RTRIM",
		"restated":      "",
		"twice":         "RTRIM",
		"after_default": "NoCase",
		"in_check":      "",
		"single_quoted": "NOCASE",
		"counted":       "NOCASE",
	})
}
