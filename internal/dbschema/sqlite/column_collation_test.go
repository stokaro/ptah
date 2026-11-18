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
// as no collation: a column that restates it is the same column. SQLite
// keeps no inherited name, so the effective collation is the declared one.
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
		"collate" TEXT COLLATE NOCASE,
		in_default TEXT DEFAULT ('x' COLLATE NOCASE),
		"code,part" TEXT COLLATE NOCASE,
		"paren(" TEXT COLLATE RTRIM,
		[bracket,name] TEXT COLLATE NOCASE,
		`+"`back,tick`"+` TEXT COLLATE NOCASE,
		-- a note, with a comma (and a parenthesis
		commented TEXT COLLATE NOCASE,
		/* x, y ) */ blocked TEXT COLLATE RTRIM,
		after_comment TEXT /* COLLATE NOCASE */,
		UNIQUE (inherited COLLATE NOCASE)
	)`)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchemaContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 1)
	type collation struct{ Declared, Effective string }
	got := make(map[string]collation)
	for _, column := range schema.Tables[0].Columns {
		got[column.Name] = collation{Declared: column.Collate, Effective: column.EffectiveCollate}
	}
	c.Assert(got, qt.DeepEquals, map[string]collation{
		"inherited":     {},
		"declared":      {"NOCASE", "NOCASE"},
		"quoted name":   {"RTRIM", "RTRIM"},
		"restated":      {},
		"twice":         {"RTRIM", "RTRIM"},
		"after_default": {"NoCase", "NoCase"},
		"in_check":      {},
		"single_quoted": {"NOCASE", "NOCASE"},
		"counted":       {"NOCASE", "NOCASE"},
		"collate":       {"NOCASE", "NOCASE"},
		"in_default":    {},
		"code,part":     {"NOCASE", "NOCASE"},
		"paren(":        {"RTRIM", "RTRIM"},
		"bracket,name":  {"NOCASE", "NOCASE"},
		"back,tick":     {"NOCASE", "NOCASE"},
		"commented":     {"NOCASE", "NOCASE"},
		"blocked":       {"RTRIM", "RTRIM"},
		"after_comment": {},
	})
}
