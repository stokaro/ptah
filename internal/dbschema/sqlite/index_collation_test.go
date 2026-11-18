package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/sqlite"
)

// TestReader_RecordsACollatedKeyAsItsText pins how a key with a COLLATE of its
// own is read: as its text, in the one spelling the SQL frontend gives the same
// key in a document, so the two compare equal however the index quotes,
// spaces or comments the key. PRAGMA index_xinfo
// names the column of such a key and reports the collation apart, and a read
// that takes the name alone describes `code COLLATE NOCASE` as `code`: a diff
// then sees no change of collation, and the key passes for its column as a
// foreign key's parent key, which SQLite refuses with "foreign key mismatch".
// Accounts declares email and "my code" with no collation and status COLLATE
// NOCASE.
func TestReader_RecordsACollatedKeyAsItsText(t *testing.T) {
	tests := []struct {
		name    string
		ddl     string
		columns []string
		parts   []catalog.IndexPart
	}{
		{
			name:    "a collation the column does not declare",
			ddl:     `CREATE INDEX accounts_email_idx ON accounts (email COLLATE NOCASE)`,
			columns: []string{"email COLLATE NOCASE"},
			parts:   []catalog.IndexPart{{Expr: "email COLLATE NOCASE"}},
		},
		{
			// PRAGMA index_xinfo reports BINARY here as it does for a key with no
			// COLLATE, and a document keeps the clause as text, so the read keeps
			// it too.
			name:    "the column's own collation, written out",
			ddl:     `CREATE INDEX accounts_email_idx ON accounts (email COLLATE BINARY)`,
			columns: []string{"email COLLATE BINARY"},
			parts:   []catalog.IndexPart{{Expr: "email COLLATE BINARY"}},
		},
		{
			// The direction is the catalog's, beside the text, as it is for a
			// column key.
			name:    "a quoted collated key that descends",
			ddl:     `CREATE INDEX accounts_email_idx ON accounts ("email" COLLATE NOCASE DESC)`,
			columns: []string{"email COLLATE NOCASE"},
			parts:   []catalog.IndexPart{{Expr: "email COLLATE NOCASE", Desc: true}},
		},
		{
			name:    "a bracketed collated key with a comment and an ASC",
			ddl:     `CREATE INDEX accounts_email_idx ON accounts ([email]  COLLATE /*x*/ NOCASE ASC)`,
			columns: []string{"email COLLATE NOCASE"},
			parts:   []catalog.IndexPart{{Expr: "email COLLATE NOCASE"}},
		},
		{
			name:    "a bracketed collated key with a space in its name",
			ddl:     `CREATE INDEX accounts_code_idx ON accounts ([my code] COLLATE NOCASE)`,
			columns: []string{`"my code" COLLATE NOCASE`},
			parts:   []catalog.IndexPart{{Expr: `"my code" COLLATE NOCASE`}},
		},
		{
			// An expression key sends the whole list through the DDL, where a
			// bracketed name runs to its closing bracket.
			name:    "a bracketed column with a space in its name beside an expression",
			ddl:     `CREATE INDEX accounts_code_idx ON accounts ([my code] DESC, lower(email))`,
			columns: []string{"my code", "lower(email)"},
			parts:   []catalog.IndexPart{{Name: "my code", Desc: true}, {Expr: "lower(email)"}},
		},
		{
			name:    "a column beside a collated key",
			ddl:     `CREATE INDEX accounts_pair_idx ON accounts (id, email COLLATE NOCASE)`,
			columns: []string{"id", "email COLLATE NOCASE"},
			parts:   []catalog.IndexPart{{Name: "id"}, {Expr: "email COLLATE NOCASE"}},
		},
		{
			// The control: a key with no COLLATE takes its column's, NOCASE here,
			// and stays a column.
			name:    "a key that takes its column's collation",
			ddl:     `CREATE INDEX accounts_status_idx ON accounts (status)`,
			columns: []string{"status"},
			parts:   []catalog.IndexPart{{Name: "status"}},
		},
		{
			name:    "a COLLATE in the predicate alone",
			ddl:     `CREATE INDEX accounts_status_idx ON accounts (status DESC) WHERE email COLLATE NOCASE <> 'x'`,
			columns: []string{"status"},
			parts:   []catalog.IndexPart{{Name: "status", Desc: true}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			db := openMemoryDB(t)
			execSQL(t, db, `CREATE TABLE accounts (
				id INTEGER PRIMARY KEY,
				email TEXT NOT NULL,
				status TEXT NOT NULL COLLATE NOCASE,
				"my code" TEXT
			)`)
			execSQL(t, db, tt.ddl)

			schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(schema.Indexes, qt.HasLen, 1)
			c.Assert(schema.Indexes[0].Columns, qt.DeepEquals, tt.columns)
			c.Assert(schema.Indexes[0].Parts, qt.DeepEquals, tt.parts)
		})
	}
}
