package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/sqlite"
)

// TestReader_RecordsAnExpressionKeyAsAnExpression pins the distinction only the
// catalog has.
//
// PRAGMA index_xinfo answers with a NULL name for a key that is an expression;
// the index's DDL carries the text of every key and says nothing about which
// kind each one is. Recording the expression as a column name made the HCL
// document say `columns = [column["(lower(email))"]]` -- a reference to a column
// that does not exist. The pinned Atlas community binary refuses that document,
// and Ptah replaying its own copy built an index over the STRING
// `"(lower(email))"`, because SQLite reads a double-quoted name that matches no
// column as a string literal. An index over a constant holds the same value for
// every row (stokaro/ptah#2088).
func TestReader_RecordsAnExpressionKeyAsAnExpression(t *testing.T) {
	tests := []struct {
		name  string
		ddl   string
		parts []catalog.IndexPart
	}{
		{
			name:  "one expression",
			ddl:   `CREATE INDEX accounts_lower_email_idx ON accounts (lower(email))`,
			parts: []catalog.IndexPart{{Expr: "lower(email)"}},
		},
		{
			// The control: a plain key keeps the column representation, so the
			// document keeps its compact `columns = [...]` spelling.
			name:  "one column",
			ddl:   `CREATE INDEX accounts_email_idx ON accounts (email)`,
			parts: []catalog.IndexPart{{Name: "email"}},
		},
		{
			// Key order has to survive, and the two kinds have to keep their
			// positions within it.
			name: "a column and an expression together",
			ddl:  `CREATE INDEX accounts_mixed_idx ON accounts (status, lower(email))`,
			parts: []catalog.IndexPart{
				{Name: "status"},
				{Expr: "lower(email)"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			db := openMemoryDB(t)
			execSQL(t, db, `CREATE TABLE accounts (
				id INTEGER PRIMARY KEY,
				email TEXT NOT NULL,
				status TEXT NOT NULL
			)`)
			execSQL(t, db, tt.ddl)

			schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(schema.Indexes, qt.HasLen, 1)
			c.Assert(schema.Indexes[0].Parts, qt.DeepEquals, tt.parts)
		})
	}
}
