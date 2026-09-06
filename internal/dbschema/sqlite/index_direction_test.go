package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/sqlite"
)

// TestReader_RecordsAnIndexKeysDirection pins the direction of each key.
//
// SQLite records it only in PRAGMA index_xinfo's `desc` column: sqlite_schema
// holds the CREATE INDEX text and index_list says nothing about ordering, so a
// read that does not ask for that column cannot tell `t(a DESC)` from
// `t(a)`. It did not ask, so both were described as ascending and replaying the
// description built an index that no longer served the descending scan it was
// created for (stokaro/ptah#2197).
func TestReader_RecordsAnIndexKeysDirection(t *testing.T) {
	tests := []struct {
		name  string
		ddl   string
		parts []catalog.IndexPart
	}{
		{
			name:  "a descending key",
			ddl:   `CREATE INDEX accounts_desc_idx ON accounts (status DESC)`,
			parts: []catalog.IndexPart{{Name: "status", Desc: true}},
		},
		{
			// The control. An ascending key is the default spelling, and a read
			// that reported every key as descending would pass the case above
			// while describing every index in the database wrongly.
			name:  "an explicitly ascending key",
			ddl:   `CREATE INDEX accounts_asc_idx ON accounts (status ASC)`,
			parts: []catalog.IndexPart{{Name: "status"}},
		},
		{
			name:  "a key with no direction at all",
			ddl:   `CREATE INDEX accounts_plain_idx ON accounts (status)`,
			parts: []catalog.IndexPart{{Name: "status"}},
		},
		{
			// Two keys pointing opposite ways. A one-key index cannot tell a
			// per-key direction from an index-wide one, and it is the per-key
			// answer SQLite gives.
			name: "one key each way",
			ddl:  `CREATE INDEX accounts_mixed_idx ON accounts (status DESC, email ASC)`,
			parts: []catalog.IndexPart{
				{Name: "status", Desc: true},
				{Name: "email"},
			},
		},
		{
			// An expression key carries its direction inside its own text,
			// because that text comes from the DDL rather than from the
			// catalog. Setting the flag as well renders `lower(email) DESC DESC`,
			// which SQLite refuses outright -- measured, and the reason the two
			// branches differ.
			name:  "a descending expression key",
			ddl:   `CREATE INDEX accounts_expr_idx ON accounts (lower(email) DESC)`,
			parts: []catalog.IndexPart{{Expr: "lower(email) DESC"}},
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
