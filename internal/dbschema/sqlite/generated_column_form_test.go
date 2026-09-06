package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/sqlite"
)

// TestReader_ReadsBothSpellingsOfAGeneratedColumn pins that an optional keyword
// is optional.
//
// SQLite's grammar makes GENERATED ALWAYS optional: `scaled REAL AS (raw * 100)
// VIRTUAL` declares the same column as the spelling that carries it, and the
// short form is what the pinned Atlas community binary writes. The reader gated
// on the keyword, so the short form came back marked generated -- PRAGMA
// table_xinfo's hidden value says so -- with no expression, and a column that is
// generated with no expression renders as a plain one. Replaying such a schema
// built a column holding NULL where the original computed a value
// (stokaro/ptah#2090).
func TestReader_ReadsBothSpellingsOfAGeneratedColumn(t *testing.T) {
	tests := []struct {
		name       string
		ddl        string
		expression string
		kind       string
	}{
		{
			name:       "virtual, written the long way",
			ddl:        `CREATE TABLE t (id INTEGER PRIMARY KEY, raw REAL NOT NULL, scaled REAL GENERATED ALWAYS AS (raw * 100) VIRTUAL)`,
			expression: "raw * 100",
			kind:       "VIRTUAL",
		},
		{
			name:       "virtual, written the short way",
			ddl:        `CREATE TABLE t (id INTEGER PRIMARY KEY, raw REAL NOT NULL, scaled REAL AS (raw * 100) VIRTUAL)`,
			expression: "raw * 100",
			kind:       "VIRTUAL",
		},
		{
			name:       "stored, written the long way",
			ddl:        `CREATE TABLE t (id INTEGER PRIMARY KEY, raw REAL NOT NULL, scaled REAL GENERATED ALWAYS AS (raw * 1000) STORED)`,
			expression: "raw * 1000",
			kind:       "STORED",
		},
		{
			name:       "stored, written the short way",
			ddl:        `CREATE TABLE t (id INTEGER PRIMARY KEY, raw REAL NOT NULL, scaled REAL AS (raw * 1000) STORED)`,
			expression: "raw * 1000",
			kind:       "STORED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			db := openMemoryDB(t)
			execSQL(t, db, tt.ddl)

			schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(schema.Tables, qt.HasLen, 1)
			desired := schema.Tables[0].Columns[2]
			c.Assert(desired.Name, qt.Equals, "scaled")
			c.Assert(desired.GeneratedExpression, qt.IsNotNil)
			c.Assert(*desired.GeneratedExpression, qt.Equals, tt.expression)
			c.Assert(desired.GeneratedKind, qt.Equals, tt.kind)
		})
	}
}

// TestReader_LeavesAPlainColumnUngenerated is the control the fix needs: the
// keyword gate is gone, so nothing but an `AS (…)` may produce an expression.
func TestReader_LeavesAPlainColumnUngenerated(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
	}{
		{
			name: "a plain column",
			ddl:  `CREATE TABLE t (id INTEGER PRIMARY KEY, raw REAL NOT NULL, plain TEXT)`,
		},
		{
			name: "a column with a default",
			ddl:  `CREATE TABLE t (id INTEGER PRIMARY KEY, raw REAL NOT NULL, plain TEXT DEFAULT 'as (x)')`,
		},
		{
			name: "a column with a collation",
			ddl:  `CREATE TABLE t (id INTEGER PRIMARY KEY, raw REAL NOT NULL, plain TEXT COLLATE NOCASE)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			db := openMemoryDB(t)
			execSQL(t, db, tt.ddl)

			schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			plain := schema.Tables[0].Columns[2]
			c.Assert(plain.Name, qt.Equals, "plain")
			c.Assert(plain.GeneratedExpression, qt.IsNil)
			c.Assert(plain.GeneratedKind, qt.Equals, "")
		})
	}
}
