package sqlite_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/dbschema/sqlite"
)

func TestReaderTableOptions(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:?_pragma=foreign_keys(1)")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(ctx, `CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT NOT NULL) WITHOUT ROWID, STRICT`)
	c.Assert(err, qt.IsNil)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)

	c.Assert(schema.Tables, qt.HasLen, 1)
	c.Assert(schema.Tables[0].Strict, qt.IsTrue)
	c.Assert(schema.Tables[0].WithoutRowID, qt.IsTrue)
}
