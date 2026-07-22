package sqlite_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/dbschema/sqlite"
)

func TestReaderTriggerBodyExcludesCreateTriggerHeader(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:?_pragma=foreign_keys(1)")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, `CREATE TRIGGER trg_users_email AFTER UPDATE ON users FOR EACH ROW BEGIN SELECT NEW.email; END`)
	c.Assert(err, qt.IsNil)

	schema, err := sqlite.NewSQLiteReader(db, "main").ReadSchema()
	c.Assert(err, qt.IsNil)

	c.Assert(schema.Triggers, qt.HasLen, 1)
	c.Assert(schema.Triggers[0].Body, qt.Contains, "BEGIN SELECT NEW.email; END")
	c.Assert(schema.Triggers[0].Body, qt.Not(qt.Contains), "CREATE TRIGGER")
}
