//go:build !js

package servercode_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	// The SQLite driver registers "sqlite", whose own errors the test reads.
	_ "modernc.org/sqlite"

	"ptah.run/internal/servercode"
)

// The driver does not build for js/wasm, so these rows live apart from the
// others, as the Oracle row does.
//
// The SQLite driver's errors come from a real engine: its error type has no
// exported fields to build one from. A key a row already holds is a duplicate
// whether it is the primary key or a UNIQUE column, and the other constraints
// SQLite enforces are not.
func TestUniqueViolation_SQLite(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      bool
	}{
		{name: "primary key", statement: "INSERT INTO t (id, code, label) VALUES (1, 'b', 'x')", want: true},
		{name: "unique column", statement: "INSERT INTO t (id, code, label) VALUES (2, 'a', 'x')", want: true},
		{name: "not null", statement: "INSERT INTO t (id, code, label) VALUES (3, 'c', NULL)"},
		{name: "check", statement: "INSERT INTO t (id, code, label) VALUES (4, 'd', '')"},
		{name: "missing table", statement: "INSERT INTO missing (id) VALUES (1)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := sql.Open("sqlite", ":memory:")
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = db.Close() })
			for _, statement := range []string{
				"CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT UNIQUE, label TEXT NOT NULL CHECK (label <> ''))",
				"INSERT INTO t (id, code, label) VALUES (1, 'a', 'x')",
			} {
				_, err := db.ExecContext(context.Background(), statement)
				c.Assert(err, qt.IsNil)
			}

			_, err = db.ExecContext(context.Background(), test.statement)

			c.Assert(err, qt.IsNotNil)
			c.Assert(servercode.UniqueViolation(err), qt.Equals, test.want)
		})
	}
}
