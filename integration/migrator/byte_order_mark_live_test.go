//go:build integration

package migrator_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// byteOrderMark is the UTF-8 byte order mark, EF BB BF. A server that
// receives it as the first character of a statement refuses the statement as
// a syntax error.
const byteOrderMark = "\xEF\xBB\xBF"

// byteOrderMarkRepeatable is a repeatable body that starts with a byte order
// mark, kept apart so the record can be compared with the bytes.
const byteOrderMarkRepeatable = byteOrderMark + repeatableEnginesView

// byteOrderMarkMigrations is a versioned migration and a repeatable migration,
// each starting with a byte order mark, plus extra files.
func byteOrderMarkMigrations(extra map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{
		"0000000001_widgets.up.sql":   &fstest.MapFile{Data: []byte(byteOrderMark + repeatableEnginesTable)},
		"0000000001_widgets.down.sql": &fstest.MapFile{Data: []byte(byteOrderMark + "DROP TABLE widgets;\n")},
		"R__widget_ids.sql":           &fstest.MapFile{Data: []byte(byteOrderMarkRepeatable)},
	}
	for name, body := range extra {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fsys
}

// count reads the number of rows a query over the target returns.
func (r *repeatableEngineTarget) count(c *qt.C, query string) int {
	c.Helper()
	var n int
	c.Assert(r.conn.QueryRowContext(context.Background(), query).Scan(&n), qt.IsNil)
	return n
}

// The byte order mark at the start of a file is not sent to the server: the
// table and the view are created and can be read. The repeatable's record
// keeps the bytes of the file, mark included, and its digest is the h1 hash of
// those bytes, the entry ptah.sum carries for the file.
func TestByteOrderMarkOnEnginesLive_HappyPath(t *testing.T) {
	for _, engine := range repeatableEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := byteOrderMarkMigrations(nil)

			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)

			c.Assert(r.columns(c, "widgets"), qt.Equals, 1)
			c.Assert(r.columns(c, "widget_ids"), qt.Equals, 1)
			c.Assert(r.body(c, "widget_ids"), qt.DeepEquals, keptBody{Body: byteOrderMarkRepeatable, Kept: true})
			record := r.record(c, fsys, "widget_ids")
			c.Assert(record.AppliedChecksum, qt.Equals, repeatableDigest(byteOrderMarkRepeatable))
		})
	}
}

// A directive on the first line of a file still decides how the file runs
// when a byte order mark comes before it. PostgreSQL refuses CREATE INDEX
// CONCURRENTLY inside a transaction, so the index exists only if the file ran
// outside one, as `-- +ptah no_transaction` asks.
func TestByteOrderMarkDirectiveOnPostgresLive_HappyPath(t *testing.T) {
	c := qt.New(t)
	r := newRepeatableEngineTarget(c, t, repeatablePostgres)
	fsys := byteOrderMarkMigrations(map[string]string{
		"0000000002_widgets_index.up.sql": byteOrderMark + "-- +ptah no_transaction\n" +
			"CREATE INDEX CONCURRENTLY widgets_id_idx ON widgets (id);\n",
		"0000000002_widgets_index.down.sql": "DROP INDEX widgets_id_idx;\n",
	})

	c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)

	c.Assert(r.count(c, "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'widgets_id_idx'"), qt.Equals, 1)
}

// Only the mark at the very start of a file is removed. U+FEFF in front of a
// later statement is part of that statement, and the server refuses it.
func TestByteOrderMarkOnEnginesLive_FailurePath(t *testing.T) {
	for _, engine := range repeatableEngines {
		t.Run(engine.name+": a mark in the middle of a file reaches the server", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := byteOrderMarkMigrations(map[string]string{
				"0000000002_gadgets.up.sql":   "CREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n" + byteOrderMark + "CREATE TABLE gizmos (id INTEGER PRIMARY KEY);\n",
				"0000000002_gadgets.down.sql": "DROP TABLE gadgets;\n",
			})

			err := r.up(c, fsys, migrator.MigrateUpOptions{})

			c.Assert(err, qt.ErrorMatches, `(?s).*failed to apply migration 2: .*SQL: \x{FEFF}CREATE TABLE gizmos.*`)
			c.Assert(r.columns(c, "widgets"), qt.Equals, 1)
			c.Assert(r.columns(c, "gizmos"), qt.Equals, 0)
		})
	}
}
