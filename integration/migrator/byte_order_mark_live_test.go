//go:build integration

package migrator_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// byteOrderMark is the UTF-8 byte order mark, EF BB BF. A server that
// receives it as the first character of a statement refuses the statement as
// a syntax error.
const byteOrderMark = "\xEF\xBB\xBF"

// byteOrderMarkRepeatable is a repeatable body that starts with a byte order
// mark, kept apart so the record can be compared with the bytes.
func byteOrderMarkRepeatable(engine repeatableEngine) string {
	return byteOrderMark + engine.widgetIDs()
}

// byteOrderMarkMigrations is a versioned migration and a repeatable migration,
// each starting with a byte order mark, plus extra files.
func byteOrderMarkMigrations(engine repeatableEngine, extra map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{
		"0000000001_widgets.up.sql":   &fstest.MapFile{Data: []byte(byteOrderMark + repeatableEnginesTable)},
		"0000000001_widgets.down.sql": &fstest.MapFile{Data: []byte(byteOrderMark + "DROP TABLE widgets;\n")},
		"R__widget_ids.sql":           &fstest.MapFile{Data: []byte(byteOrderMarkRepeatable(engine))},
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

// revisionChecksum reads the checksum the revision row of version keeps.
func (r *repeatableEngineTarget) revisionChecksum(c *qt.C, version int64) string {
	c.Helper()
	var checksum string
	query := sqlutil.Rebind(r.engine.dialect,
		"SELECT checksum FROM "+sqlident.Qualified(r.engine.dialect, "", "schema_migrations")+" WHERE version = ?")
	c.Assert(r.conn.QueryRowContext(context.Background(), query, version).Scan(&checksum), qt.IsNil)
	return checksum
}

// upWith runs every pending migration of fsys through a migrator built with
// opts, on a connection of its own.
func (r *repeatableEngineTarget) upWith(c *qt.C, fsys fstest.MapFS, opts ...migrator.FSProviderOption) error {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), r.url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, fsys, opts...)
	c.Assert(err, qt.IsNil)
	return mig.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{})
}

// sha256Hex is the checksum a native revision row keeps for SQL text,
// computed here rather than taken from the code under test.
func sha256Hex(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// The byte order mark at the start of a file is not sent to the server: the
// table and the view are created and can be read. The repeatable's record
// keeps the bytes of the file, mark included, and its digest is the h1 hash of
// those bytes, the entry ptah.sum carries for the file. The revision row of
// the versioned file hashes the SQL that ran, which leaves the mark out.
func TestByteOrderMarkOnEnginesLive_HappyPath(t *testing.T) {
	for _, engine := range repeatableEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := byteOrderMarkMigrations(engine, nil)

			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)

			c.Assert(r.columns(c, "widgets"), qt.Equals, 1)
			c.Assert(r.columns(c, "widget_ids"), qt.Equals, 1)
			c.Assert(r.body(c, "widget_ids"), qt.DeepEquals, keptBody{Body: byteOrderMarkRepeatable(engine), Kept: true})
			record := r.record(c, fsys, "widget_ids")
			c.Assert(record.AppliedChecksum, qt.Equals, repeatableDigest(byteOrderMarkRepeatable(engine)))
			c.Assert(r.revisionChecksum(c, 1), qt.Equals, sha256Hex(repeatableEnginesTable))
		})
	}
}

// A down file that starts with a byte order mark rolls its version back.
func TestByteOrderMarkDownOnEnginesLive_HappyPath(t *testing.T) {
	for _, engine := range repeatableEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			fsys := fstest.MapFS{
				"0000000001_widgets.up.sql":   &fstest.MapFile{Data: []byte(byteOrderMark + repeatableEnginesTable)},
				"0000000001_widgets.down.sql": &fstest.MapFile{Data: []byte(byteOrderMark + "DROP TABLE widgets;\n")},
			}
			c.Assert(r.up(c, fsys, migrator.MigrateUpOptions{}), qt.IsNil)
			c.Assert(r.columns(c, "widgets"), qt.Equals, 1)

			c.Assert(r.migrator(c, fsys).MigrateDownTo(context.Background(), 0), qt.IsNil)

			c.Assert(r.columns(c, "widgets"), qt.Equals, 0)
		})
	}
}

// A directive on the first line of a file still decides how the file runs
// when a byte order mark comes before it. PostgreSQL refuses CREATE INDEX
// CONCURRENTLY inside a transaction, so the index exists only if the file ran
// outside one, as `-- +ptah no_transaction` and `-- atlas:txmode none` ask.
func TestByteOrderMarkDirectiveOnPostgresLive_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		fsys   fstest.MapFS
		format migrationfile.DirFormat
	}{
		{
			name: "+ptah no_transaction",
			fsys: fstest.MapFS{
				"0000000001_widgets.up.sql":   &fstest.MapFile{Data: []byte(byteOrderMark + repeatableEnginesTable)},
				"0000000001_widgets.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE widgets;\n")},
				"0000000002_widgets_index.up.sql": &fstest.MapFile{Data: []byte(byteOrderMark +
					"-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY widgets_id_idx ON widgets (id);\n")},
				"0000000002_widgets_index.down.sql": &fstest.MapFile{Data: []byte("DROP INDEX widgets_id_idx;\n")},
			},
			format: migrationfile.DirFormatPtah,
		},
		{
			name: "atlas:txmode none",
			fsys: fstest.MapFS{
				"20260101000000_widgets.sql": &fstest.MapFile{Data: []byte(byteOrderMark + repeatableEnginesTable)},
				"20260101000001_widgets_index.sql": &fstest.MapFile{Data: []byte(byteOrderMark +
					"-- atlas:txmode none\n\nCREATE INDEX CONCURRENTLY widgets_id_idx ON widgets (id);\n")},
			},
			format: migrationfile.DirFormatAtlas,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, repeatablePostgres)

			c.Assert(r.upWith(c, tt.fsys, migrator.WithMigrationDirFormat(tt.format)), qt.IsNil)

			c.Assert(r.count(c, "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'widgets_id_idx'"), qt.Equals, 1)
		})
	}
}

// byteOrderMarkRefusingEngines are the engines whose server refuses U+FEFF
// in front of a statement as a syntax error.
var byteOrderMarkRefusingEngines = []repeatableEngine{
	repeatablePostgres, repeatableCockroachDB, repeatableYugabyteDB, repeatableMySQL, repeatableMariaDB,
	repeatableSQLServer, repeatableOracle,
}

// byteOrderMarkWhitespaceEngines are the engines whose server reads U+FEFF
// between statements as whitespace: ClickHouse's lexer skips it, so a mark in
// the middle of a file runs there as if it were absent.
var byteOrderMarkWhitespaceEngines = []repeatableEngine{repeatableClickHouse}

// byteOrderMarkInTheMiddle is a second versioned migration whose second
// statement starts with a byte order mark, which no reader removes.
func byteOrderMarkInTheMiddle(engine repeatableEngine) fstest.MapFS {
	return byteOrderMarkMigrations(engine, map[string]string{
		"0000000002_gadgets.up.sql":   "CREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n" + byteOrderMark + "CREATE TABLE gizmos (id INTEGER PRIMARY KEY);\n",
		"0000000002_gadgets.down.sql": "DROP TABLE gadgets;\n",
	})
}

// Every engine the repeatable suite runs on says what its server does with a
// mark in the middle of a file, so an engine added to the suite is not left
// out of both tests below.
func TestByteOrderMarkEngineListsCoverTheSuite_HappyPath(t *testing.T) {
	c := qt.New(t)
	var listed, suite []string
	for _, engine := range slices.Concat(byteOrderMarkRefusingEngines, byteOrderMarkWhitespaceEngines) {
		listed = append(listed, engine.name)
	}
	for _, engine := range repeatableEngines {
		suite = append(suite, engine.name)
	}
	c.Assert(listed, qt.ContentEquals, suite)
}

// Only the mark at the very start of a file is removed. U+FEFF in front of a
// later statement reaches the server as part of that statement, and a server
// that reads it as whitespace runs the statement.
func TestByteOrderMarkInTheMiddleOnEnginesLive_HappyPath(t *testing.T) {
	for _, engine := range byteOrderMarkWhitespaceEngines {
		t.Run(engine.name+": a mark in the middle of a file reaches the server", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)

			c.Assert(r.up(c, byteOrderMarkInTheMiddle(engine), migrator.MigrateUpOptions{}), qt.IsNil)

			c.Assert(r.columns(c, "gadgets"), qt.Equals, 1)
			c.Assert(r.columns(c, "gizmos"), qt.Equals, 1)
		})
	}
}

// Only the mark at the very start of a file is removed. U+FEFF in front of a
// later statement is part of that statement, and a server that does not read
// it as whitespace refuses it.
func TestByteOrderMarkOnEnginesLive_FailurePath(t *testing.T) {
	for _, engine := range byteOrderMarkRefusingEngines {
		t.Run(engine.name+": a mark in the middle of a file reaches the server", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)

			err := r.up(c, byteOrderMarkInTheMiddle(engine), migrator.MigrateUpOptions{})

			c.Assert(err, qt.ErrorMatches, `(?s).*failed to apply migration 2: .*SQL: \x{FEFF}CREATE TABLE gizmos.*`)
			c.Assert(r.columns(c, "widgets"), qt.Equals, 1)
			c.Assert(r.columns(c, "gizmos"), qt.Equals, 0)
		})
	}
}
