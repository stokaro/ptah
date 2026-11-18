//go:build integration

package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// SQLite accepts a byte order mark in front of a statement, so on SQLite a
// mark that hid the directive behind it would not fail: the file would run in
// a transaction it asked not to have. VACUUM refuses to run inside a
// transaction, so the file applies only if the directive held.
func TestByteOrderMarkDirectiveOnSQLite_HappyPath(t *testing.T) {
	const body = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\nVACUUM;\n"
	tests := []struct {
		name   string
		fsys   fstest.MapFS
		format migrationfile.DirFormat
	}{
		{
			name: "+ptah no_transaction",
			fsys: fstest.MapFS{
				"0000000001_widgets.up.sql":   &fstest.MapFile{Data: []byte(byteOrderMark + "-- +ptah no_transaction\n" + body)},
				"0000000001_widgets.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE widgets;\n")},
			},
			format: migrationfile.DirFormatPtah,
		},
		{
			name: "atlas:txmode none",
			fsys: fstest.MapFS{
				"20260101000000_widgets.sql": &fstest.MapFile{Data: []byte(byteOrderMark + "-- atlas:txmode none\n\n" + body)},
			},
			format: migrationfile.DirFormatAtlas,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+filepath.Join(t.TempDir(), "bom.db"))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			mig, err := migrator.NewFSMigrator(conn, tt.fsys, migrator.WithMigrationDirFormat(tt.format))
			c.Assert(err, qt.IsNil)

			c.Assert(mig.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{}), qt.IsNil)

			var tables int
			c.Assert(conn.QueryRowContext(context.Background(),
				"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'widgets'").Scan(&tables), qt.IsNil)
			c.Assert(tables, qt.Equals, 1)
		})
	}
}
