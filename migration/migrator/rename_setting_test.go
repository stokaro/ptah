package migrator_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// An in-memory SQLite database is one connection, so a setting one migration
// file leaves on it is the setting the next file runs under, whatever the
// transaction mode. The first file turns legacy_alter_table on; the second
// renames a table a view reads, which rewrites the view only with the setting
// off. The run gives the connection back its value after each file, so the
// view reads the renamed table. The file-database form of this test, through
// the shipped binary, is integration/sqlite_rename_setting_e2e_test.go.
func TestMigrateUpGivesEachFileTheRenameSettingOnOneConnection(t *testing.T) {
	tests := []struct {
		name string
		mode migrator.MigrationTxMode
	}{
		{name: "one transaction per file", mode: migrator.MigrationTxModeFile},
		{name: "one transaction for all", mode: migrator.MigrationTxModeAll},
		{name: "no transaction", mode: migrator.MigrationTxModeNone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://rename?mode=memory")
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
				"000001_setting.up.sql":   {Data: []byte("PRAGMA legacy_alter_table = on;\n")},
				"000001_setting.down.sql": {Data: []byte("SELECT 1;\n")},
				"000002_rename.up.sql": {Data: []byte("CREATE TABLE t (a int);\nCREATE VIEW v AS SELECT a FROM t;\n" +
					"ALTER TABLE t RENAME TO t2;\n")},
				"000002_rename.down.sql": {Data: []byte("SELECT 1;\n")},
			})
			c.Assert(err, qt.IsNil)

			c.Assert(mig.WithTransactionMode(test.mode).MigrateUp(t.Context()), qt.IsNil)

			var view string
			c.Assert(conn.QueryRow("SELECT sql FROM sqlite_schema WHERE name = 'v'").Scan(&view), qt.IsNil)
			c.Assert(view, qt.Equals, `CREATE VIEW v AS SELECT a FROM "t2"`)
		})
	}
}
