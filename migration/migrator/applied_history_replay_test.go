package migrator_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// The fixtures replay on SQLite, which refuses VACUUM inside a transaction.

// replayVacuumUp is a file a run in tx-mode file refuses: VACUUM beside a
// statement a transaction accepts.
const replayVacuumUp = "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\nVACUUM;\n"

// replayFixture is a directory whose up files are ups, numbered from 1.
func replayFixture(ups ...string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for index, up := range ups {
		name := fmt.Sprintf("%010d_step", index+1)
		fsys[name+".up.sql"] = &fstest.MapFile{Data: []byte(up)}
		fsys[name+".down.sql"] = &fstest.MapFile{Data: []byte("SELECT 1;\n")}
	}
	return fsys
}

// replayTables is what the replay left of the fixtures' tables, nil for none.
func replayTables(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(),
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('a', 'items') ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var tables []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		tables = append(tables, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}

// historyReplayInto replays fsys into a new SQLite database under the rule, then
// under each of modes in turn, and returns the database with the result.
func historyReplayInto(c *qt.C, fsys fstest.MapFS, modes ...migrator.MigrationTxMode) (*dbschema.DatabaseConnection, error) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.ToSlash(filepath.Join(c.TempDir(), "replay.db")))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, fsys)
	c.Assert(err, qt.IsNil)
	mig = mig.WithAppliedHistoryReplay()
	for _, mode := range modes {
		mig = mig.WithTransactionMode(mode)
	}
	return conn, mig.MigrateUp(c.Context())
}

func TestWithAppliedHistoryReplay_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		ups        []string
		wantTables []string
	}{
		{
			name:       "a file a transaction refuses runs without one",
			ups:        []string{replayVacuumUp},
			wantTables: []string{"items"},
		},
		{
			name:       "a file whose directive names none",
			ups:        []string{"-- atlas:txmode none\n" + replayVacuumUp},
			wantTables: []string{"items"},
		},
		{
			name:       "a file whose Ptah directive names none",
			ups:        []string{"-- +ptah no_transaction\n" + replayVacuumUp},
			wantTables: []string{"items"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			conn, err := historyReplayInto(c, replayFixture(test.ups...))

			c.Assert(err, qt.IsNil)
			c.Assert(replayTables(c, conn), qt.DeepEquals, test.wantTables)
		})
	}
}

func TestWithAppliedHistoryReplay_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		ups        []string
		modes      []migrator.MigrationTxMode
		wantErr    string
		wantTables []string
	}{
		{
			// Run in a transaction, the failed INSERT takes the CREATE TABLE
			// back with it.
			name:    "a file no transaction refuses runs in one",
			ups:     []string{"CREATE TABLE a (id INTEGER PRIMARY KEY);\nINSERT INTO missing (id) VALUES (1);\n"},
			wantErr: `(?s).*no such table: missing.*`,
		},
		{
			name:    "a file whose directive names file runs in a transaction",
			ups:     []string{"-- atlas:txmode file\n" + replayVacuumUp},
			wantErr: `(?s).*cannot run inside a transaction: line 3 VACUUM is refused inside a transaction block.*`,
		},
		{
			// Both modes refuse a COMMIT, so the file keeps tx-mode file and
			// the replay reports what that mode refuses.
			name:    "a file both modes refuse",
			ups:     []string{replayVacuumUp + "COMMIT;\n"},
			wantErr: `(?s).*cannot run tx-mode file up statement 3 because it controls transaction state.*`,
		},
		{
			name:    "a transaction mode set after the rule",
			ups:     []string{replayVacuumUp},
			modes:   []migrator.MigrationTxMode{migrator.MigrationTxModeFile},
			wantErr: `(?s).*cannot run inside a transaction: line 2 VACUUM is refused inside a transaction block.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			conn, err := historyReplayInto(c, replayFixture(test.ups...), test.modes...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(replayTables(c, conn), qt.DeepEquals, test.wantTables)
		})
	}
}
