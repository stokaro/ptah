package migrationreplay_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/migrationreplay"
	"ptah.run/migration/migrationfile"
)

// TestReplay_MigrationErrorNamesTheStatementTheEngineRefused pins what a
// replay that stopped inside a migration says about it as data: which
// migration, which statement, and the driver's own answer with nothing Ptah
// wrapped around it.
//
// The second row is the bundled engine deciding. SQLite 3.54 refuses to rename
// a column to a rowid alias, so `RENAME COLUMN id TO oid` stops the replay
// there, in the base migration, on every engine Ptah ships.
func TestReplay_MigrationErrorNamesTheStatementTheEngineRefused(t *testing.T) {
	tests := []struct {
		name        string
		files       fstest.MapFS
		wantVersion int64
		wantKey     string
		wantSQL     string
		wantIndex   int
		wantAnswer  string
		wantMessage string
	}{
		{
			name: "a column the table does not have",
			files: fstest.MapFS{
				"1.sql": {Data: []byte("CREATE TABLE users (id int);\n")},
				"2.sql": {Data: []byte("CREATE TABLE pets (id int);\n\nALTER TABLE users RENAME COLUMN nope TO x;\n")},
			},
			wantVersion: 2,
			wantKey:     "2",
			wantSQL:     "ALTER TABLE users RENAME COLUMN nope TO x",
			wantIndex:   2,
			wantAnswer:  `SQL logic error: no such column: "nope" (1)`,
			wantMessage: `(?s)replay migration 2 on dev database: failed to execute migration SQL: ` +
				`sqlite: SQL execution failed: SQL logic error: no such column: "nope" \(1\)\n` +
				`SQL: ALTER TABLE users RENAME COLUMN nope TO x`,
		},
		{
			name: "a rename to a rowid alias",
			files: fstest.MapFS{
				"1.sql": {Data: []byte("CREATE TABLE users (id int);\n\nALTER TABLE users RENAME COLUMN id TO oid;\n")},
				"2.sql": {Data: []byte("DROP TABLE users;\n")},
			},
			wantVersion: 1,
			wantKey:     "1",
			wantSQL:     "ALTER TABLE users RENAME COLUMN id TO oid",
			wantIndex:   2,
			wantAnswer:  "SQL logic error: cannot rename to rowid alias: oid (1)",
			wantMessage: `(?s)replay migration 1 on dev database: failed to execute migration SQL: .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := migrationreplay.Replay(t.Context(), migrationreplay.Options{
				DirFormat: migrationfile.DirFormatAtlas,
				DevURL:    "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
				FS:        test.files,
			})

			c.Assert(err, qt.ErrorMatches, test.wantMessage)
			failed, ok := errors.AsType[*migrationreplay.MigrationError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(failed.Version, qt.Equals, test.wantVersion)
			c.Assert(failed.VersionKey, qt.Equals, test.wantKey)
			c.Assert(failed.Statement, qt.IsNotNil)
			c.Assert(failed.Statement.SQL, qt.Equals, test.wantSQL)
			c.Assert(failed.Statement.Index, qt.Equals, test.wantIndex)
			c.Assert(failed.Statement.Answer.Error(), qt.Equals, test.wantAnswer)
			c.Assert(errors.Unwrap(failed.Statement.Answer), qt.IsNil)
		})
	}
}

// TestReplay_MigrationErrorLeavesARefusalWithoutAStatement pins the other half:
// a statement the replay guard refused never reached the dev database, so the
// migration error names the migration and no statement the database answered.
func TestReplay_MigrationErrorLeavesARefusalWithoutAStatement(t *testing.T) {
	c := qt.New(t)

	err := migrationreplay.Replay(t.Context(), migrationreplay.Options{
		DirFormat: migrationfile.DirFormatAtlas,
		DevURL:    "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
		FS: fstest.MapFS{
			"1_attach.sql": {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\nATTACH DATABASE ':memory:' AS aux;\n")},
		},
	})

	c.Assert(err, qt.ErrorMatches, `(?s)replay migration 1 on dev database: .*sqlite migration replay rejects ATTACH .*`)
	failed, ok := errors.AsType[*migrationreplay.MigrationError](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(failed.VersionKey, qt.Equals, "1")
	c.Assert(failed.Statement, qt.IsNil)
}

// TestReplay_MigrationErrorIsAbsentBeforeTheFirstMigration pins that a replay
// refused before it ran any migration names none: the dev database here holds
// a table, so the replay stops at the clean check.
func TestReplay_MigrationErrorIsAbsentBeforeTheFirstMigration(t *testing.T) {
	c := qt.New(t)
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	conn, err := dbschema.ConnectToDatabase(t.Context(), devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(t.Context(), "CREATE TABLE stale_replay_runs (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1.sql"), []byte("CREATE TABLE users (id int);\n"), 0o600), qt.IsNil)

	err = migrationreplay.Replay(t.Context(), migrationreplay.Options{
		Dir:       migrationsDir,
		DirFormat: migrationfile.DirFormatAtlas,
		DevURL:    devURL,
	})

	c.Assert(err, qt.ErrorMatches, devNotCleanRefusal)
	_, ok := errors.AsType[*migrationreplay.MigrationError](err)
	c.Assert(ok, qt.IsFalse)
}
