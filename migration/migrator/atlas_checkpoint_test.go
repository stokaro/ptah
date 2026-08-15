package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// The fixture mirrors the measured Atlas checkpoint directory from
// stokaro/ptah#954: two pre-checkpoint migrations, a checkpoint whose first
// line is the `-- atlas:checkpoint` directive, and (optionally) one
// post-checkpoint migration. Every migration creates its own marker table so
// tests can prove exactly which files executed.
const (
	atlasCheckpointPreOneVersion = int64(20250801000001)
	atlasCheckpointPreTwoVersion = int64(20250801000002)
	atlasCheckpointVersion       = int64(20260801100335)
	atlasCheckpointPostVersion   = int64(20260801100400)
)

const atlasCheckpointSQL = `-- atlas:checkpoint

-- Create "users" table
CREATE TABLE users (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT NULL
);
CREATE TABLE checkpoint_ran (id INTEGER PRIMARY KEY);
`

func atlasCheckpointDirFiles() map[string]string {
	return map[string]string{
		"20250801000001_create_users.sql": "-- create \"users\" table\n" +
			"CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);\n" +
			"CREATE TABLE pre_one_ran (id INTEGER PRIMARY KEY);\n",
		"20250801000002_add_email.sql": "-- add \"email\" column\n" +
			"ALTER TABLE users ADD COLUMN email TEXT NULL;\n" +
			"CREATE TABLE pre_two_ran (id INTEGER PRIMARY KEY);\n",
		"20260801100335_checkpoint.sql": atlasCheckpointSQL,
	}
}

func atlasCheckpointFS(tb testing.TB, files map[string]string) fstest.MapFS {
	c := qt.New(tb)
	c.Helper()
	fsys := fstest.MapFS{}
	for name, content := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(content)}
	}
	return fsys
}

func newAtlasCheckpointConn(tb testing.TB) *dbschema.DatabaseConnection {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+filepath.Join(c.TempDir(), "checkpoint.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newAtlasFormatMigrator(tb testing.TB, conn *dbschema.DatabaseConnection, fsys fstest.MapFS) *migrator.Migrator {
	c := qt.New(tb)
	c.Helper()
	m, err := migrator.NewFSMigrator(conn, fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	c.Assert(err, qt.IsNil)
	return m
}

func sqliteHasTable(tb testing.TB, conn *dbschema.DatabaseConnection, name string) bool {
	c := qt.New(tb)
	c.Helper()
	var count int
	err := conn.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name = ?", name).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

func appliedPtahVersions(tb testing.TB, conn *dbschema.DatabaseConnection) []int64 {
	c := qt.New(tb)
	c.Helper()
	rows, err := conn.Query("SELECT version FROM schema_migrations WHERE state = 'applied' ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var versions []int64
	for rows.Next() {
		var version int64
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

func TestAtlasCheckpointDirective_Detection_HappyPath(t *testing.T) {

	tests := []struct {
		name string
		sql  string
	}{
		{name: "plain first-line directive", sql: atlasCheckpointSQL},
		{
			name: "directive with trailing spaces",
			sql:  "-- atlas:checkpoint   \nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		},
		{
			name: "directive with trailing arguments",
			sql:  "-- atlas:checkpoint v2\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		},
		{
			name: "directive with CRLF line ending",
			sql:  "-- atlas:checkpoint\r\nCREATE TABLE users (id INTEGER PRIMARY KEY);\r\n",
		},
		{
			// Deliberate tolerance, matching the txtar matcher: leading
			// whitespace before the directive still marks a checkpoint. The
			// Atlas-emitted shape is column 0; hand-indented copies in hashed
			// directories trip the sum gate before this matters.
			name: "directive with leading whitespace",
			sql:  "  -- atlas:checkpoint\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := atlasCheckpointFS(c.TB, map[string]string{
				"20260801100335_checkpoint.sql": test.sql,
			})
			provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
			c.Assert(err, qt.IsNil)
			migrations := provider.Migrations()
			c.Assert(migrations, qt.HasLen, 1)
			c.Assert(migrations[0].IsCheckpoint, qt.IsTrue)
		})
	}
}

func TestAtlasCheckpointDirective_NotACheckpoint(t *testing.T) {

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "directive on line two after a comment is not a checkpoint",
			sql:  "-- ordinary comment\n-- atlas:checkpoint\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		},
		{
			name: "directive on line two after a blank line is not a checkpoint",
			sql:  "\n-- atlas:checkpoint\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		},
		{
			name: "directive text inside the body is not a checkpoint",
			sql:  "CREATE TABLE users (id INTEGER PRIMARY KEY);\n-- atlas:checkpoint\n",
		},
		{
			name: "similarly named directive is not a checkpoint",
			sql:  "-- atlas:checkpointing\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := atlasCheckpointFS(c.TB, map[string]string{
				"20260801100335_checkpoint.sql": test.sql,
			})
			provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
			c.Assert(err, qt.IsNil)
			migrations := provider.Migrations()
			c.Assert(migrations, qt.HasLen, 1)
			c.Assert(migrations[0].IsCheckpoint, qt.IsFalse)
		})
	}
}

func TestAtlasCheckpointDirective_TxtarConflict_FailurePath(t *testing.T) {

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "checkpoint before txtar",
			sql:  "-- atlas:checkpoint\n-- atlas:txtar\n\n-- migration.sql --\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		},
		{
			name: "txtar before checkpoint",
			sql:  "-- atlas:txtar\n-- atlas:checkpoint\n\n-- migration.sql --\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		},
		{
			name: "blank line between conflicting directives",
			sql:  "-- atlas:checkpoint\n\n-- atlas:txtar\n\n-- migration.sql --\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := atlasCheckpointFS(c.TB, map[string]string{
				"20260801100335_checkpoint.sql": test.sql,
			})
			var conflict *migrator.AtlasCheckpointTxtarConflictError
			_, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
			c.Assert(err, qt.ErrorAs, &conflict)
			c.Assert(conflict.Path, qt.Equals, "20260801100335_checkpoint.sql")
		})
	}
}

func TestAtlasCheckpointDirective_TxtarSectionContentIsNotAConflict(t *testing.T) {
	c := qt.New(t)

	// A `-- atlas:checkpoint` line inside a txtar section is section content,
	// not a leading directive: the file loads as an ordinary txtar migration.
	fsys := atlasCheckpointFS(c.TB, map[string]string{
		"20260801100335_widgets.sql": "-- atlas:txtar\n\n-- migration.sql --\n-- atlas:checkpoint\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
	})

	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))

	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].IsCheckpoint, qt.IsFalse)
}

func TestAtlasCheckpoint_FreshDatabaseAppliesOnlyCheckpointAndLater(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newAtlasCheckpointConn(c.TB)

	files := atlasCheckpointDirFiles()
	files["20260801100400_add_widgets.sql"] = "CREATE TABLE post_ran (id INTEGER PRIMARY KEY);\n"
	m := newAtlasFormatMigrator(c.TB, conn, atlasCheckpointFS(c.TB, files))

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	// Only the checkpoint and post-checkpoint migrations executed; the
	// squashed pre-checkpoint files never ran.
	c.Assert(sqliteHasTable(c.TB, conn, "pre_one_ran"), qt.IsFalse)
	c.Assert(sqliteHasTable(c.TB, conn, "pre_two_ran"), qt.IsFalse)
	c.Assert(sqliteHasTable(c.TB, conn, "checkpoint_ran"), qt.IsTrue)
	c.Assert(sqliteHasTable(c.TB, conn, "post_ran"), qt.IsTrue)
	c.Assert(sqliteHasTable(c.TB, conn, "users"), qt.IsTrue)

	// Bookkeeping records exactly the executed migrations.
	c.Assert(appliedPtahVersions(c.TB, conn), qt.DeepEquals, []int64{atlasCheckpointVersion, atlasCheckpointPostVersion})

	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.HasPendingChanges, qt.IsFalse)
	c.Assert(status.CurrentVersion, qt.Equals, atlasCheckpointPostVersion)
}

func TestAtlasCheckpoint_PreCheckpointDatabaseSkipsCheckpointSilently(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newAtlasCheckpointConn(c.TB)

	// Seed the database through ordinary pre-checkpoint history, mirroring a
	// project that migrated before the checkpoint was cut.
	files := atlasCheckpointDirFiles()
	preOnly := map[string]string{
		"20250801000001_create_users.sql": files["20250801000001_create_users.sql"],
		"20250801000002_add_email.sql":    files["20250801000002_add_email.sql"],
	}
	c.Assert(newAtlasFormatMigrator(c.TB, conn, atlasCheckpointFS(c.TB, preOnly)).MigrateUp(ctx), qt.IsNil)
	c.Assert(appliedPtahVersions(c.TB, conn), qt.DeepEquals, []int64{atlasCheckpointPreOneVersion, atlasCheckpointPreTwoVersion})

	// The full directory now contains the checkpoint. It must be skipped
	// silently: no execution, no bookkeeping row, clean status.
	m := newAtlasFormatMigrator(c.TB, conn, atlasCheckpointFS(c.TB, files))
	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
	c.Assert(status.HasPendingChanges, qt.IsFalse)

	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(sqliteHasTable(c.TB, conn, "checkpoint_ran"), qt.IsFalse)
	c.Assert(appliedPtahVersions(c.TB, conn), qt.DeepEquals, []int64{atlasCheckpointPreOneVersion, atlasCheckpointPreTwoVersion})
}

func TestAtlasCheckpoint_PreCheckpointDatabaseAppliesOnlyPostCheckpointHistory(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newAtlasCheckpointConn(c.TB)

	files := atlasCheckpointDirFiles()
	preOnly := map[string]string{
		"20250801000001_create_users.sql": files["20250801000001_create_users.sql"],
		"20250801000002_add_email.sql":    files["20250801000002_add_email.sql"],
	}
	c.Assert(newAtlasFormatMigrator(c.TB, conn, atlasCheckpointFS(c.TB, preOnly)).MigrateUp(ctx), qt.IsNil)

	files["20260801100400_add_widgets.sql"] = "CREATE TABLE post_ran (id INTEGER PRIMARY KEY);\n"
	m := newAtlasFormatMigrator(c.TB, conn, atlasCheckpointFS(c.TB, files))

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	// The checkpoint stays skipped; only genuinely new history runs.
	c.Assert(sqliteHasTable(c.TB, conn, "checkpoint_ran"), qt.IsFalse)
	c.Assert(sqliteHasTable(c.TB, conn, "post_ran"), qt.IsTrue)
	c.Assert(appliedPtahVersions(c.TB, conn), qt.DeepEquals, []int64{
		atlasCheckpointPreOneVersion,
		atlasCheckpointPreTwoVersion,
		atlasCheckpointPostVersion,
	})
}

func TestAtlasCheckpoint_MultipleCheckpointsLatestWins(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newAtlasCheckpointConn(c.TB)

	files := atlasCheckpointDirFiles()
	files["20260801200000_checkpoint.sql"] = "-- atlas:checkpoint\n" +
		"CREATE TABLE users (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT NULL);\n" +
		"CREATE TABLE second_checkpoint_ran (id INTEGER PRIMARY KEY);\n"
	m := newAtlasFormatMigrator(c.TB, conn, atlasCheckpointFS(c.TB, files))

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	// Only the newest checkpoint bootstraps a fresh database.
	c.Assert(sqliteHasTable(c.TB, conn, "checkpoint_ran"), qt.IsFalse)
	c.Assert(sqliteHasTable(c.TB, conn, "second_checkpoint_ran"), qt.IsTrue)
	c.Assert(appliedPtahVersions(c.TB, conn), qt.DeepEquals, []int64{20260801200000})
}

func TestAtlasCheckpoint_CheckpointAsOnlyFile(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newAtlasCheckpointConn(c.TB)

	fsys := atlasCheckpointFS(c.TB, map[string]string{
		"20260801100335_checkpoint.sql": atlasCheckpointSQL,
	})
	m := newAtlasFormatMigrator(c.TB, conn, fsys)

	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(sqliteHasTable(c.TB, conn, "checkpoint_ran"), qt.IsTrue)
	c.Assert(appliedPtahVersions(c.TB, conn), qt.DeepEquals, []int64{atlasCheckpointVersion})

	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.HasPendingChanges, qt.IsFalse)
}

// newAtlasRevisionCheckpointMigrator mirrors the ptah-compat apply runtime: an
// Atlas-format directory recorded in the Atlas revision table format.
func newAtlasRevisionCheckpointMigrator(tb testing.TB, conn *dbschema.DatabaseConnection, fsys fstest.MapFS) *migrator.Migrator {
	c := qt.New(tb)
	c.Helper()
	return newAtlasFormatMigrator(c.TB, conn, fsys).WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
}

type atlasRevisionRow struct {
	Version     int64
	Description string
	Type        int64
	Applied     int64
	Total       int64
}

func atlasRevisionRows(tb testing.TB, conn *dbschema.DatabaseConnection) []atlasRevisionRow {
	c := qt.New(tb)
	c.Helper()
	rows, err := conn.Query("SELECT version, description, type, applied, total FROM atlas_schema_revisions ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var revisions []atlasRevisionRow
	for rows.Next() {
		var row atlasRevisionRow
		c.Assert(rows.Scan(&row.Version, &row.Description, &row.Type, &row.Applied, &row.Total), qt.IsNil)
		revisions = append(revisions, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return revisions
}

func TestAtlasCheckpoint_FreshDatabaseAtlasRevisionRowShape(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newAtlasCheckpointConn(c.TB)

	// The checkpoint carries exactly one statement, like the measured fixture,
	// so applied/total pin the per-statement counters to 1/1.
	files := atlasCheckpointDirFiles()
	files["20260801100335_checkpoint.sql"] = "-- atlas:checkpoint\n\n" +
		"-- Create \"users\" table\n" +
		"CREATE TABLE users (\n" +
		"  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\n" +
		"  name TEXT NOT NULL,\n" +
		"  email TEXT NULL\n" +
		");\n"
	m := newAtlasRevisionCheckpointMigrator(c.TB, conn, atlasCheckpointFS(c.TB, files))

	c.Assert(m.MigrateUp(ctx), qt.IsNil)

	// Measured Atlas writes exactly one revision row for a checkpoint
	// bootstrap: `20260801100335|checkpoint|2|1|1` (stokaro/ptah#954 sqlite
	// dump). No rows are recorded for the squashed pre-checkpoint history.
	c.Assert(atlasRevisionRows(c.TB, conn), qt.DeepEquals, []atlasRevisionRow{
		{
			Version:     atlasCheckpointVersion,
			Description: "checkpoint",
			Type:        2,
			Applied:     1,
			Total:       1,
		},
	})
}

func TestAtlasCheckpoint_PreCheckpointAtlasRevisionsUnchanged(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := newAtlasCheckpointConn(c.TB)

	files := atlasCheckpointDirFiles()
	preOnly := map[string]string{
		"20250801000001_create_users.sql": files["20250801000001_create_users.sql"],
		"20250801000002_add_email.sql":    files["20250801000002_add_email.sql"],
	}
	c.Assert(newAtlasRevisionCheckpointMigrator(c.TB, conn, atlasCheckpointFS(c.TB, preOnly)).MigrateUp(ctx), qt.IsNil)
	seeded := atlasRevisionRows(c.TB, conn)
	c.Assert(seeded, qt.HasLen, 2)

	// Measured Atlas skips the checkpoint on a pre-checkpoint database with
	// "No migration files to execute" and writes no revision row.
	m := newAtlasRevisionCheckpointMigrator(c.TB, conn, atlasCheckpointFS(c.TB, files))
	status, err := m.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.HasPendingChanges, qt.IsFalse)

	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(atlasRevisionRows(c.TB, conn), qt.DeepEquals, seeded)
	c.Assert(sqliteHasTable(c.TB, conn, "checkpoint_ran"), qt.IsFalse)
}
