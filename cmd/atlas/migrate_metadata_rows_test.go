package atlas_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
)

// Atlas's `migrate down` inserts a `.atlas_cloud_identifier` metadata row
// into atlas_schema_revisions even in purely local mode (measured). The compat
// surface must keep reading such
// databases: dot-prefixed versions are metadata, not migrations (#957).

const compatTxtarPostsWithDown = `-- atlas:txtar

-- migration.sql --
CREATE TABLE posts (id INTEGER PRIMARY KEY);

-- down.sql --
DROP TABLE posts;
`

func setupMetadataRowDatabase(c *qt.C, dir string) (migrationsDir, dbPath string) {
	c.Helper()
	migrationsDir = filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260801000001_create_users.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n")
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260801000002_create_posts.sql", compatTxtarPostsWithDown)
	writeAtlasApplyProjectSum(c, migrationsDir)
	dbPath = filepath.Join(dir, "state.db")

	out, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("apply output:\n%s", out))

	// The measured Atlas metadata row shape: UUID description, applied=0,
	// total=0, hash='', NULL error/error_stmt/partial_hashes.
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(
		context.Background(),
		`INSERT INTO atlas_schema_revisions
(version, description, type, applied, total, executed_at, execution_time, error, error_stmt, hash, partial_hashes, operator_version)
VALUES ('.atlas_cloud_identifier', '472fecf4-5a9c-431f-8ff1-8e1facd1d50b', 2, 0, 0, '2026-08-01 12:04:21.291103+02:00', 0, NULL, NULL, '', NULL, 'Atlas')`,
	)
	c.Assert(err, qt.IsNil)
	return migrationsDir, dbPath
}

func TestMigrateStatusToleratesMetadataDotRow(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := setupMetadataRowDatabase(c, t.TempDir())

	out, err := executeAtlasProjectCommand(
		"migrate", "status",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("status output:\n%s", out))
	c.Assert(out, qt.Contains, "Current Version: 20260801000002")
	c.Assert(out, qt.Not(qt.Contains), ".atlas_cloud_identifier")
}

func TestMigrateDownDryRunReadsRealVersionWithMetadataDotRow(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath := setupMetadataRowDatabase(c, t.TempDir())

	// Regression (#957): before the fix, --dry-run misread an existing Atlas
	// revision table as version 0 and reported the database as already at or
	// below the target.
	out, err := executeAtlasProjectCommand(
		"migrate", "down",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
		"--to-version", "20260801000001",
		"--dry-run",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("down output:\n%s", out))
	c.Assert(out, qt.Contains, "Current version: 20260801000002")
	c.Assert(out, qt.Not(qt.Contains), "already at or below target")
	// Dry run: the schema and the metadata row are untouched.
	c.Assert(sqliteTableCount(c, dbPath, "posts"), qt.Equals, 1)
}
