package atlas_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
)

func TestMigrateApplyExecutesAtlasRepeatableMigration(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "repeatable.db")
	writeAtlasApplyProjectMigration(c.TB, migrationsDir, "1_users.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectMigration(c.TB, migrationsDir, "2R_repeatable.sql", "CREATE TABLE active_users (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectSum(c.TB, migrationsDir)

	output, err := runCompatCommand(t,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(output, qt.Contains, "Migrating to version 2R from 2 pending migrations.")
	c.Assert(output, qt.Contains, "Migration complete. Current version: 2R")
	assertRepeatableSQLiteTableExists(c.TB, dbPath, "users")
	assertRepeatableSQLiteTableExists(c.TB, dbPath, "active_users")
	c.Assert(repeatableSQLiteAtlasAppliedVersions(c.TB, dbPath), qt.DeepEquals, []string{"1", "2R"})
}

func TestMigrateApplyExecutesAtlasBareRepeatableMigration(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "bare-repeatable.db")
	writeAtlasApplyProjectMigration(c.TB, migrationsDir, "R__bootstrap.sql", "CREATE TABLE repeatable_bootstrap (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectSum(c.TB, migrationsDir)

	output, err := runCompatCommand(t,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(output, qt.Contains, "Migrating to version R from 1 pending migrations.")
	c.Assert(output, qt.Contains, "Migration complete. Current version: R")
	assertRepeatableSQLiteTableExists(c.TB, dbPath, "repeatable_bootstrap")
	c.Assert(repeatableSQLiteAtlasAppliedVersions(c.TB, dbPath), qt.DeepEquals, []string{"R"})
}

func TestMigrateApplyDoesNotReapplyAtlasRepeatableOnChecksumChange(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "repeatable-no-reapply.db")
	writeAtlasApplyProjectMigration(c.TB, migrationsDir, "1_users.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectMigration(c.TB, migrationsDir, "2R_repeatable.sql", "CREATE TABLE repeatable_once (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectSum(c.TB, migrationsDir)

	firstOutput, firstErr := runCompatCommand(t,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(firstErr, qt.IsNil)
	c.Assert(firstOutput, qt.Contains, "Migration complete. Current version: 2R")

	writeAtlasApplyProjectMigration(c.TB, migrationsDir, "2R_repeatable.sql", "CREATE TABLE repeatable_changed (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectSum(c.TB, migrationsDir)

	secondOutput, secondErr := runCompatCommand(t,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+migrationsDir,
	)

	c.Assert(secondErr, qt.IsNil)
	c.Assert(secondOutput, qt.Contains, "No migration files to execute")
	assertRepeatableSQLiteTableExists(c.TB, dbPath, "repeatable_once")
	assertRepeatableSQLiteTableMissing(c.TB, dbPath, "repeatable_changed")
	c.Assert(repeatableSQLiteAtlasAppliedVersions(c.TB, dbPath), qt.DeepEquals, []string{"1", "2R"})
}

func assertRepeatableSQLiteTableExists(tb testing.TB, dbPath, table string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(repeatableSQLiteTableCount(c.TB, dbPath, table), qt.Equals, 1)
}

func assertRepeatableSQLiteTableMissing(tb testing.TB, dbPath, table string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(repeatableSQLiteTableCount(c.TB, dbPath, table), qt.Equals, 0)
}

func repeatableSQLiteTableCount(tb testing.TB, dbPath, table string) int {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var count int
	err = conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func repeatableSQLiteAtlasAppliedVersions(tb testing.TB, dbPath string) []string {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	rows, err := conn.QueryContext(
		context.Background(),
		"SELECT version FROM atlas_schema_revisions ORDER BY CASE WHEN version = 'R' THEN 0 ELSE CAST(version AS INTEGER) END, version",
	)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	versions := make([]string, 0)
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}
