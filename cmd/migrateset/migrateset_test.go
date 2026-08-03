package migrateset_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/atlascompat"
	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/migrateset"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// writePtahMigrations writes a two-migration ptah-format directory.
func writePtahMigrations(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"0000000001_users.up.sql":    "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"0000000001_users.down.sql":  "DROP TABLE users;\n",
		"0000000002_orders.up.sql":   "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
		"0000000002_orders.down.sql": "DROP TABLE orders;\n",
	}
	for name, content := range files {
		qt.Assert(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// writeAtlasMigrations writes a two-migration atlas-format directory, hashed.
//
// The atlas.sum is not decoration: `ptah-compat migrate set` verifies it before
// writing revision rows (#974), so an unhashed fixture would be refused the way
// the community binary refuses one. Hashing the fixture is the fix; weakening
// the gate to accept it would re-open the bug.
func writeAtlasMigrations(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"1_users.sql":  "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"2_orders.sql": "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
	}
	for name, content := range files {
		qt.Assert(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	sum, err := atlascompat.ComputeSum(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, os.WriteFile(filepath.Join(dir, atlascompat.AtlasSumFileName), sum.Bytes(), 0o600), qt.IsNil)
	return dir
}

func runSet(args ...string) (string, error) {
	cmd := migrateset.NewMigrateSetCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// queryVersions returns the version column values of the revision table.
func queryVersions(c *qt.C, dbPath, table string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(), "SELECT version FROM "+table+" ORDER BY CAST(version AS INTEGER)")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var versions []string
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

func TestMigrationsSetMovesBoundaryBothDirections(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	// Upward: mark both migrations applied without executing SQL.
	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "2")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Current version is 2 (2 set):")
	c.Assert(out, qt.Contains, "+ 1 (Users)")
	c.Assert(out, qt.Contains, "+ 2 (Orders)")
	c.Assert(queryVersions(c, dbPath, "schema_migrations"), qt.DeepEquals, []string{"1", "2"})

	// Downward: remove the revision row above the target version, still
	// without executing SQL.
	out, err = runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "1")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Current version is 1 (1 removed):")
	c.Assert(out, qt.Contains, "- 2 (Orders)")
	c.Assert(queryVersions(c, dbPath, "schema_migrations"), qt.DeepEquals, []string{"1"})
}

func TestMigrationsSetIsIdempotent(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "1")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	out, err = runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "1")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Revision state already at version 1; no changes to be made.")
}

func TestMigrationsSetRejectsUnknownVersion(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "9")
	c.Assert(err, qt.ErrorMatches, `migration with version "9" not found`, qt.Commentf("%s", out))
}

func TestMigrationsSetRequiresVersion(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir)
	c.Assert(err, qt.ErrorMatches, "--version is required", qt.Commentf("%s", out))
}

// assertNoRevisionTable asserts the revision table was never created.
func assertNoRevisionTable(c *qt.C, dbPath string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'`).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestMigrationsSetDryRunChangesNothing(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writePtahMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "set.db")

	out, err := runSet("--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--version", "2", "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Dry run: would set the revision boundary to version 2.")
	assertNoRevisionTable(c, dbPath)
}

// TestMigrationsSetAtlasFormatMatchesAtlasMigrateSet proves the native verb
// with --revision-format atlas and its Atlas twin leave identical revision
// state behind on identical databases: both wrap atlasmigrate.Set.
func TestMigrationsSetAtlasFormatMatchesAtlasMigrateSet(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeAtlasMigrations(t)
	nativeDB := filepath.Join(t.TempDir(), "native.db")
	atlasDB := filepath.Join(t.TempDir(), "atlas.db")

	nativeOut, err := runSet(
		"--db-url", "sqlite://"+nativeDB,
		"--migrations-dir", migrationsDir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
		"--version", "2",
	)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", nativeOut))

	atlasCmd := atlas.NewCompatCommand("atlas")
	var atlasOut bytes.Buffer
	atlasCmd.SetOut(&atlasOut)
	atlasCmd.SetErr(&atlasOut)
	atlasCmd.SetArgs([]string{"migrate", "set", "2",
		"--url", "sqlite://" + atlasDB,
		"--dir", "file://" + migrationsDir,
	})
	c.Assert(atlasCmd.Execute(), qt.IsNil, qt.Commentf("%s", atlasOut.String()))

	// Same summary output and identical revision rows in both databases.
	c.Assert(nativeOut, qt.Equals, atlasOut.String())
	c.Assert(
		queryVersions(c, nativeDB, "atlas_schema_revisions"),
		qt.DeepEquals,
		queryVersions(c, atlasDB, "atlas_schema_revisions"),
	)
}
