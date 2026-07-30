package migrationsource_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/migrationsource"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestResolve_LocalDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeFile(c, filepath.Join(dir, "0000000001_create_users.up.sql"), "CREATE TABLE users (id INTEGER);\n")
	writeFile(c, filepath.Join(dir, "README.md"), "not a migration input\n")

	source, err := migrationsource.Resolve(context.Background(), dir, migrationsource.Options{
		DirFormat: migrator.MigrationDirFormatPtah,
	})

	c.Assert(err, qt.IsNil)
	expectedDisplay, err := filepath.EvalSymlinks(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(source.Display, qt.Equals, expectedDisplay)
	c.Assert(source.DirFormat, qt.Equals, migrator.MigrationDirFormatPtah)
	c.Assert(source.OCI, qt.IsNil)
	contents, err := fs.ReadFile(source.FileSystem, "0000000001_create_users.up.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "CREATE TABLE users (id INTEGER);\n")
	_, err = fs.Stat(source.FileSystem, "README.md")
	c.Assert(err, qt.ErrorIs, fs.ErrNotExist)
}

func TestResolve_LocalDirectoryFailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := migrationsource.Resolve(
		context.Background(),
		filepath.Join(t.TempDir(), "missing"),
		migrationsource.Options{DirFormat: migrator.MigrationDirFormatPtah},
	)
	c.Assert(err, qt.ErrorMatches, "open migrations directory: .*")
}

func TestResolve_EmptyLocalDirectory(t *testing.T) {
	c := qt.New(t)

	source, err := migrationsource.Resolve(
		context.Background(),
		t.TempDir(),
		migrationsource.Options{DirFormat: migrator.MigrationDirFormatAuto},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(source.FileSystem, qt.IsNotNil)
	c.Assert(source.DirFormat, qt.Equals, migrator.MigrationDirFormatAuto)
	c.Assert(source.OCI, qt.IsNil)
}

func TestCaptureLocal_SnapshotIgnoresLaterPathAndFileChanges(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	writeFile(c, filepath.Join(dir, "1_init.sql"), "CREATE TABLE original (id INTEGER);\n")
	writeFile(c, filepath.Join(dir, "1_init.down.sql"), "DROP TABLE original;\n")
	writeFile(c, filepath.Join(dir, "atlas.sum"), "h1:original\n")

	source, err := migrationsource.CaptureLocal(
		dir,
		migrationsource.LocalOptions{AllowedRoot: root},
	)
	c.Assert(err, qt.IsNil)

	c.Assert(
		os.WriteFile(filepath.Join(dir, "1_init.sql"), []byte("CREATE TABLE changed (id INTEGER);\n"), 0o600),
		qt.IsNil,
	)
	c.Assert(os.Rename(dir, filepath.Join(root, "captured")), qt.IsNil)
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	writeFile(c, filepath.Join(dir, "1_init.sql"), "CREATE TABLE replacement (id INTEGER);\n")

	up, err := fs.ReadFile(source.FileSystem, "1_init.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Equals, "CREATE TABLE original (id INTEGER);\n")
	down, err := fs.ReadFile(source.FileSystem, "1_init.down.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Equals, "DROP TABLE original;\n")
	sum, err := fs.ReadFile(source.FileSystem, "atlas.sum")
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Equals, "h1:original\n")
}

func TestCaptureLocal_ExecutionUsesCapturedBytesAfterPathReplacement(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	writeFile(c, filepath.Join(dir, "1_init.sql"), "CREATE TABLE captured_input (id INTEGER PRIMARY KEY);\n")

	source, err := migrationsource.CaptureLocal(
		dir,
		migrationsource.LocalOptions{AllowedRoot: root},
	)
	c.Assert(err, qt.IsNil)

	c.Assert(os.Rename(dir, filepath.Join(root, "captured")), qt.IsNil)
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	writeFile(c, filepath.Join(dir, "1_init.sql"), "CREATE TABLE replacement_input (id INTEGER PRIMARY KEY);\n")

	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(root, "apply.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
	})
	plan, err := atlasmigrate.PrepareApply(c.Context(), conn, atlasmigrate.ApplyOptions{
		Dir:       source.Display,
		FS:        source.FileSystem,
		ExecOrder: migrator.ExecOrderLinear,
		TxMode:    migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)

	result, err := plan.Execute(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.IsTrue)
	c.Assert(sqliteTableCount(c, conn, "captured_input"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, conn, "replacement_input"), qt.Equals, 0)
}

func sqliteTableCount(c *qt.C, conn *dbschema.DatabaseConnection, table string) int {
	c.Helper()
	var count int
	err := conn.QueryRowContext(
		c.Context(),
		`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?`,
		table,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

func writeFile(c *qt.C, path, contents string) {
	c.Helper()
	err := os.WriteFile(path, []byte(contents), 0o600)
	c.Assert(err, qt.IsNil)
}
