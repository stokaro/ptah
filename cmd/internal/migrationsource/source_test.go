package migrationsource_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/migrationsource"
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

func writeFile(c *qt.C, path, contents string) {
	c.Helper()
	err := os.WriteFile(path, []byte(contents), 0o600)
	c.Assert(err, qt.IsNil)
}
