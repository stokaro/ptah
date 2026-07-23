package migratevalidate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/migratevalidate"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

func execute(args ...string) (stdout, stderr string, err error) {
	cmd := migratevalidate.NewMigrateValidateCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// migrationsDir writes a clean pair plus a matching ptah.sum and returns the dir.
func migrationsDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	write := func(name, content string) {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	write("0000000001_init.up.sql", "CREATE TABLE t (id INT);\n")
	write("0000000001_init.down.sql", "DROP TABLE t;\n")
	_, err := migratesum.Write(dir)
	c.Assert(err, qt.IsNil)
	return dir
}

func TestValidate_CleanDirectoryExitsZero(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := execute("--dir", migrationsDir(c))
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "OK: migrations directory matches ptah.sum")
}

func TestValidate_AutoReadsAtlasSum(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_initial.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	stdout, _, err := execute("--dir", dir)
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "OK: migrations directory matches atlas.sum")
}

func TestValidate_EditedMigrationExitsOneWithDiff(t *testing.T) {
	c := qt.New(t)

	dir := migrationsDir(c)
	// Tamper with an already-hashed migration.
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE t (id BIGINT);\n"), 0o600), qt.IsNil)

	_, stderr, err := execute("--dir", dir)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stderr, qt.Contains, "changed: 0000000001_init.up.sql")
}

func TestValidate_MissingSumFileExitsTwo(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)

	_, stderr, err := execute("--dir", dir)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(err, qt.ErrorMatches, ".*ptah.sum not found.*")
	// The actionable guidance must reach the user, not be swallowed.
	c.Assert(stderr, qt.Contains, "run `ptah migrations hash`")
}

func TestValidate_MissingDirectoryExitsTwo(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := execute("--dir", filepath.Join(t.TempDir(), "does-not-exist"))
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	// The message surfaces the directory and the underlying stat error.
	c.Assert(stderr, qt.Contains, "migrations directory")
	c.Assert(err, qt.ErrorMatches, ".*does-not-exist.*")
}

func TestValidate_PositionalArgExitsTwoWithMessage(t *testing.T) {
	c := qt.New(t)

	// A stray positional (e.g. the path typed without --dir) is a usage
	// error (exit 2 with a message), not a silent exit 1 that would look
	// like drift.
	_, stderr, err := execute(migrationsDir(c), "stray")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "unexpected positional arguments")
}

func TestValidate_CorruptSumFileExitsTwoNotOne(t *testing.T) {
	c := qt.New(t)

	dir := migrationsDir(c)
	// A structurally broken ptah.sum (an h1: hash that is not valid base64)
	// is a usage failure (exit 2), not content drift (exit 1).
	c.Assert(os.WriteFile(filepath.Join(dir, "ptah.sum"),
		[]byte("h1:not-valid-base64!!\n"), 0o600), qt.IsNil)

	_, stderr, err := execute("--dir", dir)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "malformed directory hash line")
}
