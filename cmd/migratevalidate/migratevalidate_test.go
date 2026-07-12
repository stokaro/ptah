package migratevalidate

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/migration/migratesum"
)

func execute(args ...string) (stdout, stderr string, err error) {
	cmd := NewMigrateValidateCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// migrationsDir writes a clean pair plus a matching ptah.sum and returns the dir.
func migrationsDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("0000000001_init.up.sql", "CREATE TABLE t (id INT);\n")
	write("0000000001_init.down.sql", "DROP TABLE t;\n")
	if _, err := migratesum.Write(dir); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestValidate_CleanDirectoryExitsZero(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := execute("--dir", migrationsDir(t))
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "OK: migrations directory matches ptah.sum")
}

func TestValidate_EditedMigrationExitsOneWithDiff(t *testing.T) {
	c := qt.New(t)

	dir := migrationsDir(t)
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
	c.Assert(stderr, qt.Contains, "run `ptah migrate-hash`")
}

func TestValidate_MissingDirectoryExitsTwo(t *testing.T) {
	c := qt.New(t)

	_, stderr, err := execute("--dir", filepath.Join(t.TempDir(), "does-not-exist"))
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	// The message surfaces the directory and the underlying stat error.
	c.Assert(stderr, qt.Contains, "migrations directory")
	c.Assert(err, qt.ErrorMatches, ".*does-not-exist.*")
}

func TestValidate_CorruptSumFileExitsTwoNotOne(t *testing.T) {
	c := qt.New(t)

	dir := migrationsDir(t)
	// A structurally broken ptah.sum (an h1: hash that is not valid base64)
	// is a usage failure (exit 2), not content drift (exit 1).
	c.Assert(os.WriteFile(filepath.Join(dir, "ptah.sum"),
		[]byte("h1:not-valid-base64!!\n"), 0o600), qt.IsNil)

	_, stderr, err := execute("--dir", dir)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "malformed directory hash line")
}
