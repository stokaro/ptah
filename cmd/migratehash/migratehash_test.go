package migratehash

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/migration/migratesum"
)

func execute(args ...string) (stdout string, err error) {
	cmd := NewMigrateHashCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), err
}

func TestHash_WritesSumFileForMigrations(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE t;\n"), 0o600), qt.IsNil)

	stdout, err := execute("--dir", dir)
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "2 migration file(s) hashed")

	// The written sum file makes the directory validate clean.
	raw, err := os.ReadFile(filepath.Join(dir, migratesum.FileName))
	c.Assert(err, qt.IsNil)
	c.Assert(len(raw) > 0, qt.IsTrue)
	res, err := migratesum.VerifyDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(res.OK(), qt.IsTrue)
}

func TestHash_UpdatesSumFileAfterAddingAMigration(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	_, err := execute("--dir", dir)
	c.Assert(err, qt.IsNil)

	// Add a migration; the directory now drifts from the recorded sum.
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000002_more.up.sql"),
		[]byte("CREATE TABLE u (id INT);\n"), 0o600), qt.IsNil)
	res, err := migratesum.VerifyDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(res.OK(), qt.IsFalse)

	// Re-hash: the sum file is updated and validation passes again.
	_, err = execute("--dir", dir)
	c.Assert(err, qt.IsNil)
	res, err = migratesum.VerifyDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(res.OK(), qt.IsTrue)
}

func TestHash_MissingDirectoryExitsTwo(t *testing.T) {
	c := qt.New(t)

	_, err := execute("--dir", filepath.Join(t.TempDir(), "nope"))
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
}
