package migrateedit_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrateedit"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

func fixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "0000000001_first.up.sql"), []byte("CREATE TABLE t (id int);\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "0000000001_first.down.sql"), []byte("DROP TABLE t;\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatPtah); err != nil {
		t.Fatal(err)
	}
	return dir
}

func execute(args ...string) (string, error) {
	cmd := migrateedit.NewMigrateEditCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func TestEdit_UpFileReplacesAndRehashes(t *testing.T) {
	c := qt.New(t)
	dir := fixture(t)
	newUp := filepath.Join(t.TempDir(), "new.sql")
	c.Assert(os.WriteFile(newUp, []byte("CREATE TABLE t (id bigint);\n"), 0o600), qt.IsNil)

	stdout, err := execute("--migrations-dir", dir, "--version", "1", "--up-file", newUp)
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Edited migration 1")

	// The up file now holds the new content and the directory validates.
	got, err := os.ReadFile(filepath.Join(dir, "0000000001_first.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(got), qt.Contains, "bigint")
	res, err := migratesum.VerifyDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(res.OK(), qt.IsTrue)
}

func TestEdit_NoEditorConfigured(t *testing.T) {
	c := qt.New(t)
	dir := fixture(t)
	t.Setenv("EDITOR", "")
	t.Setenv("VISUAL", "")

	// With no --up-file/--down-file and no editor, the command explains how to proceed.
	_, err := execute("--migrations-dir", dir, "--version", "1")
	c.Assert(err, qt.ErrorMatches, `no editor configured.*`)
}
