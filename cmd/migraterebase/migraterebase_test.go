package migraterebase_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/migraterebase"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

func fixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, v := range []string{"0000000001_first", "0000000002_second", "0000000003_third"} {
		if err := os.WriteFile(filepath.Join(dir, v+".up.sql"), []byte("CREATE TABLE t (id int);\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, v+".down.sql"), []byte("DROP TABLE t;\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatPtah); err != nil {
		t.Fatal(err)
	}
	return dir
}

func execute(args ...string) (string, error) {
	cmd := migraterebase.NewMigrateRebaseCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func TestRebase_MovesToEndAndRehashes(t *testing.T) {
	c := qt.New(t)
	dir := fixture(t)

	stdout, err := execute("--migrations-dir", dir, "--version", "1")
	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Rebased migration 1 to ")

	_, statErr := os.Stat(filepath.Join(dir, "0000000001_first.up.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
	res, err := migratesum.VerifyDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(res.OK(), qt.IsTrue)
}

func TestRebase_AlreadyLast(t *testing.T) {
	c := qt.New(t)
	dir := fixture(t)
	_, err := execute("--migrations-dir", dir, "--version", "3")
	c.Assert(err, qt.ErrorMatches, `migration version 3 is already last.*`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
}
