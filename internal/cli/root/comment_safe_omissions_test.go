package root_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestMigrationsGenerate_KeepsDeclaredTextInsideItsComments declares a table
// comment that holds line breaks and a statement. SQLite keeps no table
// comment, so the file says so, and no line of the file outside a comment is
// anything but the CREATE TABLE.
func TestMigrationsGenerate_KeepsDeclaredTextInsideItsComments(t *testing.T) {
	c := qt.New(t)
	f := newOmissionFixture(c)
	c.Assert(os.WriteFile(f.schema, []byte("CREATE TABLE ro_c (id INT PRIMARY KEY) COMMENT='first\nDROP TABLE victim;\n-- end';\n"), 0o600), qt.IsNil)

	code, stdout, stderr := runStreams(c, "migrations", "generate", "--db-url", f.dbURL, "--schema-file", f.schema,
		"--migrations-dir", f.migrations, "--name", "first")

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(skippedWarnings(stderr), qt.DeepEquals, []string{
		`warning: sqlite: table ro_c: comment=first\nDROP TABLE victim;\n-- end was skipped (unsupported)`,
	})
	up, globErr := filepath.Glob(filepath.Join(f.migrations, "*_first.up.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(up, qt.HasLen, 1)
	upSQL, readErr := os.ReadFile(up[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(upSQL), qt.Not(qt.Contains), "\nDROP TABLE victim;")
	c.Assert(string(upSQL), qt.Contains, `-- sqlite: table ro_c: comment=first\nDROP TABLE victim;\n-- end was skipped (unsupported)`)
}

// TestMigrationsCreate_KeepsTheNameInsideItsComment names a migration with a
// line break and a statement. The name reaches the file's first comment, and
// the statement stays inside it.
func TestMigrationsCreate_KeepsTheNameInsideItsComment(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()

	code, stdout, stderr := runStreams(c, "migrations", "create", "--migrations-dir", dir,
		"--name", "abc\nDROP TABLE victim;--x")

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	up, globErr := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(up, qt.HasLen, 1)
	upSQL, readErr := os.ReadFile(up[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(upSQL), qt.Not(qt.Contains), "\nDROP TABLE victim;")
	c.Assert(string(upSQL), qt.Contains, `-- Migration: abc\nDROP TABLE victim;--x`+"\n")
}
