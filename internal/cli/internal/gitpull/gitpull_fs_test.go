package gitpull_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/gitpull"
	"ptah.run/internal/gitsource"
	"ptah.run/internal/sourcecontent"
)

func TestWriteFile_HappyPath(t *testing.T) {
	c := qt.New(t)
	out := filepath.Join(c.TempDir(), "nested", "schema.sql")
	data := []byte("CREATE TABLE t (id int);\r\n")

	written, err := gitpull.WriteFile(gitsource.Result{Files: []sourcecontent.File{{Path: "schema.sql", Data: data}}}, out)

	c.Assert(err, qt.IsNil)
	c.Assert(sameFile(c, written, out), qt.IsTrue, qt.Commentf("wrote %s, not %s", written, out))
	got, err := os.ReadFile(out)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, data, qt.Commentf("the bytes are written as fetched"))
}

func TestWriteFile_FailurePath(t *testing.T) {
	c := qt.New(t)
	out := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(out, []byte("mine"), 0o600), qt.IsNil)

	written, err := gitpull.WriteFile(gitsource.Result{Files: []sourcecontent.File{{Path: "schema.sql", Data: []byte("theirs")}}}, out)

	c.Assert(err, qt.ErrorMatches, `the output already exists: .*`)
	c.Assert(written, qt.Equals, "")
	got, err := os.ReadFile(out)
	c.Assert(err, qt.IsNil)
	c.Assert(string(got), qt.Equals, "mine")
}

func TestWriteDirectory_HappyPath(t *testing.T) {
	c := qt.New(t)
	out := filepath.Join(c.TempDir(), "migrations")
	files := []sourcecontent.File{
		{Path: "0001_create.up.sql", Data: []byte("CREATE TABLE t (id int);\n")},
		{Path: "data/seed.sql", Data: []byte("INSERT INTO t VALUES (1);\n")},
	}

	written, err := gitpull.WriteDirectory(gitsource.Result{Files: files}, out)

	c.Assert(err, qt.IsNil)
	c.Assert(sameFile(c, written, out), qt.IsTrue, qt.Commentf("wrote %s, not %s", written, out))
	for _, file := range files {
		got, err := os.ReadFile(filepath.Join(out, filepath.FromSlash(file.Path)))
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.DeepEquals, file.Data)
	}
}

func TestWriteDirectory_FailurePath(t *testing.T) {
	c := qt.New(t)
	out := filepath.Join(c.TempDir(), "migrations")
	files := []sourcecontent.File{
		{Path: "0001_create.up.sql", Data: []byte("CREATE TABLE t (id int);\n")},
		// The same path twice cannot both be created; the first file written
		// has to go with the directory.
		{Path: "0001_create.up.sql", Data: []byte("again")},
	}

	written, err := gitpull.WriteDirectory(gitsource.Result{Files: files}, out)

	c.Assert(err, qt.ErrorMatches, `create 0001_create.up.sql: .*`)
	c.Assert(written, qt.Equals, "")
	_, err = os.Lstat(out)
	c.Assert(os.IsNotExist(err), qt.IsTrue, qt.Commentf("a directory a failed pull began was left behind"))
}

// sameFile reports whether two paths name one file: the written path is
// resolved, and a temporary directory may sit behind a symbolic link.
func sameFile(c *qt.C, a, b string) bool {
	first, err := os.Stat(a)
	c.Assert(err, qt.IsNil)
	second, err := os.Stat(b)
	c.Assert(err, qt.IsNil)
	return os.SameFile(first, second)
}
