package pathguard_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/pathguard"
)

func TestOpenedDirectoryOperations_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "published"), []byte("old"), 0o600), qt.IsNil)
	opened, err := pathguard.OpenDirectory(dir)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(opened.Close(), qt.IsNil)
	})

	staged, stagedName, err := opened.CreateTemp("staged-*.tmp")
	c.Assert(err, qt.IsNil)
	c.Assert(filepath.Base(stagedName), qt.Equals, stagedName)
	c.Assert(strings.HasPrefix(stagedName, "staged-"), qt.IsTrue)
	c.Assert(strings.HasSuffix(stagedName, ".tmp"), qt.IsTrue)
	_, err = staged.WriteString("new")
	c.Assert(err, qt.IsNil)
	c.Assert(staged.Sync(), qt.IsNil)
	c.Assert(staged.Close(), qt.IsNil)

	info, err := opened.Lstat(stagedName)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().IsRegular(), qt.IsTrue)
	contents, err := opened.ReadFile(stagedName)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "new")
	handle, err := opened.Open(stagedName)
	c.Assert(err, qt.IsNil)
	c.Assert(handle.Close(), qt.IsNil)

	other, otherName, err := opened.CreateTemp("other-")
	c.Assert(err, qt.IsNil)
	c.Assert(stagedName, qt.Not(qt.Equals), otherName)
	c.Assert(other.Close(), qt.IsNil)
	c.Assert(opened.Remove(otherName), qt.IsNil)
	random, randomName, err := opened.CreateTemp("")
	c.Assert(err, qt.IsNil)
	c.Assert(randomName, qt.Not(qt.Equals), "")
	c.Assert(filepath.Base(randomName), qt.Equals, randomName)
	c.Assert(random.Close(), qt.IsNil)
	c.Assert(opened.Remove(randomName), qt.IsNil)

	c.Assert(opened.ReplaceFile(stagedName, "published"), qt.IsNil)
	c.Assert(opened.Sync(), qt.IsNil)
	contents, err = os.ReadFile(filepath.Join(dir, "published"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "new")
	_, err = os.Stat(filepath.Join(dir, stagedName))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)

	c.Assert(opened.Remove("published"), qt.IsNil)
	_, err = os.Stat(filepath.Join(dir, "published"))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestOpenedDirectoryOperations_FailurePath(t *testing.T) {
	c := qt.New(t)
	opened, err := pathguard.OpenDirectory(c.TempDir())
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(opened.Close(), qt.IsNil)
	})

	handle, err := opened.Open("missing")
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	var openErr *fs.PathError
	c.Assert(err, qt.ErrorAs, &openErr)
	c.Assert(openErr.Op, qt.Equals, "openat")
	c.Assert(openErr.Path, qt.Equals, "missing")
	c.Assert(handle, qt.IsNil)
	contents, err := opened.ReadFile("missing")
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	var readErr *fs.PathError
	c.Assert(err, qt.ErrorAs, &readErr)
	c.Assert(readErr.Op, qt.Equals, "openat")
	c.Assert(readErr.Path, qt.Equals, "missing")
	c.Assert(contents, qt.IsNil)
	info, err := opened.Lstat("missing")
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	var statErr *fs.PathError
	c.Assert(err, qt.ErrorAs, &statErr)
	c.Assert(statErr.Op, qt.Equals, "statat")
	c.Assert(statErr.Path, qt.Equals, "missing")
	c.Assert(info, qt.IsNil)
	err = opened.Remove("missing")
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	var removeErr *fs.PathError
	c.Assert(err, qt.ErrorAs, &removeErr)
	c.Assert(removeErr.Op, qt.Equals, "removeat")
	c.Assert(removeErr.Path, qt.Equals, "missing")
	err = opened.ReplaceFile("missing", "published")
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	var replaceErr *os.LinkError
	c.Assert(err, qt.ErrorAs, &replaceErr)
	c.Assert(replaceErr.Op, qt.Equals, "renameat")
	c.Assert(replaceErr.Old, qt.Equals, "missing")
	c.Assert(replaceErr.New, qt.Equals, "published")
}

func TestOpenedDirectoryCreateTemp_FailurePath(t *testing.T) {
	c := qt.New(t)
	opened, err := pathguard.OpenDirectory(c.TempDir())
	c.Assert(err, qt.IsNil)
	c.Assert(opened.Close(), qt.IsNil)

	file, name, err := opened.CreateTemp("staged-*")

	c.Assert(err, qt.ErrorMatches, `create rooted temporary file staged-.*: .*`)
	c.Assert(err, qt.ErrorIs, os.ErrClosed)
	var pathErr *fs.PathError
	c.Assert(err, qt.ErrorAs, &pathErr)
	c.Assert(pathErr.Op, qt.Equals, "openat")
	c.Assert(pathErr.Path, qt.Matches, `staged-.*`)
	c.Assert(file, qt.IsNil)
	c.Assert(name, qt.Equals, "")
}

func TestOpenedDirectoryCreateTempRejectsPathComponents(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	opened, err := pathguard.OpenDirectory(dir)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(opened.Close(), qt.IsNil)
	})

	tests := []struct {
		name    string
		pattern string
	}{
		{name: "current directory", pattern: "."},
		{name: "parent directory", pattern: ".."},
		{name: "parent traversal", pattern: filepath.Join("..", "escape-*")},
		{name: "nested path", pattern: filepath.Join("nested", "file-*")},
		{name: "absolute path", pattern: filepath.Join(dir, "absolute-*")},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			file, name, err := opened.CreateTemp(test.pattern)
			c.Assert(err, qt.ErrorIs, fs.ErrInvalid)
			var pathErr *fs.PathError
			c.Assert(err, qt.ErrorAs, &pathErr)
			c.Assert(pathErr.Op, qt.Equals, "createtemp")
			c.Assert(pathErr.Path, qt.Equals, test.pattern)
			c.Assert(file, qt.IsNil)
			c.Assert(name, qt.Equals, "")
		})
	}
}
