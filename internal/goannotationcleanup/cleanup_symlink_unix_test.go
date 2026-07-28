//go:build unix

package goannotationcleanup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/goannotationcleanup"
)

func TestCleanDir_FailurePath_RejectsSymlinkedGoSource(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	target := filepath.Join(outside, "outside.go")
	link := filepath.Join(root, "model.go")
	original := "package outside\n\n//migrator:schema:table name=\"outside\"\ntype Outside struct{}\n"
	c.Assert(os.WriteFile(target, []byte(original), 0o600), qt.IsNil)
	c.Assert(os.Symlink(target, link), qt.IsNil)

	results, err := goannotationcleanup.CleanDir(goannotationcleanup.Options{RootDir: root})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "refuse to clean symlinked Go source")
	c.Assert(results, qt.HasLen, 0)
	content, err := os.ReadFile(target)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, original)
	info, err := os.Lstat(link)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode()&os.ModeSymlink, qt.Equals, os.ModeSymlink)
}
