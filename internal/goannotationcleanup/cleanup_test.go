package goannotationcleanup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/goannotationcleanup"
)

func TestCleanDirDryRunDiffAndWrite(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "model.go")
	original := `package models

// User is business documentation.
//migrator:schema:table name="users"
type User struct {
	// ID is business documentation.
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:embedded mode="inline"
	Timestamps
}
`
	c.Assert(os.WriteFile(path, []byte(original), 0o600), qt.IsNil)
	c.Assert(os.Chmod(path, 0o644), qt.IsNil)

	results, err := goannotationcleanup.CleanDir(goannotationcleanup.Options{
		RootDir: dir,
		DryRun:  true,
		Diff:    true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.HasLen, 1)
	c.Assert(results[0].RemovedLines, qt.Equals, 3)
	c.Assert(results[0].Diff, qt.Contains, `-//migrator:schema:table name="users"`)
	content, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, original)

	results, err = goannotationcleanup.CleanDir(goannotationcleanup.Options{RootDir: dir})

	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.HasLen, 1)
	content, err = os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Contains, "// User is business documentation.")
	c.Assert(string(content), qt.Contains, "// ID is business documentation.")
	c.Assert(string(content), qt.Not(qt.Contains), "migrator:schema")
	c.Assert(string(content), qt.Not(qt.Contains), "migrator:embedded")
	info, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o644))

	results, err = goannotationcleanup.CleanDir(goannotationcleanup.Options{RootDir: dir})
	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.HasLen, 0)
}
