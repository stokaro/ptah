package goannotationcleanup_test

import (
	"os"
	"path/filepath"
	"strings"
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

func TestCleanDirPreservesUnrelatedFormattingByteForByte(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "model.go")
	original := "package models\n\n// User is business documentation.\n//migrator:schema:table name=\"users\"\ntype User struct{ID int64}\n"
	expected := "package models\n\n// User is business documentation.\ntype User struct{ID int64}\n"
	c.Assert(os.WriteFile(path, []byte(original), 0o600), qt.IsNil)

	results, err := goannotationcleanup.CleanDir(goannotationcleanup.Options{RootDir: dir})

	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.HasLen, 1)
	content, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, expected)
}

func TestCleanDirDiffReportsDuplicateRemovedLinesByPosition(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "model.go")
	annotation := "//migrator:schema:field name=\"id\" type=\"SERIAL\"\n"
	original := "package models\n\ntype User struct {\n" +
		annotation +
		"ID int64\n\n" +
		annotation +
		"OtherID int64\n}\n"
	c.Assert(os.WriteFile(path, []byte(original), 0o600), qt.IsNil)

	results, err := goannotationcleanup.CleanDir(goannotationcleanup.Options{
		RootDir: dir,
		DryRun:  true,
		Diff:    true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(results, qt.HasLen, 1)
	c.Assert(results[0].RemovedLines, qt.Equals, 2)
	c.Assert(strings.Count(results[0].Diff, "-"+annotation), qt.Equals, 2)
	content, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, original)
}
