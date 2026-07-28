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

	plan, err := goannotationcleanup.PlanDir(root)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "refuse to clean symlinked Go source")
	c.Assert(plan, qt.IsNil)
	content, err := os.ReadFile(target)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, original)
	info, err := os.Lstat(link)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode()&os.ModeSymlink, qt.Equals, os.ModeSymlink)
}

func TestPlanSourceAlias_HappyPath_ReportsSymlinkAndHardLinkAliases(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	source := filepath.Join(root, "model.go")
	symlinkAlias := filepath.Join(outside, "schema-symlink.hcl")
	hardLinkAlias := filepath.Join(outside, "schema-hardlink.hcl")
	original := "package models\n\n//migrator:schema:table name=\"users\"\ntype User struct{}\n"
	c.Assert(os.WriteFile(source, []byte(original), 0o600), qt.IsNil)
	c.Assert(os.Symlink(source, symlinkAlias), qt.IsNil)
	c.Assert(os.Link(source, hardLinkAlias), qt.IsNil)

	plan, err := goannotationcleanup.PlanDir(root)
	c.Assert(err, qt.IsNil)

	alias, err := plan.SourceAlias(symlinkAlias)
	c.Assert(err, qt.IsNil)
	c.Assert(alias, qt.Equals, source)

	alias, err = plan.SourceAlias(hardLinkAlias)
	c.Assert(err, qt.IsNil)
	c.Assert(alias, qt.Equals, source)
}

func TestPlanApply_FailurePath_StagingFailureLeavesEverySourceUnchanged(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	firstDir := filepath.Join(root, "first")
	blockedDir := filepath.Join(root, "blocked")
	c.Assert(os.MkdirAll(firstDir, 0o700), qt.IsNil)
	c.Assert(os.MkdirAll(blockedDir, 0o700), qt.IsNil)
	firstPath := filepath.Join(firstDir, "model.go")
	blockedPath := filepath.Join(blockedDir, "model.go")
	firstData := []byte("package first\n\n//migrator:schema:table name=\"first\"\ntype First struct{}\n")
	blockedData := []byte("package blocked\n\n//migrator:schema:table name=\"blocked\"\ntype Blocked struct{}\n")
	c.Assert(os.WriteFile(firstPath, firstData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(blockedPath, blockedData, 0o600), qt.IsNil)

	plan, err := goannotationcleanup.PlanDir(root)
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chmod(blockedDir, 0o500), qt.IsNil)
	defer func() {
		c.Check(os.Chmod(blockedDir, 0o700), qt.IsNil)
	}()

	err = plan.Apply()

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "create staged")
	firstAfter, err := os.ReadFile(firstPath)
	c.Assert(err, qt.IsNil)
	c.Assert(firstAfter, qt.DeepEquals, firstData)
	blockedAfter, err := os.ReadFile(blockedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(blockedAfter, qt.DeepEquals, blockedData)
	firstEntries, err := os.ReadDir(firstDir)
	c.Assert(err, qt.IsNil)
	c.Assert(firstEntries, qt.HasLen, 1)
}
