package fsdurable_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fsdurable"
)

func TestPublishFileAt_HappyPath_AppliesReadOnlyMode(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	publishedPath := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(stagedPath, []byte("new"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(publishedPath, []byte("old"), 0o600), qt.IsNil)
	c.Assert(os.Chmod(publishedPath, 0o400), qt.IsNil)
	stagedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(os.Chmod(publishedPath, 0o600), qt.IsNil)
		c.Check(root.Close(), qt.IsNil)
	})

	err = fsdurable.PublishFileAt(root, "staged", "published", stagedInfo, 0o400)

	c.Assert(err, qt.IsNil)
	contents, err := os.ReadFile(publishedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "new")
	publishedInfo, err := os.Stat(publishedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(os.SameFile(stagedInfo, publishedInfo), qt.IsTrue)
	c.Assert(publishedInfo.Mode().Perm()&0o200, qt.Equals, os.FileMode(0))
	_, err = os.Stat(stagedPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestFinalizeFileAt_HappyPath_AppliesReadOnlyMode(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	c.Assert(os.WriteFile(stagedPath, []byte("backup"), 0o600), qt.IsNil)
	stagedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(os.Chmod(stagedPath, 0o600), qt.IsNil)
		c.Check(root.Close(), qt.IsNil)
	})

	err = fsdurable.FinalizeFileAt(root, "staged", stagedInfo, 0o400)

	c.Assert(err, qt.IsNil)
	contents, err := os.ReadFile(stagedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "backup")
	finalInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(os.SameFile(stagedInfo, finalInfo), qt.IsTrue)
	c.Assert(finalInfo.Mode().Perm()&0o200, qt.Equals, os.FileMode(0))
}

func TestPublishFileAt_HappyPath_CreatesMissingDestination(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	publishedPath := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(stagedPath, []byte("new"), 0o600), qt.IsNil)
	stagedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})

	err = fsdurable.PublishFileAt(root, "staged", "published", stagedInfo, 0o600)

	c.Assert(err, qt.IsNil)
	contents, err := os.ReadFile(publishedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "new")
	_, err = os.Stat(stagedPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestPublishFileAt_FailurePath_RejectsReplacedStagedEntry(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	publishedPath := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(stagedPath, []byte("expected"), 0o600), qt.IsNil)
	expectedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(os.Remove(stagedPath), qt.IsNil)
	c.Assert(os.WriteFile(stagedPath, []byte("replacement"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(publishedPath, []byte("old"), 0o600), qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})

	err = fsdurable.PublishFileAt(root, "staged", "published", expectedInfo, 0o640)

	c.Assert(err, qt.ErrorIs, fsdurable.ErrStagedFileChanged)
	contents, err := os.ReadFile(stagedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "replacement")
	contents, err = os.ReadFile(publishedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "old")
}

func TestPublishFileAt_FailurePath_RejectsModifiedStagedFile(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	publishedPath := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(stagedPath, []byte("expected"), 0o600), qt.IsNil)
	expectedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(stagedPath, []byte("modified staged contents"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(publishedPath, []byte("old"), 0o600), qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})

	err = fsdurable.PublishFileAt(root, "staged", "published", expectedInfo, 0o640)

	c.Assert(err, qt.ErrorIs, fsdurable.ErrStagedFileChanged)
	contents, err := os.ReadFile(stagedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "modified staged contents")
	contents, err = os.ReadFile(publishedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "old")
}

func TestPublishFileAt_FailurePath_RejectsMissingStagedEntry(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	expectedPath := filepath.Join(dir, "expected")
	publishedPath := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(expectedPath, []byte("expected"), 0o600), qt.IsNil)
	expectedInfo, err := os.Stat(expectedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(publishedPath, []byte("old"), 0o600), qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})

	err = fsdurable.PublishFileAt(root, "missing", "published", expectedInfo, 0o600)

	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	contents, err := os.ReadFile(publishedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "old")
}

func TestPublishFileAt_FailurePath_RejectsChangedStagedMode(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	publishedPath := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(stagedPath, []byte("expected"), 0o600), qt.IsNil)
	expectedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chmod(stagedPath, 0o400), qt.IsNil)
	c.Assert(os.WriteFile(publishedPath, []byte("old"), 0o600), qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(os.Chmod(stagedPath, 0o600), qt.IsNil)
		c.Check(root.Close(), qt.IsNil)
	})

	err = fsdurable.PublishFileAt(root, "staged", "published", expectedInfo, 0o600)

	c.Assert(err, qt.ErrorIs, fsdurable.ErrStagedFileChanged)
	contents, err := os.ReadFile(publishedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "old")
}

func TestPublishFileAt_FailurePath_RejectsChangedStagedModTime(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	publishedPath := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(stagedPath, []byte("expected"), 0o600), qt.IsNil)
	expectedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	changedTime := expectedInfo.ModTime().Add(time.Hour)
	c.Assert(os.Chtimes(stagedPath, changedTime, changedTime), qt.IsNil)
	c.Assert(os.WriteFile(publishedPath, []byte("old"), 0o600), qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})

	err = fsdurable.PublishFileAt(root, "staged", "published", expectedInfo, 0o600)

	c.Assert(err, qt.ErrorIs, fsdurable.ErrStagedFileChanged)
	contents, err := os.ReadFile(publishedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "old")
}

func TestPublishFileAt_FailurePath_RejectsPathComponents(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	c.Assert(os.WriteFile(stagedPath, []byte("expected"), 0o600), qt.IsNil)
	expectedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})
	tests := []struct {
		name       string
		stagedName string
		targetName string
	}{
		{name: "empty staged name", stagedName: "", targetName: "published"},
		{name: "current staged directory", stagedName: ".", targetName: "published"},
		{name: "parent staged directory", stagedName: "..", targetName: "published"},
		{name: "nested staged path", stagedName: filepath.Join("nested", "staged"), targetName: "published"},
		{name: "nested target path", stagedName: "staged", targetName: filepath.Join("nested", "published")},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := fsdurable.PublishFileAt(
				root,
				test.stagedName,
				test.targetName,
				expectedInfo,
				0o600,
			)
			c.Assert(err, qt.ErrorIs, fs.ErrInvalid)
		})
	}
}

func TestFinalizeFileAt_FailurePath_RejectsReplacedStagedEntry(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	c.Assert(os.WriteFile(stagedPath, []byte("expected"), 0o600), qt.IsNil)
	expectedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(os.Remove(stagedPath), qt.IsNil)
	c.Assert(os.WriteFile(stagedPath, []byte("replacement"), 0o600), qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})

	err = fsdurable.FinalizeFileAt(root, "staged", expectedInfo, 0o400)

	c.Assert(err, qt.ErrorIs, fsdurable.ErrStagedFileChanged)
	contents, err := os.ReadFile(stagedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "replacement")
	info, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm()&0o200, qt.Equals, os.FileMode(0o200))
}

func TestFinalizeFileAt_FailurePath_RejectsPathComponents(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	c.Assert(os.WriteFile(stagedPath, []byte("expected"), 0o600), qt.IsNil)
	expectedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})
	tests := []struct {
		name       string
		stagedName string
	}{
		{name: "empty name", stagedName: ""},
		{name: "current directory", stagedName: "."},
		{name: "parent directory", stagedName: ".."},
		{name: "nested path", stagedName: filepath.Join("nested", "staged")},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := fsdurable.FinalizeFileAt(root, test.stagedName, expectedInfo, 0o600)
			c.Assert(err, qt.ErrorIs, fs.ErrInvalid)
		})
	}
}
