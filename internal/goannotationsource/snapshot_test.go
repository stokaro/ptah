package goannotationsource_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/goannotationsource"
)

func TestCapture_HappyPath_UsesStableSharedSourcePolicy(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	writeSource(c, root, "models.go", "package models\n")
	writeSource(c, root, ".generated.go", "package models\n")
	writeSource(c, root, "models_test.go", "package models_test\n")
	writeSource(c, root, "myvendor/models.go", "package myvendor\n")
	writeSource(c, root, "vendor/models.go", "package vendor\n")
	writeSource(c, root, ".hidden/models.go", "package hidden\n")

	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)
	c.Assert(sourcePaths(snapshot.Files()), qt.DeepEquals, []string{
		".generated.go",
		"models.go",
		"myvendor/models.go",
	})
	c.Assert(fstest.TestFS(
		snapshot.FS(),
		".generated.go",
		"models.go",
		"myvendor/models.go",
	), qt.IsNil)

	writeSource(c, root, "models.go", "package changed\n")
	data, err := fs.ReadFile(snapshot.FS(), "models.go")
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, "package models\n")
}

func TestCapture_HappyPath_ReturnsIndependentFileBytes(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	writeSource(c, root, "models.go", "package models\n")

	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)
	files := snapshot.Files()
	files[0].Contents[0] = 'X'

	data, err := fs.ReadFile(snapshot.FS(), "models.go")
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, "package models\n")
}

func TestSnapshotRevalidate_HappyPath_AcceptsUnchangedSources(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	writeSource(c, root, "models.go", "package models\n")

	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.Revalidate(), qt.IsNil)
}

func TestSnapshotRevalidate_HappyPath_IgnoresExcludedChanges(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	writeSource(c, root, "models.go", "package models\n")
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)

	writeSource(c, root, "models_test.go", "package models_test\n")
	writeSource(c, root, ".hidden/model.go", "package hidden\n")
	writeSource(c, root, "vendor/model.go", "package vendor\n")
	writeSource(c, root, "notes.txt", "not Go source\n")

	c.Assert(snapshot.Revalidate(), qt.IsNil)
}

func TestCapture_HappyPath_TraversesHiddenSelectedRoot(t *testing.T) {
	c := qt.New(t)
	root := filepath.Join(t.TempDir(), ".models")
	writeSource(c, root, "models.go", "package models\n")

	snapshot, err := goannotationsource.Capture(root)

	c.Assert(err, qt.IsNil)
	c.Assert(sourcePaths(snapshot.Files()), qt.DeepEquals, []string{"models.go"})
}

func TestSnapshotRevalidate_FailurePath_RejectsChangedContents(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	writeSource(c, root, "models.go", "package models\n")
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)

	writeSource(c, root, "models.go", "package modals\n")

	c.Assert(snapshot.Revalidate(), qt.ErrorIs, goannotationsource.ErrChanged)
}

func TestSnapshotRevalidate_FailurePath_RejectsAddedSource(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	writeSource(c, root, "models.go", "package models\n")
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)

	writeSource(c, root, "added.go", "package models\n")

	c.Assert(snapshot.Revalidate(), qt.ErrorIs, goannotationsource.ErrChanged)
}

func TestSnapshotRevalidate_FailurePath_RejectsRemovedSource(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := writeSource(c, root, "models.go", "package models\n")
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)

	c.Assert(os.Remove(path), qt.IsNil)

	c.Assert(snapshot.Revalidate(), qt.ErrorIs, goannotationsource.ErrChanged)
}

func TestSnapshotRevalidate_FailurePath_RejectsRenamedSource(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := writeSource(c, root, "models.go", "package models\n")
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)

	c.Assert(os.Rename(path, filepath.Join(root, "renamed.go")), qt.IsNil)

	c.Assert(snapshot.Revalidate(), qt.ErrorIs, goannotationsource.ErrChanged)
}

func TestSnapshotRevalidate_FailurePath_RejectsReplacedIdentity(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := writeSource(c, root, "models.go", "package models\n")
	replacement := writeSource(c, root, "replacement.tmp", "package models\n")
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)

	c.Assert(os.Remove(path), qt.IsNil)
	c.Assert(os.Rename(replacement, path), qt.IsNil)

	c.Assert(snapshot.Revalidate(), qt.ErrorIs, goannotationsource.ErrChanged)
}

func TestSnapshotRevalidate_FailurePath_RejectsChangedMode(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := writeSource(c, root, "models.go", "package models\n")
	c.Assert(os.Chmod(path, 0o600), qt.IsNil)
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)

	c.Assert(os.Chmod(path, 0o400), qt.IsNil)

	c.Assert(snapshot.Revalidate(), qt.ErrorIs, goannotationsource.ErrChanged)
}

func TestSnapshotRevalidate_FailurePath_RejectsReplacedEmptyRoot(t *testing.T) {
	c := qt.New(t)
	parent := t.TempDir()
	root := filepath.Join(parent, "models")
	c.Assert(os.Mkdir(root, 0o755), qt.IsNil)
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)

	c.Assert(os.Rename(root, filepath.Join(parent, "old-models")), qt.IsNil)
	c.Assert(os.Mkdir(root, 0o755), qt.IsNil)

	c.Assert(snapshot.Revalidate(), qt.ErrorIs, goannotationsource.ErrChanged)
}

func TestSnapshotSourceAlias_HappyPath_ReportsExactSourceAndMissingPath(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := writeSource(c, root, "models.go", "package models\n")
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)

	alias, err := snapshot.SourceAlias(path)
	c.Assert(err, qt.IsNil)
	c.Assert(alias, qt.Equals, path)

	alias, err = snapshot.SourceAlias(filepath.Join(root, "schema.hcl"))
	c.Assert(err, qt.IsNil)
	c.Assert(alias, qt.Equals, "")
}

func TestCapture_FailurePath_RejectsNonDirectoryRoot(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := writeSource(c, root, "models.go", "package models\n")

	snapshot, err := goannotationsource.Capture(path)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "not a directory")
	c.Assert(snapshot, qt.IsNil)
}

func sourcePaths(files []goannotationsource.File) []string {
	paths := make([]string, len(files))
	for i, file := range files {
		paths[i] = file.RelativePath
	}
	return paths
}

func writeSource(c *qt.C, root, name, data string) string {
	c.Helper()
	path := filepath.Join(root, name)
	c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte(data), 0o600), qt.IsNil)
	return path
}
