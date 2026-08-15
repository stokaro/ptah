//go:build unix

package goannotationcleanup

// White-box testing required: this file replaces a source ancestor after the
// unexported post-staging revalidation barrier to verify that cleanup aborts
// without mutating either the selected or replacement directory.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/goannotationsource"
	"go.5x5.cz/ptah/internal/pathguard"
)

func TestApplyPlans_FailurePath_AncestorSwapAbortsCleanup(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	sourceDir := filepath.Join(root, "models")
	capturedDir := filepath.Join(root, "captured-models")
	outsideDir := c.TempDir()
	c.Assert(os.MkdirAll(sourceDir, 0o700), qt.IsNil)
	sourcePath := filepath.Join(sourceDir, "model.go")
	outsidePath := filepath.Join(outsideDir, "model.go")
	sourceData := []byte(
		"package models\n\n//ptah:schema:table name=\"users\"\ntype User struct{}\n",
	)
	outsideData := []byte("package outside\n\ntype Outside struct{}\n")
	c.Assert(os.WriteFile(sourcePath, sourceData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(outsidePath, outsideData, 0o600), qt.IsNil)
	snapshot, err := goannotationsource.Capture(root)
	c.Assert(err, qt.IsNil)
	plan, err := NewPlan(snapshot)
	c.Assert(err, qt.IsNil)

	err = applyPlans(plan.changes, applyHooks{
		revalidate: func() error {
			c.Assert(snapshot.Revalidate(), qt.IsNil)
			c.Assert(os.Rename(sourceDir, capturedDir), qt.IsNil)
			c.Assert(os.Symlink(outsideDir, sourceDir), qt.IsNil)
			return nil
		},
		afterCommit: func() {},
	})

	c.Assert(err, qt.ErrorIs, pathguard.ErrDirectoryChanged)
	assertInternalFileBytes(c, filepath.Join(capturedDir, "model.go"), sourceData)
	assertInternalFileBytes(c, outsidePath, outsideData)
	entries, err := os.ReadDir(capturedDir)
	c.Assert(err, qt.IsNil)
	c.Assert(internalEntryNames(entries), qt.DeepEquals, []string{"model.go"})
}
