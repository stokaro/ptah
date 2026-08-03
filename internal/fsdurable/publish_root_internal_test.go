package fsdurable

// White-box testing required: this file exercises the deterministic
// post-rename verification boundary, which is not exposed by the public API.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func Test_publishFileAt_FailurePath_ReportsPostCommitIdentityChange(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	publishedPath := filepath.Join(dir, "published")
	displacedPath := filepath.Join(dir, "displaced")
	c.Assert(os.WriteFile(stagedPath, []byte("staged"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(publishedPath, []byte("old"), 0o600), qt.IsNil)
	stagedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	originalInfo, err := os.Stat(publishedPath)
	c.Assert(err, qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})

	err = publishFileAt(
		root,
		"staged",
		"published",
		stagedInfo,
		0o600,
		ExpectFile(originalInfo),
		publicationHooks{afterCommit: func() {
			c.Assert(root.Rename("published", "displaced"), qt.IsNil)
			replacement, openErr := root.OpenFile(
				"published",
				os.O_WRONLY|os.O_CREATE|os.O_EXCL,
				0o600,
			)
			c.Assert(openErr, qt.IsNil)
			_, writeErr := replacement.WriteString("replacement")
			c.Assert(writeErr, qt.IsNil)
			c.Assert(replacement.Close(), qt.IsNil)
		}},
	)

	c.Assert(err, qt.ErrorIs, ErrReplacementCommitted)
	c.Assert(err, qt.ErrorIs, ErrStagedFileChanged)
	contents, err := os.ReadFile(displacedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "staged")
	contents, err = os.ReadFile(publishedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "replacement")
}
