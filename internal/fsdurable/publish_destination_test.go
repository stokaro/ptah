package fsdurable_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fsdurable"
)

// TestPublishFileAt_FailurePath_RefusesChangedDestination covers the guarantee
// the callers cannot provide for themselves: their own destination checks are a
// separate, earlier syscall, so only the commit primitive can bind what it
// replaces. Every row mutates the destination after the expectation was
// captured and asserts the rival's bytes, not just the exit status, because a
// silent overwrite and a refusal both leave a file at the target name.
func TestPublishFileAt_FailurePath_RefusesChangedDestination(t *testing.T) {
	c := qt.New(t)
	rival := "concurrent writer bytes\n"
	tests := []struct {
		name        string
		prepare     func(c *qt.C, publishedPath string) fsdurable.Destination
		mutate      func(c *qt.C, publishedPath string)
		wantErrorIs error
	}{
		{
			name: "expected file edited in place",
			prepare: func(c *qt.C, publishedPath string) fsdurable.Destination {
				c.Assert(os.WriteFile(publishedPath, []byte("old"), 0o600), qt.IsNil)
				info, err := os.Stat(publishedPath)
				c.Assert(err, qt.IsNil)
				return fsdurable.ExpectFile(info)
			},
			mutate: func(c *qt.C, publishedPath string) {
				c.Assert(os.WriteFile(publishedPath, []byte(rival), 0o600), qt.IsNil)
			},
			wantErrorIs: fsdurable.ErrDestinationChanged,
		},
		{
			name: "expected file replaced by rename",
			prepare: func(c *qt.C, publishedPath string) fsdurable.Destination {
				c.Assert(os.WriteFile(publishedPath, []byte("old"), 0o600), qt.IsNil)
				info, err := os.Stat(publishedPath)
				c.Assert(err, qt.IsNil)
				return fsdurable.ExpectFile(info)
			},
			mutate: func(c *qt.C, publishedPath string) {
				replacement := publishedPath + ".rival"
				c.Assert(os.WriteFile(replacement, []byte(rival), 0o600), qt.IsNil)
				c.Assert(os.Rename(replacement, publishedPath), qt.IsNil)
			},
			wantErrorIs: fsdurable.ErrDestinationChanged,
		},
		{
			name: "expected file removed and recreated",
			prepare: func(c *qt.C, publishedPath string) fsdurable.Destination {
				c.Assert(os.WriteFile(publishedPath, []byte("old"), 0o600), qt.IsNil)
				info, err := os.Stat(publishedPath)
				c.Assert(err, qt.IsNil)
				return fsdurable.ExpectFile(info)
			},
			mutate: func(c *qt.C, publishedPath string) {
				c.Assert(os.Remove(publishedPath), qt.IsNil)
				c.Assert(os.WriteFile(publishedPath, []byte(rival), 0o600), qt.IsNil)
			},
			wantErrorIs: fsdurable.ErrDestinationChanged,
		},
		{
			name: "expected absent destination created",
			prepare: func(*qt.C, string) fsdurable.Destination {
				return fsdurable.ExpectAbsent()
			},
			mutate: func(c *qt.C, publishedPath string) {
				c.Assert(os.WriteFile(publishedPath, []byte(rival), 0o600), qt.IsNil)
			},
			wantErrorIs: fsdurable.ErrDestinationChanged,
		},
		{
			name: "unstated destination expectation",
			prepare: func(c *qt.C, publishedPath string) fsdurable.Destination {
				c.Assert(os.WriteFile(publishedPath, []byte(rival), 0o600), qt.IsNil)
				return fsdurable.Destination{}
			},
			mutate:      func(*qt.C, string) {},
			wantErrorIs: fsdurable.ErrDestinationChanged,
		},
	}
	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dir := c.TempDir()
			stagedPath := filepath.Join(dir, "staged")
			publishedPath := filepath.Join(dir, "published")
			c.Assert(os.WriteFile(stagedPath, []byte("new"), 0o600), qt.IsNil)
			stagedInfo, err := os.Stat(stagedPath)
			c.Assert(err, qt.IsNil)
			dest := test.prepare(c, publishedPath)
			root, err := os.OpenRoot(dir)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				c.Check(root.Close(), qt.IsNil)
			})
			test.mutate(c, publishedPath)

			err = fsdurable.PublishFileAt(
				root,
				"staged",
				"published",
				stagedInfo,
				0o600,
				dest,
			)

			c.Assert(err, qt.ErrorIs, test.wantErrorIs)
			c.Assert(err, qt.Not(qt.ErrorIs), fsdurable.ErrReplacementCommitted)
			assertPublicationBytes(c, publishedPath, rival)
			assertPublicationBytes(c, stagedPath, "new")
		})
	}
}

// TestPublishFileAt_HappyPath_PublishesOverTheExpectedFile pins the other side
// of the same comparison: the identical sequence with an accurate expectation
// must still publish, so the refusals above cannot be satisfied by a primitive
// that rejects everything.
func TestPublishFileAt_HappyPath_PublishesOverTheExpectedFile(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	publishedPath := filepath.Join(dir, "published")
	c.Assert(os.WriteFile(stagedPath, []byte("new"), 0o600), qt.IsNil)
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

	err = fsdurable.PublishFileAt(
		root,
		"staged",
		"published",
		stagedInfo,
		0o600,
		fsdurable.ExpectFile(originalInfo),
	)

	c.Assert(err, qt.IsNil)
	assertPublicationBytes(c, publishedPath, "new")
	publishedInfo, err := os.Stat(publishedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(os.SameFile(stagedInfo, publishedInfo), qt.IsTrue)
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(publicationEntryNames(entries), qt.DeepEquals, []string{"published"})
}

func assertPublicationBytes(c *qt.C, path, want string) {
	c.Helper()
	contents, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, want)
}

func publicationEntryNames(entries []fs.DirEntry) []string {
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	return names
}
