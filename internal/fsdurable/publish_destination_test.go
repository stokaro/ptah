package fsdurable_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fsdurable"
)

// rivalPublicationBytes is what a concurrent writer leaves at the destination.
// It is never what this test publishes, so finding it afterwards is what tells
// a refusal apart from an overwrite.
const rivalPublicationBytes = "concurrent writer bytes\n"

// The functions below are the fixture each row publishes over and the rival
// write that follows it. None of them takes a checker, because none of them
// asserts anything: they report their own failure to the loop, which is where
// the assertions live.

func expectExistingPublication(publishedPath string) (fsdurable.Destination, error) {
	if err := os.WriteFile(publishedPath, []byte("old"), 0o600); err != nil {
		return fsdurable.Destination{}, err
	}
	info, err := os.Stat(publishedPath)
	if err != nil {
		return fsdurable.Destination{}, err
	}
	return fsdurable.ExpectFile(info), nil
}

func expectAbsentPublication(string) (fsdurable.Destination, error) {
	return fsdurable.ExpectAbsent(), nil
}

// stateNoPublicationExpectation seeds the rival bytes itself, because a
// publication that states no expectation has no window to lose: the row exists
// to show that omission is refused rather than treated as "publish over
// anything".
func stateNoPublicationExpectation(publishedPath string) (fsdurable.Destination, error) {
	return fsdurable.Destination{},
		os.WriteFile(publishedPath, []byte(rivalPublicationBytes), 0o600)
}

func editPublicationInPlace(publishedPath string) error {
	return os.WriteFile(publishedPath, []byte(rivalPublicationBytes), 0o600)
}

func replacePublicationByRename(publishedPath string) error {
	replacement := publishedPath + ".rival"
	if err := os.WriteFile(replacement, []byte(rivalPublicationBytes), 0o600); err != nil {
		return err
	}
	return os.Rename(replacement, publishedPath)
}

func recreatePublication(publishedPath string) error {
	if err := os.Remove(publishedPath); err != nil {
		return err
	}
	return os.WriteFile(publishedPath, []byte(rivalPublicationBytes), 0o600)
}

func leavePublicationAlone(string) error { return nil }

// TestPublishFileAt_FailurePath_RefusesChangedDestination covers the guarantee
// the callers cannot provide for themselves: their own destination checks are a
// separate, earlier syscall, so only the commit primitive can bind what it
// replaces. Every row mutates the destination after the expectation was
// captured and asserts the rival's bytes, not just the exit status, because a
// silent overwrite and a refusal both leave a file at the target name.
func TestPublishFileAt_FailurePath_RefusesChangedDestination(t *testing.T) {
	tests := []struct {
		name        string
		prepare     func(publishedPath string) (fsdurable.Destination, error)
		mutate      func(publishedPath string) error
		wantErrorIs error
	}{
		{
			name:        "expected file edited in place",
			prepare:     expectExistingPublication,
			mutate:      editPublicationInPlace,
			wantErrorIs: fsdurable.ErrDestinationChanged,
		},
		{
			name:        "expected file replaced by rename",
			prepare:     expectExistingPublication,
			mutate:      replacePublicationByRename,
			wantErrorIs: fsdurable.ErrDestinationChanged,
		},
		{
			name:        "expected file removed and recreated",
			prepare:     expectExistingPublication,
			mutate:      recreatePublication,
			wantErrorIs: fsdurable.ErrDestinationChanged,
		},
		{
			name:        "expected absent destination created",
			prepare:     expectAbsentPublication,
			mutate:      editPublicationInPlace,
			wantErrorIs: fsdurable.ErrDestinationChanged,
		},
		{
			name:        "unstated destination expectation",
			prepare:     stateNoPublicationExpectation,
			mutate:      leavePublicationAlone,
			wantErrorIs: fsdurable.ErrDestinationChanged,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			stagedPath := filepath.Join(dir, "staged")
			publishedPath := filepath.Join(dir, "published")
			c.Assert(os.WriteFile(stagedPath, []byte("new"), 0o600), qt.IsNil)
			stagedInfo, err := os.Stat(stagedPath)
			c.Assert(err, qt.IsNil)
			dest, err := test.prepare(publishedPath)
			c.Assert(err, qt.IsNil)
			root, err := os.OpenRoot(dir)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				c.Check(root.Close(), qt.IsNil)
			})
			c.Assert(test.mutate(publishedPath), qt.IsNil)

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
			assertPublicationBytes(c, publishedPath, rivalPublicationBytes)
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
