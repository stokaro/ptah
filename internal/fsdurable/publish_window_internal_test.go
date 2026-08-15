package fsdurable

// White-box testing required: the window these tests enter is a single syscall
// wide. It is reachable only through the unexported publication hook, which
// fires after every check this package could make and immediately before the
// platform commit primitive.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const windowRivalBytes = "concurrent writer bytes\n"

// Test_publishFileAt_FailurePath_RefusesDestinationTakenInsideTheCommitWindow
// is the case that separates a conditional commit primitive from another
// pre-rename Lstat, which is the design #948 rules out.
//
// Every other publication test mutates the destination before publishFileAt is
// entered, so all of them are satisfied by an implementation that stats the
// target and then commits with an unconditional rename. This one mutates
// through beforeCommit instead, where no earlier check can observe it: only a
// commit that binds the destination at the instant it takes effect refuses it.
//
// Its control is Test_publishFileAt_HappyPath_PublishesWhenNothingEntersTheCommitWindow
// below, which runs the identical sequence with nothing entering the window, so
// the refusal cannot be satisfied by a primitive that rejects everything.
func Test_publishFileAt_FailurePath_RefusesDestinationTakenInsideTheCommitWindow(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	publishedPath := filepath.Join(dir, "published")

	err := publishAbsentDestinationAt(c, dir, func() {
		c.Assert(os.WriteFile(publishedPath, []byte(windowRivalBytes), 0o600), qt.IsNil)
	})

	c.Assert(err, qt.ErrorIs, ErrDestinationChanged)
	c.Assert(err, qt.Not(qt.ErrorIs), ErrReplacementCommitted)
	assertWindowBytes(c, publishedPath, windowRivalBytes)
	assertWindowBytes(c, stagedPath, "new")
}

// Test_publishFileAt_HappyPath_PublishesWhenNothingEntersTheCommitWindow is the
// control for the refusal above: same sequence, same hook, nothing arriving
// through it.
func Test_publishFileAt_HappyPath_PublishesWhenNothingEntersTheCommitWindow(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	stagedPath := filepath.Join(dir, "staged")
	publishedPath := filepath.Join(dir, "published")

	err := publishAbsentDestinationAt(c, dir, func() {})

	c.Assert(err, qt.IsNil)
	assertWindowBytes(c, publishedPath, "new")
	assertWindowAbsent(c, stagedPath)
}

// publishAbsentDestinationAt stages "new" at dir/staged and publishes it to
// dir/published under ExpectAbsent, running beforeCommit at the instant the
// commit primitive is entered.
func publishAbsentDestinationAt(c *qt.C, dir string, beforeCommit func()) error {
	c.Helper()
	stagedPath := filepath.Join(dir, "staged")
	c.Assert(os.WriteFile(stagedPath, []byte("new"), 0o600), qt.IsNil)
	stagedInfo, err := os.Stat(stagedPath)
	c.Assert(err, qt.IsNil)
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(root.Close(), qt.IsNil)
	})

	return publishFileAt(
		root,
		"staged",
		"published",
		stagedInfo,
		0o600,
		ExpectAbsent(),
		publicationHooks{beforeCommit: beforeCommit},
	)
}

func assertWindowBytes(c *qt.C, path, want string) {
	c.Helper()
	contents, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, want)
}

func assertWindowAbsent(c *qt.C, path string) {
	c.Helper()
	_, err := os.Lstat(path)
	c.Assert(os.IsNotExist(err), qt.IsTrue)
}
