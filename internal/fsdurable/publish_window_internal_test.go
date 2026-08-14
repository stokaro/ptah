package fsdurable

// White-box testing required: the window these rows enter is a single syscall
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
// is the row that separates a conditional commit primitive from another
// pre-rename Lstat, which is the design #948 rules out.
//
// Every other publication test mutates the destination before publishFileAt is
// entered, so all of them are satisfied by an implementation that stats the
// target and then commits with an unconditional rename. These rows mutate
// through beforeCommit instead, where no earlier check can observe it: only a
// commit that binds the destination at the instant it takes effect refuses
// them.
//
// The control row runs the identical sequence with nothing entering the window,
// so the refusal cannot be satisfied by a primitive that rejects everything.
func Test_publishFileAt_FailurePath_RefusesDestinationTakenInsideTheCommitWindow(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(c *qt.C, publishedPath string) Destination
		inject  func(c *qt.C, publishedPath string)
		assert  func(c *qt.C, err error, stagedPath, publishedPath string)
	}{
		{
			name: "absent destination created inside the commit window",
			prepare: func(*qt.C, string) Destination {
				return ExpectAbsent()
			},
			inject: func(c *qt.C, publishedPath string) {
				c.Assert(os.WriteFile(publishedPath, []byte(windowRivalBytes), 0o600), qt.IsNil)
			},
			assert: func(c *qt.C, err error, stagedPath, publishedPath string) {
				c.Assert(err, qt.ErrorIs, ErrDestinationChanged)
				c.Assert(err, qt.Not(qt.ErrorIs), ErrReplacementCommitted)
				assertWindowBytes(c, publishedPath, windowRivalBytes)
				assertWindowBytes(c, stagedPath, "new")
			},
		},
		{
			name: "control: nothing enters the commit window",
			prepare: func(*qt.C, string) Destination {
				return ExpectAbsent()
			},
			inject: func(*qt.C, string) {},
			assert: func(c *qt.C, err error, stagedPath, publishedPath string) {
				c.Assert(err, qt.IsNil)
				assertWindowBytes(c, publishedPath, "new")
				assertWindowAbsent(c, stagedPath)
			},
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
			dest := test.prepare(c, publishedPath)
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
				dest,
				publicationHooks{beforeCommit: func() {
					test.inject(c, publishedPath)
				}},
			)

			test.assert(c, err, stagedPath, publishedPath)
		})
	}
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
