//go:build unix

package fsdurable

// White-box testing required: these rows enter the commit window of the
// expected-file path, which is an atomic exchange here and is reachable only
// through the unexported publication hook.
//
// Unix-only on purpose. The Windows path holds an open handle on the
// destination across the same window, so replacing that entry from a test
// would measure Windows sharing semantics rather than the publication
// contract. The portable expected-absent row in publish_window_internal_test.go
// covers every platform.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// Test_publishFileAt_FailurePath_RefusesExpectedFileTakenInsideTheCommitWindow
// exercises the exchange path with the rival arriving after the last check and
// before the swap. The displaced side is verified after the exchange and put
// back, so the rival's bytes must still be at the target and the staged file
// must still be staged.
//
// The control row publishes through the identical sequence, which keeps the
// refusals from being satisfiable by a primitive that never commits.
func Test_publishFileAt_FailurePath_RefusesExpectedFileTakenInsideTheCommitWindow(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name   string
		inject func(c *qt.C, publishedPath string)
		assert func(c *qt.C, err error, stagedPath, publishedPath string)
	}{
		{
			name: "expected file replaced by rename inside the commit window",
			inject: func(c *qt.C, publishedPath string) {
				replacement := publishedPath + ".rival"
				c.Assert(os.WriteFile(replacement, []byte(windowRivalBytes), 0o600), qt.IsNil)
				c.Assert(os.Rename(replacement, publishedPath), qt.IsNil)
			},
			assert: func(c *qt.C, err error, stagedPath, publishedPath string) {
				c.Assert(err, qt.ErrorIs, ErrDestinationChanged)
				c.Assert(err, qt.Not(qt.ErrorIs), ErrReplacementCommitted)
				assertWindowBytes(c.TB, publishedPath, windowRivalBytes)
				assertWindowBytes(c.TB, stagedPath, "new")
			},
		},
		{
			name: "expected file edited in place inside the commit window",
			inject: func(c *qt.C, publishedPath string) {
				c.Assert(os.WriteFile(publishedPath, []byte(windowRivalBytes), 0o600), qt.IsNil)
			},
			assert: func(c *qt.C, err error, stagedPath, publishedPath string) {
				c.Assert(err, qt.ErrorIs, ErrDestinationChanged)
				c.Assert(err, qt.Not(qt.ErrorIs), ErrReplacementCommitted)
				assertWindowBytes(c.TB, publishedPath, windowRivalBytes)
				assertWindowBytes(c.TB, stagedPath, "new")
			},
		},
		{
			name:   "control: nothing enters the commit window",
			inject: func(*qt.C, string) {},
			assert: func(c *qt.C, err error, stagedPath, publishedPath string) {
				c.Assert(err, qt.IsNil)
				assertWindowBytes(c.TB, publishedPath, "new")
				assertWindowAbsent(c.TB, stagedPath)
			},
		},
	}
	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
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

			err = publishFileAt(
				root,
				"staged",
				"published",
				stagedInfo,
				0o600,
				ExpectFile(originalInfo),
				publicationHooks{beforeCommit: func() {
					test.inject(c, publishedPath)
				}},
			)

			test.assert(c, err, stagedPath, publishedPath)
		})
	}
}
