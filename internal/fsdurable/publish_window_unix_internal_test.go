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
	"errors"
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
// Test_publishFileAt_HappyPath_ExchangesWhenNothingEntersTheCommitWindow
// publishes through the identical sequence, which keeps these refusals from
// being satisfiable by a primitive that never commits.
func Test_publishFileAt_FailurePath_RefusesExpectedFileTakenInsideTheCommitWindow(t *testing.T) {
	tests := []struct {
		name string
		// inject takes the destination inside the commit window. It asserts
		// nothing: the rows differ in how the rival arrives, not in what
		// publishFileAt owes afterwards.
		inject func(publishedPath string) error
	}{
		{
			name: "expected file replaced by rename inside the commit window",
			inject: func(publishedPath string) error {
				replacement := publishedPath + ".rival"
				// Joined rather than chained so the rename still runs after a
				// failed write and the row reports both, instead of a
				// conditional deciding which half to report.
				return errors.Join(
					os.WriteFile(replacement, []byte(windowRivalBytes), 0o600),
					os.Rename(replacement, publishedPath),
				)
			},
		},
		{
			name: "expected file edited in place inside the commit window",
			inject: func(publishedPath string) error {
				return os.WriteFile(publishedPath, []byte(windowRivalBytes), 0o600)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stagedPath, publishedPath, err := exchangeWithCommitWindow(c, test.inject)

			c.Assert(err, qt.ErrorIs, ErrDestinationChanged)
			c.Assert(err, qt.Not(qt.ErrorIs), ErrReplacementCommitted)
			assertWindowBytes(c, publishedPath, windowRivalBytes)
			assertWindowBytes(c, stagedPath, "new")
		})
	}
}

// Test_publishFileAt_HappyPath_ExchangesWhenNothingEntersTheCommitWindow is the
// control for the refusals above: the same exchange, the same hook, an empty
// window.
func Test_publishFileAt_HappyPath_ExchangesWhenNothingEntersTheCommitWindow(t *testing.T) {
	c := qt.New(t)

	stagedPath, publishedPath, err := exchangeWithCommitWindow(c, func(string) error { return nil })

	c.Assert(err, qt.IsNil)
	assertWindowBytes(c, publishedPath, "new")
	assertWindowAbsent(c, stagedPath)
}

// exchangeWithCommitWindow publishes a staged file over an existing
// destination through the expected-file exchange, running inject after the last
// check and before the swap. It returns the staged and published paths and the
// publication's own error.
func exchangeWithCommitWindow(
	c *qt.C,
	inject func(publishedPath string) error,
) (staged, published string, err error) {
	c.Helper()

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
			c.Assert(inject(publishedPath), qt.IsNil)
		}},
	)

	return stagedPath, publishedPath, err
}
