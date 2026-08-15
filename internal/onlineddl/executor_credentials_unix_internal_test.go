//go:build unix

package onlineddl

// White-box testing required: this file exercises unexported executor internals,
// the same reason executor_test.go gives.

// The credential-file mode is a unix property, and this whole test moved here
// rather than skipping in place. Its mode assertion lives inside the e.run
// closure, which executeStatement calls synchronously -- a Skip there would
// runtime.Goexit and take the tool-failure cleanup coverage with it, silently.
// Moving the test drops the same coverage visibly, and
// TestExecuteStatement_CredentialFileCleanedAfterCancellation still covers
// cleanup on every platform through the cancellation path.
//
// It is only a test-side move. Production cannot enforce the restriction on
// Windows either: executor.go chmods an os.CreateTemp file to 0o600, and on
// Windows that call only moves the read-only attribute, so the credentials are
// left readable by anyone who can reach the temp directory. docs/online-ddl.md
// promises mode 0600 without saying so.

import (
	"context"
	"errors"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestExecuteStatement_CredentialFileIs0600AndCleanedAfterToolFailure(t *testing.T) {
	c := qt.New(t)

	var credentialFile string
	e := New(Config{})
	e.lookPath = func(string) (string, error) { return "/bin/gh-ost", nil }
	e.run = func(_ context.Context, _ string, args []string) error {
		credentialFile = requireArgPrefix(t, args, "--conf=")
		info, err := os.Stat(credentialFile)
		c.Assert(err, qt.IsNil)
		c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o600))
		content, err := os.ReadFile(credentialFile)
		c.Assert(err, qt.IsNil)
		c.Assert(string(content), qt.Contains, `user="app"`)
		c.Assert(string(content), qt.Contains, `password="secret"`)
		return errors.New("exit status 1")
	}

	_, err := e.executeStatement(context.Background(), mysqlConn(),
		"ALTER TABLE users ADD COLUMN bio TEXT",
		map[string]string{DirectiveTool: ToolGhost})

	c.Assert(err, qt.ErrorMatches, "online-DDL tool gh-ost failed for table users: exit status 1")
	requireCredentialFileRemoved(t, credentialFile)
}
