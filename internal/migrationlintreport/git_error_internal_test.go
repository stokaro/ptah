package migrationlintreport

// White-box testing required: gitOutput is unexported and is the only producer
// of GitCommandError. A black-box test would have to drive a full lint run with
// a real repository and a dev database to reach it, which would prove the
// error's identity only incidentally. The compatibility adapter in cmd/atlas
// matches on that identity, so the identity is what needs pinning here.

import (
	"os/exec"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestGitOutputReturnsATypedFailure runs a real git invocation that cannot
// succeed. Without this, the adapter's rows would only prove that a hand-built
// GitCommandError renders correctly, not that a failed invocation produces one.
func TestGitOutputReturnsATypedFailure(t *testing.T) {
	c := qt.New(t)
	_, lookErr := exec.LookPath("git")
	c.Assert(lookErr, qt.IsNil, qt.Commentf("git is required to exercise the producer"))

	// A directory that is not a repository, with the search ceiling set so the
	// walk cannot escape into whatever repository holds the test binary.
	dir := t.TempDir()
	t.Setenv("GIT_CEILING_DIRECTORIES", dir)

	_, err := gitOutput(t.Context(), dir, "rev-parse", "--show-toplevel")

	c.Assert(err, qt.IsNotNil)
	var gitErr *GitCommandError
	c.Assert(err, qt.ErrorAs, &gitErr)
	c.Check(gitErr.Subcommand, qt.Equals, "rev-parse")
	c.Check(gitErr.Args, qt.DeepEquals, []string{"rev-parse", "--show-toplevel"})
	c.Check(gitErr.Output, qt.Not(qt.Equals), "")
	// Unwrap reaches the process failure, so errors.Is on an *exec.ExitError
	// keeps working for callers that classify on it.
	var exitErr *exec.ExitError
	c.Check(err, qt.ErrorAs, &exitErr)
}

// TestGitOutputSucceedsWithoutWrapping is the control. Without it, a producer
// that returned a GitCommandError unconditionally would still satisfy the row
// above.
func TestGitOutputSucceedsWithoutWrapping(t *testing.T) {
	c := qt.New(t)
	_, lookErr := exec.LookPath("git")
	c.Assert(lookErr, qt.IsNil, qt.Commentf("git is required to exercise the producer"))

	out, err := gitOutput(t.Context(), t.TempDir(), "--version")

	c.Assert(err, qt.IsNil)
	c.Check(out, qt.Contains, "git version")
}

// TestGitOutputArgsAreNotAliased pins the defensive clone. The caller's slice
// is reused across invocations, so retaining it directly would let a later call
// rewrite an earlier error's recorded invocation.
func TestGitOutputArgsAreNotAliased(t *testing.T) {
	c := qt.New(t)
	_, lookErr := exec.LookPath("git")
	c.Assert(lookErr, qt.IsNil, qt.Commentf("git is required to exercise the producer"))

	dir := t.TempDir()
	t.Setenv("GIT_CEILING_DIRECTORIES", dir)
	args := []string{"rev-parse", "--show-toplevel"}

	_, err := gitOutput(t.Context(), dir, args...)

	c.Assert(err, qt.IsNotNil)
	var gitErr *GitCommandError
	c.Assert(err, qt.ErrorAs, &gitErr)
	args[1] = "--rewritten"
	c.Check(gitErr.Args, qt.DeepEquals, []string{"rev-parse", "--show-toplevel"})
}
