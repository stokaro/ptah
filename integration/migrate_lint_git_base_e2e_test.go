//go:build integration

package integration_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestMigrateLintGitBaseE2E covers stokaro/ptah#1235 cell 9.12, the last of the
// two cells the original sweep carried over unverified because it could not
// build a git repository in a scratch directory.
//
// The oracle answer for an unresolvable --git-base is `git diff: exit status
// 128`: the verb and the process status, with neither the argument vector nor
// git's own stderr. Ptah reported the whole command line, which is 203 bytes
// against 33 and leaks the invocation into an operator-facing diagnostic.
//
// A valid --git-base runs first as the control. Without it a fixture whose git
// plumbing never worked at all would still satisfy the failing row, because a
// repository that cannot be read fails the same way as a bad revision.
func TestMigrateLintGitBaseE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)

	_, lookErr := exec.LookPath("git")
	c.Assert(lookErr, qt.IsNil, qt.Commentf("git is required by this contour"))

	repoRoot := e2eRepoRoot(t)
	compatBinary := filepath.Join(t.TempDir(), "ptah-compat")
	nativeBinary := filepath.Join(t.TempDir(), "ptah")
	buildPtahCompat(c, ctx, repoRoot, compatBinary)
	buildPtah(c, ctx, repoRoot, nativeBinary)

	dir := newGitFixtureRepository(c, ctx, compatBinary)

	t.Run("a resolvable --git-base analyzes the changeset", func(t *testing.T) {
		c := qt.New(t)
		stdout, stderr, err := runCLIProcess(ctx, dir, compatBinary,
			"migrate", "lint",
			"--dir", "file://migrations",
			"--dev-url", "sqlite://file?mode=memory&_fk=1",
			"--git-base", "main",
		)

		c.Check(err, qt.IsNil)
		c.Check(stderr, qt.Equals, "")
		c.Check(stdout, qt.Contains, "analyzing version 20240102000000")
		c.Check(stdout, qt.Contains, "1 version ok")
	})

	c.Run("an unresolvable --git-base reports the verb and the status", func(c *qt.C) {
		stdout, stderr, err := runCLIProcess(ctx, dir, compatBinary,
			"migrate", "lint",
			"--dir", "file://migrations",
			"--dev-url", "sqlite://file?mode=memory&_fk=1",
			"--git-base", "nosuchbranch",
		)

		c.Check(exitStatusOf(c, err), qt.Equals, 1)
		c.Check(stdout, qt.Equals, "")
		c.Check(stderr, qt.Equals, "Error: git diff: exit status 128\n")
	})

	c.Run("native lint keeps the reproducible invocation", func(c *qt.C) {
		stdout, stderr, err := runCLIProcess(ctx, dir, nativeBinary,
			"migrations", "lint",
			"--dir", "migrations",
			"--dev-url", "sqlite://file?mode=memory&_fk=1",
			"--git-base", "nosuchbranch",
		)

		c.Check(exitStatusOf(c, err), qt.Equals, 2)
		c.Check(stdout, qt.Equals, "")
		// The full command line is what makes a failed selection reproducible
		// by hand, so the native surface keeps it. Removing it there would be
		// the compatibility rule taking a capability away.
		c.Check(stderr, qt.Contains, "--diff-filter=ACMR")
		c.Check(stderr, qt.Contains, "nosuchbranch...HEAD")
		c.Check(stderr, qt.Contains, "exit status 128")
	})
}

// newGitFixtureRepository builds the throwaway repository cell 9.12 needs: two
// branches, two commits, a hashed migration directory on each. Global and
// system git configuration are neutralized so the fixture cannot inherit a
// signing requirement or a default branch name from the host.
func newGitFixtureRepository(c *qt.C, ctx context.Context, hashBinary string) string {
	c.Helper()
	dir := c.TempDir()
	environment := append(environmentWithoutPtahVariables(),
		"GIT_CONFIG_GLOBAL="+os.DevNull,
		"GIT_CONFIG_SYSTEM="+os.DevNull,
		"GIT_AUTHOR_NAME=Fixture",
		"GIT_AUTHOR_EMAIL=fixture@example.invalid",
		"GIT_COMMITTER_NAME=Fixture",
		"GIT_COMMITTER_EMAIL=fixture@example.invalid",
	)
	git := func(args ...string) {
		c.Helper()
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = dir
		cmd.Env = environment
		output, err := cmd.CombinedOutput()
		c.Assert(err, qt.IsNil, qt.Commentf("git %v: %s", args, output))
	}
	hash := func() {
		c.Helper()
		_, stderr, err := runCLIProcess(ctx, dir, hashBinary, "migrate", "hash", "--dir", "file://migrations")
		c.Assert(err, qt.IsNil, qt.Commentf("migrate hash: %s", stderr))
	}

	git("init", "-q", "-b", "main")
	git("config", "commit.gpgsign", "false")
	c.Assert(os.MkdirAll(filepath.Join(dir, "migrations"), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "migrations", "20240101000000_init.sql"),
		[]byte("CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL);\n"), 0o600,
	), qt.IsNil)
	hash()
	git("add", "-A")
	git("commit", "-q", "-m", "first")

	git("checkout", "-q", "-b", "feature")
	c.Assert(os.WriteFile(
		filepath.Join(dir, "migrations", "20240102000000_add_email.sql"),
		[]byte("ALTER TABLE users ADD COLUMN email text;\n"), 0o600,
	), qt.IsNil)
	hash()
	git("add", "-A")
	git("commit", "-q", "-m", "second")

	return dir
}
