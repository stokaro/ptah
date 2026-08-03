package teststyleguard_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// violatingTestSource renders a test file carrying a prohibited conditional in a
// top-level Test function. Every fixture file uses it, so nothing but the path
// source can separate them.
func violatingTestSource(pkg, name string) string {
	return fmt.Sprintf(`package %s_test

import "testing"

func Test%s(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
}
`, pkg, name)
}

// TestScanPathsSelectsTheWorkingTreeOnly pins both halves of the #1069 fix at
// once. Rows 2 and 4 are a matched pair: row 2 fails if worktree checkouts are
// scanned, and rows 1, 3 and 4 fail if the selection was merely narrowed until
// nothing was left to report.
func TestScanPathsSelectsTheWorkingTreeOnly(t *testing.T) {
	tests := []struct {
		name  string
		root  func(t *testing.T) string
		check func(c *qt.C, paths []string)
	}{
		{
			name: "tracked violation is selected",
			root: fixtureRepo,
			check: func(c *qt.C, paths []string) {
				c.Assert(paths, qt.Contains, "pkg/tracked_bad_test.go")
			},
		},
		{
			name: "linked worktree violation is not selected",
			root: fixtureRepo,
			check: func(c *qt.C, paths []string) {
				c.Assert(paths, qt.Not(qt.Contains), "wt/wt_bad_test.go")
				c.Assert(pathsUnder(paths, "wt/"), qt.HasLen, 0)
			},
		},
		{
			name: "never-staged new test is still selected",
			root: fixtureRepo,
			check: func(c *qt.C, paths []string) {
				c.Assert(paths, qt.Contains, "fresh/fresh_bad_test.go")
			},
		},
		{
			name: "real repository selection is not narrowed",
			root: moduleRoot,
			check: func(c *qt.C, paths []string) {
				c.Assert(len(paths) > 500, qt.IsTrue, qt.Commentf("selected only %d test files", len(paths)))
				c.Assert(paths, qt.Contains, "internal/apiguard/snapshot_test.go")
				c.Assert(paths, qt.Contains, "internal/teststyleguard/scanpaths_test.go")
				c.Assert(pathsUnder(paths, ".claude/worktrees/"), qt.HasLen, 0)
				c.Assert(pathsUnder(paths, ".codex/"), qt.HasLen, 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(qt.New(t), listScanPaths(t, tt.root(t)))
		})
	}
}

// fixtureRepo builds a throwaway git repository holding three copies of the same
// violation, reachable only through different path sources:
//
//	pkg/tracked_bad_test.go     committed
//	wt/wt_bad_test.go           inside a linked git worktree
//	fresh/fresh_bad_test.go     present on disk, never staged
//
// The linked worktree also carries its own checkout of pkg/tracked_bad_test.go,
// so a filesystem walk finds four candidate files where git finds two.
func fixtureRepo(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()

	git(t, dir, "init", "-q", ".")
	writeFixtureFile(t, filepath.Join(dir, "pkg", "tracked_bad_test.go"), violatingTestSource("pkg", "TrackedViolation"))
	git(t, dir, "add", "-A")
	git(t, dir, "commit", "-qm", "fixture")

	git(t, dir, "worktree", "add", "-q", "--detach", "wt", "HEAD")
	writeFixtureFile(t, filepath.Join(dir, "wt", "wt_bad_test.go"), violatingTestSource("wt", "WorktreeViolation"))

	writeFixtureFile(t, filepath.Join(dir, "fresh", "fresh_bad_test.go"), violatingTestSource("fresh", "FreshViolation"))

	return dir
}

// listScanPaths runs the shipped gate script in its path-listing mode with the
// working directory set to dir, and returns the selected paths.
func listScanPaths(t *testing.T, dir string) []string {
	t.Helper()
	c := qt.New(t)

	script := filepath.Join(moduleRoot(t), "scripts", "check-test-style.sh")
	cmd := exec.Command("sh", script, "--list-scan-paths")
	cmd.Dir = dir
	cmd.Env = isolatedGitEnv()

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	c.Assert(err, qt.IsNil, qt.Commentf("--list-scan-paths failed: %v\nstderr:\n%s", err, stderr.String()))

	return nonEmptyLines(string(out))
}

// git runs a git subcommand in dir and asserts it succeeded. Identity and
// signing are pinned on the command line so the fixture does not depend on the
// developer's global git configuration.
func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	c := qt.New(t)

	full := []string{
		"-c", "user.name=ptah test",
		"-c", "user.email=test@example.invalid",
		"-c", "commit.gpgsign=false",
		"-c", "init.defaultBranch=main",
	}
	full = append(full, args...)

	// #nosec G204 -- every argument is a literal from this file's fixture builder.
	cmd := exec.Command("git", full...)
	cmd.Dir = dir
	cmd.Env = isolatedGitEnv()

	out, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("git %s failed: %v\noutput:\n%s", strings.Join(args, " "), err, out))
}

// isolatedGitEnv keeps the fixture repository independent of any git state the
// test runner inherited, in particular a GIT_DIR pointing at the Ptah checkout.
func isolatedGitEnv() []string {
	env := make([]string, 0, len(os.Environ())+2)

	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "GIT_DIR") || strings.HasPrefix(name, "GIT_WORK_TREE") || strings.HasPrefix(name, "GIT_INDEX_FILE") {
			continue
		}

		env = append(env, entry)
	}

	return append(env, "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0")
}

// writeFixtureFile writes content at path, creating parent directories.
func writeFixtureFile(t *testing.T, path, content string) {
	t.Helper()
	c := qt.New(t)

	err := os.MkdirAll(filepath.Dir(path), 0o750)
	c.Assert(err, qt.IsNil)

	err = os.WriteFile(path, []byte(content), 0o600)
	c.Assert(err, qt.IsNil)
}

// pathsUnder returns the selected paths that live under prefix.
func pathsUnder(paths []string, prefix string) []string {
	var found []string

	for _, path := range paths {
		if strings.HasPrefix(path, prefix) {
			found = append(found, path)
		}
	}

	return found
}

// nonEmptyLines splits command output into lines, dropping the trailing blank.
func nonEmptyLines(out string) []string {
	var lines []string

	for line := range strings.SplitSeq(out, "\n") {
		if line == "" {
			continue
		}

		lines = append(lines, line)
	}

	return lines
}

// moduleRoot returns the repository root. This package lives at a fixed depth
// (internal/teststyleguard) and go test runs with the working directory set to
// the package source directory, so the root is two directories up.
func moduleRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	qt.New(t).Assert(err, qt.IsNil)

	return filepath.Dir(filepath.Dir(wd))
}
