// Package clirun builds and runs Ptah's shipped binaries for end-to-end tests.
//
// It sits beside the integration trees rather than inside one, because an
// integration tree holds nothing but tagged test files and this is library code
// with its own unit tests.
//
// It exists because the same three jobs were written out per file: a `go build`
// per test, a hand-rolled wrapper around exec, and a private way of reading back
// what the command printed. Five copies of the build helper stood in the tree,
// and every one of them rebuilt the binary for each test that called it. The
// cost is what makes a new end-to-end test expensive enough to skip, which is
// how a shipped binary ends up exercised for a fraction of its verbs.
//
// Two properties matter more than the convenience. The build happens once per
// test binary, so a package with thirty end-to-end tests pays for one
// compilation; and a run returns stdout, stderr and the exit code separately, so
// a test asserts on the stream that carries the claim instead of on a merged
// blob where a diagnostic and a result read alike.
//
// The exit code is why a process is involved at all. Where the subject of a
// test is what the program returns to a shell, calling the command in-process
// compares an error value against an exit status and cannot see a regression in
// how one becomes the other.
//
// The build outlives every test that uses it, so no test can remove it. The
// test binary does, through [Main], which a package that builds calls from its
// TestMain; [Build] refuses to run without it. A test binary that dies before
// Main returns, killed by its timeout for one, leaves its directory behind, and
// the next build in any process removes it: see [Build].
package clirun

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/exeext"
)

// DefaultTimeout bounds one invocation.
//
// A command that hangs would otherwise take the whole package down with the Go
// test timeout, which reports the package rather than the command and leaves a
// reader guessing which invocation stopped.
const DefaultTimeout = 2 * time.Minute

// Target names a program this package can build.
type Target string

const (
	// Ptah is the native binary.
	Ptah Target = "ptah.run/cmd/ptah"
	// Compat is the Atlas-shaped binary. It ships as `ptah-compat` and installs
	// as `atlas`, and a test that cares about the name should say which it
	// means through [Options.As].
	Compat Target = "ptah.run/cmd/ptah-compat"
)

// Result is one invocation's whole observable outcome.
//
// Stdout and Stderr stay apart. Ptah writes results to one and diagnostics to
// the other, and a test asserting on a merged stream cannot tell a command that
// printed a plan from one that printed why it would not.
type Result struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// Options are the parts of an invocation a test usually needs to set.
type Options struct {
	// Dir is the working directory. Empty means the test's own, which is
	// almost never what an end-to-end test wants: the commands read and write
	// files relative to where they run.
	Dir string
	// Env replaces the environment when non-nil, exactly as exec.Cmd.Env does.
	// Nil inherits, which is what a test wants unless it is measuring a
	// variable.
	Env []string
	// Stdin is fed to the command.
	Stdin string
	// Timeout overrides DefaultTimeout.
	Timeout time.Duration
}

// built memoizes one compilation per target for the life of the test binary.
//
// The directory is deliberately not cleaned up per test: a t.Cleanup would
// remove a binary other tests in the same package still hold, and the package
// has no single owner to hang the removal on. [Main] removes it when the tests
// end.
var built sync.Map

// mainRunning records that [Main] is running this test binary, so the build
// will be removed.
var mainRunning atomic.Bool

// owned are the directories this process created, each with the open owner
// file that holds its lock.
var owned struct {
	sync.Mutex
	dirs []ownedDir
}

type ownedDir struct {
	path  string
	owner *os.File
}

// sweepOnce runs [sweep] on the first compilation of the process.
var sweepOnce sync.Once

const (
	// dirPattern names the directory a compilation goes into.
	dirPattern = "ptah-clirun-*"
	// ownerName is the file in that directory whose lock says the process that
	// created the directory still runs.
	ownerName = "owner.lock"
	// unownedAge is how old a directory without an owner file must be before a
	// sweep removes it. A directory has no owner file for a moment after it is
	// created, and forever when a build from before the owner file created it;
	// the age tells the two apart. It is longer than any test timeout in this
	// repository, so a build still in use is not taken.
	unownedAge = 3 * time.Hour
)

type buildResult struct {
	path string
	err  error
}

// Main runs the tests of a package that uses [Build] or [Run] and removes every
// program Build compiled in this process. A package that builds calls it from
// its TestMain, in place of os.Exit(m.Run()):
//
//	func TestMain(m *testing.M) {
//		clirun.Main(m)
//	}
//
// It returns rather than exits, and the test binary exits with the code m.Run
// returned once TestMain returns.
//
// A compiled program is about 120 MB, so a test process that builds both
// targets and does not remove them leaves a quarter of a gigabyte in the temp
// directory (stokaro/ptah#3869). A directory Main cannot remove, because a
// program in it still runs on Windows, is left to the next sweep.
func Main(m *testing.M) {
	mainRunning.Store(true)
	m.Run()
	for _, err := range removeOwned() {
		_, _ = fmt.Fprintf(os.Stderr, "clirun: %v; the next build in any process removes it\n", err)
	}
}

// removeOwned releases and removes every directory this process created, and
// returns what it could not remove.
func removeOwned() []error {
	owned.Lock()
	defer owned.Unlock()
	var failed []error
	for _, dir := range owned.dirs {
		// The owner file is closed first: Windows refuses to remove a file
		// that is open, and closing it releases the lock a sweep reads.
		if err := dir.owner.Close(); err != nil {
			failed = append(failed, fmt.Errorf("release %s: %w", dir.path, err))
		}
		if err := os.RemoveAll(dir.path); err != nil {
			failed = append(failed, fmt.Errorf("remove %s: %w", dir.path, err))
		}
	}
	owned.dirs = nil
	return failed
}

// Build compiles the target once per test binary and returns its path.
//
// Every caller of the same target in the same process gets the same file, and
// parallel callers wait for the one compilation rather than starting their own.
// The failure is reported through the checker rather than returned, because a
// test that cannot build the program under test has nothing left to measure.
//
// Build refuses a test binary that [Main] does not run, because nothing would
// remove the program. Its first compilation in a process also sweeps the temp
// directory: a directory whose owner file no running process holds a lock on
// was left by a test binary that died before Main returned, and is removed. The
// lock rather than a process id is what says a process still runs, because an
// id is reused, and a test in another container sharing the temp directory has
// an id this process cannot see.
func Build(c *qt.C, target Target) string {
	c.Helper()
	c.Assert(mainRunning.Load(), qt.IsTrue, qt.Commentf(
		"clirun.Build needs clirun.Main: call it from the package's TestMain, "+
			"or the %s binary stays in the temp directory after the tests end", target))

	memo, _ := built.LoadOrStore(target, sync.OnceValue(func() buildResult {
		return compile(target)
	}))
	result := memo.(func() buildResult)()

	c.Assert(result.err, qt.IsNil, qt.Commentf("build %s", target))
	return result.path
}

func compile(target Target) buildResult {
	sweepOnce.Do(func() { sweep(os.TempDir(), time.Now()) })

	dir, err := os.MkdirTemp("", dirPattern)
	if err != nil {
		return buildResult{err: err}
	}
	owner, err := claim(dir)
	if err != nil {
		return buildResult{err: errors.Join(err, os.RemoveAll(dir))}
	}
	owned.Lock()
	owned.dirs = append(owned.dirs, ownedDir{path: dir, owner: owner})
	owned.Unlock()
	path := filepath.Join(dir, filepath.Base(string(target))+exeext.Suffix)

	// The build runs from the repository so `go build` resolves the module
	// without depending on which package's directory the test happens to sit
	// in.
	cmd := exec.Command("go", "build", "-o", path, string(target))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return buildResult{err: errors.New(string(output))}
	}
	return buildResult{path: path}
}

// claim creates the owner file in dir and locks it for the life of the
// process. The lock goes when the process does, however it ends, which is what
// lets a sweep tell a live directory from a stale one.
func claim(dir string) (*os.File, error) {
	owner, err := os.OpenFile(filepath.Join(dir, ownerName), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create the owner file of %s: %w", dir, err)
	}
	locked, err := tryLock(owner)
	if err == nil && !locked {
		err = errors.New("another process holds it")
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("lock the owner file of %s: %w", dir, err), owner.Close())
	}
	return owner, nil
}

// sweep removes each compilation directory under tempDir that no running
// process owns. A removal that fails, as it does on Windows while a program in
// the directory still runs, is left for the next sweep.
func sweep(tempDir string, now time.Time) {
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		return
	}
	prefix := strings.TrimSuffix(dirPattern, "*")
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		dir := filepath.Join(tempDir, entry.Name())
		if stale(dir, now) {
			_ = os.RemoveAll(dir)
		}
	}
}

// stale reports whether no running process owns dir: nobody holds the lock on
// its owner file, or it has no owner file and is older than [unownedAge].
// Anything it cannot read counts as owned.
func stale(dir string, now time.Time) bool {
	owner, err := os.OpenFile(filepath.Join(dir, ownerName), os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		info, err := os.Stat(dir)
		return err == nil && now.Sub(info.ModTime()) > unownedAge
	}
	if err != nil {
		return false
	}
	locked, err := tryLock(owner)
	// Closing releases the lock this sweep may have taken; the directory is
	// removed after it, since Windows refuses to remove an open file.
	closeErr := owner.Close()
	return err == nil && locked && closeErr == nil
}

// Run executes the built target and returns what it produced.
//
// A non-zero exit is a result, not a failure: refusing badly is behavior Ptah
// promises, and most of what an end-to-end test measures about a diagnostic is
// only reachable through a command that exited non-zero.
func Run(c *qt.C, target Target, opts Options, args ...string) Result {
	c.Helper()

	path := Build(c, target)

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(c.Context(), timeout)
	defer cancel()

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Dir = opts.Dir
	cmd.Env = opts.Env
	cmd.Stdin = bytes.NewBufferString(opts.Stdin)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	c.Assert(ctx.Err(), qt.IsNil,
		qt.Commentf("%s %v did not finish within %s\nstdout:\n%s\nstderr:\n%s",
			filepath.Base(path), args, timeout, stdout.String(), stderr.String()))

	return Result{Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: exitCode(c, err)}
}

// exitCode reads the status out of what exec returned.
//
// Anything that is not an ExitError means the program did not run at all -- a
// missing file, a permission, a working directory that does not exist -- and
// reporting that as an exit code would let a test assert on a number no program
// produced.
func exitCode(c *qt.C, err error) int {
	c.Helper()
	if err == nil {
		return 0
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return exit.ExitCode()
	}
	c.Fatalf("the command did not run: %v", err)
	return 0
}
