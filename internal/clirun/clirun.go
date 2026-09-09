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
package clirun

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
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
// has no single owner to hang the removal on. It is a few megabytes under the
// system temp directory, and a test binary's process is short-lived.
var built sync.Map

type buildResult struct {
	path string
	err  error
}

// Build compiles the target once per test binary and returns its path.
//
// Every caller of the same target in the same process gets the same file, and
// parallel callers wait for the one compilation rather than starting their own.
// The failure is reported through the checker rather than returned, because a
// test that cannot build the program under test has nothing left to measure.
func Build(c *qt.C, target Target) string {
	c.Helper()

	memo, _ := built.LoadOrStore(target, sync.OnceValue(func() buildResult {
		return compile(target)
	}))
	result := memo.(func() buildResult)()

	c.Assert(result.err, qt.IsNil, qt.Commentf("build %s", target))
	return result.path
}

func compile(target Target) buildResult {
	dir, err := os.MkdirTemp("", "ptah-clirun-*")
	if err != nil {
		return buildResult{err: err}
	}
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
