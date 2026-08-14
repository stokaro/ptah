package main_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// These tests build the ptah-ls binary at run time and exercise it as a
// subprocess, which is the only way to assert real process exit codes and the
// only way to observe the defect this file exists for: ptah-ls is a stdio
// language server, so "answered a version query" and "started serving and then
// saw EOF" are indistinguishable from inside the process.
//
// Caution when iterating on behavior these tests pin: the Go test cache keys on
// this package's own inputs, so an edit under cmd/internal/buildinfo or
// internal/ptahls does not invalidate a cached PASS here even though it changes
// the binary being built. Run `go test ./cmd/ptah-ls/... -count=1` after
// touching either, or a mutation you expect to fail will silently report a
// stale PASS.

// versionBlockPattern is the shape of buildinfo.Write's output. It pins line
// prefixes and never a literal version, because the Version line differs
// between a stamped release build and a `go build` from a checkout.
const versionBlockPattern = `Version: [^\n]+\n` +
	`Commit: [^\n]+\n` +
	`Date: [^\n]+\n` +
	`Go: [^\n]+\n` +
	`Platform: [^\n]+\n`

// commandBudget bounds every row. Before stokaro/ptah#1064, `ptah-ls version`
// dropped the positional and fell through to the server loop: with stdin at
// EOF that was a silent exit 0, and with stdin held open it never returned at
// all. The budget is what turns the second case into a failing test instead of
// a hung run.
const commandBudget = 20 * time.Second

// TestPtahLSArgumentHandling pins the whole argv surface of ptah-ls in one
// table (stokaro/ptah#1064).
//
// Every row asserts on stdout BYTES, never on the exit code alone. That is not
// belt and braces: measured on the pre-fix binary, `ptah-ls version` exits 0
// having written nothing, so an exit-status assertion -- the check a health
// probe or an install smoke test performs -- passes on the broken binary and
// proves exactly nothing. Only the bytes separate "answered" from "said
// nothing and exited".
func TestPtahLSArgumentHandling(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahLSBinary(c)

	tests := []struct {
		name string
		args []string
		// stdin supplies the child's standard input. Rows returning nil get
		// exec.Cmd's default, which is /dev/null: an immediate EOF.
		stdin    func(c *qt.C) io.Reader
		wantExit int
		assert   func(c *qt.C, stdout, stderr string)
	}{
		{
			// The defect. Pre-fix: exit 0, stdout 0 bytes.
			name:     "version positional answers",
			args:     []string{"version"},
			stdin:    noStdin,
			wantExit: 0,
			assert:   assertVersionBlock,
		},
		{
			// Passes pre-fix too; present so both spellings are pinned to the
			// same shape, and so TestPtahLSVersionSpellingsPrintIdenticalBytes
			// below has a stated companion.
			name:     "version flag answers",
			args:     []string{"--version"},
			stdin:    noStdin,
			wantExit: 0,
			assert:   assertVersionBlock,
		},
		{
			// The same defect seen from its worse side. Pre-fix this row does
			// not fail, it hangs: the process is still blocked in the server's
			// read loop when the budget expires.
			name:     "version positional answers with stdin held open",
			args:     []string{"version"},
			stdin:    heldOpenStdin,
			wantExit: 0,
			assert:   assertVersionBlock,
		},
		{
			// Pre-fix: exit 0, stdout 0 bytes -- an unannounced launch of the
			// language server, not a rejected command.
			name:     "unknown positional is a usage error",
			args:     []string{"definitely-not-a-command"},
			stdin:    noStdin,
			wantExit: 2,
			assert: assertUsageError(
				`ptah-ls: unexpected positional arguments \["definitely-not-a-command"\]\n`),
		},
		{
			// The surplus starts AFTER the version query. Naming args[0] here
			// would report `version` -- the one word that is supported -- as
			// the unknown argument, and send a reader looking in the wrong
			// place. The native binary words the same case the same way.
			name:     "surplus after the version subcommand names the surplus",
			args:     []string{"version", "extra"},
			stdin:    noStdin,
			wantExit: 2,
			assert: assertUsageError(
				`ptah-ls: unexpected positional arguments \["extra"\]\n`),
		},
		{
			// A leftover positional is rejected even when the version flag
			// already answered the question: silently discarding argv is the
			// defect, and it is not less of one on the path that happens to
			// print something.
			name:     "unknown positional after the version flag is a usage error",
			args:     []string{"--version", "definitely-not-a-command"},
			stdin:    noStdin,
			wantExit: 2,
			assert: assertUsageError(
				`ptah-ls: unexpected positional arguments \["definitely-not-a-command"\]\n`),
		},
		{
			// Unchanged by this fix, and the reason the positional rejection
			// above exits 2: flag.ExitOnError already owns that code, so the
			// binary has one usage-error status rather than one per site.
			name:     "unknown flag is a usage error",
			args:     []string{"--bogus-flag"},
			stdin:    noStdin,
			wantExit: 2,
			assert: assertUsageError(
				`flag provided but not defined: -bogus-flag\n`),
		},
		{
			// The server path, untouched. Writing nothing is correct here --
			// stdin was already at EOF, so there was no session to serve. It
			// is the identical observable that made the `version` row above a
			// silent success, which is why exit status cannot discriminate.
			name:     "no arguments still serves",
			args:     nil,
			stdin:    noStdin,
			wantExit: 0,
			assert:   assertSilent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), commandBudget)
			defer cancel()

			run := newPtahLSProcess(ctx, binPath, tt.args...)
			run.Stdin = tt.stdin(c)
			var stdout, stderr bytes.Buffer
			run.Stdout = &stdout
			run.Stderr = &stderr

			runErr := run.Run()

			c.Assert(run.ProcessState, qt.IsNotNil, qt.Commentf("run error: %v", runErr))
			c.Assert(ctx.Err(), qt.IsNil,
				qt.Commentf("process did not exit within %s", commandBudget))
			c.Assert(run.ProcessState.ExitCode(), qt.Equals, tt.wantExit,
				qt.Commentf("stdout: %q stderr: %q", stdout.String(), stderr.String()))
			tt.assert(c, stdout.String(), stderr.String())
		})
	}
}

// TestPtahLSVersionSpellingsPrintIdenticalBytes pins the two spellings to each
// other rather than to a literal, so the format has to drift on both at once
// or not at all.
func TestPtahLSVersionSpellingsPrintIdenticalBytes(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahLSBinary(c)

	fromCommand := captureStdout(c, binPath, "version")
	fromFlag := captureStdout(c, binPath, "--version")

	c.Assert(fromCommand, qt.Matches, versionBlockPattern)
	c.Assert(fromCommand, qt.Equals, fromFlag)
}

func assertVersionBlock(c *qt.C, stdout, stderr string) {
	c.Helper()
	c.Assert(stdout, qt.Matches, versionBlockPattern)
	c.Assert(stderr, qt.Equals, "")
}

func assertSilent(c *qt.C, stdout, stderr string) {
	c.Helper()
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
}

// assertUsageError pins the diagnostic line this binary owns byte-exactly and
// only requires that the flag package's usage block follows it, so a stdlib
// change to PrintDefaults' spacing is not a failure of this fix.
func assertUsageError(wantFirstLine string) func(c *qt.C, stdout, stderr string) {
	return func(c *qt.C, stdout, stderr string) {
		c.Helper()
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Matches, `(?s)`+wantFirstLine+`Usage of ptah-ls:\n.*`)
	}
}

// noStdin gives the child exec.Cmd's default standard input, /dev/null.
func noStdin(*qt.C) io.Reader { return nil }

// heldOpenStdin gives the child a pipe whose write end stays open for the
// lifetime of the test, so the child never observes EOF. A version query must
// answer and exit regardless; the pre-fix binary blocks here forever.
func heldOpenStdin(c *qt.C) io.Reader {
	c.Helper()
	reader, writer, err := os.Pipe()
	c.Assert(err, qt.IsNil)
	// An *os.File is handed to the child directly, so os/exec starts no copying
	// goroutine and Wait cannot block on this reader after the child exits.
	c.Cleanup(func() {
		writer.Close()
		reader.Close()
	})
	return reader
}

func captureStdout(c *qt.C, binPath string, args ...string) string {
	c.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), commandBudget)
	c.Cleanup(cancel)

	run := newPtahLSProcess(ctx, binPath, args...)
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	c.Assert(run.Run(), qt.IsNil, qt.Commentf("stderr: %s", stderr.String()))
	return stdout.String()
}

// newPtahLSProcess builds the child command. Every caller passes a context so
// that no row of this file can hang the package: the pre-fix binary blocks in
// the language server's read loop rather than answering.
func newPtahLSProcess(ctx context.Context, binPath string, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, binPath, args...)
}

func buildPtahLSBinary(c *qt.C) string {
	c.Helper()
	binPath := filepath.Join(c.TempDir(), "ptah-ls")
	build := exec.Command("go", "build", "-o", binPath, ".")
	build.Env = append(os.Environ(), "GOWORK=off")
	buildOut, err := build.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", buildOut))
	return binPath
}
