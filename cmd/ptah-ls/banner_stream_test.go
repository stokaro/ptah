package main_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// wordmark is one line of the shared banner, enough to find it in a stream.
//
// Written out rather than imported: cmd/internal/banner is internal to a tree
// this package is outside of, and asking the package under test what it emits
// would pass whatever it emitted.
const wordmark = `|  ___/| |_ | (_| | | | |`

// TestPtahLSKeepsTheBannerOffBothStreams is the requirement that a language
// server never decorates its protocol.
//
// stdout here IS the protocol stream: a client reads framed JSON-RPC off it,
// and ASCII art in front of the first `Content-Length:` is a session that
// fails to initialize. stderr is where a client sends the server's log, so a
// banner there is noise in a place people go to read errors.
//
// A subprocess with piped streams is the only shape that measures this. In
// process, the banner's writer gate answers on whatever writer a test hands it
// and the question — what does a real client see — is not asked.
func TestPtahLSKeepsTheBannerOffBothStreams(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahLSBinary(c)
	ctx, cancel := context.WithTimeout(context.Background(), commandBudget)
	c.Cleanup(cancel)

	// stdin closed immediately, so the server starts, reads EOF, and returns.
	// That path runs every statement between argument handling and the read
	// loop, which is where the banner is written.
	run := newPtahLSProcess(ctx, binPath)
	run.Stdin = strings.NewReader("")
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	_ = run.Run()

	c.Assert(stdout.String(), qt.Not(qt.Contains), wordmark)
	c.Assert(stderr.String(), qt.Not(qt.Contains), wordmark)
	// The control. Both assertions above pass for a binary that failed to
	// build, failed to start, or exited before reaching the banner, and this
	// test would then report a guarantee it never measured. A version query on
	// the same binary proves it runs and writes to stdout.
	c.Assert(captureStdout(c, binPath, "version"), qt.Contains, "Version: ")
}

// TestPtahLSKeepsTheBannerOffAFileRedirect is the same guarantee for the shape
// an editor's launcher usually takes.
//
// A pipe and a file are different answers to "is this a terminal" only if the
// gate asks about the character device rather than about the type, and this is
// the case a gate stopping at *os.File would get wrong -- the client that
// redirects its server's stderr into a log file on disk.
func TestPtahLSKeepsTheBannerOffAFileRedirect(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahLSBinary(c)
	ctx, cancel := context.WithTimeout(context.Background(), commandBudget)
	c.Cleanup(cancel)

	logPath := c.TempDir() + string(os.PathSeparator) + "server.log"
	logFile, err := os.Create(logPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = logFile.Close() })

	run := newPtahLSProcess(ctx, binPath)
	run.Stdin = strings.NewReader("")
	run.Stdout = logFile
	run.Stderr = logFile

	_ = run.Run()
	c.Assert(logFile.Close(), qt.IsNil)

	// #nosec G304 -- the path is this test's own temporary directory.
	written, err := os.ReadFile(logPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Not(qt.Contains), wordmark)
}
