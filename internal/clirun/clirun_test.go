package clirun_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// TestRun_HappyPath drives the real binary and reads back each stream on its
// own.
//
// `version` is the cheapest command that proves the whole path: the binary
// built, it ran, it wrote where it says it writes, and it exited 0.
func TestRun_HappyPath(t *testing.T) {
	c := qt.New(t)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{}, "version")

	c.Assert(got.ExitCode, qt.Equals, 0)
	c.Assert(got.Stdout, qt.Contains, "Version:")
	c.Assert(got.Stdout, qt.Contains, "Platform:")
	c.Assert(got.Stderr, qt.Equals, "")
}

// TestRun_FailurePath is the half most end-to-end assertions need.
//
// A refusal is behavior Ptah promises, so a harness that failed the test on a
// non-zero exit would make the promise unmeasurable. The diagnostic belongs on
// stderr, and keeping the streams apart is what lets a test say so.
func TestRun_FailurePath(t *testing.T) {
	c := qt.New(t)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{}, "no-such-command")

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(got.Stderr, qt.Not(qt.Equals), "")
	c.Assert(got.Stdout, qt.Equals, "")
}

// TestRun_RunsInTheDirectoryItIsGiven pins the option an end-to-end test needs
// most.
//
// These commands read and write files relative to where they run, so a harness
// that ignored Dir would measure the test process's directory and quietly agree
// with whatever was already there.
func TestRun_RunsInTheDirectoryItIsGiven(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "ls", "--dir", ".")

	c.Assert(got.ExitCode, qt.Not(qt.Equals), -1)
	c.Assert(got.Stdout+got.Stderr, qt.Not(qt.Contains), filepath.Dir(dir)+"/..")
}

// TestBuild_CompilesOncePerTarget is the property that makes a new end-to-end
// test cheap.
//
// Five copies of the old helper each rebuilt per test. Asserting on the path is
// how the memo is observable at all: a second compilation would land in a
// second temp directory.
func TestBuild_CompilesOncePerTarget(t *testing.T) {
	c := qt.New(t)

	first := clirun.Build(c, clirun.Ptah)
	second := clirun.Build(c, clirun.Ptah)

	c.Assert(second, qt.Equals, first)
	c.Assert(first, qt.Not(qt.Equals), "")
}

// TestBuild_KeepsTargetsApart is the control for the row above.
//
// A memo keyed on nothing would hand every caller the first binary it built,
// and a test asking for the compatibility surface would measure the native one
// while reading as if it had not.
func TestBuild_KeepsTargetsApart(t *testing.T) {
	c := qt.New(t)

	native := clirun.Build(c, clirun.Ptah)
	compat := clirun.Build(c, clirun.Compat)

	c.Assert(compat, qt.Not(qt.Equals), native)
}
