package main

// White-box testing required: run's exit codes are this gate's entire contract
// with CI, and run is unexported in package main, which no external test package
// can reach. A gate whose failure exit code has never been executed is a gate
// nobody has watched fail, and this repository has shipped two checks that
// reported success while looking at nothing.

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

const conformingSource = `package fixture

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestConforming(t *testing.T) {
	c := qt.New(t)
	c.Assert(1, qt.Equals, 1)

	t.Run("subtest", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(2, qt.Equals, 2)
	})
}
`

const violatingSource = `package fixture

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestViolating(t *testing.T) {
	qt.Assert(t, 1, qt.Equals, 1)

	c := qt.New(t)
	c.Run("subtest", func(c *qt.C) {
		c.Assert(2, qt.Equals, 2)
	})
}
`

func TestRunReportsTheRightExitCode(t *testing.T) {
	tests := []struct {
		name       string
		stdin      func(t *testing.T) string
		want       int
		wantStdout func(c *qt.C, stdout string)
	}{
		{
			name:  "the target shape exits 0 and prints nothing",
			stdin: func(t *testing.T) string { return writeFixture(t, "conforming_test.go", conformingSource) },
			want:  exitOK,
			wantStdout: func(c *qt.C, stdout string) {
				c.Check(stdout, qt.Equals, "")
			},
		},
		{
			name:  "both violations exit 1 and are named with their rule",
			stdin: func(t *testing.T) string { return writeFixture(t, "violating_test.go", violatingSource) },
			want:  exitFindings,
			wantStdout: func(c *qt.C, stdout string) {
				c.Check(stdout, qt.Contains, ": R1: ")
				c.Check(stdout, qt.Contains, ": R2: ")
				c.Check(strings.Count(stdout, "\n"), qt.Equals, 2)
			},
		},
		{
			name:  "an empty selection exits 2 rather than reporting a clean tree",
			stdin: func(t *testing.T) string { return "" },
			want:  exitBroken,
			wantStdout: func(c *qt.C, stdout string) {
				c.Check(stdout, qt.Equals, "")
			},
		},
		{
			name: "a file that cannot be read exits 2",
			stdin: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "absent_test.go") + "\n"
			},
			want: exitBroken,
			wantStdout: func(c *qt.C, stdout string) {
				c.Check(stdout, qt.Equals, "")
			},
		},
		{
			name:  "a file that cannot be parsed exits 2 rather than being skipped",
			stdin: func(t *testing.T) string { return writeFixture(t, "broken_test.go", "package fixture\n\nfunc {\n") },
			want:  exitBroken,
			wantStdout: func(c *qt.C, stdout string) {
				c.Check(stdout, qt.Equals, "")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			var stdout, stderr bytes.Buffer
			got := run(strings.NewReader(test.stdin(t)), &stdout, &stderr)

			c.Assert(got, qt.Equals, test.want, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String()))
			test.wantStdout(c, stdout.String())
		})
	}
}

// TestRunReportsHowManyFilesItScanned pins the anti-vacuity counter. The count
// on stderr is the only thing that separates "clean" from "looked at almost
// nothing and called it clean", so it is asserted rather than merely printed.
func TestRunReportsHowManyFilesItScanned(t *testing.T) {
	c := qt.New(t)

	first := writeFixture(t, "one_test.go", conformingSource)
	second := writeFixture(t, "two_test.go", conformingSource)

	var stdout, stderr bytes.Buffer
	got := run(strings.NewReader(first+second), &stdout, &stderr)

	c.Assert(got, qt.Equals, exitOK, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Check(stderr.String(), qt.Contains, "qtshape: scanned 2 test files")
}

// writeFixture writes source into a fresh temporary directory and returns the
// path as one stdin line.
func writeFixture(t *testing.T, name, source string) string {
	t.Helper()
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)

	return path + "\n"
}
