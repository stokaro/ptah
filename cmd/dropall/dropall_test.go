package dropall

import (
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/pflag"
)

func TestDropAllCommandDeclinedConfirmationPrintsCanceled(t *testing.T) {
	c := qt.New(t)

	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	cmd := NewDropAllCommand()
	resetDropAllCommandForTest(c, cmd)
	cmd.SetArgs([]string{"--db-url", dbURL})

	out, err := captureStdIO(c, "no\n", cmd.Execute)
	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Operation canceled.")
	resetDropAllCommandForTest(c, cmd)
}

func TestDropAllCommandAutoApproveSkipsConfirmation(t *testing.T) {
	c := qt.New(t)

	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	cmd := NewDropAllCommand()
	resetDropAllCommandForTest(c, cmd)
	cmd.SetArgs([]string{"--db-url", dbURL, "--auto-approve"})

	out, err := captureStdIO(c, "", cmd.Execute)
	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "Auto-approval enabled; skipping interactive confirmation.")
	c.Assert(out, qt.Not(qt.Contains), "Type 'DELETE EVERYTHING'")
	resetDropAllCommandForTest(c, cmd)
}

func captureStdIO(c *qt.C, input string, run func() error) (string, error) {
	c.Helper()

	oldStdin := os.Stdin
	oldStdout := os.Stdout
	defer func() {
		os.Stdin = oldStdin
		os.Stdout = oldStdout
	}()

	inR, inW, err := os.Pipe()
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(inR.Close(), qt.IsNil) }()

	_, err = inW.WriteString(input)
	c.Assert(err, qt.IsNil)
	c.Assert(inW.Close(), qt.IsNil)

	outR, outW, err := os.Pipe()
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(outR.Close(), qt.IsNil) }()

	os.Stdin = inR
	os.Stdout = outW

	runErr := run()
	c.Assert(outW.Close(), qt.IsNil)

	output, err := io.ReadAll(outR)
	c.Assert(err, qt.IsNil)
	return string(output), runErr
}

func resetDropAllCommandForTest(c *qt.C, cmd interface{ Flag(string) *pflag.Flag }) {
	c.Helper()
	for name, value := range map[string]string{
		"db-url":          "",
		"dry-run":         "false",
		"auto-approve":    "false",
		"connect-timeout": "10s",
	} {
		flag := cmd.Flag(name)
		c.Assert(flag, qt.IsNotNil, qt.Commentf("flag %s", name))
		c.Assert(flag.Value.Set(value), qt.IsNil)
		flag.Changed = false
	}
}
