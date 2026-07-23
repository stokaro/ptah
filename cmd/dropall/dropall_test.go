package dropall_test

import (
	"bytes"
	"net/url"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/pflag"

	"github.com/stokaro/ptah/cmd/dropall"
)

func TestDropAllCommandDeclinedConfirmationPrintsCanceled(t *testing.T) {
	c := qt.New(t)

	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	cmd := dropall.NewDropAllCommand()
	resetDropAllCommandForTest(c, cmd)
	cmd.SetArgs([]string{"--db-url", dbURL})
	cmd.SetIn(bytes.NewBufferString("no\n"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	err := cmd.Execute()
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Operation canceled.")
	resetDropAllCommandForTest(c, cmd)
}

func TestDropAllCommandAutoApproveSkipsConfirmation(t *testing.T) {
	c := qt.New(t)

	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	cmd := dropall.NewDropAllCommand()
	resetDropAllCommandForTest(c, cmd)
	cmd.SetArgs([]string{"--db-url", dbURL, "--auto-approve"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	err := cmd.Execute()
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Auto-approval enabled; skipping interactive confirmation.")
	c.Assert(out.String(), qt.Not(qt.Contains), "Type 'DELETE EVERYTHING'")
	resetDropAllCommandForTest(c, cmd)
}

func TestDropAllCommandAcceptsTwoLineConfirmationFromCobraInput(t *testing.T) {
	c := qt.New(t)

	dbURL := (&url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "ptah.db")}).String()
	cmd := dropall.NewDropAllCommand()
	resetDropAllCommandForTest(c, cmd)
	cmd.SetArgs([]string{"--db-url", dbURL})
	cmd.SetIn(bytes.NewBufferString("DELETE EVERYTHING\nYES I AM SURE\n"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)

	err := cmd.Execute()
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "All tables and enums dropped successfully!")
	resetDropAllCommandForTest(c, cmd)
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
