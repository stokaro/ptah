package db

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestNewDBCommand_RegistersNativePaths(t *testing.T) {
	c := qt.New(t)

	cmd := NewDBCommand()
	for _, path := range [][]string{
		{"read"},
		{"drop-all"},
	} {
		found, _, err := cmd.Find(path)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsNotNil)
	}
}

func TestNewDBCommand_HelpShowsNativeBoundary(t *testing.T) {
	c := qt.New(t)

	cmd := NewDBCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Ptah's native live-database namespace")
	c.Assert(out.String(), qt.Contains, "read")
	c.Assert(out.String(), qt.Contains, "drop-all")
}

func TestNewDBCommand_RejectsUnknownPositionalCommand(t *testing.T) {
	c := qt.New(t)

	cmd := NewDBCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"inspect"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unexpected positional arguments \["inspect"\]`)
	c.Assert(stderr.String(), qt.Contains, `unexpected positional arguments ["inspect"]`)
}

func TestNewDBCommand_ReadHelpShowsTargetFlags(t *testing.T) {
	c := qt.New(t)

	cmd := NewDBCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"read", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Usage:\n  db read [flags]")
	c.Assert(out.String(), qt.Not(qt.Contains), "Usage:\n  read-db")
	c.Assert(out.String(), qt.Contains, "--db-url")
}

func TestNewDBCommand_ForwardsReadFlagErrors(t *testing.T) {
	c := qt.New(t)

	cmd := NewDBCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"read", "--bogus-flag"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "unknown flag: --bogus-flag")
}
