package migrations_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migrations"
)

func TestNewMigrationsCommand_RegistersNativePaths(t *testing.T) {
	c := qt.New(t)

	cmd := migrations.NewMigrationsCommand()
	for _, path := range [][]string{
		{"plan"},
		{"generate"},
		{"create"},
		{"up"},
		{"down"},
		{"status"},
		{"baseline"},
		{"repair"},
		{"hash"},
		{"validate"},
		{"lint"},
	} {
		found, _, err := cmd.Find(path)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsNotNil)
	}
}

func TestNewMigrationsCommand_HelpShowsNativeBoundary(t *testing.T) {
	c := qt.New(t)

	cmd := migrations.NewMigrationsCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Ptah's native migration namespace")
	c.Assert(out.String(), qt.Contains, "plan")
	c.Assert(out.String(), qt.Contains, "up")
}

func TestNewMigrationsCommand_RejectsUnknownPositionalCommand(t *testing.T) {
	c := qt.New(t)

	cmd := migrations.NewMigrationsCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"apply"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unexpected positional arguments \["apply"\]`)
	c.Assert(stderr.String(), qt.Contains, `unexpected positional arguments ["apply"]`)
}

func TestNewMigrationsCommand_ForwardsCreateHelpToMigrateNew(t *testing.T) {
	c := qt.New(t)

	cmd := migrations.NewMigrationsCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"create", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Usage:\n  migrations create [name] [flags]")
	c.Assert(out.String(), qt.Not(qt.Contains), "Usage:\n  new")
	c.Assert(out.String(), qt.Contains, "--migrations-dir")
	c.Assert(out.String(), qt.Contains, "--name")
}

func TestNewMigrationsCommand_UpHelpShowsTargetFlags(t *testing.T) {
	c := qt.New(t)

	cmd := migrations.NewMigrationsCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"up", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Usage:\n  migrations up [flags]")
	c.Assert(out.String(), qt.Not(qt.Contains), "Usage:\n  migrate-up")
	c.Assert(out.String(), qt.Contains, "--db-url")
	c.Assert(out.String(), qt.Contains, "--migrations-dir")
}

func TestNewMigrationsCommand_ForwardsUpFlagErrors(t *testing.T) {
	c := qt.New(t)

	cmd := migrations.NewMigrationsCommand()
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"up", "--bogus-flag"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "unknown flag: --bogus-flag")
}
