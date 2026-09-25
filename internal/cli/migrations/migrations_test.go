package migrations_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrations"
)

func TestNewMigrationsCommand_RegistersNativePaths(t *testing.T) {
	c := qt.New(t)

	cmd := migrations.NewMigrationsCommand()
	for _, path := range [][]string{
		{"plan"},
		{"generate"},
		{"create"},
		{"push"},
		{"pull"},
		{"up"},
		{"down"},
		{"status"},
		{"ls"},
		{"show"},
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

func TestNewMigrationsCommand_TestHelpShowsStepContract(t *testing.T) {
	c := qt.New(t)

	cmd := migrations.NewMigrationsCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"test", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "apply_schema")
	c.Assert(out.String(), qt.Contains, "--root-dir")
	c.Assert(out.String(), qt.Contains, "row_count")
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

// TestNewMigrationsCommand_DataHelpNamesItsGates holds `ptah migrations data
// --help` to the flags that refuse a change.
//
// The namespace once replaced the command's help with a summary saying it
// applied no gating of its own, while the command refused updates and deletes
// of existing rows without --allow-destructive and any change to a
// --protected-table without --allow-prod. Each gating flag has to exist on the
// command and be named in the help the command prints.
func TestNewMigrationsCommand_DataHelpNamesItsGates(t *testing.T) {
	for _, flag := range []string{"allow-destructive", "protected-table", "allow-prod"} {
		t.Run(flag, func(t *testing.T) {
			c := qt.New(t)
			data, _, err := migrations.NewMigrationsCommand().Find([]string{"data"})
			c.Assert(err, qt.IsNil)
			c.Assert(data.Flags().Lookup(flag), qt.IsNotNil)
			c.Assert(data.Long, qt.Contains, "--"+flag)
		})
	}
}

// TestNewMigrationsCommand_DataHelpDoesNotDenyItsGates is the other half: the
// sentence that denied the gates is gone from the help the command prints.
func TestNewMigrationsCommand_DataHelpDoesNotDenyItsGates(t *testing.T) {
	c := qt.New(t)
	data, _, err := migrations.NewMigrationsCommand().Find([]string{"data"})
	c.Assert(err, qt.IsNil)

	var help bytes.Buffer
	data.SetOut(&help)
	c.Assert(data.Help(), qt.IsNil)
	c.Assert(help.String(), qt.Not(qt.Contains), "no safety/risk gating")
	c.Assert(help.String(), qt.Contains, "--allow-destructive")
}
