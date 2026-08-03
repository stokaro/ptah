package atlas_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

func TestNewCompatCommandNamedAtlas_UsageMatchesAtlasCE(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		args      []string
		wantUsage string
	}{
		{
			name:      "root",
			args:      []string{"--help"},
			wantUsage: "Usage:\n  atlas [command]",
		},
		{
			name:      "migrate",
			args:      []string{"migrate", "--help"},
			wantUsage: "Usage:\n  atlas migrate [command]",
		},
		{
			name:      "schema",
			args:      []string{"schema", "--help"},
			wantUsage: "Usage:\n  atlas schema [command]",
		},
		{
			name:      "migrate_apply",
			args:      []string{"migrate", "apply", "--help"},
			wantUsage: "Usage:\n  atlas migrate apply [flags] [amount]",
		},
		{
			name:      "migrate_diff",
			args:      []string{"migrate", "diff", "--help"},
			wantUsage: "Usage:\n  atlas migrate diff [flags] [name]",
		},
		{
			name:      "migrate_new",
			args:      []string{"migrate", "new", "--help"},
			wantUsage: "Usage:\n  atlas migrate new [flags] [name]",
		},
		{
			name:      "migrate_set",
			args:      []string{"migrate", "set", "--help"},
			wantUsage: "Usage:\n  atlas migrate set [flags] [version]",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			out, err := executeAtlasUsageTestCommand(atlas.NewCompatCommand("atlas"), tt.args)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, tt.wantUsage)
			c.Assert(out, qt.Not(qt.Contains), "Usage:\n  atlas [flags]")
		})
	}
}

func TestCompatCommand_ForwardedNativeFailureExits1(t *testing.T) {
	c := qt.New(t)
	missingDir := filepath.Join(t.TempDir(), "missing")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "hash", "--dir", "file://" + missingDir})

	err := executeAtlasTestCommand(cmd)

	c.Assert(err, qt.ErrorMatches, "migrations directory .*")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	// The message is printed by the native implementation, which the adapter
	// runs detached from this tree, so it cannot reach the compat prefix by
	// walking parents. It still has to answer with it: per stokaro/ptah#1019
	// the prefix belongs to the surface the user invoked, not to whichever
	// package happens to own the failing code.
	c.Assert(out.String(), qt.Contains, "Error: migrations directory")
	c.Assert(out.String(), qt.Not(qt.Contains), "error: migrations directory")
}

func TestAtlasCompatibilityRoots_UnknownCommandMatchesAtlasCE(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		cmd  *cobra.Command
		args []string
	}{
		{
			name: "compatibility binary",
			cmd:  atlas.NewCompatCommand("ptah-compat"),
			args: []string{"definitely-not-a-command", "ignored-extra-token"},
		},
		{
			name: "compatibility binary named atlas",
			cmd:  atlas.NewCompatCommand("atlas"),
			args: []string{"definitely-not-a-command", "ignored-extra-token"},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			var stdout, stderr bytes.Buffer
			tt.cmd.SetOut(&stdout)
			tt.cmd.SetErr(&stderr)
			tt.cmd.SetArgs(tt.args)

			err := executeAtlasTestCommand(tt.cmd)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(err, qt.ErrorMatches, `unknown command "definitely-not-a-command" for "atlas"`)
			c.Assert(stdout.String(), qt.Equals, "")
			c.Assert(stderr.String(), qt.Equals,
				"Error: unknown command \"definitely-not-a-command\" for \"atlas\"\n"+
					"Run 'atlas --help' for usage.\n")
		})
	}
}

func TestAtlasCompatibilityRoot_UnknownCommandQuotesSafely(t *testing.T) {
	c := qt.New(t)
	cmd := atlas.NewCompatCommand("ptah-compat")
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"bad\ncommand"})

	err := executeAtlasTestCommand(cmd)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stdout.String(), qt.Equals, "")
	c.Assert(stderr.String(), qt.Equals,
		"Error: unknown command \"bad\\ncommand\" for \"atlas\"\n"+
			"Run 'atlas --help' for usage.\n")
}

func TestAtlasCompatibilityRoot_UnknownCommandSuggestsAtlasVerb(t *testing.T) {
	c := qt.New(t)
	cmd := atlas.NewCompatCommand("ptah-compat")
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"migrat"})

	err := executeAtlasTestCommand(cmd)

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(stdout.String(), qt.Equals, "")
	c.Assert(stderr.String(), qt.Equals,
		"Error: unknown command \"migrat\" for \"atlas\"\n\n"+
			"Did you mean this?\n"+
			"\tmigrate\n\n"+
			"Run 'atlas --help' for usage.\n")
}

func TestAtlasCompatibilityDiagnostics_WriterFailure(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "root command",
			args:    []string{"unknown"},
			wantErr: `unknown command "unknown" for "atlas": write diagnostic: write failed`,
		},
		{
			name:    "completion shell",
			args:    []string{"completion", "bash", "extra"},
			wantErr: `unknown command "extra" for "atlas completion bash": write diagnostic: write failed`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			cmd := atlas.NewCompatCommand("atlas")
			var stdout bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(atlasFailingWriter{})
			cmd.SetArgs(tt.args)

			err := executeAtlasTestCommand(cmd)

			c.Assert(err, qt.ErrorIs, errAtlasWriteFailed)
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(stdout.String(), qt.Equals, "")
		})
	}
}

func TestAtlasCompatibilityGroups_ExtraTokenMatchesAtlasCEHelp(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		args      []string
		wantUsage string
	}{
		{
			name:      "migrate",
			args:      []string{"migrate", "aplly"},
			wantUsage: "Usage:\n  atlas migrate [command]",
		},
		{
			name:      "schema",
			args:      []string{"schema", "definitely-not-a-command"},
			wantUsage: "Usage:\n  atlas schema [command]",
		},
		{
			name:      "completion",
			args:      []string{"completion", "sh"},
			wantUsage: "Usage:\n  atlas completion [command]",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			cmd := atlas.NewCompatCommand("atlas")
			var stdout, stderr bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(&stderr)
			cmd.SetArgs(tt.args)

			err := executeAtlasTestCommand(cmd)

			c.Assert(err, qt.IsNil)
			c.Assert(stdout.String(), qt.Contains, tt.wantUsage)
			c.Assert(stderr.String(), qt.Equals, "")
		})
	}
}

func TestAtlasCompatibilityGroups_HelpWriterFailure(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		args []string
	}{
		{
			name: "migrate",
			args: []string{"migrate", "aplly"},
		},
		{
			name: "schema",
			args: []string{"schema", "unknown"},
		},
		{
			name: "completion",
			args: []string{"completion", "sh"},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			cmd := atlas.NewCompatCommand("atlas")
			var stderr bytes.Buffer
			cmd.SetOut(atlasFailingWriter{})
			cmd.SetErr(&stderr)
			cmd.SetArgs(tt.args)

			err := executeAtlasTestCommand(cmd)

			c.Assert(err, qt.ErrorIs, errAtlasWriteFailed)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(stderr.String(), qt.Equals, "")
		})
	}
}

func TestAtlasCompatibilityCompletion_ExtraTokenMatchesAtlasCE(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		cmd  *cobra.Command
		args []string
	}{
		{
			name: "compatibility binary",
			cmd:  atlas.NewCompatCommand("ptah-compat"),
			args: []string{"completion", "bash", "extra"},
		},
		{
			name: "compatibility binary named atlas",
			cmd:  atlas.NewCompatCommand("atlas"),
			args: []string{"completion", "bash", "extra"},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			var stdout, stderr bytes.Buffer
			tt.cmd.SetOut(&stdout)
			tt.cmd.SetErr(&stderr)
			tt.cmd.SetArgs(tt.args)

			err := executeAtlasTestCommand(tt.cmd)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(err, qt.ErrorMatches, `unknown command "extra" for "atlas completion bash"`)
			c.Assert(stdout.String(), qt.Equals, "")
			c.Assert(stderr.String(), qt.Equals,
				"Error: unknown command \"extra\" for \"atlas completion bash\"\n")
		})
	}
}

func executeAtlasUsageTestCommand(cmd *cobra.Command, args []string) (string, error) {
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := executeAtlasTestCommand(cmd)
	return out.String(), err
}

func executeAtlasTestCommand(cmd *cobra.Command) error {
	executed, err := cmd.ExecuteC()
	return cmdutil.NormalizeCommandError(executed, err, 2)
}

type atlasFailingWriter struct{}

var errAtlasWriteFailed = errors.New("write failed")

func (atlasFailingWriter) Write(_ []byte) (int, error) {
	return 0, errAtlasWriteFailed
}
