package atlas_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/atlas"
)

func TestNewAtlasCommand_UsageMatchesAtlasCE(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name      string
		args      []string
		wantUsage string
	}{
		{
			name:      "root",
			args:      []string{"atlas", "--help"},
			wantUsage: "Usage:\n  ptah atlas [command]",
		},
		{
			name:      "migrate",
			args:      []string{"atlas", "migrate", "--help"},
			wantUsage: "Usage:\n  ptah atlas migrate [command]",
		},
		{
			name:      "schema",
			args:      []string{"atlas", "schema", "--help"},
			wantUsage: "Usage:\n  ptah atlas schema [command]",
		},
		{
			name:      "migrate_apply",
			args:      []string{"atlas", "migrate", "apply", "--help"},
			wantUsage: "Usage:\n  ptah atlas migrate apply [flags] [amount]",
		},
		{
			name:      "migrate_diff",
			args:      []string{"atlas", "migrate", "diff", "--help"},
			wantUsage: "Usage:\n  ptah atlas migrate diff [flags] [name]",
		},
		{
			name:      "migrate_new",
			args:      []string{"atlas", "migrate", "new", "--help"},
			wantUsage: "Usage:\n  ptah atlas migrate new [flags] [name]",
		},
		{
			name:      "migrate_set",
			args:      []string{"atlas", "migrate", "set", "--help"},
			wantUsage: "Usage:\n  ptah atlas migrate set [flags] [version]",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			cmd := &cobra.Command{Use: "ptah"}
			cmd.AddCommand(atlas.NewAtlasCommand())

			out, err := executeAtlasUsageTestCommand(cmd, tt.args)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, tt.wantUsage)
			c.Assert(out, qt.Not(qt.Contains), "Usage:\n  atlas [flags]")
		})
	}
}

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

func executeAtlasUsageTestCommand(cmd *cobra.Command, args []string) (string, error) {
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}
