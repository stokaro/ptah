package atlas

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/migrateup"
)

func TestNewAtlasCommand_OSSCommandPathsResolve(t *testing.T) {
	paths := [][]string{
		{"version"},
		{"license"},
		{"schema", "inspect"},
		{"schema", "apply"},
		{"schema", "diff"},
		{"schema", "fmt"},
		{"schema", "clean"},
		{"migrate", "apply"},
		{"migrate", "diff"},
		{"migrate", "down"},
		{"migrate", "hash"},
		{"migrate", "import"},
		{"migrate", "lint"},
		{"migrate", "new"},
		{"migrate", "set"},
		{"migrate", "status"},
		{"migrate", "validate"},
	}

	for _, path := range paths {
		t.Run(strings.Join(path, "_"), func(t *testing.T) {
			c := qt.New(t)
			cmd := NewAtlasCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append(path, "--help"))

			err := cmd.Execute()

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Contains, "Usage:")
			c.Assert(out.String(), qt.Contains, "atlas "+strings.Join(path, " "))
		})
	}
}

func TestNewAtlasCommand_AdvertisesEssentialAtlasFlags(t *testing.T) {
	tests := []struct {
		name  string
		path  []string
		flags []string
	}{
		{
			name:  "schema_inspect",
			path:  []string{"schema", "inspect"},
			flags: []string{"--url", "--dev-url", "--schema", "--exclude", "--format"},
		},
		{
			name:  "schema_apply",
			path:  []string{"schema", "apply"},
			flags: []string{"--url", "--to", "--dev-url", "--dry-run", "--auto-approve"},
		},
		{
			name:  "schema_diff",
			path:  []string{"schema", "diff"},
			flags: []string{"--from", "--to", "--dev-url", "--format"},
		},
		{
			name:  "schema_clean",
			path:  []string{"schema", "clean"},
			flags: []string{"--url", "--dry-run", "--auto-approve"},
		},
		{
			name:  "migrate_diff",
			path:  []string{"migrate", "diff"},
			flags: []string{"--to", "--dev-url", "--dir", "--format"},
		},
		{
			name:  "migrate_apply",
			path:  []string{"migrate", "apply"},
			flags: []string{"--url", "--dir", "--dry-run", "--tx-mode"},
		},
		{
			name:  "migrate_lint",
			path:  []string{"migrate", "lint"},
			flags: []string{"--dev-url", "--dir", "--latest"},
		},
		{
			name:  "migrate_hash",
			path:  []string{"migrate", "hash"},
			flags: []string{"--dir"},
		},
		{
			name:  "migrate_status",
			path:  []string{"migrate", "status"},
			flags: []string{"--url", "--dir"},
		},
		{
			name:  "migrate_validate",
			path:  []string{"migrate", "validate"},
			flags: []string{"--dev-url", "--dir"},
		},
		{
			name:  "migrate_new",
			path:  []string{"migrate", "new"},
			flags: []string{"--dir"},
		},
		{
			name:  "migrate_set",
			path:  []string{"migrate", "set"},
			flags: []string{"--url", "--dir"},
		},
		{
			name:  "migrate_import",
			path:  []string{"migrate", "import"},
			flags: []string{"--from", "--to"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := NewAtlasCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append(tt.path, "--help"))

			err := cmd.Execute()

			c.Assert(err, qt.IsNil)
			for _, flag := range tt.flags {
				c.Assert(out.String(), qt.Contains, flag)
			}
		})
	}
}

func TestNewAtlasCommand_ForwardsSupportedCommands(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "apply"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "database URL is required")
}

func TestNewAtlasCommand_MapsAtlasFlagFormsToNativeFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "long_value",
			args: []string{"migrate", "apply", "--url", "postgres://localhost/db"},
		},
		{
			name: "long_equals_value",
			args: []string{"migrate", "apply", "--url=postgres://localhost/db"},
		},
		{
			name: "shorthand_value",
			args: []string{"migrate", "apply", "-u", "postgres://localhost/db"},
		},
		{
			name: "bool",
			args: []string{"migrate", "apply", "--url", "postgres://localhost/db", "--dry-run"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := NewAtlasCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(tt.args)

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, "migrations directory is required")
		})
	}
}

func TestNewAtlasCommand_UnsupportedAtlasFlagsFailExplicitly(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "inspect", "--url", "postgres://localhost/db", "--format", "{{ sql . }}"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "atlas schema inspect accepts --format, but Ptah does not implement its behavior yet")
}

func TestNewAtlasCommand_ForwardsParentedNativeCommand(t *testing.T) {
	c := qt.New(t)
	root := &cobra.Command{Use: "ptah"}
	root.AddCommand(migrateup.NewMigrateUpCommand())
	root.AddCommand(NewAtlasCommand())
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"atlas", "migrate", "apply"})

	err := root.Execute()

	c.Assert(err, qt.ErrorMatches, "database URL is required")
}

func TestNewAtlasCommand_MigrateNewCreatesSkeletonFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "new", "manual_hotfix", "--migrations-dir", dir})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Generated empty migration files:")
	matches, globErr := filepath.Glob(filepath.Join(dir, "*_manual_hotfix.*.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 2)
}

func TestNewAtlasCommand_MigrateNewAcceptsAtlasDirFlag(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "new", "manual_hotfix", "--dir", dir})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Generated empty migration files:")
	matches, globErr := filepath.Glob(filepath.Join(dir, "*_manual_hotfix.*.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 2)
}

func TestNewAtlasCommand_HelpUsesAtlasPathForForwardedParentedCommand(t *testing.T) {
	c := qt.New(t)
	root := &cobra.Command{Use: "ptah"}
	root.AddCommand(migrateup.NewMigrateUpCommand())
	root.AddCommand(NewAtlasCommand())
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"atlas", "migrate", "apply", "--help"})

	err := root.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "ptah atlas migrate apply")
	c.Assert(out.String(), qt.Not(qt.Contains), "Usage:\n  migrate-up")
}

func TestNewAtlasCommand_HelpAdvertisesGroupedNativeEquivalents(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantNative string
		oldRoot    string
	}{
		{
			name:       "migrate_apply",
			args:       []string{"migrate", "apply", "--help"},
			wantNative: "ptah migrations up",
			oldRoot:    "ptah migrate-up",
		},
		{
			name:       "schema_inspect",
			args:       []string{"schema", "inspect", "--help"},
			wantNative: "ptah db read",
			oldRoot:    "ptah read-db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := NewAtlasCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(tt.args)

			err := cmd.Execute()

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Contains, tt.wantNative)
			c.Assert(out.String(), qt.Not(qt.Contains), tt.oldRoot)
		})
	}
}

func TestNewAtlasCommand_UnsupportedCommandsAreExplicit(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "apply"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "atlas schema apply compatibility is not implemented yet")
}
