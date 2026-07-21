package atlas

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
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

func TestNewCompatCommand_OSSCommandPathsResolveAtRoot(t *testing.T) {
	paths := [][]string{
		{"migrate", "apply"},
		{"migrate", "down"},
		{"migrate", "status"},
		{"schema", "inspect"},
	}

	for _, path := range paths {
		t.Run(strings.Join(path, "_"), func(t *testing.T) {
			c := qt.New(t)
			cmd := NewCompatCommand("ptah-compat")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append(path, "--help"))

			err := cmd.Execute()

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Contains, "Usage:")
			c.Assert(out.String(), qt.Contains, "ptah-compat "+strings.Join(path, " "))
			c.Assert(out.String(), qt.Not(qt.Contains), "ptah-compat atlas "+strings.Join(path, " "))
		})
	}
}

func TestNewCompatCommand_UsesExecutableNameForAtlasSymlink(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "inspect", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "atlas schema inspect")
	c.Assert(out.String(), qt.Not(qt.Contains), "ptah atlas schema inspect")
}

func TestNewCompatCommand_RootHelpShowsAtlasCompatibleTree(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("ptah-compat")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()

	help := out.String()
	c.Assert(err, qt.IsNil)
	c.Assert(help, qt.Contains, "Atlas-compatible Ptah command tree")
	c.Assert(help, qt.Contains, "migrate")
	c.Assert(help, qt.Contains, "schema")
	c.Assert(help, qt.Not(qt.Contains), "ptah-compat atlas")
}

func TestNewAtlasCommand_VersionPrintsBuildInfo(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"version"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Version: ")
	c.Assert(out.String(), qt.Contains, "Commit: ")
	c.Assert(out.String(), qt.Contains, "Go: ")
	c.Assert(out.String(), qt.Not(qt.Contains), "not implemented")
}

func TestNewCompatCommand_VersionResolvesAtRoot(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"version"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Version: ")
	c.Assert(out.String(), qt.Not(qt.Contains), "not implemented")
}

func TestNewAtlasCommand_LicensePrintsPtahNotice(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"license"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "License: MIT")
	c.Assert(out.String(), qt.Contains, "independent implementation")
	c.Assert(out.String(), qt.Contains, "does not use Atlas source code")
	c.Assert(out.String(), qt.Not(qt.Contains), "not implemented")
}

func TestNewCompatCommand_LicenseResolvesAtRoot(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"license"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "License: MIT")
	c.Assert(out.String(), qt.Not(qt.Contains), "not implemented")
}

func TestNewCompatCommand_UnknownNestedCommandFails(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("ptah-compat")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "aplly"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unexpected positional arguments \["aplly"\]`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(out.String(), qt.Contains, `error: unexpected positional arguments ["aplly"]`)
}

func TestNewAtlasCommand_UnknownNestedCommandFails(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "aplly"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unexpected positional arguments \["aplly"\]`)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(out.String(), qt.Contains, `error: unexpected positional arguments ["aplly"]`)
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
			name: "migrate_down",
			path: []string{"migrate", "down"},
			flags: []string{
				"--url",
				"--dir",
				"--dev-url",
				"--to-version",
				"--to-tag",
				"--dry-run",
				"--format",
				"--revisions-schema",
				"--lock-timeout",
				"--skip-checks",
				"--plan",
			},
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

func TestNewAtlasCommand_MigrateDownHelpUsesAtlasFlagKinds(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "down", "--help"})

	err := cmd.Execute()

	help := out.String()
	c.Assert(err, qt.IsNil)
	c.Assert(help, qt.Contains, "--plan")
	c.Assert(help, qt.Not(qt.Contains), "--plan string")
	c.Assert(help, qt.Contains, "--lock-timeout string")
}

func TestMapAtlasArgs_MigrateDownNativeFlags(t *testing.T) {
	c := qt.New(t)

	got, err := mapAtlasArgs("migrate", atlasMigrateDownVerb(), []string{
		"--url", "postgres://localhost/db",
		"--dir=file://migrations",
		"--to-version", "20260721120000",
		"--dry-run",
		"--revisions-schema", "atlas_schema_revisions",
		"--lock-timeout=10s",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--db-url", "postgres://localhost/db",
		"--migrations-dir=migrations",
		"--target", "20260721120000",
		"--dry-run",
		"--migrations-schema", "atlas_schema_revisions",
		"--migration-lock-timeout=10s",
	})
}

func TestMapAtlasArgs_AtlasEnvFlagsMapToNativeFlags(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_URL", "postgres://env/db")
	t.Setenv("PTAH_DIR", "file://env-migrations")

	got, err := mapAtlasArgs("migrate", atlasMigrateDownVerb(), nil)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--db-url=postgres://env/db",
		"--migrations-dir=env-migrations",
	})
}

func TestMapAtlasArgs_CLIFlagWinsOverAtlasEnvFlag(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_URL", "postgres://env/db")

	got, err := mapAtlasArgs("migrate", atlasMigrateDownVerb(), []string{
		"--url", "postgres://cli/db",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"--db-url", "postgres://cli/db"})
}

func TestMapAtlasArgs_FalseBoolEnvDoesNotEnableAtlasFlag(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_PLAN", "false")

	got, err := mapAtlasArgs("migrate", atlasMigrateDownVerb(), nil)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 0)
}

func TestMapAtlasArgs_MigrateDownRejectsRemoteDir(t *testing.T) {
	c := qt.New(t)

	_, err := mapAtlasArgs("migrate", atlasMigrateDownVerb(), []string{
		"--dir", "atlas://repo/migrations",
	})

	c.Assert(err, qt.ErrorMatches, `atlas migrate down --dir: only local file:// migration directories are supported`)
}

func TestMapAtlasArgs_MigrateDownUnsupportedFlagsFailExplicitly(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "dev_url",
			args: []string{"--dev-url", "sqlite://dev"},
			want: "atlas migrate down accepts --dev-url, but Ptah does not implement its behavior yet",
		},
		{
			name: "skip_checks",
			args: []string{"--skip-checks"},
			want: "atlas migrate down accepts --skip-checks, but Ptah does not implement its behavior yet",
		},
		{
			name: "to_tag",
			args: []string{"--to-tag", "release-v1"},
			want: "atlas migrate down accepts --to-tag, but Ptah does not implement its behavior yet",
		},
		{
			name: "format",
			args: []string{"--format", "{{ json . }}"},
			want: "atlas migrate down accepts --format, but Ptah does not implement its behavior yet",
		},
		{
			name: "plan",
			args: []string{"--plan"},
			want: "atlas migrate down accepts --plan, but Ptah does not implement its behavior yet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := mapAtlasArgs("migrate", atlasMigrateDownVerb(), tt.args)

			c.Assert(err, qt.ErrorMatches, tt.want)
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

func TestNewAtlasCommand_SchemaFmtFormatsHCLFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`schema "main"{}
table "users"{
schema=schema.main
column "id"{
type=int
}
}
`), 0o600), qt.IsNil)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "fmt", path})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, path+"\n")
	formatted, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(formatted), qt.Contains, `schema "main" {}`)
	c.Assert(string(formatted), qt.Contains, "schema = schema.main")
	c.Assert(string(formatted), qt.Not(qt.Contains), "schema=schema.main")
}

func TestNewAtlasCommand_SchemaFmtWalksDirectoriesAndPrintsOnlyChangedFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	changed := filepath.Join(dir, "changed.hcl")
	nestedChanged := filepath.Join(dir, "nested", "changed.hcl")
	unchanged := filepath.Join(dir, "nested", "unchanged.hcl")
	ignored := filepath.Join(dir, "notes.txt")
	c.Assert(os.MkdirAll(filepath.Dir(unchanged), 0o755), qt.IsNil)
	c.Assert(os.WriteFile(changed, []byte(`schema "main"{}`+"\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(nestedChanged, []byte(`schema "nested"{}`+"\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(unchanged, []byte(`schema "main" {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(ignored, []byte(`schema "main"{}`+"\n"), 0o600), qt.IsNil)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "fmt", dir})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, changed+"\n"+nestedChanged+"\n")
	nestedData, readErr := os.ReadFile(nestedChanged)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(nestedData), qt.Equals, `schema "nested" {}
`)
	ignoredData, readErr := os.ReadFile(ignored)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(ignoredData), qt.Equals, `schema "main"{}`+"\n")
}

func TestNewAtlasCommand_SchemaFmtDefaultsToCurrentDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`schema "main"{}`+"\n"), 0o600), qt.IsNil)
	originalDir, getwdErr := os.Getwd()
	c.Assert(getwdErr, qt.IsNil)
	t.Cleanup(func() {
		c.Assert(os.Chdir(originalDir), qt.IsNil)
	})
	c.Assert(os.Chdir(dir), qt.IsNil)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "fmt"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "schema.hcl\n")
	formatted, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(formatted), qt.Equals, `schema "main" {}
`)
}

func TestNewAtlasCommand_SchemaFmtRejectsInvalidHCLWithoutRewriting(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.hcl")
	original := []byte(`schema "main" {
`)
	c.Assert(os.WriteFile(path, original, 0o600), qt.IsNil)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "fmt", path})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `schema fmt .*bad\.hcl: .*`)
	data, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	c.Assert(data, qt.DeepEquals, original)
}

func TestNewCompatCommand_SchemaFmtResolvesAtRoot(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`schema "main"{}`+"\n"), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "fmt", path})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, path+"\n")
	formatted, readErr := os.ReadFile(path)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(formatted), qt.Equals, `schema "main" {}
`)
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

	c.Assert(err, qt.ErrorMatches, "atlas schema apply is not implemented yet")
}
