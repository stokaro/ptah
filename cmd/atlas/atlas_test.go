package atlas

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/cmd/migrateup"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
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
			flags: []string{"--url", "--dev-url", "--schema", "--exclude", "--include", "--format"},
		},
		{
			name:  "schema_apply",
			path:  []string{"schema", "apply"},
			flags: []string{"--url", "--to", "--dev-url", "--dry-run", "--auto-approve", "--format", "--schema", "--exclude", "--include", "--tx-mode"},
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
			flags: []string{"--to", "--dev-url", "--dir", "--dir-format", "--format"},
		},
		{
			name: "migrate_apply",
			path: []string{"migrate", "apply"},
			flags: []string{
				"--url",
				"--dir",
				"--dry-run",
				"--tx-mode",
				"--exec-order",
				"--to-version",
				"--allow-dirty",
				"--baseline",
				"--revisions-schema",
				"--lock-name",
				"--lock-timeout",
				"--format",
			},
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
			flags: []string{"--from", "--to", "--dir-format"},
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

func TestNewAtlasCommand_MigrateLintHelpUsesAtlasFlagKinds(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "lint", "--help"})

	err := cmd.Execute()

	help := out.String()
	c.Assert(err, qt.IsNil)
	c.Assert(help, qt.Contains, "--latest uint")
	c.Assert(help, qt.Not(qt.Contains), "--latest string")
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

func TestMapAtlasArgs_MigrateLintLatestMapsToNativeFlag(t *testing.T) {
	c := qt.New(t)

	got, err := mapAtlasArgs("migrate", atlasMigrateLintVerb(), []string{
		"--dir=file://migrations",
		"--latest", "2",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--dir=migrations",
		"--latest",
		"2",
	})
}

func TestMapAtlasArgs_SchemaCleanAutoApproveMapsToNativeFlag(t *testing.T) {
	c := qt.New(t)

	got, err := mapAtlasArgs("schema", atlasSchemaCleanVerb(), []string{
		"--url", "sqlite://test.db",
		"--auto-approve",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--db-url", "sqlite://test.db",
		"--auto-approve",
	})
}

func TestMapAtlasArgs_SchemaCleanAutoApproveEnvIsIgnored(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_AUTO_APPROVE", "true")

	got, err := mapAtlasArgs("schema", atlasSchemaCleanVerb(), []string{
		"--url", "sqlite://test.db",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"--db-url", "sqlite://test.db",
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

func TestNewAtlasCommand_SchemaInspectOutputsAtlasHCLWithoutNativeBanners(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect.db")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "inspect", "--url", "sqlite://" + dbPath})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `table "users"`)
	c.Assert(out.String(), qt.Contains, `column "email"`)
	c.Assert(out.String(), qt.Not(qt.Contains), "Reading schema from database")
	c.Assert(out.String(), qt.Not(qt.Contains), "Connected to sqlite database successfully")
}

func TestNewAtlasCommand_SchemaInspectOutputsSQLFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect-sql.db")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://" + dbPath,
		"--format", "{{ sql . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "CREATE TABLE")
	c.Assert(out.String(), qt.Contains, "users")
	c.Assert(out.String(), qt.Not(qt.Contains), `table "users"`)
}

func TestNewCompatCommand_SchemaInspectUsesAtlasRoot(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "compat-inspect.db")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "inspect", "--url", "sqlite://" + dbPath})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `table "users"`)
}

func TestNewAtlasCommand_SchemaInspectRejectsUnsupportedFormat(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "inspect", "--url", "sqlite://inspect.db", "--format", "{{ json . }}"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "atlas schema inspect accepts JSON output, but Ptah does not implement Atlas-compatible JSON yet")
}

func TestNewAtlasCommand_SchemaInspectRejectsUnsupportedFilters(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "inspect", "--url", "sqlite://inspect.db", "--exclude", "*.secret"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "atlas schema inspect accepts --exclude, but Ptah does not implement its behavior yet")
}

func TestParseAtlasSchemaInspectSchemasAcceptsRepeatedAndCommaValues(t *testing.T) {
	c := qt.New(t)

	schemas := parseAtlasSchemaInspectSchemas([]string{"public, auth", "tenant"})

	c.Assert(schemas, qt.DeepEquals, []string{"public", "auth", "tenant"})
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

func TestNewAtlasCommand_SchemaDiffPrintsLocalFileDiff(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := filepath.Join(dir, "from.hcl")
	to := filepath.Join(dir, "to.hcl")
	c.Assert(os.WriteFile(from, []byte(`
table "users" {
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(to, []byte(`
table "users" {
  column "id" {
    type = int
  }
  column "email" {
    null = false
    type = varchar(255)
  }
  primary_key {
    columns = [column.id]
  }
}
`), 0o600), qt.IsNil)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + from,
		"--to", "file://" + to,
		"--dev-url", "postgres://localhost/dev",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `ALTER TABLE "users" ADD COLUMN "email" varchar(255) NOT NULL;`)
}

func TestNewAtlasCommand_SchemaDiffReportsSyncedLocalFiles(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`
table "users" {
  column "id" {
    type = int
  }
}
`), 0o600), qt.IsNil)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + path,
		"--to", "file://" + path,
		"--dev-url", "postgres://localhost/dev",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "Schemas are synced, no changes to be made.\n")
}

func TestNewAtlasCommand_SchemaDiffRejectsUnsupportedRemoteTarget(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "postgres://localhost/db",
		"--to", "file://schema.hcl",
		"--dev-url", "sqlite://dev?mode=memory",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--from "postgres://localhost/db": only local file:// schema files are supported`)
	c.Assert(out.String(), qt.Contains, `error: --from "postgres://localhost/db": only local file:// schema files are supported`)
}

func TestNewAtlasCommand_SchemaDiffRejectsUnsupportedFormat(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://from.hcl",
		"--to", "file://to.hcl",
		"--dev-url", "sqlite://dev?mode=memory",
		"--format", "{{ sql . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas schema diff accepts --format, but Ptah does not implement its behavior yet`)
}

func TestNewAtlasCommand_SchemaApplyAppliesLocalSchemaToSQLite(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);
`), 0o600), qt.IsNil)

	first := NewAtlasCommand()
	var firstOut bytes.Buffer
	first.SetOut(&firstOut)
	first.SetErr(&firstOut)
	first.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://dev.db",
		"--auto-approve",
	})

	err := first.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(firstOut.String(), qt.Contains, "Planned schema changes:")
	c.Assert(firstOut.String(), qt.Contains, "CREATE TABLE")
	c.Assert(firstOut.String(), qt.Contains, "Schema apply completed successfully.")
	assertSQLiteTableExists(c, dbPath, "users")

	second := NewAtlasCommand()
	var secondOut bytes.Buffer
	second.SetOut(&secondOut)
	second.SetErr(&secondOut)
	second.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
	})

	err = second.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(secondOut.String(), qt.Equals, "Schema is synced, no changes to be made.\n")
}

func TestNewAtlasCommand_SchemaApplyDryRunDoesNotApply(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "dry-run.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Planned schema changes:")
	c.Assert(out.String(), qt.Contains, "CREATE TABLE")
	assertSQLiteTableMissing(c, dbPath, "users")
}

func TestNewAtlasCommand_SchemaApplyAcceptsTxMode(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tx-mode-command.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE tx_mode_users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--tx-mode", "none",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema apply completed successfully.")
	assertSQLiteTableExists(c, dbPath, "tx_mode_users")
}

func TestNewAtlasCommand_SchemaApplyRejectsInvalidTxMode(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE tx_mode_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + filepath.Join(dir, "tx-mode-invalid.db"),
		"--to", "file://" + schemaPath,
		"--tx-mode", "statement",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid tx-mode "statement": expected file, all, or none`)
}

func TestApplyAtlasSchemaChangesTxModeRollbackBehavior(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	sqlText := `
CREATE TABLE tx_mode_first (id INTEGER PRIMARY KEY);
CREATE TABLE tx_mode_first (id INTEGER PRIMARY KEY);
`

	allDB := filepath.Join(dir, "tx-mode-all.db")
	allConn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+allDB)
	c.Assert(err, qt.IsNil)
	err = applyAtlasSchemaChanges(context.Background(), allConn, migrator.MigrationTxModeAll, sqlText)
	dbschema.CloseAndWarn(allConn)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to execute SQL statement")
	assertSQLiteTableMissing(c, allDB, "tx_mode_first")

	noneDB := filepath.Join(dir, "tx-mode-none.db")
	noneConn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+noneDB)
	c.Assert(err, qt.IsNil)
	err = applyAtlasSchemaChanges(context.Background(), noneConn, migrator.MigrationTxModeNone, sqlText)
	dbschema.CloseAndWarn(noneConn)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "failed to execute SQL statement")
	assertSQLiteTableExists(c, noneDB, "tx_mode_first")
}

func TestSplitAtlasSchemaApplyStatementsUsesDialect(t *testing.T) {
	c := qt.New(t)
	sqlText := `
CREATE TABLE tx_mode_batch_one (id INT);
GO
CREATE TABLE tx_mode_batch_two (id INT);
GO
`

	statements := splitAtlasSchemaApplyStatements(sqlText, "sqlserver")

	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE TABLE tx_mode_batch_one (id INT)",
		"CREATE TABLE tx_mode_batch_two (id INT)",
	})
}

func TestNewCompatCommand_SchemaApplyDryRunUsesAtlasRoot(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "compat-dry-run.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Planned schema changes:")
	assertSQLiteTableMissing(c, dbPath, "users")
}

func TestNewAtlasCommand_SchemaApplyRejectsUnsupportedFormat(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://apply.db",
		"--to", "file://schema.sql",
		"--format", "{{ sql . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas schema apply accepts --format, but Ptah does not implement its behavior yet`)
}

func TestNewAtlasCommand_SchemaApplyRejectsDevURLDialectMismatch(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + filepath.Join(dir, "apply.db"),
		"--to", "file://" + schemaPath,
		"--dev-url", "docker://postgres/16/dev",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--dev-url dialect "postgres" does not match --url dialect "sqlite"`)
}

func TestNewAtlasCommand_MigrateApplyAmountAndToVersionSQLite(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-migrations.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_one.sql", "CREATE TABLE apply_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "2_two.sql", "CREATE TABLE apply_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "3_three.sql", "CREATE TABLE apply_three (id INTEGER PRIMARY KEY);")

	first := NewAtlasCommand()
	var firstOut bytes.Buffer
	first.SetOut(&firstOut)
	first.SetErr(&firstOut)
	first.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"2",
	})

	err := first.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(firstOut.String(), qt.Contains, "Migrating to version 2 from 2 pending migrations.")
	c.Assert(firstOut.String(), qt.Contains, "Migration complete. Current version: 2")
	assertSQLiteTableExists(c, dbPath, "apply_one")
	assertSQLiteTableExists(c, dbPath, "apply_two")
	assertSQLiteTableMissing(c, dbPath, "apply_three")

	second := NewAtlasCommand()
	var secondOut bytes.Buffer
	second.SetOut(&secondOut)
	second.SetErr(&secondOut)
	second.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--to-version", "3",
	})

	err = second.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(secondOut.String(), qt.Contains, "Migrating to version 3 from 1 pending migrations.")
	c.Assert(secondOut.String(), qt.Contains, "Migration complete. Current version: 3")
	assertSQLiteTableExists(c, dbPath, "apply_three")
}

func TestNewAtlasCommand_MigrateApplyBaselineUsesAtlasRevisions(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "baseline.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_one.sql", "CREATE TABLE baseline_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "2_two.sql", "CREATE TABLE baseline_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "3_three.sql", "CREATE TABLE baseline_three (id INTEGER PRIMARY KEY);")

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--baseline", "2",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Migrating to version 3 from 1 pending migrations.")
	assertSQLiteTableMissing(c, dbPath, "baseline_one")
	assertSQLiteTableMissing(c, dbPath, "baseline_two")
	assertSQLiteTableExists(c, dbPath, "baseline_three")
	c.Assert(sqliteAtlasAppliedVersions(c, dbPath), qt.DeepEquals, []string{"1", "2", "3"})
}

func TestNewAtlasCommand_MigrateApplyDryRunBaselinePlansRemainingMigrations(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "baseline-dry-run.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_one.sql", "CREATE TABLE dry_baseline_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "2_two.sql", "CREATE TABLE dry_baseline_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "3_three.sql", "CREATE TABLE dry_baseline_three (id INTEGER PRIMARY KEY);")

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--baseline", "2",
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Would baseline migrations at version 2.")
	c.Assert(out.String(), qt.Contains, "Migrating to version 3 from 1 pending migrations.")
	c.Assert(out.String(), qt.Contains, "Would have applied 1 migrations.")
	assertSQLiteTableMissing(c, dbPath, "dry_baseline_one")
	assertSQLiteTableMissing(c, dbPath, "dry_baseline_two")
	assertSQLiteTableMissing(c, dbPath, "dry_baseline_three")
}

func TestNewAtlasCommand_MigrateApplyRejectsFormatAndAmbiguousTarget(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "apply", "--format", "{{ json . }}"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate apply accepts --format, but Ptah does not implement its behavior yet`)

	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_one.sql", "CREATE TABLE ambiguous_one (id INTEGER PRIMARY KEY);")
	cmd = NewAtlasCommand()
	out.Reset()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + filepath.Join(dir, "ambiguous.db"),
		"--dir", "file://" + migrationsDir,
		"--to-version", "1",
		"1",
	})

	err = cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `amount argument and --to-version cannot both be set`)
}

func TestNewAtlasCommand_MigrateApplyAcceptsLockName(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "lock-name.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_lock_name.sql", "CREATE TABLE lock_name_applied (id INTEGER PRIMARY KEY);")

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--lock-name", "custom-lock",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Migration complete. Current version: 1")
	assertSQLiteTableExists(c, dbPath, "lock_name_applied")
}

func TestNewAtlasCommand_MigrateApplyRejectsEmptyLockName(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "apply", "--lock-name", " "})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--lock-name must not be empty`)
}

func TestNewAtlasCommand_MigrateDiffCreatesAtlasMigrationFromLocalSchema(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)
	devURL := "sqlite://" + filepath.Join(dir, "dev.db")

	first := NewAtlasCommand()
	var firstOut bytes.Buffer
	first.SetOut(&firstOut)
	first.SetErr(&firstOut)
	first.SetArgs([]string{
		"migrate", "diff",
		"--dev-url", devURL,
		"--dir", "file://" + migrationsDir,
		"--to", "file://" + schemaPath,
		"--lock-timeout", "1s",
		"add_email",
	})

	err := first.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(firstOut.String(), qt.Contains, "Created migration file:")
	c.Assert(firstOut.String(), qt.Contains, "Updated migration checksum:")
	migrationFiles := atlasSQLFiles(c, migrationsDir)
	c.Assert(migrationFiles, qt.HasLen, 2)
	newMigration := nonInitialAtlasMigration(c, migrationFiles)
	newSQL, err := os.ReadFile(newMigration)
	c.Assert(err, qt.IsNil)
	c.Assert(string(newSQL), qt.Contains, "ADD COLUMN")
	c.Assert(string(newSQL), qt.Contains, "email")
	sum, err := os.ReadFile(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, filepath.Base(newMigration))

	second := NewAtlasCommand()
	var secondOut bytes.Buffer
	second.SetOut(&secondOut)
	second.SetErr(&secondOut)
	second.SetArgs([]string{
		"migrate", "diff",
		"--dev-url", devURL,
		"--dir", "file://" + migrationsDir,
		"--to", "file://" + schemaPath,
		"add_email",
	})

	err = second.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(secondOut.String(), qt.Equals, "The migration directory is synced with the desired state, no changes to be made\n")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 2)
}

func TestNewAtlasCommand_MigrateDiffRejectsChecksumDrift(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT
);
`), 0o600), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dir", "file://" + migrationsDir,
		"--to", "file://" + schemaPath,
		"add_email",
	})

	err = cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `(?s)migration directory checksum verification failed:.*migration directory does not match atlas\.sum:.*changed: 1_init\.sql.*`)
	c.Assert(out.String(), qt.Contains, "migration directory checksum verification failed:")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
}

func TestNewAtlasCommand_MigrateDiffRejectsInvalidLockTimeout(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--dev-url", "sqlite://dev.db",
		"--dir", "file://migrations",
		"--to", "file://schema.sql",
		"--lock-timeout", "0s",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `invalid migration lock timeout: must be greater than zero`)
}

func TestNewAtlasCommand_MigrateDiffLockTimeout(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	lock, err := acquireAtlasMigrateDiffDirLock(context.Background(), migrationsDir, 0)
	c.Assert(err, qt.IsNil)
	defer func() {
		c.Assert(lock.release(), qt.IsNil)
	}()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE locked_diff (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dir", "file://" + migrationsDir,
		"--to", "file://" + schemaPath,
		"--lock-timeout", "1ms",
		"locked_diff",
	})

	err = cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `migration directory lock timeout after 1ms: .*\.ptah-migrate-diff\.lock`)
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 0)
}

func TestNewAtlasCommand_MigrateDiffRejectsUnsupportedFormat(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--dev-url", "sqlite://dev.db",
		"--dir", "file://migrations",
		"--to", "file://schema.sql",
		"--format", "{{ sql . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate diff accepts --format, but Ptah does not implement its behavior yet`)
}

func writeAtlasApplyMigration(c *qt.C, dir, name, sql string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql+"\n"), 0o600), qt.IsNil)
}

func sqliteAtlasAppliedVersions(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	rows, err := conn.Query("SELECT version FROM atlas_schema_revisions ORDER BY CAST(version AS INTEGER)")
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	versions := make([]string, 0)
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

func atlasSQLFiles(c *qt.C, dir string) []string {
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	return files
}

func nonInitialAtlasMigration(c *qt.C, files []string) string {
	var generated string
	for _, file := range files {
		if filepath.Base(file) != "1_init.sql" {
			generated = file
			break
		}
	}
	c.Assert(generated, qt.Not(qt.Equals), "", qt.Commentf("generated migration file not found in %v", files))
	return generated
}

func assertSQLiteTableExists(c *qt.C, dbPath, table string) {
	c.Helper()
	c.Assert(sqliteTableExists(c, dbPath, table), qt.IsTrue)
}

func assertSQLiteTableMissing(c *qt.C, dbPath, table string) {
	c.Helper()
	c.Assert(sqliteTableExists(c, dbPath, table), qt.IsFalse)
}

func createAtlasInspectSQLiteSchema(c *qt.C, dbPath string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(), `
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);
CREATE UNIQUE INDEX users_email_key ON users (email);
`)
	c.Assert(err, qt.IsNil)
}

func sqliteTableExists(c *qt.C, dbPath, table string) bool {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	schema, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	c.Assert(err, qt.IsNil)
	for _, dbTable := range schema.Tables {
		if dbTable.Name == table {
			return true
		}
	}
	return false
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

func TestNewAtlasCommand_MigrateImportConvertsFlywayDirectory(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeAtlasTestFile(c, source, "V1__initial.sql", "CREATE TABLE skipped (id int);\n")
	writeAtlasTestFile(c, source, "B1__baseline.sql", "CREATE TABLE baseline (id int);\n")
	writeAtlasTestFile(c, source, "V2__add_posts.sql", "CREATE TABLE posts (id int);\n")
	writeAtlasTestFile(c, source, "U1__initial.sql", "DROP TABLE skipped;\n")

	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "import", "--from", "file://" + source + "?format=flyway", "--to", "file://" + target})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Imported migration files:")
	c.Assert(out.String(), qt.Contains, filepath.Join(target, "1_baseline.sql"))
	c.Assert(out.String(), qt.Contains, filepath.Join(target, "2_add_posts.sql"))
	c.Assert(out.String(), qt.Contains, filepath.Join(target, "atlas.sum"))
	c.Assert(readAtlasTestFile(c, target, "1_baseline.sql"), qt.Equals, "CREATE TABLE baseline (id int);\n")
	c.Assert(readAtlasTestFile(c, target, "2_add_posts.sql"), qt.Equals, "CREATE TABLE posts (id int);\n")
	c.Assert(readAtlasTestFile(c, target, "atlas.sum"), qt.Contains, "2_add_posts.sql h1:")
}

func TestNewCompatCommand_MigrateImportResolvesAtRoot(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeAtlasTestFile(c, source, "1_initial.up.sql", "CREATE TABLE users (id int);\n")
	writeAtlasTestFile(c, source, "1_initial.down.sql", "DROP TABLE users;\n")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "import", "--from", "file://" + source + "?format=golang-migrate", "--to", "file://" + target})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, filepath.Join(target, "1_initial.sql"))
	c.Assert(readAtlasTestFile(c, target, "1_initial.sql"), qt.Equals, "CREATE TABLE users (id int);\n")
	c.Assert(readAtlasTestFile(c, target, "atlas.sum"), qt.Contains, "1_initial.sql h1:")
}

func TestNewAtlasCommand_MigrateImportRejectsRemoteSource(t *testing.T) {
	c := qt.New(t)
	cmd := NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "import", "--from", "atlas://repo/migrations?format=flyway"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `import --from: only local file:// migration directories are supported`)
	c.Assert(out.String(), qt.Contains, "error: import --from: only local file:// migration directories are supported")
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
		name     string
		args     []string
		wantText string
		oldRoot  string
	}{
		{
			name:     "migrate_apply",
			args:     []string{"migrate", "apply", "--help"},
			wantText: "ptah migrations up",
			oldRoot:  "ptah migrate-up",
		},
		{
			name:     "schema_inspect",
			args:     []string{"schema", "inspect", "--help"},
			wantText: "Atlas HCL",
			oldRoot:  "ptah read-db",
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
			c.Assert(out.String(), qt.Contains, tt.wantText)
			c.Assert(out.String(), qt.Not(qt.Contains), tt.oldRoot)
		})
	}
}

func writeAtlasTestFile(c *qt.C, dir, name, content string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}

func readAtlasTestFile(c *qt.C, dir, name string) string {
	c.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	c.Assert(err, qt.IsNil)
	return string(data)
}
