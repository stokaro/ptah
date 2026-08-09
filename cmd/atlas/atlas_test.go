package atlas

// White-box testing required: this file covers Atlas compatibility helpers,
// argument mappers, and command constructors whose correctness cannot be fully
// exercised through the exported CLI command tree alone.

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/testutils"
	migrationlint "go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestCompatCommand_OSSCommandPathsResolve(t *testing.T) {
	paths := [][]string{
		{"version"},
		{"license"},
		{"schema", "inspect"},
		{"schema", "apply"},
		{"schema", "diff"},
		{"schema", "fmt"},
		{"schema", "clean"},
		{"schema", "test"},
		{"migrate", "apply"},
		{"migrate", "diff"},
		{"migrate", "down"},
		{"migrate", "edit"},
		{"migrate", "hash"},
		{"migrate", "import"},
		{"migrate", "lint"},
		{"migrate", "new"},
		{"migrate", "rebase"},
		{"migrate", "rm"},
		{"migrate", "set"},
		{"migrate", "status"},
		{"migrate", "test"},
		{"migrate", "validate"},
	}

	for _, path := range paths {
		t.Run(strings.Join(path, "_"), func(t *testing.T) {
			c := qt.New(t)
			cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_VersionPrintsBuildInfo(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_LicensePrintsPtahNotice(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_AdvertisesEssentialAtlasFlags(t *testing.T) {
	tests := []struct {
		name      string
		path      []string
		flags     []string
		forbidden []string
	}{
		{
			name: "schema_inspect",
			path: []string{"schema", "inspect"},
			// --include and --output are Pro-surface flags the pinned Atlas CE
			// binary does not register on inspect; compat implements both so
			// Pro pipelines port. --web is registered as a named refusal
			// (see schema_ui_flags.go). --export stays unregistered: it is a
			// separate item of stokaro/ptah#951.
			flags: []string{
				"--url", "--dev-url", "--env", "--schema", "--exclude",
				"--format", "--include", "--output", "--web",
			},
			forbidden: []string{"--export"},
		},
		{
			name: "schema_apply",
			path: []string{"schema", "apply"},
			// --lock-name, --skip-lock and --skip-lint are Pro-surface flags the
			// pinned Atlas CE binary does not register; Atlas's published CLI
			// reference does register all three on this verb, so compat
			// implements them.
			flags: []string{"--url", "--to", "--dev-url", "--dry-run", "--auto-approve", "--format", "--schema", "--exclude", "--include", "--tx-mode", "--plan", "--edit", "--lock-timeout", "--lock-name", "--skip-lock", "--skip-lint"},
		},
		{
			name:      "schema_diff",
			path:      []string{"schema", "diff"},
			flags:     []string{"--from", "--to", "--dev-url", "--env", "--format", "--schema", "--exclude", "--include"},
			forbidden: []string{"--web"},
		},
		{
			name:  "schema_clean",
			path:  []string{"schema", "clean"},
			flags: []string{"--url", "--dry-run", "--format", "--auto-approve"},
		},
		{
			name:  "migrate_diff",
			path:  []string{"migrate", "diff"},
			flags: []string{"--to", "--dev-url", "--env", "--dir", "--dir-format", "--format", "--schema", "--lock-timeout", "--qualifier", "--edit"},
		},
		{
			name: "migrate_apply",
			path: []string{"migrate", "apply"},
			flags: []string{
				"--url",
				"--dir",
				"--env",
				"--dry-run",
				"--tx-mode",
				"--exec-order",
				"--allow-dirty",
				"--baseline",
				"--revisions-schema",
				"--lock-timeout",
				"--format",
				// Atlas's published CLI reference registers --to-version on
				// this verb; the pinned community binary does not, which is
				// what makes it a Pro-surface addition rather than a CE parity
				// row (stokaro/ptah#951).
				"--to-version",
				// Pro-surface flags Atlas's published CLI reference registers
				// on this verb; the pinned CE binary registers neither.
				"--lock-name",
				"--skip-lock",
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
			name:      "migrate_checkpoint",
			path:      []string{"migrate", "checkpoint"},
			flags:     []string{"--dir", "--dev-url", "--dir-format", "--schema", "--qualifier", "--lock-timeout", "--edit"},
			forbidden: []string{"--editor"},
		},
		{
			name:  "migrate_lint",
			path:  []string{"migrate", "lint"},
			flags: []string{"--dev-url", "--dir", "--dir-format", "--env", "--format", "--latest", "--git-base", "--git-dir"},
		},
		{
			name:  "migrate_hash",
			path:  []string{"migrate", "hash"},
			flags: []string{"--dir", "--dir-format"},
		},
		{
			name:  "migrate_status",
			path:  []string{"migrate", "status"},
			flags: []string{"--url", "--dir", "--dir-format", "--revisions-schema", "--format"},
		},
		{
			name:  "migrate_validate",
			path:  []string{"migrate", "validate"},
			flags: []string{"--dev-url", "--dir", "--dir-format"},
		},
		{
			name:  "migrate_test",
			path:  []string{"migrate", "test"},
			flags: []string{"--dir", "--dir-format", "--dev-url", "--run", "--revisions-schema"},
		},
		{
			name:  "schema_test",
			path:  []string{"schema", "test"},
			flags: []string{"--url", "--dev-url", "--run"},
		},
		{
			name:  "migrate_new",
			path:  []string{"migrate", "new"},
			flags: []string{"--dir", "--dir-format", "--edit"},
		},
		{
			name:  "migrate_edit",
			path:  []string{"migrate", "edit"},
			flags: []string{"--dir", "--dir-format"},
		},
		{
			name:  "migrate_rebase",
			path:  []string{"migrate", "rebase"},
			flags: []string{"--dir", "--dir-format"},
		},
		{
			name:  "migrate_rm",
			path:  []string{"migrate", "rm"},
			flags: []string{"--dir", "--dir-format"},
		},
		{
			name:  "migrate_set",
			path:  []string{"migrate", "set"},
			flags: []string{"--url", "--dir", "--dir-format", "--revisions-schema"},
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
			cmd := NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append(tt.path, "--help"))

			err := cmd.Execute()

			c.Assert(err, qt.IsNil)
			for _, flag := range tt.flags {
				c.Assert(out.String(), qt.Contains, flag)
			}
			for _, flag := range tt.forbidden {
				c.Assert(out.String(), qt.Not(qt.Contains), flag)
			}
		})
	}
}

func TestCompatCommand_RegistersAtlasShorthandFlags(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name      string
		path      []string
		flag      string
		shorthand string
	}{
		{
			name:      "schema_inspect_url",
			path:      []string{"schema", "inspect"},
			flag:      "url",
			shorthand: "u",
		},
		{
			name:      "schema_inspect_schema",
			path:      []string{"schema", "inspect"},
			flag:      "schema",
			shorthand: "s",
		},
		{
			name:      "schema_apply_url",
			path:      []string{"schema", "apply"},
			flag:      "url",
			shorthand: "u",
		},
		{
			name:      "schema_apply_schema",
			path:      []string{"schema", "apply"},
			flag:      "schema",
			shorthand: "s",
		},
		{
			name:      "schema_apply_hidden_file",
			path:      []string{"schema", "apply"},
			flag:      "file",
			shorthand: "f",
		},
		{
			name:      "schema_diff_from",
			path:      []string{"schema", "diff"},
			flag:      "from",
			shorthand: "f",
		},
		{
			name:      "schema_diff_schema",
			path:      []string{"schema", "diff"},
			flag:      "schema",
			shorthand: "s",
		},
		{
			name:      "migrate_diff_schema",
			path:      []string{"migrate", "diff"},
			flag:      "schema",
			shorthand: "s",
		},
		{
			name:      "schema_test_url",
			path:      []string{"schema", "test"},
			flag:      "url",
			shorthand: "u",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			root := NewCompatCommand("atlas")
			cmd, _, err := root.Find(tt.path)

			c.Assert(err, qt.IsNil)
			c.Assert(cmd.Flags().Lookup(tt.flag), qt.IsNotNil)
			c.Assert(cmd.Flags().Lookup(tt.flag).Shorthand, qt.Equals, tt.shorthand)
		})
	}
}

func TestCompatCommand_DoesNotRegisterUnsupportedAtlasShorthandFlags(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name string
		path []string
		flag string
	}{
		{
			name: "schema_apply_to",
			path: []string{"schema", "apply"},
			flag: "to",
		},
		{
			name: "schema_diff_to",
			path: []string{"schema", "diff"},
			flag: "to",
		},
		{
			name: "migrate_diff_to",
			path: []string{"migrate", "diff"},
			flag: "to",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			root := NewCompatCommand("atlas")
			cmd, _, err := root.Find(tt.path)

			c.Assert(err, qt.IsNil)
			c.Assert(cmd.Flags().Lookup(tt.flag), qt.IsNotNil)
			c.Assert(cmd.Flags().Lookup(tt.flag).Shorthand, qt.Equals, "")
		})
	}
}

func TestCompatCommand_SchemaApplyFileShorthandIsHidden(t *testing.T) {
	c := qt.New(t)
	root := NewCompatCommand("atlas")
	cmd, _, err := root.Find([]string{"schema", "apply"})

	c.Assert(err, qt.IsNil)
	c.Assert(cmd.Flags().Lookup("file"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("file").Hidden, qt.IsTrue)
}

func TestCompatCommand_SchemaApplyFileShorthandConflictsWithTo(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://schema.db",
		"--to", "file://schema.sql",
		"-f", "schema.sql",
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `if any flags in the group \[file to\] are set none of the others can be; \[file to\] were all set`)
}

func TestCompatCommand_SchemaApplySchemaShorthandParses(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev-940.db"),
		"--url", "sqlite://" + filepath.Join(dir, "schema.db"),
		"--to", "file://" + schemaPath,
		"-s", "public",
		"--dry-run",
	})

	err := cmd.Execute()

	// SQLite owns unqualified objects in "main", so a "public" schema scope
	// selects nothing and the apply reports a synced schema.
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schema is synced, no changes to be made")
}

func TestCompatCommand_SchemaDiffSchemaShorthandParses(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	fromPath := filepath.Join(dir, "from.sql")
	toPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(fromPath, []byte(""), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"-f", "file://" + fromPath,
		"--to", "file://" + toPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"-s", "public",
	})

	err := cmd.Execute()

	// SQLite owns unqualified objects in "main", so a "public" schema scope
	// selects nothing on either side.
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Schemas are synced, no changes to be made.")
}

func TestCompatCommand_MigrateDownHelpUsesAtlasFlagKinds(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_MigrateLintHelpUsesAtlasFlagKinds(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_MigrateMetadataDirFormatDefaultsToAtlas(t *testing.T) {
	tests := []struct {
		name string
		path []string
	}{
		{
			name: "edit",
			path: []string{"migrate", "edit"},
		},
		{
			name: "hash",
			path: []string{"migrate", "hash"},
		},
		{
			name: "lint",
			path: []string{"migrate", "lint"},
		},
		{
			name: "new",
			path: []string{"migrate", "new"},
		},
		{
			name: "rebase",
			path: []string{"migrate", "rebase"},
		},
		{
			name: "rm",
			path: []string{"migrate", "rm"},
		},
		{
			name: "set",
			path: []string{"migrate", "set"},
		},
		{
			name: "status",
			path: []string{"migrate", "status"},
		},
		{
			name: "test",
			path: []string{"migrate", "test"},
		},
		{
			name: "validate",
			path: []string{"migrate", "validate"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			root := NewCompatCommand("atlas")

			cmd, _, err := root.Find(tt.path)

			c.Assert(err, qt.IsNil)
			flag := cmd.Flags().Lookup("dir-format")
			c.Assert(flag, qt.IsNotNil)
			c.Assert(flag.DefValue, qt.Equals, "atlas")
			c.Assert(flag.Shorthand, qt.Equals, "")
			c.Assert(flag.Hidden, qt.IsFalse)
		})
	}
}

func TestCompatCommand_MigrateApplyDoesNotRegisterDirFormat(t *testing.T) {
	c := qt.New(t)
	root := NewCompatCommand("atlas")

	cmd, _, err := root.Find([]string{"migrate", "apply"})

	c.Assert(err, qt.IsNil)
	c.Assert(cmd.Flags().Lookup("dir-format"), qt.IsNil)
}

func TestCompatCommand_AdvertisesAtlasProjectFlags(t *testing.T) {
	tests := []struct {
		name string
		path []string
	}{
		{
			name: "schema_parent",
			path: []string{"schema"},
		},
		{
			name: "schema_inspect",
			path: []string{"schema", "inspect"},
		},
		{
			name: "schema_clean",
			path: []string{"schema", "clean"},
		},
		{
			name: "migrate_parent",
			path: []string{"migrate"},
		},
		{
			name: "migrate_apply",
			path: []string{"migrate", "apply"},
		},
		{
			name: "migrate_hash",
			path: []string{"migrate", "hash"},
		},
		{
			name: "migrate_status",
			path: []string{"migrate", "status"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append(tt.path, "--help"))

			err := cmd.Execute()

			help := out.String()
			c.Assert(err, qt.IsNil)
			c.Assert(help, qt.Contains, "--config string")
			c.Assert(help, qt.Contains, "-c, --config")
			c.Assert(help, qt.Contains, "--env string")
			// `<name>=<value>`, not `stringArray`: the flag is registered with a
			// value type that refuses an assignment carrying no `=`
			// (stokaro/ptah#1231 case 7), and the community binary spells the
			// same row `--var <name>=<value>   input variables (default [])`.
			c.Assert(help, qt.Contains, "--var <name>=<value>")
		})
	}
}

func TestCompatCommand_ForwardsSupportedCommands(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "apply"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "database URL is required")
}

func TestCompatCommand_MapsAtlasFlagFormsToNativeFlags(t *testing.T) {
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
			// Run somewhere with no ./migrations. Since stokaro/ptah#1241 item
			// 2 gave `migrate apply` the Atlas-documented `--dir` default,
			// omitting the flag no longer stops at `migrations directory is
			// required`; the run reaches the directory and fails to open it.
			// The proxy still proves what this test is about — every spelling
			// of --url got past `database URL is required` — but it now
			// depends on the working directory, so it is pinned rather than
			// inherited from the package directory.
			t.Chdir(t.TempDir())
			cmd := NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(tt.args)

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, `atlas migrate apply --dir: open migrations directory: .*`)
		})
	}
}

func TestCompatCommand_MigrateCheckpointForwardsToNative(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	writePair := func(name, up, down string) {
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name+".up.sql"), []byte(up), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(migrationsDir, name+".down.sql"), []byte(down), 0o600), qt.IsNil)
	}
	writePair("0000000001_init", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n", "DROP TABLE users;\n")
	writePair("0000000002_email", "ALTER TABLE users ADD COLUMN email TEXT;\n", "ALTER TABLE users DROP COLUMN email;\n")
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	// --dir-format is pinned to ptah so this stays a test of flag forwarding.
	// The compat default is atlas; which convention the default selects is
	// covered by TestCompatCommand_MigrateCheckpointDefaultsToAtlasFormat.
	cmd.SetArgs([]string{"migrate", "checkpoint", "--dir", "file://" + migrationsDir, "--dev-url", shadow, "--dir-format", "ptah", "snapshot"})

	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))

	// The Atlas verb forwards to `ptah migrations checkpoint`: --dir maps to the
	// native --migrations-dir, --dev-url to --shadow-db (the dialect is inferred
	// from it), and the positional tag maps to --description, so the checkpoint
	// pair carries that name and the cumulative schema (users with email).
	up, err := os.ReadFile(filepath.Join(migrationsDir, "0000000003_snapshot.checkpoint.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Contains, "CREATE TABLE")
	c.Assert(string(up), qt.Contains, "email")
	_, err = os.Stat(filepath.Join(migrationsDir, "0000000003_snapshot.checkpoint.down.sql"))
	c.Assert(err, qt.IsNil)
}

func TestCompatCommand_MigrateCheckpointRequiresDevURL(t *testing.T) {
	c := qt.New(t)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "checkpoint", "--dir", "file://" + t.TempDir()})

	// Forwarding reaches the native command, which reports the missing shadow
	// database rather than the old community-unsupported stub message.
	err := cmd.Execute()
	c.Assert(err, qt.IsNotNil)
	c.Assert(out.String(), qt.Contains, "shadow database URL is required")
}

func TestCompatCommand_AdapterCommandUsesAtlasProjectFlags(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte("CREATE TABLE users (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("project.hcl", []byte(`variable "dir" {}

env "local" {
  migration {
    dir = "file://${var.dir}"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "hash",
		"-c", "project.hcl",
		"--env", "local",
		"--var", "dir=migrations",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	_, err = os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
}

func TestCompatCommand_AdapterCommandUsesAttachedConfigShorthand(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte("CREATE TABLE users (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("project.hcl", []byte(`variable "dir" {}

env "local" {
  migration {
    dir = "file://${var.dir}"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "hash",
		"-cproject.hcl",
		"--env", "local",
		"--var", "dir=migrations",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	_, err = os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
}

func TestCompatCommand_AdapterCommandUsesParentAtlasProjectFlags(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte("CREATE TABLE users (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("project.hcl", []byte(`variable "dir" {}

env "local" {
  migration {
    dir = "file://${var.dir}"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate",
		"-c", "project.hcl",
		"--env", "local",
		"--var", "dir=migrations",
		"hash",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	_, err = os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
}

// The opt-in is set because this fixture lints a destructive migration with no
// dev database on purpose: replaying `ALTER TABLE users DROP COLUMN legacy`
// against an empty one would fail for a reason that has nothing to do with the
// `--var` value reaching the native loader, which is what this test measures.
func TestCompatCommand_AdapterCommandForwardsAtlasProjectConfigToNativeLoader(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "1")
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_drop_column.up.sql"), []byte("ALTER TABLE users DROP COLUMN legacy;\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_drop_column.down.sql"), []byte("ALTER TABLE users ADD COLUMN legacy TEXT;\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("project.hcl", []byte(`variable "dir" {}

env "ci" {
  migration {
    dir = "file://${var.dir}"
  }
  lint {
    destructive {
      error = false
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate",
		"--config", "project.hcl",
		"--env", "ci",
		"--var", "dir=migrations",
		"lint",
		"--latest", "1",
		"--format", "{{ json . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	var report struct {
		Files []struct {
			Findings []migrationlint.Finding `json:"Findings"`
		} `json:"Files"`
	}
	c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
	c.Assert(report.Files, qt.HasLen, 1)
	c.Assert(report.Files[0].Findings, qt.HasLen, 1)
	c.Assert(report.Files[0].Findings[0].Rule, qt.Equals, "DS103")
	c.Assert(report.Files[0].Findings[0].Severity, qt.Equals, migrationlint.SeverityWarning)
}

func TestCompatCommand_SchemaInspectOutputsAtlasHCLWithoutNativeBanners(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect.db")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_SchemaInspectOutputsHCLFormatAlias(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect-hcl.db")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://" + dbPath,
		"--format", "hcl",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `table "users"`)
	c.Assert(out.String(), qt.Contains, `column "email"`)
	c.Assert(out.String(), qt.Not(qt.Contains), "Reading schema from database")
}

func TestCompatCommand_SchemaInspectOutputsSQLFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect-sql.db")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_SchemaInspectOutputsJSONFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect-json.db")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://" + dbPath,
		"--format", "json",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	var result atlasSchemaInspectJSONResult
	c.Assert(json.Unmarshal(out.Bytes(), &result), qt.IsNil)
	c.Assert(result.Schemas, qt.HasLen, 1)
	c.Assert(result.Schemas[0].Name, qt.Equals, "main")
	users := atlasSchemaInspectJSONTableByName(c, result.Schemas[0].Tables, "users")
	c.Assert(users.Columns, qt.HasLen, 2)
	email := atlasSchemaInspectJSONColumnByName(c, users.Columns, "email")
	c.Assert(email.Type, qt.Equals, "TEXT")
	c.Assert(email.Null, qt.IsFalse)
	c.Assert(users.PrimaryKey, qt.IsNotNil)
	c.Assert(users.PrimaryKey.Name, qt.Equals, "")
	c.Assert(users.PrimaryKey.Parts, qt.DeepEquals, []atlasSchemaInspectJSONIndexPartResult{{Column: "id"}})
	c.Assert(users.Indexes, qt.HasLen, 1)
	c.Assert(users.Indexes[0].Name, qt.Equals, "users_email_key")
	c.Assert(users.Indexes[0].Unique, qt.IsTrue)
	c.Assert(users.Indexes[0].Parts, qt.DeepEquals, []atlasSchemaInspectJSONIndexPartResult{{Column: "email"}})
	posts := atlasSchemaInspectJSONTableByName(c, result.Schemas[0].Tables, "posts")
	c.Assert(posts.ForeignKeys, qt.HasLen, 1)
	c.Assert(posts.ForeignKeys[0].Columns, qt.DeepEquals, []string{"user_id"})
	c.Assert(posts.ForeignKeys[0].References.Table, qt.Equals, "users")
	c.Assert(posts.ForeignKeys[0].References.Columns, qt.DeepEquals, []string{"id"})
}

func TestCompatCommand_SchemaInspectFormatsCustomTemplate(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect-template.db")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://" + dbPath,
		"--format", `{{ len .Realm.Schemas }}/{{ len (index .Schema.Schemas 0).Tables }}/{{ base64url "a+b/c=" }}/{{ printf "%.6s" (sql .) }}`,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1/2/a-b_c/CREATE")
}

func TestCompatCommand_SchemaInspectRejectsInvalidFormatBeforeConnect(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://does-not-need-to-exist.db",
		"--format", "{{ if }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*`)
	c.Assert(out.String(), qt.Not(qt.Contains), "connect to --url")
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

func TestCompatCommand_SchemaInspectRejectsUnsupportedFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect-unsupported-format.db")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "inspect", "--url", "sqlite://" + dbPath, "--format", "{{ split . }}"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*split requires hcl or sql schema output`)
}

func TestCompatCommand_SchemaInspectWritesSplitSQLFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect-write-sql.db")
	outDir := filepath.Join(dir, "schema")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://" + dbPath,
		"--format", `{{ sql . | split | write "` + outDir + `" }}`,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	mainSQL := readAtlasTestFile(c, outDir, "main.sql")
	c.Assert(mainSQL, qt.Contains, "-- atlas:import ./tables/posts.sql")
	c.Assert(mainSQL, qt.Contains, "-- atlas:import ./tables/users.sql")
	usersSQL := readAtlasTestFile(c, filepath.Join(outDir, "tables"), "users.sql")
	c.Assert(usersSQL, qt.Contains, "CREATE TABLE")
	c.Assert(usersSQL, qt.Contains, "users")
	postsSQL := readAtlasTestFile(c, filepath.Join(outDir, "tables"), "posts.sql")
	c.Assert(postsSQL, qt.Contains, "REFERENCES")
}

func TestCompatCommand_SchemaInspectWritesSplitHCLFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect-write-hcl.db")
	outDir := filepath.Join(dir, "schema-hcl")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://" + dbPath,
		"--format", `{{ hcl . | split | write "` + outDir + `" }}`,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	// Schema-qualified since the table declares its schema (stokaro/ptah#1234).
	usersHCL := readAtlasTestFile(c, filepath.Join(outDir, "tables"), "main_users.hcl")
	c.Assert(usersHCL, qt.Contains, `table "users"`)
	c.Assert(usersHCL, qt.Contains, `column "email"`)
}

func TestCompatCommand_SchemaInspectAllowsLiteralUnsupportedFormatWords(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect-literal-format.db")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--url", "sqlite://" + dbPath,
		"--format", `{{ printf "split/write text only" }}`,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "split/write text only")
}

func TestCompatCommand_SchemaInspectExcludeFiltersResources(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "inspect-exclude.db")
	createAtlasInspectSQLiteSchema(c, dbPath)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "inspect", "--url", "sqlite://" + dbPath, "--exclude", "posts"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `table "users"`)
	c.Assert(out.String(), qt.Not(qt.Contains), `table "posts"`)
	c.Assert(out.String(), qt.Not(qt.Contains), `posts_user_fk`)
}

func TestCompatCommand_SchemaInspectUsesAtlasProjectFormatAndSchemaMode(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "inspect-env.db")
	createAtlasInspectSQLiteSchema(c, dbPath)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  schema {
    mode {
      tables = false
    }
  }
  format {
    schema {
      inspect = "json"
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "inspect",
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	// `mode { tables = false }` drops the tables, not the schema they lived in.
	// The document still describes `main`, which is what the pinned community
	// binary v1.3.0 does with the same project file: measured on SQLite, it
	// renders `schema "main" {}` — in fact it renders the tables too, so it
	// ignores this mode block entirely on inspect. Rendering `{}` for a
	// database that has a schema is the defect stokaro/ptah#1264 is about, and
	// this row is the one place it survived a filter rather than an empty
	// database.
	c.Assert(out.String(), qt.Equals, `{"schemas":[{"name":"main"}]}`)
}

// TestCompatCommand_SchemaInspectRejectsProOnlyOutputFlags pins the inspect
// flags Atlas registers that compat deliberately does not.
//
// The list has shrunk to --export. --output is implemented
// (schema_inspect_output_test.go) and --web is a registered refusal
// (schema_ui_flags_test.go); --export on inspect is the twin of the
// `schema diff --export` decision and stays a separate item of
// stokaro/ptah#951. --include is registered and covered by
// schema_inspect_include_test.go.
func TestCompatCommand_SchemaInspectRejectsProOnlyOutputFlags(t *testing.T) {
	c := qt.New(t)

	for _, flag := range []string{"--export"} {
		c.Run(flag, func(c *qt.C) {
			cmd := NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{"schema", "inspect", "--url", "sqlite://inspect.db", flag, "x"})

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, "unknown flag: "+flag)
		})
	}
}

func TestCompatCommand_ForwardsParentedNativeCommand(t *testing.T) {
	c := qt.New(t)
	root := NewCompatCommand("atlas")
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"migrate", "apply"})

	err := root.Execute()

	c.Assert(err, qt.ErrorMatches, "database URL is required")
}

func TestCompatCommand_MigrateNewCreatesAtlasSkeletonFileByDefault(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	// The scheme is required on this verb since stokaro/ptah#1186: it WRITES,
	// and the community binary refuses `--dir <bare path>` there.
	cmd.SetArgs([]string{"migrate", "new", "manual_hotfix", "--dir", "file://" + dir})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Generated empty migration file:")
	matches, globErr := filepath.Glob(filepath.Join(dir, "*_manual_hotfix.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	downMatches, globErr := filepath.Glob(filepath.Join(dir, "*.down.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(downMatches, qt.HasLen, 0)
	_, err = os.Stat(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
}

func TestCompatCommand_MigrateNewAcceptsExplicitAtlasDirFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "new", "manual_hotfix", "--dir", "file://" + dir, "--dir-format", "atlas"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Generated empty migration file:")
	matches, globErr := filepath.Glob(filepath.Join(dir, "*_manual_hotfix.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	_, err = os.Stat(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
}

func TestCompatCommand_MigrateHashDefaultsToAtlasSum(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeAtlasApplyMigration(c, dir, "20260723120000_init.sql", "CREATE TABLE users (id integer primary key)")
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "hash", "--dir", "file://" + dir})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	_, err = os.Stat(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(filepath.Join(dir, "ptah.sum"))
	c.Assert(os.IsNotExist(err), qt.IsTrue)
}

func TestCompatCommand_MigrateStatusReadsAtlasRevisionsByDefault(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "status.db")
	writeAtlasApplyMigration(c, dir, "20260723120000_init.sql", "CREATE TABLE users (id integer primary key)")

	apply := NewCompatCommand("atlas")
	var applyOut bytes.Buffer
	apply.SetOut(&applyOut)
	apply.SetErr(&applyOut)
	apply.SetArgs([]string{"migrate", "apply", "--url", "sqlite://" + dbPath, "--dir", "file://" + dir})
	err := apply.Execute()
	c.Assert(err, qt.IsNil)

	status := NewCompatCommand("atlas")
	var statusOut bytes.Buffer
	status.SetOut(&statusOut)
	status.SetErr(&statusOut)
	status.SetArgs([]string{"migrate", "status", "--url", "sqlite://" + dbPath, "--dir", "file://" + dir})
	err = status.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(statusOut.String(), qt.Contains, "-- Current Version: 20260723120000")
	c.Assert(statusOut.String(), qt.Contains, "Migration Status: OK")
}

func TestCompatCommand_MigrateStatusFormatRendersAtlasReport(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "status-format.db")
	writeAtlasApplyMigration(c, dir, "20260723120000_init.sql", "CREATE TABLE users (id integer primary key)")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "status",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + dir,
		"--format", "{{ .Status }}|{{ .Current }}|{{ .Next }}|{{ len .Available }}|{{ len .Pending }}|{{ (index .Pending 0).Name }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "PENDING|No migration applied yet|20260723120000|1|1|20260723120000_init.sql")
}

func TestCompatCommand_MigrateStatusFormatRendersAppliedRevisionReport(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "status-applied-format.db")
	writeAtlasApplyMigration(c, dir, "20260723120000_create_users.sql", "CREATE TABLE users (id integer primary key)")

	apply := NewCompatCommand("atlas")
	var applyOut bytes.Buffer
	apply.SetOut(&applyOut)
	apply.SetErr(&applyOut)
	apply.SetArgs([]string{"migrate", "apply", "--url", "sqlite://" + dbPath, "--dir", "file://" + dir})
	err := apply.Execute()
	c.Assert(err, qt.IsNil)

	status := NewCompatCommand("atlas")
	var statusOut bytes.Buffer
	status.SetOut(&statusOut)
	status.SetErr(&statusOut)
	status.SetArgs([]string{
		"migrate", "status",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + dir,
		"--format", "{{ .Status }}|{{ len .Applied }}|{{ (index .Applied 0).Version }}|{{ (index .Applied 0).Description }}|{{ (index .Applied 0).Type }}|{{ (index .Applied 0).Applied }}|{{ (index .Applied 0).Total }}|{{ (index .Applied 0).OperatorVersion }}",
	})
	err = status.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(statusOut.String(), qt.Equals, "OK|1|20260723120000|create_users|applied|1|1|Ptah")
}

func TestCompatCommand_MigrateStatusUsesDefaultDirWithoutEnvWhenURLExplicit(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "status-default-dir.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "20260723120000_init.sql", "CREATE TABLE users (id integer primary key)")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://local.db"
}
env "prod" {
  url = "sqlite://prod.db"
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "status",
		"--url", "sqlite://" + dbPath,
		"--format", "{{ .Status }}|{{ len .Pending }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "PENDING|1")
}

func TestCompatCommand_MigrateStatusUsesAtlasProjectFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "status-config-format.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "20260723120000_init.sql", "CREATE TABLE users (id integer primary key)")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://migrations"
  }
  format {
    migrate {
      status = "{{ .Status }}|{{ len .Pending }}"
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "status",
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "PENDING|1")
}

func TestCompatCommand_MigrateStatusRejectsInvalidFormatBeforeConnecting(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "status-invalid-format.db")
	writeAtlasApplyMigration(c, dir, "20260723120000_init.sql", "CREATE TABLE users (id integer primary key)")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "status",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + dir,
		"--format", "{{ if }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*`)
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestCompatCommand_MigrateLintFormatRendersAtlasFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeAtlasApplyMigration(c, dir, "20260723120000_init.sql", "CREATE TABLE users (id integer primary key)")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", "file://" + dir,
		"--latest", "1",
		"--dev-url", "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
		"--format", "{{ len .Files }}|{{ (index .Files 0).Name }}|{{ printf \"%.6s\" (index .Files 0).Text }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1|20260723120000_init.sql|CREATE")
}

// This test analyzes with no dev database, which is Ptah's own capability and
// is refused by default so the compatibility surface matches the community
// binary's required --dev-url (stokaro/ptah#1231 case 2). The opt-in keeps the
// subject of the test -- where the format template comes from -- unchanged.
func TestCompatCommand_MigrateLintUsesAtlasProjectFormat(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "1")
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "20260723120000_init.sql", "CREATE TABLE users (id integer primary key)")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  migration {
    dir = "file://migrations"
  }
  format {
    migrate {
      lint = "{{ len .Files }}|{{ (index .Files 0).Name }}"
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--env", "local",
		"--latest", "1",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1|20260723120000_init.sql")
}

// The opt-in is set for the same reason as in the test above: the subject is
// the config-relative directory, not the dev-database precondition.
func TestCompatCommand_MigrateLintUsesConfigRelativeDirOutsideConfigDirectory(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "1")
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	otherDir := filepath.Join(dir, "other")
	migrationsDir := filepath.Join(projectDir, "migrations")
	c.Assert(os.MkdirAll(otherDir, 0o755), qt.IsNil)
	writeAtlasApplyMigration(c, migrationsDir, "20260723120000_init.sql", "CREATE TABLE users (id integer primary key)")
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "ci" {
  migration {
    dir = "file://migrations"
  }
  format {
    migrate {
      lint = "{{ len .Files }}|{{ (index .Files 0).Name }}"
    }
  }
}
`), 0o600), qt.IsNil)
	t.Chdir(otherDir)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate",
		"--config", "file://" + filepath.Join(projectDir, "atlas.hcl"),
		"--env", "ci",
		"lint",
		"--latest", "1",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1|20260723120000_init.sql")
}

func TestCompatCommand_MigrateLintFormatRendersReplayFailure(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "lint-replay-failure.db")
	// Deliberately unhashed: `migrate lint` reads the directory without the
	// apply-time integrity gate, and an unhashed directory keeps the two-step
	// report this test pins (a hashed one adds its own checksum step, which
	// TestCompatCommand_MigrateLintFormatReportsInvalidAtlasSum covers).
	c.Assert(os.WriteFile(
		filepath.Join(dir, "20260723120000_init.sql"),
		[]byte("CREATE TABLE users (id integer primary key); SELECT * FROM missing_table;\n"),
		0o600,
	), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", "file://" + dir,
		"--dev-url", "sqlite://" + dbPath,
		"--latest", "1",
		"--format", "{{ len .Steps }}|{{ (index .Steps 1).Text }}|{{ (index .Steps 1).Error }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `(?s)error validating migration SQL on dev database: .*`)
	c.Assert(out.String(), qt.Contains, "2|Failed loading changes on dev database|error validating migration SQL on dev database:")
}

func TestCompatCommand_MigrateLintFormatReportsInvalidAtlasSum(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "lint-invalid-sum.db")
	writeAtlasApplyMigration(c, dir, "20260723120000_init.sql", "CREATE TABLE users (id integer primary key)")
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.sum"), []byte("stale\n"), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", "file://" + dir,
		"--dev-url", "sqlite://" + dbPath,
		"--latest", "1",
		"--format", "{{ len .Steps }}|{{ (index .Steps 0).Text }}|{{ (index .Files 0).Name }}|{{ (index .Files 0).Error }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "checksum mismatch")
	c.Assert(out.String(), qt.Equals, "1|File atlas.sum is invalid|atlas.sum|checksum mismatch")
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestCompatCommand_MigrateLintRejectsInvalidFormatBeforeReplay(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(t.TempDir(), "lint-invalid-format.db")
	writeAtlasApplyMigration(c, dir, "20260723120000_init.sql", "CREATE TABLE users (id integer primary key)")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", "file://" + dir,
		"--dev-url", "sqlite://" + dbPath,
		"--latest", "1",
		"--format", "{{ if }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*`)
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestCompatCommand_MigrateSetAcceptsRevisionsSchema(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "set", "1",
		"--dir", "file://" + t.TempDir(),
		"--revisions-schema", "custom_revisions",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "database URL is required; pass --url")
	c.Assert(out.String(), qt.Contains, "database URL is required; pass --url")
	c.Assert(out.String(), qt.Not(qt.Contains), "unknown flag")
}

func TestCompatCommand_MigrateSetMapsPositionalRevision(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	dbPath := filepath.Join(dir, "set-positional.db")
	writeAtlasApplyMigration(c, migrationsDir, "1_set_positional.sql", "CREATE TABLE set_positional_users (id INTEGER PRIMARY KEY);")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "set", "1",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Current version is 1 (1 set)")
	c.Assert(sqliteAtlasAppliedVersions(c, dbPath), qt.DeepEquals, []string{"1"})
}

func TestCompatCommand_MigrateSetHelpShowsAtlasVersionArgument(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "set", "--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "atlas migrate set [flags] [version]")
}

func TestCompatCommand_MigrateSetFailurePathVersionArgument(t *testing.T) {
	c := qt.New(t)

	c.Run("missing version", func(c *qt.C) {
		cmd := NewCompatCommand("atlas")
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{
			"migrate", "set",
			"--url", "sqlite://" + filepath.Join(t.TempDir(), "state.db"),
			"--dir", "file://" + t.TempDir(),
		})

		err := cmd.Execute()

		c.Assert(err, qt.ErrorMatches, `accepts 1 arg\(s\), received 0`)
	})

	c.Run("multiple versions", func(c *qt.C) {
		cmd := NewCompatCommand("atlas")
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{
			"migrate", "set", "1", "2",
			"--url", "sqlite://" + filepath.Join(t.TempDir(), "state.db"),
			"--dir", "file://" + t.TempDir(),
		})

		err := cmd.Execute()

		c.Assert(err, qt.ErrorMatches, `accepts 1 arg\(s\), received 2`)
	})

	c.Run("native version flag", func(c *qt.C) {
		cmd := NewCompatCommand("atlas")
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{"migrate", "set", "--version", "1", "--url", "sqlite://state.db", "--dir", "file://" + t.TempDir()})

		err := cmd.Execute()

		c.Assert(err, qt.ErrorMatches, "unknown flag: --version")
	})
}

// TestCompatCommand_MigrateMetadataRejectsUnsupportedAtlasDirFormat covers the
// metadata verbs that still accept only the atlas layout.
//
// Six verbs are deliberately absent. Five of them READ a directory rather than
// rewrite one, so a foreign layout can be converted in memory and reported on:
// `migrate hash` and `migrate validate` have been in that set since #992 (see
// migrate_integrity_formats_test.go), `migrate status` and `migrate set` joined
// it in #1002 (see migrate_revision_converted_test.go), and `migrate lint`
// joined it in #1013 (see migrate_lint_converted_test.go). `migrate new` is the
// sixth and the only one that WRITES: since stokaro/ptah#845 it emits the
// selected layout's own skeleton files (see migrate_new_converted_test.go).
//
// The verbs that remain here rewrite migration bodies — an editor round trip, a
// renumbering, a removal, a replay — and none of them has anything to emit those
// bodies back in a source tool's convention.
func TestCompatCommand_MigrateMetadataRejectsUnsupportedAtlasDirFormat(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "edit",
			args: []string{"migrate", "edit", "1", "--dir", "file://" + t.TempDir(), "--dir-format", "goose"},
		},
		{
			name: "rebase",
			args: []string{"migrate", "rebase", "1", "--dir", "file://" + t.TempDir(), "--dir-format", "flyway"},
		},
		{
			name: "rm",
			args: []string{"migrate", "rm", "1", "--dir", "file://" + t.TempDir(), "--dir-format", "liquibase"},
		},
		{
			name: "test",
			args: []string{"migrate", "test", "--dir", "file://" + t.TempDir(), "--dir-format", "goose"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(tt.args)

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, `atlas migrate .* --dir-format: Atlas accepts --dir-format=.* but Ptah does not implement that directory format yet`)
		})
	}
}

func TestCompatCommand_MigrateApplyRejectsDirFormatFlag(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "apply", "--dir-format", "atlas"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "unknown flag: --dir-format")
	c.Assert(out.String(), qt.Contains, "unknown flag: --dir-format")
}

func TestCompatCommand_SchemaFmtFormatsHCLFiles(t *testing.T) {
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
	c.Assert(string(formatted), qt.Contains, `schema "main" {}`)
	c.Assert(string(formatted), qt.Contains, "schema = schema.main")
	c.Assert(string(formatted), qt.Not(qt.Contains), "schema=schema.main")
}

func TestCompatCommand_SchemaDiffPrintsLocalFileDiff(t *testing.T) {
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

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"-f", "file://" + from,
		"--to", "file://" + to,
		"--dev-url", "postgres://localhost/dev",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `ALTER TABLE "users" ADD COLUMN "email" varchar(255) NOT NULL;`)
}

func TestCompatCommand_SchemaDiffReportsSyncedLocalFiles(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`
table "users" {
  column "id" {
    type = int
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_SchemaDiffRejectsUnsupportedRemoteTarget(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "atlas://remote/schema",
		"--to", "file://schema.hcl",
		"--dev-url", "sqlite://dev?mode=memory",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--from "atlas://remote/schema": atlas:// registry URLs are not supported; use oci://.*`)
	c.Assert(out.String(), qt.Contains, `Error: --from "atlas://remote/schema": atlas:// registry URLs are not supported`)
}

func TestCompatCommand_SchemaDiffFormatsCustomSQLTemplate(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	from := filepath.Join(dir, "from.hcl")
	to := filepath.Join(dir, "to.hcl")
	c.Assert(os.WriteFile(from, []byte(`
table "users" {
  column "id" {
    type = int
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
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + from,
		"--to", "file://" + to,
		"--dev-url", "postgres://localhost/dev",
		"--format", `{{ len .Changes }}|{{ printf "%.2s" (.MarshalSQL) }}|{{ sql . "  " }}`,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `1|--|  -- Add/modify columns for table: users --`)
	c.Assert(out.String(), qt.Contains, `  ALTER TABLE "users" ADD COLUMN "email" varchar(255) NOT NULL;`)
}

func TestCompatCommand_SchemaDiffFormatsSyncedCustomTemplate(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`
table "users" {
  column "id" {
    type = int
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + path,
		"--to", "file://" + path,
		"--dev-url", "sqlite://dev?mode=memory",
		"--format", `{{ with .Changes }}changed{{ else }}synced{{ end }}`,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "synced")
}

func TestCompatCommand_SchemaDiffUsesAtlasProjectEnvDefaultsAndDiffSkip(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	from := filepath.Join(dir, "from.hcl")
	to := filepath.Join(dir, "to.hcl")
	c.Assert(os.WriteFile(from, []byte(`
table "old_users" {
  column "id" {
    type = int
  }
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(to, []byte(`schema "main" {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  dev = "sqlite://dev.db"
  schema {
    src = "file://to.hcl"
  }
  format {
    schema {
      diff = "{{ with .Changes }}changed{{ else }}synced{{ end }}"
    }
  }
  diff {
    skip {
      drop_table = true
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--env", "local",
		"--from", "file://" + from,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "synced")
}

func TestCompatCommand_SchemaDiffUsesConfigPathAndVariableOverride(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	from := filepath.Join(dir, "from.hcl")
	to := filepath.Join(dir, "to.hcl")
	c.Assert(os.WriteFile(from, []byte(`
table "users" {
  column "id" {
    type = int
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
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("custom-atlas.hcl", []byte(`variable "to_path" {
  default = "wrong.hcl"
}

env "local" {
  dev = "postgres://localhost/dev"
  schema {
    src = "file://${var.to_path}"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema",
		"--config", "custom-atlas.hcl",
		"--env", "local",
		"--var", "to_path=to.hcl",
		"diff",
		"--from", "file://" + from,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `ALTER TABLE "users" ADD COLUMN "email" varchar(255) NOT NULL;`)
}

func TestCompatCommand_SchemaDiffRejectsMalformedVariableOverride(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	from := filepath.Join(dir, "from.hcl")
	c.Assert(os.WriteFile(from, []byte(`schema "main" {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`variable "destructive" {}

env "local" {
  dev = "postgres://localhost/dev"
  schema {
    src = "file://to.hcl"
  }
  diff {
    skip {
      drop_table = !var.destructive
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--env", "local",
		"--var", "destructive",
		"--from", "file://" + from,
	})

	err := cmd.Execute()

	// The wording is the pinned Atlas community binary v1.3.0's own, measured
	// on 2026-08-08: it refuses the flag's SYNTAX while parsing flags, so the
	// refusal is pflag's `invalid argument %q for %q flag: %v` wrapper around
	// its --var value parser rather than anything the project loader says.
	// Before this it was Ptah's config/projectconfig message, which only fired
	// once an atlas.hcl had been found -- and a directory with no atlas.hcl got
	// no refusal at all (stokaro/ptah#1241).
	c.Assert(err, qt.ErrorMatches, `invalid argument "destructive" for "--var" flag: variables must be format as key=value, got: "destructive"`)
	c.Assert(out.String(), qt.Contains, `Error: invalid argument "destructive" for "--var" flag: variables must be format as key=value, got: "destructive"`)
}

func TestCompatCommand_SchemaDiffUsesAtlasProjectDefaultsWithExplicitTargetFlags(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	from := filepath.Join(dir, "from.hcl")
	to := filepath.Join(dir, "to.hcl")
	c.Assert(os.WriteFile(from, []byte(`
table "old_users" {
  column "id" {
    type = int
  }
}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(to, []byte(`schema "main" {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  format {
    schema {
      diff = "{{ with .Changes }}changed{{ else }}synced{{ end }}"
    }
  }
  diff {
    skip {
      drop_table = true
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + from,
		"--to", "file://" + to,
		"--dev-url", "sqlite://dev.db",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "synced")
}

// TestCompatCommand_SchemaDiffAcceptsConcurrentIndexDropPolicy pins that
// diff.concurrent_index.drop is decoded and carried into planning instead of
// aborting the run. Reverting the policy wiring prints
// `Error: atlas.hcl diff.concurrent_index.drop is not supported yet` and the
// command exits non-zero, which is what the pinned community binary does not do
// with the same file.
func TestCompatCommand_SchemaDiffAcceptsConcurrentIndexDropPolicy(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	from := filepath.Join(dir, "from.hcl")
	to := filepath.Join(dir, "to.hcl")
	c.Assert(os.WriteFile(from, []byte(`schema "main" {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(to, []byte(`schema "main" {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  dev = "sqlite://dev.db"
  diff {
    concurrent_index {
      drop = true
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--env", "local",
		"--from", "file://from.hcl",
		"--to", "file://to.hcl",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Not(qt.Contains), "not supported yet")
}

// TestAtlasDiffPolicy_CarriesConcurrentIndexDrop is the unit-level half: the
// decoded config must reach the planning policy, not just avoid an error.
// Reverting the wiring prints "values are not equal: true != false" for the
// ConcurrentIndexDrop field.
func TestAtlasDiffPolicy_CarriesConcurrentIndexDrop(t *testing.T) {
	tests := []struct {
		name   string
		config func() projectconfig.Config
		want   atlasschema.DiffPolicy
	}{
		{
			name: "drop requested",
			config: func() projectconfig.Config {
				var cfg projectconfig.Config
				cfg.Diff.ConcurrentIndex.Drop = projectconfig.ConfigBool{Value: true, Set: true}
				return cfg
			},
			want: atlasschema.DiffPolicy{ConcurrentIndexDrop: true},
		},
		{
			name: "drop explicitly declined",
			config: func() projectconfig.Config {
				var cfg projectconfig.Config
				cfg.Diff.ConcurrentIndex.Drop = projectconfig.ConfigBool{Value: false, Set: true}
				return cfg
			},
			want: atlasschema.DiffPolicy{},
		},
		{
			name:   "drop unset",
			config: func() projectconfig.Config { return projectconfig.Config{} },
			want:   atlasschema.DiffPolicy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasDiffPolicy(tt.config())

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestCompatCommand_SchemaDiffRejectsInvalidFormatBeforeLoadingFiles(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://from.hcl",
		"--to", "file://to.hcl",
		"--dev-url", "sqlite://dev?mode=memory",
		"--format", "{{ if }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*`)
	c.Assert(out.String(), qt.Not(qt.Contains), "load --from schema")
}

func TestCompatCommand_SchemaApplyAppliesLocalSchemaToSQLite(t *testing.T) {
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

	first := NewCompatCommand("atlas")
	var firstOut bytes.Buffer
	first.SetOut(&firstOut)
	first.SetErr(&firstOut)
	first.SetArgs([]string{
		"schema", "apply",
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--auto-approve",
	})

	err := first.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(firstOut.String(), qt.Contains, "Planned schema changes:")
	c.Assert(firstOut.String(), qt.Contains, "CREATE TABLE")
	c.Assert(firstOut.String(), qt.Contains, "Schema apply completed successfully.")
	assertSQLiteTableExists(c, dbPath, "users")

	second := NewCompatCommand("atlas")
	var secondOut bytes.Buffer
	second.SetOut(&secondOut)
	second.SetErr(&secondOut)
	second.SetArgs([]string{
		"schema", "apply",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev-940.db"),
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
	})

	err = second.Execute()

	c.Assert(err, qt.IsNil)
	// No trailing period: this is the byte-exact form the pinned community
	// binary v1.3.0 writes for `schema apply` (stokaro/ptah#1235 finding 9.4).
	// Its `schema diff` answer keeps its period and already matched.
	c.Assert(secondOut.String(), qt.Equals, "Schema is synced, no changes to be made\n")
}

func TestCompatCommand_SchemaApplyDryRunDoesNotApply(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "dry-run.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev-940.db"),
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

func TestCompatCommand_SchemaApplyFileShorthandDryRun(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "file-shorthand-dry-run.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev-940.db"),
		"--url", "sqlite://" + dbPath,
		"-f", schemaPath,
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Planned schema changes:")
	c.Assert(out.String(), qt.Contains, "CREATE TABLE")
	assertSQLiteTableMissing(c, dbPath, "users")
}

func TestCompatCommand_SchemaApplyAcceptsTxMode(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tx-mode-command.db")
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE tx_mode_users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev-940.db"),
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

func TestCompatCommand_SchemaApplyRejectsInvalidTxMode(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE tx_mode_users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
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
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev-940.db"),
		"--url", "sqlite://" + dbPath,
		"--to", "file://" + schemaPath,
		"--dry-run",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Planned schema changes:")
	assertSQLiteTableMissing(c, dbPath, "users")
}

func TestCompatCommand_SchemaApplyRejectsDevURLDialectMismatch(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE users (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_FlagSurfaceRejectsUnsupportedAtlasCEBehavior(t *testing.T) {
	c := qt.New(t)

	c.Run("migrate_diff_qualifier_invalid_value", func(c *qt.C) {
		cmd := NewCompatCommand("atlas")
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{"migrate", "diff", "--to", "file://schema.sql", "--dev-url", "sqlite://dev.db", "--qualifier", "bad.name"})

		err := cmd.Execute()

		// Invalid qualifier values fail before the dev database is contacted
		// and before any migration file or checksum is written.
		c.Assert(err, qt.ErrorMatches, `invalid --qualifier "bad\.name": character '\.' is not allowed in a schema qualifier`)
	})

	c.Run("migrate_diff_qualifier_unsupported_dialect", func(c *qt.C) {
		dir := c.TempDir()
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		schemaPath := filepath.Join(dir, "schema.sql")
		c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE users (id INTEGER PRIMARY KEY);
`), 0o600), qt.IsNil)
		cmd := NewCompatCommand("atlas")
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{
			"migrate", "diff",
			"--to", "file://" + schemaPath,
			"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
			"--dir", "file://" + migrationsDir,
			"--qualifier", "tenant",
			"qualified",
		})

		err := cmd.Execute()

		// A valid qualifier on a dialect without schema-qualified DDL support
		// fails explicitly before any migration file or checksum is written.
		c.Assert(err, qt.ErrorMatches, `atlas migrate diff --qualifier is not supported for dialect "sqlite"`)
		c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 0)
	})

	c.Run("schema_apply_plan_registry_url", func(c *qt.C) {
		cmd := NewCompatCommand("atlas")
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{"schema", "apply", "--url", "sqlite://apply.db", "--plan", "atlas://repo/plans/apply"})

		err := cmd.Execute()

		// Local plan files are implemented (see schema_apply_plan_test.go);
		// registry plan URLs remain a loud rejection.
		c.Assert(err, qt.ErrorMatches, `atlas schema apply accepts registry plan URLs like "atlas://repo/plans/apply", but Ptah has no plan registry; pass a local plan file saved by .schema plan. as --plan file://<path>`)
	})

	c.Run("schema_apply_lock_timeout_invalid", func(c *qt.C) {
		cmd := NewCompatCommand("atlas")
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{"schema", "apply", "--url", "sqlite://apply.db", "--to", "file://schema.sql", "--lock-timeout", "soon"})

		err := cmd.Execute()

		// --lock-timeout is implemented (see schema_apply_lock_test.go);
		// malformed values fail before the target database is touched.
		c.Assert(err, qt.ErrorMatches, `invalid --lock-timeout: time: invalid duration "soon"`)
	})
}

func TestCompatCommand_MigrateApplyAmountSQLite(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "apply-migrations.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_one.sql", "CREATE TABLE apply_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "2_two.sql", "CREATE TABLE apply_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "3_three.sql", "CREATE TABLE apply_three (id INTEGER PRIMARY KEY);")

	first := NewCompatCommand("atlas")
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

	second := NewCompatCommand("atlas")
	var secondOut bytes.Buffer
	second.SetOut(&secondOut)
	second.SetErr(&secondOut)
	second.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"1",
	})

	err = second.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(secondOut.String(), qt.Contains, "Migrating to version 3 from 1 pending migrations.")
	c.Assert(secondOut.String(), qt.Contains, "Migration complete. Current version: 3")
	assertSQLiteTableExists(c, dbPath, "apply_three")
}

func TestCompatCommand_MigrateApplyBaselineUsesAtlasRevisions(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "baseline.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_one.sql", "CREATE TABLE baseline_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "2_two.sql", "CREATE TABLE baseline_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "3_three.sql", "CREATE TABLE baseline_three (id INTEGER PRIMARY KEY);")

	cmd := NewCompatCommand("atlas")
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
	c.Assert(sqliteAtlasAppliedVersions(c, dbPath), qt.DeepEquals, []string{"2", "3"})
}

func TestCompatCommand_MigrateApplyDryRunBaselinePlansRemainingMigrations(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "baseline-dry-run.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_one.sql", "CREATE TABLE dry_baseline_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "2_two.sql", "CREATE TABLE dry_baseline_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "3_three.sql", "CREATE TABLE dry_baseline_three (id INTEGER PRIMARY KEY);")

	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_MigrateApplyFormatsJSONResult(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "format-json.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_create_users.sql", `
CREATE TABLE format_json_users (id INTEGER PRIMARY KEY);
CREATE TABLE format_json_posts (id INTEGER PRIMARY KEY);
`)
	dbURL := "sqlite://user:secret@" + dbPath + "?password=hidden"

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", dbURL,
		"--dir", "file://" + migrationsDir,
		"--format", "{{ json . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Not(qt.Contains), "Migrating to version")
	c.Assert(out.String(), qt.Not(qt.Contains), "Migration complete")

	var result atlasMigrateApplyJSONResult
	c.Assert(json.Unmarshal(out.Bytes(), &result), qt.IsNil)
	c.Assert(result.Driver, qt.Equals, "sqlite")
	c.Assert(result.URL.Scheme, qt.Equals, "sqlite")
	c.Assert(result.URL.Path, qt.Equals, dbPath)
	c.Assert(result.URL.RawQuery, qt.Equals, "password=xxxxx")
	c.Assert(result.URL.Schema, qt.Equals, "main")
	c.Assert(result.Current, qt.Equals, "")
	c.Assert(result.Target, qt.Equals, "1")
	c.Assert(result.Message, qt.Equals, "Migrated to version 1 from  (1 migrations in total)")
	c.Assert(result.Pending, qt.HasLen, 1)
	c.Assert(result.Pending[0].Name, qt.Equals, "1_create_users.sql")
	c.Assert(result.Pending[0].Version, qt.Equals, "1")
	c.Assert(result.Pending[0].Description, qt.Equals, "create_users")
	c.Assert(result.Applied, qt.HasLen, 1)
	c.Assert(result.Applied[0].Name, qt.Equals, "1_create_users.sql")
	c.Assert(result.Applied[0].Applied, qt.DeepEquals, []string{
		"CREATE TABLE format_json_users (id INTEGER PRIMARY KEY)",
		"CREATE TABLE format_json_posts (id INTEGER PRIMARY KEY)",
	})
	assertSQLiteTableExists(c, dbPath, "format_json_users")
	assertSQLiteTableExists(c, dbPath, "format_json_posts")
	c.Assert(sqliteAtlasAppliedVersions(c, dbPath), qt.DeepEquals, []string{"1"})
}

func TestCompatCommand_MigrateApplyFormatsCustomTemplate(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "format-template.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_one.sql", "CREATE TABLE format_template_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "2_two.sql", "CREATE TABLE format_template_two (id INTEGER PRIMARY KEY);")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--format", "{{ .Env.Driver }}/{{ .Driver }}:{{ .Current }}>{{ .Target }}:{{ len .Pending }}:{{ len .Applied }}:{{ (index .Applied 0).Name }}",
		"1",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "sqlite/sqlite:>1:1:1:1_one.sql")
	assertSQLiteTableExists(c, dbPath, "format_template_one")
	assertSQLiteTableMissing(c, dbPath, "format_template_two")
}

func TestCompatCommand_MigrateApplyUsesAtlasProjectEnvDefaultsAndFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "migrate-apply-env.db")
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	writeAtlasApplyMigration(c, migrationsDir, "1_apply_env.sql", "CREATE TABLE apply_env_users (id INTEGER PRIMARY KEY);")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://migrations"
  }
  format {
    migrate {
      apply = "{{ len .Applied }}"
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1")
	assertSQLiteTableExists(c, dbPath, "apply_env_users")
}

func TestCompatCommand_MigrateApplyUsesAtlasProjectDefaultsWithExplicitTargetFlags(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "migrate-apply-explicit-defaults.db")
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	writeAtlasApplyMigration(c, migrationsDir, "1_apply_explicit_defaults.sql", "CREATE TABLE apply_explicit_defaults (id INTEGER PRIMARY KEY);")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  format {
    migrate {
      apply = "{{ len .Applied }}"
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "1")
	assertSQLiteTableExists(c, dbPath, "apply_explicit_defaults")
}

func TestCompatCommand_MigrateApplyFormatsDryRunResult(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "format-dry-run.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_dry_run.sql", "CREATE TABLE format_dry_run (id INTEGER PRIMARY KEY);")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--dry-run",
		"--format", "{{ json . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Not(qt.Contains), "Dry run mode")
	var result atlasMigrateApplyJSONResult
	c.Assert(json.Unmarshal(out.Bytes(), &result), qt.IsNil)
	c.Assert(result.Target, qt.Equals, "1")
	c.Assert(result.Message, qt.Equals, "")
	c.Assert(result.Pending, qt.HasLen, 1)
	c.Assert(result.Applied, qt.HasLen, 0)
	assertSQLiteTableMissing(c, dbPath, "format_dry_run")
}

func TestCompatCommand_MigrateApplyFormatsDryRunBaselineResult(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "format-dry-baseline.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_one.sql", "CREATE TABLE format_dry_baseline_one (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "2_two.sql", "CREATE TABLE format_dry_baseline_two (id INTEGER PRIMARY KEY);")
	writeAtlasApplyMigration(c, migrationsDir, "3_three.sql", "CREATE TABLE format_dry_baseline_three (id INTEGER PRIMARY KEY);")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--baseline", "2",
		"--dry-run",
		"--format", "{{ json . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	var result atlasMigrateApplyJSONResult
	c.Assert(json.Unmarshal(out.Bytes(), &result), qt.IsNil)
	c.Assert(result.Current, qt.Equals, "2")
	c.Assert(result.Target, qt.Equals, "3")
	c.Assert(result.Pending, qt.HasLen, 1)
	c.Assert(result.Pending[0].Name, qt.Equals, "3_three.sql")
	c.Assert(result.Applied, qt.HasLen, 0)
	assertSQLiteTableMissing(c, dbPath, "format_dry_baseline_one")
	assertSQLiteTableMissing(c, dbPath, "format_dry_baseline_two")
	assertSQLiteTableMissing(c, dbPath, "format_dry_baseline_three")
}

func TestCompatCommand_MigrateApplyFormatsNoopResult(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "format-noop.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_noop.sql", "CREATE TABLE format_noop (id INTEGER PRIMARY KEY);")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
	})
	c.Assert(cmd.Execute(), qt.IsNil)

	cmd = NewCompatCommand("atlas")
	out.Reset()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--format", "{{ json . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	// The text printer terminates its line; the same sentence inside the JSON
	// document is followed by a quote. Asserting on the newline is what keeps
	// this row meaningful now that the two spellings are the same string
	// (stokaro/ptah#1235 finding 9.3 removed the text form's trailing period).
	c.Assert(out.String(), qt.Not(qt.Contains), "No migration files to execute\n")
	var result atlasMigrateApplyJSONResult
	c.Assert(json.Unmarshal(out.Bytes(), &result), qt.IsNil)
	c.Assert(result.Current, qt.Equals, "1")
	c.Assert(result.Target, qt.Equals, "1")
	c.Assert(result.Message, qt.Equals, "No migration files to execute")
	c.Assert(result.Pending, qt.HasLen, 0)
	c.Assert(result.Applied, qt.HasLen, 0)
}

func TestCompatCommand_MigrateApplyRejectsEmptyFormat(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "apply", "--format", ""})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--format must not be empty`)
}

func TestCompatCommand_MigrateApplyRejectsInvalidFormatBeforeApply(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "invalid-format.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_invalid_format.sql", "CREATE TABLE invalid_format_applied (id INTEGER PRIMARY KEY);")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--format", "{{ if }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*`)
	assertSQLiteTableMissing(c, dbPath, "invalid_format_applied")
}

func TestCompatCommand_MigrateApplyWritesFormatOnApplyError(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "format-error.db")
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_error.sql", "CREATE TABLE error_before (id INTEGER PRIMARY KEY); SELECT * FROM missing_table;")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--format", "{{ json . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `(?s)error applying migrations: .*missing_table.*`)
	var result atlasMigrateApplyJSONResult
	c.Assert(json.Unmarshal(out.Bytes(), &result), qt.IsNil)
	c.Assert(result.Error, qt.Contains, "missing_table")
	c.Assert(result.Pending, qt.HasLen, 1)
	c.Assert(result.Applied, qt.HasLen, 1)
	c.Assert(result.Applied[0].Applied, qt.DeepEquals, []string{
		"CREATE TABLE error_before (id INTEGER PRIMARY KEY)",
	})
	c.Assert(result.Applied[0].Error.Text, qt.Contains, "missing_table")
	c.Assert(result.Applied[0].Error.Stmt, qt.Equals, "SELECT * FROM missing_table")
	assertSQLiteTableMissing(c, dbPath, "error_before")
}

// TestCompatCommand_MigrateApplyRejectsNonAtlasFlags keeps the spellings this
// verb must NOT answer to.
//
// Both spellings this test originally carried have since become legitimate and
// were removed as they landed, each with an Atlas-side source:
//
//   - --to-version is registered by Atlas's published CLI reference, so
//     refusing it broke a Pro pipeline for no parity gain (stokaro/ptah#951).
//     Its behavior is pinned in migrate_apply_to_version_test.go.
//   - --lock-name is registered by the same reference on this verb, and is
//     pinned in lock_flags_test.go.
//
// --skip-checks replaces them rather than leaving this function with no
// subtest at all: a capability the compat surface deliberately resolves from
// PTAH_SKIP_CHECKS instead of a flag, because no Atlas-side source registers a
// flag for it on this verb (see ApplyOptions.SkipChecks). It is the remaining
// member of the class this test exists to guard.
func TestCompatCommand_MigrateApplyRejectsNonAtlasFlags(t *testing.T) {
	c := qt.New(t)

	c.Run("skip_checks", func(c *qt.C) {
		cmd := NewCompatCommand("atlas")
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs([]string{"migrate", "apply", "--skip-checks"})

		err := cmd.Execute()

		c.Assert(err, qt.ErrorMatches, `unknown flag: --skip-checks`)
	})
}

func TestCompatCommand_MigrateDiffSchemaShorthandParses(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"-s", "public",
		"--to", "file://schema.sql",
		"--dev-url", "docker://postgres/15/dev",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate diff accepts docker --dev-url values, but Ptah requires a directly connectable dev database URL`)
	c.Assert(out.String(), qt.Contains, `Error: atlas migrate diff accepts docker --dev-url values, but Ptah requires a directly connectable dev database URL`)
}

func TestCompatCommand_MigrateDiffCreatesAtlasMigrationFromLocalSchema(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	// The atlas.sum integrity gate refuses a directory that already holds a
	// migration and no checksum, exactly as the pinned community binary v1.3.0
	// does (stokaro/ptah#1086), so the fixture has to be a directory a real
	// caller could diff against.
	hashAtlasApplyDir(c, migrationsDir)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)
	devURL := "sqlite://" + filepath.Join(dir, "dev.db")

	first := NewCompatCommand("atlas")
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
	c.Assert(strings.HasPrefix(string(newSQL), "  ALTER TABLE"), qt.IsTrue)
	c.Assert(string(newSQL), qt.Contains, "ADD COLUMN")
	c.Assert(string(newSQL), qt.Contains, "email")
	sum, err := os.ReadFile(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, filepath.Base(newMigration))

	second := NewCompatCommand("atlas")
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

func TestCompatCommand_MigrateDiffDryRunPrintsMigrationWithoutWritingFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	// The atlas.sum integrity gate refuses a directory that already holds a
	// migration and no checksum, exactly as the pinned community binary v1.3.0
	// does (stokaro/ptah#1086), so the fixture has to be a directory a real
	// caller could diff against.
	hashAtlasApplyDir(c, migrationsDir)
	sumBefore, readErr := os.ReadFile(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(readErr, qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dir", "file://" + migrationsDir,
		"--to", "file://" + schemaPath,
		"--dry-run",
		"add_email",
	})

	err := cmd.Execute()
	sumAfter, sumErr := os.ReadFile(filepath.Join(migrationsDir, "atlas.sum"))
	lockInfo, lockStatErr := os.Stat(atlasMigrateDiffLockPath(migrationsDir))
	releaseLock, lockErr := testutils.AcquireExclusiveFileLock(
		atlasMigrateDiffLockPath(migrationsDir),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "ALTER TABLE")
	c.Assert(out.String(), qt.Contains, "ADD COLUMN")
	c.Assert(out.String(), qt.Contains, "email")
	c.Assert(out.String(), qt.Not(qt.Contains), "Created migration file:")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
	// The directory the gate verified is now hashed, so "wrote nothing" is
	// asserted as "atlas.sum is byte-identical" rather than as its absence.
	// That is the stronger statement anyway: a dry run that re-hashed the
	// directory would still leave exactly one SQL file behind.
	c.Assert(sumErr, qt.IsNil)
	c.Assert(string(sumAfter), qt.Equals, string(sumBefore))
	c.Assert(lockStatErr, qt.IsNil)
	c.Assert(lockInfo.Mode().IsRegular(), qt.IsTrue)
	c.Assert(lockErr, qt.IsNil)
	c.Assert(releaseLock(), qt.IsNil)
}

func TestCompatCommand_MigrateDiffCustomFormatWritesFormattedMigration(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	// The atlas.sum integrity gate refuses a directory that already holds a
	// migration and no checksum, exactly as the pinned community binary v1.3.0
	// does (stokaro/ptah#1086), so the fixture has to be a directory a real
	// caller could diff against.
	hashAtlasApplyDir(c, migrationsDir)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dir", "file://" + migrationsDir,
		"--to", "file://" + schemaPath,
		"--format", `{{ sql . "" }}`,
		"add_email",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	migrationFiles := atlasSQLFiles(c, migrationsDir)
	c.Assert(migrationFiles, qt.HasLen, 2)
	newMigration := nonInitialAtlasMigration(c, migrationFiles)
	newSQL, err := os.ReadFile(newMigration)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.HasPrefix(string(newSQL), "ALTER TABLE"), qt.IsTrue)
	c.Assert(string(newSQL), qt.Contains, "ADD COLUMN")
	c.Assert(string(newSQL), qt.Contains, "email")
}

func TestCompatCommand_MigrateDiffUsesAtlasProjectEnvDefaultsAndFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	// The atlas.sum integrity gate refuses a directory that already holds a
	// migration and no checksum, exactly as the pinned community binary v1.3.0
	// does (stokaro/ptah#1086), so the fixture has to be a directory a real
	// caller could diff against.
	hashAtlasApplyDir(c, migrationsDir)
	c.Assert(os.WriteFile("schema.sql", []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  dev = "sqlite://dev.db"
  schema {
    src = "file://schema.sql"
  }
  migration {
    dir = "file://migrations"
  }
  format {
    migrate {
      diff = "{{ sql . \"\" }}"
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--env", "local",
		"add_email",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Created migration file:")
	migrationFiles := atlasSQLFiles(c, migrationsDir)
	c.Assert(migrationFiles, qt.HasLen, 2)
	newMigration := nonInitialAtlasMigration(c, migrationFiles)
	newSQL, err := os.ReadFile(newMigration)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.HasPrefix(string(newSQL), "ALTER TABLE"), qt.IsTrue)
	c.Assert(string(newSQL), qt.Contains, "ADD COLUMN")
	c.Assert(string(newSQL), qt.Contains, "email")
}

func TestCompatCommand_MigrateDiffUsesAtlasProjectDiffSkipDropTable(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE old_users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	// The atlas.sum integrity gate refuses a directory that already holds a
	// migration and no checksum, exactly as the pinned community binary v1.3.0
	// does (stokaro/ptah#1086), so the fixture has to be a directory a real
	// caller could diff against.
	hashAtlasApplyDir(c, migrationsDir)
	c.Assert(os.WriteFile("schema.hcl", []byte(`schema "main" {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  dev = "sqlite://dev.db"
  schema {
    src = "file://schema.hcl"
  }
  migration {
    dir = "file://migrations"
  }
  diff {
    skip {
      drop_table = true
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--env", "local",
		"drop_old_users",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "The migration directory is synced with the desired state, no changes to be made\n")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
}

func TestCompatCommand_MigrateDiffUsesAtlasProjectDefaultsWithExplicitTargetFlags(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_init.sql"), []byte(`
CREATE TABLE old_users (
  id INTEGER PRIMARY KEY
);
`), 0o600), qt.IsNil)
	// The atlas.sum integrity gate refuses a directory that already holds a
	// migration and no checksum, exactly as the pinned community binary v1.3.0
	// does (stokaro/ptah#1086), so the fixture has to be a directory a real
	// caller could diff against.
	hashAtlasApplyDir(c, migrationsDir)
	schemaPath := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(schemaPath, []byte(`schema "main" {}
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  format {
    migrate {
      diff = "{{ with .Changes }}changed{{ else }}synced{{ end }}"
    }
  }
  diff {
    skip {
      drop_table = true
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dir", "file://" + migrationsDir,
		"--to", "file://" + schemaPath,
		"drop_old_users",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "The migration directory is synced with the desired state, no changes to be made\n")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
}

func TestCompatCommand_MigrateDiffAcceptsAtlasProjectConcurrentIndexPolicy(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.WriteFile("schema.sql", []byte(`CREATE TABLE users (id INTEGER PRIMARY KEY);
`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  dev = "sqlite://dev.db"
  schema {
    src = "file://schema.sql"
  }
  migration {
    dir = "file://migrations"
  }
  diff {
    concurrent_index {
      create = true
    }
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--env", "local",
		"add_users",
	})

	err := cmd.Execute()

	// The concurrent-index diff policy is accepted; SQLite has no concurrent
	// index capability, so the plan stays one plain transactional file.
	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Created migration file:")
	migrationFiles := atlasSQLFiles(c, migrationsDir)
	c.Assert(migrationFiles, qt.HasLen, 1)
	migrationSQL, readErr := os.ReadFile(migrationFiles[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(migrationSQL), qt.Contains, "CREATE TABLE")
	c.Assert(string(migrationSQL), qt.Not(qt.Contains), "atlas:txmode")
}

func TestCompatCommand_MigrateDiffSchemaFilterIgnoresOutOfScopeDesiredSchema(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(schemaPath, []byte(`
schema "auth" {}

table "users" {
  schema = schema.auth
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--dev-url", "sqlite://" + filepath.Join(dir, "dev.db"),
		"--dir", "file://" + migrationsDir,
		"--to", "file://" + schemaPath,
		"--schema", "billing",
		"out_of_scope",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "The migration directory is synced with the desired state, no changes to be made\n")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 0)
}

func TestCompatCommand_MigrateDiffRejectsChecksumDrift(t *testing.T) {
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

	cmd := NewCompatCommand("atlas")
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

	// Since stokaro/ptah#1086 the refusal comes from the shared atlas.sum gate
	// running BEFORE the dev database is touched, so it is byte-identical to
	// what `migrate apply`, `migrate status` and `migrate validate` print on
	// the same directory -- and to what the pinned community binary v1.3.0
	// prints, which puts the guidance block on stdout and `Error: checksum
	// mismatch` on stderr. Reverting the gate returns the library verifier's
	// "migration directory checksum verification failed" wording, which reaches
	// the user only after the dev database has been connected to and replayed.
	c.Assert(err, qt.ErrorMatches, `checksum mismatch`)
	c.Assert(out.String(), qt.Contains, "You have a checksum error in your migration directory.")
	c.Assert(out.String(), qt.Contains, "L2: 1_init.sql was edited")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.DeepEquals, []string{filepath.Join(migrationsDir, "1_init.sql")})
}

func TestCompatCommand_MigrateDiffRejectsInvalidLockTimeout(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_MigrateDiffLockTimeout(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	releaseLock, err := testutils.AcquireExclusiveFileLock(atlasMigrateDiffLockPath(migrationsDir))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(releaseLock(), qt.IsNil)
	})
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(`CREATE TABLE locked_diff (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_MigrateDiffRejectsInvalidFormat(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--dev-url", "sqlite://dev.db",
		"--dir", "file://migrations",
		"--to", "file://schema.sql",
		"--format", "{{ json . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*function "json" not defined.*`)
}

func TestCompatCommand_MigrateApplyResolvesProjectRelativeMigrationDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	migrationsDir := filepath.Join(projectDir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0755), qt.IsNil)
	t.Chdir(outsideDir)
	dbPath := filepath.Join(dir, "apply-relative.db")
	writeAtlasApplyMigration(c, migrationsDir, "1_apply_relative.sql", "CREATE TABLE apply_relative_users (id INTEGER PRIMARY KEY);")
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	assertSQLiteTableExists(c, dbPath, "apply_relative_users")
}

func TestCompatCommand_MigrateStatusResolvesProjectRelativeMigrationDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	migrationsDir := filepath.Join(projectDir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0755), qt.IsNil)
	t.Chdir(outsideDir)
	dbPath := filepath.Join(dir, "status-relative.db")
	writeAtlasApplyMigration(c, migrationsDir, "1_status_relative.sql", "CREATE TABLE status_relative_users (id INTEGER PRIMARY KEY);")
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "status",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "-- Pending Files:   1")
}

func TestCompatCommand_MigrateValidateResolvesProjectRelativeMigrationDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	migrationsDir := filepath.Join(projectDir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0755), qt.IsNil)
	t.Chdir(outsideDir)
	writeAtlasApplyMigration(c, migrationsDir, "1_validate_relative.sql", "CREATE TABLE validate_relative_users (id INTEGER PRIMARY KEY);")
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "validate",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
	})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
}

func TestCompatCommand_MigrateHashResolvesProjectRelativeMigrationDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	migrationsDir := filepath.Join(projectDir, "migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_hash_relative.sql", "CREATE TABLE hash_relative_users (id INTEGER PRIMARY KEY);")
	c.Assert(os.MkdirAll(outsideDir, 0o755), qt.IsNil)
	t.Chdir(outsideDir)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "hash",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	_, err = os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
}

func TestCompatCommand_MigrateNewResolvesProjectRelativeMigrationDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	migrationsDir := filepath.Join(projectDir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0o755), qt.IsNil)
	t.Chdir(outsideDir)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "new", "relative_create",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Generated empty migration file:")
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 1)
	_, err = os.Stat(filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
}

func TestCompatCommand_MigrateSetResolvesProjectRelativeMigrationDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	migrationsDir := filepath.Join(projectDir, "migrations")
	dbPath := filepath.Join(dir, "set-relative.db")
	writeAtlasApplyMigration(c, migrationsDir, "1_set_relative.sql", "CREATE TABLE set_relative_users (id INTEGER PRIMARY KEY);")
	c.Assert(os.MkdirAll(outsideDir, 0o755), qt.IsNil)
	t.Chdir(outsideDir)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "set", "1",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Current version is 1 (1 set)")
	c.Assert(sqliteAtlasAppliedVersions(c, dbPath), qt.DeepEquals, []string{"1"})
}

func TestCompatCommand_MigrateSetAllowsExplicitDirToOverrideProjectDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	migrationsDir := filepath.Join(dir, "explicit-migrations")
	dbPath := filepath.Join(dir, "set-explicit-dir.db")
	writeAtlasApplyMigration(c, migrationsDir, "1_set_explicit.sql", "CREATE TABLE set_explicit_users (id INTEGER PRIMARY KEY);")
	c.Assert(os.MkdirAll(projectDir, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0o755), qt.IsNil)
	t.Chdir(outsideDir)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "atlas://remote"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "set", "1",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
		"--dir", "file://" + migrationsDir,
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Current version is 1 (1 set)")
	c.Assert(sqliteAtlasAppliedVersions(c, dbPath), qt.DeepEquals, []string{"1"})
}

func TestCompatCommand_MigrateDownResolvesProjectRelativeMigrationDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	migrationsDir := filepath.Join(projectDir, "migrations")
	dbPath := filepath.Join(dir, "down-relative.db")
	writeAtlasApplyMigration(c, migrationsDir, "1_down_relative.sql", "CREATE TABLE down_relative_users (id INTEGER PRIMARY KEY);")
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_down_relative.down.sql"), []byte("DROP TABLE down_relative_users;\n"), 0o600), qt.IsNil)
	// The down file joined the directory after the last hash, so re-hash it:
	// the compat apply below verifies atlas.sum before executing anything.
	hashAtlasApplyDir(c, migrationsDir)
	c.Assert(os.MkdirAll(outsideDir, 0o755), qt.IsNil)
	t.Chdir(outsideDir)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	apply := NewCompatCommand("atlas")
	var applyOut bytes.Buffer
	apply.SetOut(&applyOut)
	apply.SetErr(&applyOut)
	apply.SetArgs([]string{
		"migrate", "apply",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
	})
	err := apply.Execute()
	c.Assert(err, qt.IsNil)
	assertSQLiteTableExists(c, dbPath, "down_relative_users")

	down := NewCompatCommand("atlas")
	var downOut bytes.Buffer
	down.SetIn(strings.NewReader("YES\n"))
	down.SetOut(&downOut)
	down.SetErr(&downOut)
	down.SetArgs([]string{
		"migrate", "down",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
		"--to-version", "0",
	})

	err = down.Execute()

	c.Assert(err, qt.IsNil)
	assertSQLiteTableMissing(c, dbPath, "down_relative_users")
	c.Assert(sqliteAtlasAppliedVersions(c, dbPath), qt.HasLen, 0)
}

func TestCompatCommand_MigrateStatusAllowsExplicitDirToOverrideUnsupportedProjectDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	migrationsDir := filepath.Join(dir, "explicit-migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_override_relative.sql", "CREATE TABLE override_relative_users (id INTEGER PRIMARY KEY);")
	c.Assert(os.MkdirAll(projectDir, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0o755), qt.IsNil)
	t.Chdir(outsideDir)
	dbPath := filepath.Join(dir, "status-override-relative.db")
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "atlas://remote"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "status",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
		"--dir", "file://" + filepath.ToSlash(migrationsDir),
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "-- Pending Files:   1")
}

func TestCompatCommand_MigrateStatusRejectsUnsupportedProjectDirWhenUsed(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	c.Assert(os.MkdirAll(projectDir, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0o755), qt.IsNil)
	t.Chdir(outsideDir)
	dbPath := filepath.Join(dir, "status-unsupported-relative.db")
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "atlas://remote"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "status",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate status --dir: only local file:// migration directories are supported`)
}

func TestCompatCommand_MigrateStatusAllowsParentRelativeProjectDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	migrationsDir := filepath.Join(dir, "shared-migrations")
	writeAtlasApplyMigration(c, migrationsDir, "1_parent_relative.sql", "CREATE TABLE parent_relative_users (id INTEGER PRIMARY KEY);")
	c.Assert(os.MkdirAll(projectDir, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0o755), qt.IsNil)
	t.Chdir(outsideDir)
	dbPath := filepath.Join(dir, "status-parent-relative.db")
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://../shared-migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "status",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "-- Pending Files:   1")
}

func TestCompatCommand_SchemaApplyResolvesProjectRelativeSchemaSrc(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	c.Assert(os.MkdirAll(projectDir, 0755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0755), qt.IsNil)
	t.Chdir(outsideDir)
	dbPath := filepath.Join(dir, "schema-apply-relative.db")
	c.Assert(os.WriteFile(filepath.Join(projectDir, "schema.sql"), []byte(`CREATE TABLE schema_apply_relative (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  dev = "sqlite://dev.db"
  schema {
    src = "file://schema.sql"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetIn(strings.NewReader("y\n"))
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "apply",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
		"--auto-approve",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	assertSQLiteTableExists(c, dbPath, "schema_apply_relative")
}

func TestCompatCommand_SchemaDiffResolvesProjectRelativeSchemaSrc(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	c.Assert(os.MkdirAll(projectDir, 0755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0755), qt.IsNil)
	t.Chdir(outsideDir)
	fromPath := filepath.Join(dir, "from.sql")
	c.Assert(os.WriteFile(fromPath, []byte(`CREATE TABLE keep_existing (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "schema.sql"), []byte(`CREATE TABLE schema_diff_relative (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  dev = "sqlite://dev.db"
  schema {
    src = "file://schema.sql"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
		"--from", "file://" + filepath.ToSlash(fromPath),
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, `CREATE TABLE "schema_diff_relative"`)
}

func TestCompatCommand_MigrateDiffResolvesProjectRelativeDirAndSchemaSrc(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	migrationsDir := filepath.Join(projectDir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0755), qt.IsNil)
	t.Chdir(outsideDir)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "schema.sql"), []byte(`CREATE TABLE migrate_diff_relative (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  dev = "sqlite://`+filepath.ToSlash(filepath.Join(dir, "migrate-diff-relative-dev.db"))+`"
  schema {
    src = "file://schema.sql"
  }
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
		"relative",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 1)
}

func TestCompatCommand_ProjectRelativeSchemaSrcRejectsUnsupportedScheme(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	c.Assert(os.MkdirAll(projectDir, 0755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0755), qt.IsNil)
	t.Chdir(outsideDir)
	fromPath := filepath.Join(dir, "from.sql")
	c.Assert(os.WriteFile(fromPath, []byte(`CREATE TABLE unsupported_scheme_from (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  dev = "sqlite://dev.db"
  schema {
    src = "env://src"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
		"--from", "file://" + filepath.ToSlash(fromPath),
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas.hcl schema.src: only local file:// schema files are supported`)
}

func TestCompatCommand_ProjectRelativeSchemaSrcRejectsQuery(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	projectDir := filepath.Join(dir, "project")
	outsideDir := filepath.Join(dir, "outside")
	c.Assert(os.MkdirAll(projectDir, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(outsideDir, 0o755), qt.IsNil)
	t.Chdir(outsideDir)
	fromPath := filepath.Join(dir, "from.sql")
	c.Assert(os.WriteFile(fromPath, []byte(`CREATE TABLE query_from (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "schema.sql"), []byte(`CREATE TABLE query_to (id INTEGER PRIMARY KEY);`), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  dev = "sqlite://dev.db"
  schema {
    src = "file://schema.sql?format=sql"
  }
}
`), 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
		"--from", "file://" + filepath.ToSlash(fromPath),
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas.hcl schema.src: schema file URL query parameters are not supported yet`)
}

type atlasMigrateApplyJSONResult struct {
	Driver  string
	URL     atlasReportJSONURL
	Pending []struct {
		Name        string
		Version     string
		Description string
	}
	Applied []struct {
		Name    string
		Applied []string
		Error   struct {
			Stmt string
			Text string
		}
	}
	Current string
	Target  string
	Error   string
	Message string
}

type atlasReportJSONURL struct {
	Scheme   string
	Opaque   string
	User     map[string]any
	Host     string
	Path     string
	Fragment string
	RawQuery string
	RawPath  string
	Schema   string
}

type atlasSchemaInspectJSONResult struct {
	Schemas []struct {
		Name   string
		Tables []atlasSchemaInspectJSONTableResult
	}
}

type atlasSchemaInspectJSONTableResult struct {
	Name        string
	Columns     []atlasSchemaInspectJSONColumnResult
	Indexes     []atlasSchemaInspectJSONIndexResult
	PrimaryKey  *atlasSchemaInspectJSONIndexResult `json:"primary_key"`
	ForeignKeys []struct {
		Name       string
		Columns    []string
		References struct {
			Table   string
			Columns []string
		}
	} `json:"foreign_keys"`
}

type atlasSchemaInspectJSONColumnResult struct {
	Name string
	Type string
	Null bool
}

type atlasSchemaInspectJSONIndexResult struct {
	Name   string
	Unique bool
	Parts  []atlasSchemaInspectJSONIndexPartResult
}

type atlasSchemaInspectJSONIndexPartResult struct {
	Column string
	Expr   string
}

func atlasSchemaInspectJSONTableByName(
	c *qt.C,
	tables []atlasSchemaInspectJSONTableResult,
	name string,
) atlasSchemaInspectJSONTableResult {
	c.Helper()
	for _, table := range tables {
		if table.Name == name {
			return table
		}
	}
	c.Fatalf("table %q not found in %+v", name, tables)
	return atlasSchemaInspectJSONTableResult{}
}

func atlasSchemaInspectJSONColumnByName(
	c *qt.C,
	columns []atlasSchemaInspectJSONColumnResult,
	name string,
) atlasSchemaInspectJSONColumnResult {
	c.Helper()
	for _, column := range columns {
		if column.Name == name {
			return column
		}
	}
	c.Fatalf("column %q not found in %+v", name, columns)
	return atlasSchemaInspectJSONColumnResult{}
}

// writeAtlasApplyMigration writes one Atlas migration into dir and refreshes
// atlas.sum, leaving the directory hashed the way `atlas migrate new` and
// `atlas migrate hash` leave it. Since stokaro/ptah#970 the compat apply path
// refuses an unhashed Atlas directory, so an apply fixture must carry a valid
// integrity file; tests that deliberately exercise an unhashed directory write
// their files directly instead.
func writeAtlasApplyMigration(c *qt.C, dir, name, sql string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql+"\n"), 0o600), qt.IsNil)
	hashAtlasApplyDir(c, dir)
}

// hashAtlasApplyDir (re)writes atlas.sum over dir's current contents.
func hashAtlasApplyDir(c *qt.C, dir string) {
	c.Helper()
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
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
CREATE TABLE posts (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  CONSTRAINT posts_user_fk FOREIGN KEY (user_id) REFERENCES users (id)
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

func TestCompatCommand_SchemaFmtWalksDirectoriesAndPrintsOnlyChangedFiles(t *testing.T) {
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

	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_SchemaFmtDefaultsToCurrentDirectory(t *testing.T) {
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

	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_SchemaFmtRejectsInvalidHCLWithoutRewriting(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.hcl")
	original := []byte(`schema "main" {
`)
	c.Assert(os.WriteFile(path, original, 0o600), qt.IsNil)

	cmd := NewCompatCommand("atlas")
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

func TestCompatCommand_MigrateImportConvertsFlywayDirectory(t *testing.T) {
	c := qt.New(t)
	source := t.TempDir()
	target := t.TempDir()
	writeAtlasTestFile(c, source, "V1__initial.sql", "CREATE TABLE skipped (id int);\n")
	writeAtlasTestFile(c, source, "B1__baseline.sql", "CREATE TABLE baseline (id int);\n")
	writeAtlasTestFile(c, source, "V2__add_posts.sql", "CREATE TABLE posts (id int);\n")
	writeAtlasTestFile(c, source, "U1__initial.sql", "DROP TABLE skipped;\n")

	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "import", "--from", "file://" + source + "?format=flyway", "--to", "file://" + target})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
	// The surviving baseline B1 lands in the low band and V2 in the versioned
	// band, so the baseline executes first whatever its own version. A silent
	// import reports itself through the destination directory, so the names are
	// asserted on disk rather than in the progress listing that used to exist.
	_, statErr := os.Stat(filepath.Join(target, "81608_baseline.sql"))
	c.Assert(statErr, qt.IsNil)
	_, statErr = os.Stat(filepath.Join(target, "4611686018427510315_add_posts.sql"))
	c.Assert(statErr, qt.IsNil)
	_, statErr = os.Stat(filepath.Join(target, "atlas.sum"))
	c.Assert(statErr, qt.IsNil)
	c.Assert(readAtlasTestFile(c, target, "81608_baseline.sql"), qt.Equals, "CREATE TABLE baseline (id int);\n")
	c.Assert(readAtlasTestFile(c, target, "4611686018427510315_add_posts.sql"), qt.Equals, "CREATE TABLE posts (id int);\n")
	c.Assert(readAtlasTestFile(c, target, "atlas.sum"), qt.Contains, "4611686018427510315_add_posts.sql h1:")
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
	c.Assert(out.String(), qt.Equals, "")
	_, statErr := os.Stat(filepath.Join(target, "1_initial.sql"))
	c.Assert(statErr, qt.IsNil)
	c.Assert(readAtlasTestFile(c, target, "1_initial.sql"), qt.Equals, "CREATE TABLE users (id int);\n")
	c.Assert(readAtlasTestFile(c, target, "atlas.sum"), qt.Contains, "1_initial.sql h1:")
}

func TestCompatCommand_MigrateImportRejectsRemoteSource(t *testing.T) {
	c := qt.New(t)
	cmd := NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "import", "--from", "atlas://repo/migrations?format=flyway"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `import --from: only local file:// migration directories are supported`)
	c.Assert(out.String(), qt.Contains, "Error: import --from: only local file:// migration directories are supported")
}

func TestCompatCommand_HelpUsesAtlasPathForForwardedParentedCommand(t *testing.T) {
	c := qt.New(t)
	root := NewCompatCommand("atlas")
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"migrate", "apply", "--help"})

	err := root.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "atlas migrate apply [flags] [amount]")
	c.Assert(out.String(), qt.Not(qt.Contains), "Usage:\n  migrate-up")
}

func TestCompatCommand_HelpAdvertisesGroupedNativeEquivalents(t *testing.T) {
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
			wantText: "The default output is HCL",
			oldRoot:  "ptah read-db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := NewCompatCommand("atlas")
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

func atlasMigrateDiffLockPath(migrationsDir string) string {
	cleanDir := filepath.Clean(migrationsDir)
	return filepath.Join(
		filepath.Dir(cleanDir),
		"."+filepath.Base(cleanDir)+".ptah-migrate-diff.lock",
	)
}

// TestCompatCommand_RootRejectsVersionFlag pins that the compat root does not
// carry a --version flag.
//
// The surface this binary mirrors answers `--version` and `-v` with an unknown
// flag at exit 1, and lists neither in --help. Accepting them here would make
// this binary exit 0 where that surface exits 1.
//
// It is pinned at the ROOT because that is where the regression appears:
// setting cobra's Version field anywhere on the tree auto-registers both
// spellings, and a comment claiming this was covered survived while a one-line
// mutation shipped green.
func TestCompatCommand_RootRejectsVersionFlag(t *testing.T) {
	tests := []struct {
		name    string
		argv    []string
		wantErr string
	}{
		{
			name:    "long spelling",
			argv:    []string{"--version"},
			wantErr: "unknown flag: --version",
		},
		{
			name:    "short spelling",
			argv:    []string{"-v"},
			wantErr: "unknown shorthand flag: 'v' in -v",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(tt.argv)

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
		})
	}
}
