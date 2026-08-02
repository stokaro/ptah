package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/cmd/migratevalidate"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

// writeMigrateMaintFixture fills dir with a two-migration Atlas-format
// directory whose atlas.sum matches the files on disk.
func writeMigrateMaintFixture(c *qt.C, dir string) {
	c.Helper()
	c.Assert(os.WriteFile(
		filepath.Join(dir, "1_init.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "2_add_email.sql"),
		[]byte("ALTER TABLE users ADD COLUMN email TEXT;\n"),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
}

// installAppendEditor points $EDITOR at a script that appends a marker line to
// every file it receives, so editor-driven paths stay hermetic and never spawn
// an interactive editor.
func installAppendEditor(t *testing.T, marker string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "editor.sh")
	script := "#!/bin/sh\nfor f in \"$@\"; do\n  printf '%s\\n' \"" + marker + "\" >> \"$f\"\ndone\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil { //nolint:gosec // test editor script must be executable
		t.Fatal(err)
	}
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", path)
}

// assertNativeValidatePasses proves the directory still verifies through the
// native `ptah migrations validate` command after an Atlas-verb mutation.
func assertNativeValidatePasses(c *qt.C, dir string) {
	c.Helper()
	cmd := migratevalidate.NewMigrateValidateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--dir", dir})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
}

func TestCompatCommand_MigrateEditForwardsToNative(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeMigrateMaintFixture(c, dir)
	installAppendEditor(t, "-- edited through atlas verb")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "edit", "1", "--dir", "file://" + dir})

	err := cmd.Execute()

	// The Atlas verb forwards to `ptah migrations edit`: the version positional
	// maps to the native --version, --dir to the native --migrations-dir, and
	// the $EDITOR session is followed by an atlas.sum rewrite.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "Edited migration 1")
	c.Assert(out.String(), qt.Contains, "Wrote "+dir+"/atlas.sum")
	content, readErr := os.ReadFile(filepath.Join(dir, "1_init.sql"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Contains, "-- edited through atlas verb")
	assertNativeValidatePasses(c, dir)
}

func TestCompatCommand_MigrateEditAcceptsMigrationFileName(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeMigrateMaintFixture(c, dir)
	installAppendEditor(t, "-- edited by name")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "edit", "1_init.sql", "--dir", "file://" + dir})

	err := cmd.Execute()

	// Atlas documents {name | version}; a file name selects its version.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "Edited migration 1")
	content, readErr := os.ReadFile(filepath.Join(dir, "1_init.sql"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Contains, "-- edited by name")
}

func TestCompatCommand_MigrateEditWithoutEditorFails(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeMigrateMaintFixture(c, dir)
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", "")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "edit", "1", "--dir", "file://" + dir})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `no editor configured.*`)
}

func TestCompatCommand_MigrateEditRequiresVersionArgument(t *testing.T) {
	c := qt.New(t)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "edit", "--dir", "file://" + t.TempDir()})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate edit requires version argument`)
}

func TestCompatCommand_MigrateEditRejectsUnparsableVersion(t *testing.T) {
	c := qt.New(t)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "edit", "nope.sql", "--dir", "file://" + t.TempDir()})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`atlas migrate edit version argument: cannot determine a migration version from "nope.sql"`)
}

func TestCompatCommand_MigrateMaintRejectsNativeOnlyFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "edit_version",
			args: []string{"migrate", "edit", "1", "--version", "1"},
			want: `atlas migrate edit does not accept native Ptah flag --version`,
		},
		{
			name: "edit_editor",
			args: []string{"migrate", "edit", "1", "--editor", "vi"},
			want: `atlas migrate edit does not accept native Ptah flag --editor`,
		},
		{
			name: "edit_up_file",
			args: []string{"migrate", "edit", "1", "--up-file", "up.sql"},
			want: `atlas migrate edit does not accept native Ptah flag --up-file`,
		},
		{
			name: "edit_db_url",
			args: []string{"migrate", "edit", "1", "--db-url", "sqlite://state.db"},
			want: `atlas migrate edit does not accept native Ptah flag --db-url`,
		},
		{
			name: "rebase_force",
			args: []string{"migrate", "rebase", "1", "--force"},
			want: `atlas migrate rebase does not accept native Ptah flag --force`,
		},
		{
			name: "rm_migrations_dir",
			args: []string{"migrate", "rm", "1", "--migrations-dir", "migrations"},
			want: `atlas migrate rm does not accept native Ptah flag --migrations-dir`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(tt.args)

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

func TestCompatCommand_MigrateRebaseForwardsToNative(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeMigrateMaintFixture(c, dir)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "rebase", "1", "--dir", "file://" + dir})

	err := cmd.Execute()

	// The Atlas verb forwards to `ptah migrations rebase`: version 1 is
	// re-timestamped past version 2 and atlas.sum is rewritten.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Matches, `(?s).*Rebased migration 1 to \d+.*`)
	c.Assert(out.String(), qt.Contains, "Wrote "+dir+"/atlas.sum")
	_, statErr := os.Stat(filepath.Join(dir, "1_init.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
	moved, globErr := filepath.Glob(filepath.Join(dir, "*_init.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(moved, qt.HasLen, 1)
	assertNativeValidatePasses(c, dir)
}

func TestCompatCommand_MigrateRebaseRejectsMultipleVersions(t *testing.T) {
	c := qt.New(t)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "rebase", "1", "2"})

	err := cmd.Execute()

	// Atlas documents a repeatable positional; Ptah forwards one migration per
	// run and rejects the rest loudly instead of silently dropping them.
	c.Assert(err, qt.ErrorMatches,
		`atlas migrate rebase accepts multiple version arguments, but Ptah does not implement processing more than one per run yet`)
}

func TestCompatCommand_MigrateRebaseRejectsVersionRanges(t *testing.T) {
	c := qt.New(t)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "rebase", "1...3"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`atlas migrate rebase version argument: Atlas accepts version ranges, but Ptah does not implement range selection yet`)
}

func TestCompatCommand_MigrateRmForwardsToNative(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeMigrateMaintFixture(c, dir)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "rm", "2", "--dir", "file://" + dir})

	err := cmd.Execute()

	// The Atlas verb forwards to `ptah migrations rm`: version 2's file is
	// deleted and atlas.sum is rewritten.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "Removed "+dir+"/2_add_email.sql")
	c.Assert(out.String(), qt.Contains, "Wrote "+dir+"/atlas.sum")
	_, statErr := os.Stat(filepath.Join(dir, "2_add_email.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
	assertNativeValidatePasses(c, dir)
}

func TestCompatCommand_MigrateRmRequiresVersionArgument(t *testing.T) {
	c := qt.New(t)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "rm", "--dir", "file://" + t.TempDir()})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate rm requires version argument`)
}

func TestCompatCommand_MigrateEditUsesAtlasProjectConfig(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	t.Chdir(dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	writeMigrateMaintFixture(c, migrationsDir)
	installAppendEditor(t, "-- edited via project config")
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "edit", "1", "--env", "local"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "Edited migration 1")
	content, readErr := os.ReadFile(filepath.Join(migrationsDir, "1_init.sql"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Contains, "-- edited via project config")
}

func TestNewCompatCommand_MigrateMaintResolvesAtRoot(t *testing.T) {
	tests := []struct {
		name  string
		verb  string
		usage string
	}{
		{name: "edit", verb: "edit", usage: "atlas migrate edit [flags] {name | version}"},
		{name: "rebase", verb: "rebase", usage: "atlas migrate rebase [flags] {name | version}..."},
		{name: "rm", verb: "rm", usage: "atlas migrate rm [flags] {name | version}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{"migrate", tt.verb, "--help"})

			err := cmd.Execute()

			// The verbs resolve as working forwards through the compatibility binary.
			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Contains, tt.usage)
			c.Assert(out.String(), qt.Contains, "--dir")
		})
	}
}
