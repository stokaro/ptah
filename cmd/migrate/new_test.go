package migrate_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/internal/migratesum"
)

func TestMigrateNewCommandCreatesSkeletonFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	cmd := migrate.NewMigrateCreateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"add_user_preferences", "--migrations-dir", dir})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Generated empty migration files:")
	matches, globErr := filepath.Glob(filepath.Join(dir, "*_add_user_preferences.*.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 2)

	upBytes, readErr := os.ReadFile(matches[0])
	c.Assert(readErr, qt.IsNil)
	content := string(upBytes)
	c.Assert(content, qt.Contains, "-- Migration: add_user_preferences")
	c.Assert(content, qt.Contains, "-- Add your migration SQL here.")
}

func TestMigrateNewCommandAcceptsNameFlag(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	cmd := migrate.NewMigrateCreateCommand()
	cmd.SetArgs([]string{"--name", "manual_hotfix", "--migrations-dir", dir})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	matches, globErr := filepath.Glob(filepath.Join(dir, "*_manual_hotfix.*.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 2)
}

func TestMigrateNewCommandValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		err  string
	}{
		{
			name: "missing name",
			args: []string{"--migrations-dir", t.TempDir()},
			err:  "migration name is required",
		},
		{
			name: "missing migrations dir",
			args: []string{"manual_hotfix"},
			err:  "migrations directory is required",
		},
		{
			name: "name argument and flag conflict",
			args: []string{"manual_hotfix", "--name", "other", "--migrations-dir", t.TempDir()},
			err:  "migration name must be provided either as an argument or --name, not both",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := migrate.NewMigrateCreateCommand()
			cmd.SetArgs(tt.args)

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, tt.err)
		})
	}
}

func TestMigrateCreateCommandUsesNativeName(t *testing.T) {
	c := qt.New(t)

	cmd := migrate.NewMigrateCreateCommand()

	c.Assert(cmd.Name(), qt.Equals, "create")
}

// installCreateEditor points $EDITOR at a script that appends a marker line to
// every file it receives, keeping the --edit path hermetic (no interactive
// editor is ever spawned).
func installCreateEditor(t *testing.T, marker string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "editor.sh")
	script := "#!/bin/sh\nfor f in \"$@\"; do\n  printf '%s\\n' \"" + marker + "\" >> \"$f\"\ndone\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil { // #nosec G306 -- test editor script must be executable
		t.Fatal(err)
	}
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", path)
}

func TestMigrateNewCommandEditRefreshesAtlasSum(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	installCreateEditor(t, "-- edited after create")

	cmd := migrate.NewMigrateCreateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"add_users", "--migrations-dir", dir, "--dir-format", "atlas", "--edit"})

	err := cmd.Execute()

	// The created Atlas-format file opens in $EDITOR; because create already
	// hashed the empty file into atlas.sum, the checksum is refreshed so the
	// directory still validates after the edit.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	created, globErr := filepath.Glob(filepath.Join(dir, "*_add_users.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(created, qt.HasLen, 1)
	content, readErr := os.ReadFile(created[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Contains, "-- edited after create")
	res, verifyErr := migratesum.VerifyDir(dir)
	c.Assert(verifyErr, qt.IsNil)
	c.Assert(res.OK(), qt.IsTrue)
}

func TestMigrateNewCommandEditOpensPtahPair(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	installCreateEditor(t, "-- edited pair")

	cmd := migrate.NewMigrateCreateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"add_users", "--migrations-dir", dir, "--edit"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	matches, globErr := filepath.Glob(filepath.Join(dir, "*_add_users.*.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 2)
	for _, path := range matches {
		content, readErr := os.ReadFile(path)
		c.Assert(readErr, qt.IsNil)
		c.Assert(string(content), qt.Contains, "-- edited pair")
	}
}

func TestMigrateNewCommandEditorRequiresEdit(t *testing.T) {
	c := qt.New(t)

	cmd := migrate.NewMigrateCreateCommand()
	cmd.SetArgs([]string{"add_users", "--migrations-dir", t.TempDir(), "--editor", "vi"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--editor requires --edit`)
}

func TestMigrateNewCommandEditWithoutEditorFails(t *testing.T) {
	c := qt.New(t)
	t.Setenv("VISUAL", "")
	t.Setenv("EDITOR", "")

	cmd := migrate.NewMigrateCreateCommand()
	cmd.SetArgs([]string{"add_users", "--migrations-dir", t.TempDir(), "--edit"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `no editor configured: set \$EDITOR or \$VISUAL, or pass --editor`)
}
