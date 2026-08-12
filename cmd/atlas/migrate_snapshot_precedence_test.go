package atlas_test

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

func TestMigrateApplyValidatesBaselineBeforeCapturingDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "ATLAS.SUM"), []byte("metadata\n"), 0o600), qt.IsNil)
	dbPath := filepath.Join(dir, "must-not-exist.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + dir,
		"--baseline", "invalid",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--baseline "invalid" is not a valid migration version: .*`)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

func TestMigrateStatusValidatesDatabaseURLBeforeDirectoryURL(t *testing.T) {
	c := qt.New(t)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "status", "--dir", "atlas://remote"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `database URL is required`)
}

func TestMigrateLintValidatesDirectoryFormatBeforeDirectoryURL(t *testing.T) {
	c := qt.New(t)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	// --dev-url is supplied because it is now required, and the requirement is a
	// cobra-level one on the community binary too: measured there,
	// `migrate lint --dir-format custom --dir file://nope` with no --dev-url
	// answers `required flag(s) "dev-url" not set`, so the format complaint this
	// test is about only becomes reachable once the flag is present.
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", "atlas://remote",
		"--dir-format", "custom",
		"--dev-url", "sqlite://" + filepath.Join(t.TempDir(), "dev.db"),
	})

	err := cmd.Execute()

	// The format refusal wins over the directory URL, and it now prints the
	// community binary's own wording; the rejected value is still named. See
	// migrate_dir_format_error.go.
	c.Assert(err, qt.ErrorMatches, `unknown dir format "custom"`)
}

func TestMigrateSetValidatesDatabaseURLBeforeCapturingDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "ATLAS.SUM"), []byte("metadata\n"), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "set", "--dir", "file://" + dir, "1"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `database URL is required; pass --url`)
}
