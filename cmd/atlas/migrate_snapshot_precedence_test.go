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

func TestMigrateApplyValidatesToVersionBeforeCapturingNativeDirectory(t *testing.T) {
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
		"--to-version", "invalid",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `--to-version "invalid" is not a valid migration version: .*`)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

// TestMigrateStatusAnswersTheDirectoryBeforeTheDatabaseURL pins the measured
// order, which is the reverse of what this test asserted before.
//
// It used to require `database URL is required` here, on the reasoning that the
// cheaper check should answer first. Measured on 2026-08-13, the pinned
// community binary v1.3.0 answers
// `atlas migrate status --dir atlas://remote` with no --url by rejecting the
// DIRECTORY -- `atlas remote directory is not supported by this release` -- so
// the URL does not outrank it there. Removing this verb's required-flag check
// (cell 9.14 of stokaro/ptah#1235: the binary has none, and opens an absent
// --url as the empty string) put Ptah into that order too.
//
// The directory refusal's own wording is a separate divergence and is not
// claimed here; what this pins is which of the two answers.
func TestMigrateStatusAnswersTheDirectoryBeforeTheDatabaseURL(t *testing.T) {
	c := qt.New(t)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "status", "--dir", "atlas://remote"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate status --dir: only local file:// migration directories are supported`)
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

// TestMigrateSetCapturesTheDirectoryBeforeTheDatabaseURL pins the measured
// order, which is the reverse of what this test asserted before.
//
// Measured on 2026-08-13 against the same fixture -- a directory holding an
// uppercase ATLAS.SUM and nothing else -- the pinned community binary v1.3.0
// answers `migrate set 1 --dir file://<dir>` with no --url by refusing the
// DIRECTORY (`checksum mismatch`), not by requiring the flag. Ptah reaches the
// same ordering now that this verb's required-flag check is gone, because the
// binary has none: cell 9.14 of stokaro/ptah#1235 measures an absent --url here
// being opened as the empty string and answered
// `sql/sqlclient: missing driver`.
//
// The refusal Ptah prints for this directory is its own canonical-name
// diagnostic rather than the binary's `checksum mismatch`; that wording is a
// separate cell. What this pins is that the directory answers first.
func TestMigrateSetCapturesTheDirectoryBeforeTheDatabaseURL(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "ATLAS.SUM"), []byte("metadata\n"), 0o600), qt.IsNil)
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "set", "--dir", "file://" + dir, "1"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches,
		`atlas migrate set --dir: capture migrations directory: migration metadata file "ATLAS.SUM" must use canonical name "atlas.sum"`)
}
