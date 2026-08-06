package atlas_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// refusalPattern is the ErrorMatches expression for the repeatable refusal, with
// one %s for the already-escaped file list. It lives in one place so a wording
// change cannot be half-applied across the rows below.
const refusalPattern = `error registering migrations: ` +
	`migration directory contains Atlas repeatable migrations Ptah cannot execute: %s; ` +
	`rename each one to a versioned Atlas migration \(<version>_<name>\.sql\) and re-run .migrate hash., ` +
	`or move it out of the migration directory`

// The directory this test builds is the one the pinned community binary's own
// `migrate import` emits for a Flyway source: a versioned file plus an
// R-suffixed one, both covered by atlas.sum. The community binary executes both
// (`Migrating to version 1R (2 migrations in total)`). Ptah has no executable
// representation for the R-suffixed file, and until stokaro/ptah#846 it dropped
// it silently: `migrate apply` printed "Migrating to version 1 from 1 pending
// migrations." and exited 0, `migrate status` then printed "Database is up to
// date", and the view in `1R_view.sql` existed nowhere.
//
// Refusing diverges from the community binary, which succeeds here. That is
// deliberate: the alternative measured on master is applying a strict subset of
// an atlas.sum-covered directory and reporting success, with no exit code,
// stdout byte or status line saying so.
//
// Measured under the revert mutant (the provider skip put back):
//   - "apply refuses" prints `got nil error but want non-nil` and echoes the
//     refusal regexp it wanted.
//   - "status refuses" prints the same.
//   - "versioned twin still applies" PASSES under that revert. It is the
//     non-interference control: a refusal keyed on anything wider than the
//     repeatable file name reddens it with a non-nil error.
func TestMigrateApplyRefusesAtlasRepeatableMigrations(t *testing.T) {
	const (
		usersSQL = "CREATE TABLE users (id INTEGER PRIMARY KEY);\n"
		viewSQL  = "CREATE VIEW active_users AS SELECT id FROM users;\n"
	)

	tests := []struct {
		name     string
		viewFile string
		verb     string
		assert   func(c *qt.C, err error, output string)
	}{
		{
			name:     "apply refuses the directory and executes nothing",
			viewFile: "1R_view.sql",
			verb:     "apply",
			assert: func(c *qt.C, err error, output string) {
				c.Assert(err, qt.ErrorMatches, fmt.Sprintf(refusalPattern, `1R_view\.sql`))
				c.Assert(output, qt.Not(qt.Contains), "Migrating to version")
				c.Assert(output, qt.Not(qt.Contains), "Migration complete")
			},
		},
		{
			name:     "status refuses instead of reporting up to date",
			viewFile: "1R_view.sql",
			verb:     "status",
			assert: func(c *qt.C, err error, output string) {
				c.Assert(err, qt.ErrorMatches, fmt.Sprintf(refusalPattern, `1R_view\.sql`))
				c.Assert(output, qt.Not(qt.Contains), "Database is up to date")
			},
		},
		{
			name:     "versioned twin of the same SQL still applies",
			viewFile: "2_view.sql",
			verb:     "apply",
			assert: func(c *qt.C, err error, output string) {
				c.Assert(err, qt.IsNil)
				c.Assert(output, qt.Contains, "Migration complete. Current version: 2")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := filepath.Join(dir, "migrations")
			writeAtlasApplyProjectMigration(c, migrationsDir, "1_users.sql", usersSQL)
			writeAtlasApplyProjectMigration(c, migrationsDir, test.viewFile, viewSQL)
			writeAtlasApplyProjectSum(c, migrationsDir)

			cmd := atlas.NewCompatCommand("atlas")
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			cmd.SetArgs([]string{
				"migrate", test.verb,
				"--url", "sqlite://" + filepath.Join(dir, "repeatable.db"),
				"--dir", "file://" + migrationsDir,
			})

			test.assert(c, cmd.Execute(), output.String())
		})
	}
}

// TestMigrateApplyRepeatableRefusalNamesEveryFile pins the diagnostic on a
// directory holding more than one repeatable: an operator who fixed only the
// file the message named would run the command again and hit the next one.
//
// Under the revert mutant it prints `got nil value but want non-nil`; naming
// only the first file prints a message without "R__later.sql" in it.
func TestMigrateApplyRepeatableRefusalNamesEveryFile(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "1_users.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectMigration(c, migrationsDir, "1R_view.sql", "CREATE VIEW a AS SELECT id FROM users;\n")
	writeAtlasApplyProjectMigration(c, migrationsDir, "R__later.sql", "CREATE VIEW b AS SELECT id FROM users;\n")
	writeAtlasApplyProjectSum(c, migrationsDir)

	cmd := atlas.NewCompatCommand("atlas")
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + filepath.Join(dir, "repeatable.db"),
		"--dir", "file://" + migrationsDir,
	})

	err := cmd.Execute()
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "1R_view.sql, R__later.sql")
	c.Assert(output.String(), qt.Not(qt.Contains), "Migrating to version")
}
