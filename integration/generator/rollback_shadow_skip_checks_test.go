//go:build integration

package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
	"ptah.run/migration/shadow"
)

// writeRollbackCheckedMigrations builds an Atlas-format directory whose second
// migration carries a down body and a pre-migration check that holds while the
// migration is applied and fails once it is rolled back. Verifying that rollback
// therefore reaches the check, which is the whole point of the fixture.
func writeRollbackCheckedMigrations(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	write := func(name, body string) {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	write("20260721120000_create_users.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	write("20260721120100_create_posts.sql", "-- atlas:txtar\n\n"+
		"-- checks.sql --\n"+
		"-- atlas:assert\n"+
		"SELECT NOT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'posts');\n\n"+
		"-- migration.sql --\n"+
		"CREATE TABLE posts (id INTEGER PRIMARY KEY);\n\n"+
		"-- down.sql --\n"+
		"DROP TABLE IF EXISTS posts;\n")
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

// verifyCheckedRollback runs the verification over that fixture with the flag
// the caller is measuring.
func verifyCheckedRollback(c *qt.C, dir string, skipChecks bool) error {
	c.Helper()
	targetConn := openRollbackTarget(c, "sqlite://"+filepath.Join(c.TempDir(), "target.db"))
	return shadow.VerifyRollback(c.Context(), shadow.RollbackVerifyOptions{
		TargetConnection:  targetConn,
		ShadowDatabaseURL: "sqlite://" + filepath.Join(c.TempDir(), "shadow.db"),
		FS:                os.DirFS(dir),
		CurrentVersion:    20260721120100,
		TargetVersion:     20260721120000,
		ProviderOptions: []migrator.FSProviderOption{
			migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
		},
		SkipChecks: skipChecks,
	})
}

// TestVerifyRollback_SkipChecksWaivesTheReplayChecks_HappyPath is the
// reproduction from stokaro/ptah#3114.
//
// --skip-checks reached the target database and not this replay, so naming a
// dev database turned a rollback the operator had explicitly waived the checks
// for back into a refusal, and the target was left untouched. The flag and
// --dev-url could not be used together at all.
func TestVerifyRollback_SkipChecksWaivesTheReplayChecks_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := writeRollbackCheckedMigrations(c)

	err := verifyCheckedRollback(c, dir, true)

	c.Assert(err, qt.IsNil)
}

// TestVerifyRollback_WithoutSkipChecksTheReplayStillChecks_FailurePath is the
// control. Waiving the checks has to be something the flag does, not something
// the verification stopped doing.
func TestVerifyRollback_WithoutSkipChecksTheReplayStillChecks_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := writeRollbackCheckedMigrations(c)

	err := verifyCheckedRollback(c, dir, false)

	c.Assert(err, qt.ErrorMatches,
		`(?s)rollback verification failed: roll back to version 20260721120000 on shadow database: `+
			`pre-migration check failed for migration 20260721120100.*`)
}
