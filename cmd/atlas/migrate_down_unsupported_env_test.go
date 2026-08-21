package atlas_test

import (
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `migrate down`'s --skip-checks was explicit-only until stokaro/ptah#1621: it
// was never synthesized from PTAH_SKIP_CHECKS, because `migrate apply` reads
// that variable as its pre-migration check bypass (cmd/atlas/migrate_apply.go)
// while down had no checks of its own, so on this verb an ambient value was not
// an ask at all. Before that exclusion, exporting the variable for an apply made
// every `migrate down` in the same shell fail with "accepts --skip-checks, but
// Ptah does not implement its behavior".
//
// Both halves of that premise are gone. stokaro/ptah#1715 taught `-- +ptah
// check` the down direction, so down bodies carry checks that abort a rollback,
// and #1621 implemented --skip-checks to bypass them. The variable now means the
// same thing on both verbs, so it is honored here like any other twin.
//
// --plan followed in the same issue, so `migrate down` now waives nothing and
// the tests that pinned its refusals are gone. What replaced them is
// TestCompatCommand_MigrateDownImplementedFlags, which holds that all three
// flags are accepted: with no refusal left to word, the failure worth catching
// is a flag silently reaching the native command as unknown.
//
// The environment twins still matter, and for the reason the refusals did. An
// ignored flag on this verb is not a harmless no-op: the rollback target parses
// as 0, and the whole history rolls back — silent data loss where the operator
// asked for something bounded. That is now held by the twins being honored
// rather than refused.
//
// Both down paths are covered because they parse flags separately: the default
// path goes through the atlasargs mapper, while --format has its own flag set
// in parseAtlasMigrateDownFormatArgs. A fix to one does not fix the other.

const downUnsupportedFlagRefusal = "but Ptah does not implement its behavior"

// formatPathArgs routes a down invocation through the dedicated --format path,
// which parses flags in parseAtlasMigrateDownFormatArgs rather than through the
// atlasargs mapper.
var formatPathArgs = []string{"--format", "{{ .Current }}"}

// writeDownableMigrationsDir builds an Atlas txtar directory where BOTH
// migrations carry a down.sql section, so `migrate down` reaches real rollback
// machinery instead of stopping at the missing-down-body waiver.
//
// Both directions matter. The second migration's down body is what a bounded
// `--to-version` rollback runs; the first migration's is what an unbounded
// rollback to version 0 runs, and it drops the table. The seeded row makes the
// difference observable: a refusal leaves it, a full rollback destroys it.
func writeDownableMigrationsDir(c *qt.C, dir string) string {
	c.Helper()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260801000001_create_users.sql",
		`-- atlas:txtar

-- migration.sql --
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users (id, name) VALUES (1, 'alice');

-- down.sql --
DROP TABLE users;
`)
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260801000002_add_users_email.sql",
		`-- atlas:txtar

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;

-- down.sql --
ALTER TABLE users DROP COLUMN email;
`)
	writeAtlasApplyProjectSum(c, migrationsDir)
	return migrationsDir
}

// TestMigrateDownHonorsTheSkipChecksEnvironmentTwin replaces the test that
// pinned the opposite. The run succeeding is the same observation either way --
// this fixture has no blocking check -- so the assertion that separates
// "honored" from "ignored" is the paired pair below it, not this one. This case
// remains because it is the one that regressed historically: an ambient
// PTAH_SKIP_CHECKS from an apply must not make a rollback fail.
func TestMigrateDownHonorsTheSkipChecksEnvironmentTwin(t *testing.T) {
	for _, tc := range []struct {
		name string
		// env is the PTAH_<FLAG> twin of an unsupported Atlas down flag.
		env string
		// value is what that variable is set to.
		value string
		// pathArgs selects the down path: empty for the default one, --format
		// for the dedicated format path.
		pathArgs []string
	}{
		{name: "skip-checks on the default path", env: "PTAH_SKIP_CHECKS", value: "1"},
		{name: "skip-checks on the format path", env: "PTAH_SKIP_CHECKS", value: "1", pathArgs: formatPathArgs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := writeDownableMigrationsDir(c, dir)
			dbPath := filepath.Join(dir, "down.db")

			_, err := executeAtlasProjectCommand(
				"migrate", "apply",
				"--url", "sqlite://"+dbPath,
				"--dir", "file://"+migrationsDir,
			)
			c.Assert(err, qt.IsNil)

			t.Setenv(tc.env, tc.value)
			args := append([]string{
				"migrate", "down",
				"--url", "sqlite://" + dbPath,
				"--dir", "file://" + migrationsDir,
				"--to-version", "20260801000001",
			}, tc.pathArgs...)

			out, err := executeAtlasProjectCommand(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out))
			c.Assert(out, qt.Not(qt.Contains), downUnsupportedFlagRefusal)
			c.Assert(sqliteUsersEmailColumnCount(c, dbPath), qt.Equals, 0)
		})
	}
}

// downHelpEnvAnnotation returns the "[env: ...]" suffix cobra printed for a
// flag's usage line, or the empty string when it printed none. Returning the
// annotation rather than asserting on it lets every case below make the same
// assertion, so "advertises nothing" and "advertises PTAH_X" are one rule.
func downHelpEnvAnnotation(c *qt.C, help, flag string) string {
	c.Helper()
	for line := range strings.SplitSeq(help, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, flag+" ") {
			continue
		}
		start := strings.Index(trimmed, "[env:")
		if start < 0 {
			return ""
		}
		end := strings.Index(trimmed[start:], "]")
		c.Assert(end, qt.Not(qt.Equals), -1, qt.Commentf("unterminated annotation: %s", trimmed))
		return trimmed[start : start+end+1]
	}
	c.Fatalf("flag %s not found in help:\n%s", flag, help)
	return ""
}

// The help must not advertise a variable that does nothing. --skip-checks is
// the only down flag the environment cannot set, so it is the only one whose
// usage carries no suffix — and --format keeps its suffix because PTAH_FORMAT
// genuinely routes to the format path, which is the trap in narrowing this.
func TestMigrateDownHelpAdvertisesOnlyLiveEnvironmentTwins(t *testing.T) {
	c := qt.New(t)
	help, err := executeAtlasProjectCommand("migrate", "down", "--help")
	c.Assert(err, qt.IsNil)

	for _, tc := range []struct {
		name string
		flag string
		// want is the annotation the usage line must carry, empty for none.
		want string
	}{
		// --skip-checks stopped being explicit-only in stokaro/ptah#1621: it is
		// a native flag whose variable means on down exactly what it means on
		// apply, so the help advertises it like any other.
		{name: "skip-checks keeps its twin", flag: "--skip-checks", want: "[env: PTAH_SKIP_CHECKS]"},
		{name: "to-tag keeps its twin", flag: "--to-tag", want: "[env: PTAH_TO_TAG]"},
		{name: "plan keeps its twin", flag: "--plan", want: "[env: PTAH_PLAN]"},
		{name: "format keeps its twin", flag: "--format", want: "[env: PTAH_FORMAT]"},
		{name: "dry-run keeps its twin", flag: "--dry-run", want: "[env: PTAH_DRY_RUN]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(downHelpEnvAnnotation(c, help, tc.flag), qt.Equals, tc.want)
		})
	}
}
