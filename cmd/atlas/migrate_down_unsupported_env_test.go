package atlas_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// An Atlas flag that ptah-compat accepts for help parity but refuses at runtime
// is explicit-only: it is never synthesized from its PTAH_<FLAG> environment
// twin.
//
// The refusal means "you asked for something Ptah does not implement". An
// ambient environment variable is not an ask, and treating it as one collides
// head-on with the flag/environment twin convention: PTAH_SKIP_CHECKS is the
// sanctioned pre-migration check bypass for `migrate apply`
// (cmd/atlas/migrate_apply.go), and while `migrate down` also synthesized its
// unsupported --skip-checks from that variable, exporting it for an apply made
// every `migrate down` in the same shell fail with "accepts --skip-checks, but
// Ptah does not implement its behavior".
//
// Both down paths are covered because they parse flags separately: the default
// path goes through the atlasargs mapper, while --format has its own flag set
// in parseAtlasMigrateDownFormatArgs. A fix to one does not fix the other.

const downUnsupportedFlagRefusal = "but Ptah does not implement its behavior"

// writeDownableMigrationsDir builds an Atlas txtar directory whose second
// migration has a down.sql section, so `migrate down` reaches real rollback
// machinery instead of stopping at the missing-down-body waiver.
func writeDownableMigrationsDir(c *qt.C, dir string) string {
	c.Helper()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "20260801000001_create_users.sql",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n")
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

func TestMigrateDownIgnoresUnsupportedFlagEnvironmentTwins(t *testing.T) {
	for _, tc := range []struct {
		name string
		// env is the PTAH_<FLAG> twin of an unsupported Atlas down flag.
		env string
		// value is what that variable is set to.
		value string
		// format selects the dedicated --format path instead of the default one.
		format bool
	}{
		{name: "skip-checks on the default path", env: "PTAH_SKIP_CHECKS", value: "1"},
		{name: "skip-checks on the format path", env: "PTAH_SKIP_CHECKS", value: "1", format: true},
		{name: "plan on the default path", env: "PTAH_PLAN", value: "1"},
		{name: "plan on the format path", env: "PTAH_PLAN", value: "1", format: true},
		{name: "to-tag on the default path", env: "PTAH_TO_TAG", value: "v1"},
		{name: "to-tag on the format path", env: "PTAH_TO_TAG", value: "v1", format: true},
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
			args := []string{
				"migrate", "down",
				"--url", "sqlite://" + dbPath,
				"--dir", "file://" + migrationsDir,
				"--to-version", "20260801000001",
				"--confirm",
			}
			if tc.format {
				args = append(args, "--format", "{{ .Current }}")
			}

			out, err := executeAtlasProjectCommand(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out))
			c.Assert(out, qt.Not(qt.Contains), downUnsupportedFlagRefusal)
			c.Assert(sqliteUsersEmailColumnCount(c, dbPath), qt.Equals, 0)
		})
	}
}

// The refusal itself must survive: passing the flag explicitly is still an ask,
// and still fails loudly on both paths. Without this, "ignore the environment"
// could be implemented by dropping the waiver altogether.
func TestMigrateDownStillRefusesExplicitUnsupportedFlags(t *testing.T) {
	for _, tc := range []struct {
		name   string
		flag   string
		format bool
	}{
		{name: "skip-checks on the default path", flag: "--skip-checks"},
		{name: "skip-checks on the format path", flag: "--skip-checks", format: true},
		{name: "plan on the default path", flag: "--plan"},
		{name: "plan on the format path", flag: "--plan", format: true},
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

			args := []string{
				"migrate", "down",
				"--url", "sqlite://" + dbPath,
				"--dir", "file://" + migrationsDir,
				"--to-version", "20260801000001",
				"--confirm",
				tc.flag,
			}
			if tc.format {
				args = append(args, "--format", "{{ .Current }}")
			}

			out, err := executeAtlasProjectCommand(args...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("command output:\n%s", out))
			c.Assert(err.Error(), qt.Contains, downUnsupportedFlagRefusal)
			c.Assert(sqliteUsersEmailColumnCount(c, dbPath), qt.Equals, 1)
		})
	}
}
