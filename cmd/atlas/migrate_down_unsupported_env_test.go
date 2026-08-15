package atlas_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
)

// `migrate down`'s --skip-checks waiver is explicit-only: it is never
// synthesized from PTAH_SKIP_CHECKS, because `migrate apply` reads that
// variable as its pre-migration check bypass (cmd/atlas/migrate_apply.go), so
// on this verb an ambient value is not a request for hosted down checks.
// Before that exclusion, exporting the variable for an apply made every
// `migrate down` in the same shell fail with "accepts --skip-checks, but Ptah
// does not implement its behavior".
//
// The exclusion stops there, and the tests below pin that it does. --to-tag and
// --plan have no competing meaning, so setting PTAH_TO_TAG or PTAH_PLAN IS a
// request for a capability Ptah lacks and must still be refused. Ignoring them
// is not a harmless no-op: the tag is discarded, the rollback target parses as
// 0, and the whole history rolls back — a silent data loss where the operator
// asked for a bounded rollback.
//
// Both down paths are covered because they parse flags separately: the default
// path goes through the atlasargs mapper, while --format has its own flag set
// in parseAtlasMigrateDownFormatArgs. A fix to one does not fix the other.

const downUnsupportedFlagRefusal = "but Ptah does not implement its behavior"

// formatPathArgs routes a down invocation through the dedicated --format path,
// which parses flags in parseAtlasMigrateDownFormatArgs rather than through the
// atlasargs mapper.
var formatPathArgs = []string{"--format", "{{ .Current }}"}

// noSkipChecksEnv is the control setup for cases that must not depend on an
// ambient PTAH_SKIP_CHECKS.
func noSkipChecksEnv(t *testing.T) {
	t.Helper()
	unsetSkipChecksEnv(t)
}

// sqliteUsersRowCount reports how many rows survive in the seeded table, which
// is what separates a refused rollback from one that ran to completion.
func sqliteUsersRowCount(tb testing.TB, dbPath string) int {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	row := conn.QueryRowContext(context.Background(), `SELECT count(*) FROM users`)
	var count int
	c.Assert(row.Scan(&count), qt.IsNil)
	return count
}

// writeDownableMigrationsDir builds an Atlas txtar directory where BOTH
// migrations carry a down.sql section, so `migrate down` reaches real rollback
// machinery instead of stopping at the missing-down-body waiver.
//
// Both directions matter. The second migration's down body is what a bounded
// `--to-version` rollback runs; the first migration's is what an unbounded
// rollback to version 0 runs, and it drops the table. The seeded row makes the
// difference observable: a refusal leaves it, a full rollback destroys it.
func writeDownableMigrationsDir(tb testing.TB, dir string) string {
	c := qt.New(tb)
	c.Helper()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c.TB, migrationsDir, "20260801000001_create_users.sql",
		`-- atlas:txtar

-- migration.sql --
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users (id, name) VALUES (1, 'alice');

-- down.sql --
DROP TABLE users;
`)
	writeAtlasApplyProjectMigration(c.TB, migrationsDir, "20260801000002_add_users_email.sql",
		`-- atlas:txtar

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;

-- down.sql --
ALTER TABLE users DROP COLUMN email;
`)
	writeAtlasApplyProjectSum(c.TB, migrationsDir)
	return migrationsDir
}

func TestMigrateDownIgnoresTheSkipChecksEnvironmentTwin(t *testing.T) {
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
			migrationsDir := writeDownableMigrationsDir(c.TB, dir)
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
			c.Assert(sqliteUsersEmailColumnCount(c.TB, dbPath), qt.Equals, 0)
		})
	}
}

// The other waivers keep their environment twin, and refusing them is what
// protects the database. A discarded PTAH_TO_TAG leaves an empty rollback
// target that parses as version 0, so "ignore the environment" applied to this
// flag does not skip the rollback — it rolls back everything. The row count is
// the assertion that matters; the error message alone would pass even if the
// refusal came after the damage.
func TestMigrateDownRefusesUnsupportedFlagEnvironmentTwins(t *testing.T) {
	for _, tc := range []struct {
		name string
		// env is the PTAH_<FLAG> twin of an unsupported Atlas down flag that
		// has no competing meaning on any other verb.
		env string
		// value is what that variable is set to.
		value string
		// wantFlag is the flag name the refusal must name.
		wantFlag string
		// pathArgs selects the down path.
		pathArgs []string
	}{
		{name: "to-tag on the default path", env: "PTAH_TO_TAG", value: "v1", wantFlag: "--to-tag"},
		{name: "to-tag on the format path", env: "PTAH_TO_TAG", value: "v1", wantFlag: "--to-tag", pathArgs: formatPathArgs},
		{name: "plan on the default path", env: "PTAH_PLAN", value: "1", wantFlag: "--plan"},
		{name: "plan on the format path", env: "PTAH_PLAN", value: "1", wantFlag: "--plan", pathArgs: formatPathArgs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := writeDownableMigrationsDir(c.TB, dir)
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
			}, tc.pathArgs...)

			out, err := executeAtlasProjectCommand(args...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("command output:\n%s", out))
			c.Assert(err.Error(), qt.Contains, tc.wantFlag)
			c.Assert(err.Error(), qt.Contains, downUnsupportedFlagRefusal)
			// Nothing was rolled back: the seeded row and its table survive.
			c.Assert(sqliteTableCount(c.TB, dbPath, "users"), qt.Equals, 1)
			c.Assert(sqliteUsersRowCount(c.TB, dbPath), qt.Equals, 1)
		})
	}
}

// downHelpEnvAnnotation returns the "[env: ...]" suffix cobra printed for a
// flag's usage line, or the empty string when it printed none. Returning the
// annotation rather than asserting on it lets every case below make the same
// assertion, so "advertises nothing" and "advertises PTAH_X" are one rule.
func downHelpEnvAnnotation(tb testing.TB, help, flag string) string {
	c := qt.New(tb)
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
		{name: "skip-checks is explicit-only", flag: "--skip-checks", want: ""},
		{name: "to-tag keeps its twin", flag: "--to-tag", want: "[env: PTAH_TO_TAG]"},
		{name: "plan keeps its twin", flag: "--plan", want: "[env: PTAH_PLAN]"},
		{name: "format keeps its twin", flag: "--format", want: "[env: PTAH_FORMAT]"},
		{name: "dry-run keeps its twin", flag: "--dry-run", want: "[env: PTAH_DRY_RUN]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(downHelpEnvAnnotation(c.TB, help, tc.flag), qt.Equals, tc.want)
		})
	}
}

// The refusal itself must survive: passing the flag explicitly is still an ask,
// and still fails loudly on both paths. Without this, "ignore the environment"
// could be implemented by dropping the waiver altogether.
func TestMigrateDownStillRefusesExplicitUnsupportedFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag string
		// setEnv installs the flag's environment twin as well, so the case
		// separates "explicit-only" from "suppressed whenever the twin is set".
		setEnv func(t *testing.T)
		// pathArgs selects the down path, as above.
		pathArgs []string
	}{
		{name: "skip-checks on the default path", flag: "--skip-checks", setEnv: noSkipChecksEnv},
		{name: "skip-checks on the format path", flag: "--skip-checks", setEnv: noSkipChecksEnv, pathArgs: formatPathArgs},
		{
			name:   "skip-checks on the default path with its twin set",
			flag:   "--skip-checks",
			setEnv: setSkipChecksEnv("1"),
		},
		{
			name:     "skip-checks on the format path with its twin set",
			flag:     "--skip-checks",
			setEnv:   setSkipChecksEnv("1"),
			pathArgs: formatPathArgs,
		},
		{name: "plan on the default path", flag: "--plan", setEnv: noSkipChecksEnv},
		{name: "plan on the format path", flag: "--plan", setEnv: noSkipChecksEnv, pathArgs: formatPathArgs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := writeDownableMigrationsDir(c.TB, dir)
			dbPath := filepath.Join(dir, "down.db")

			_, err := executeAtlasProjectCommand(
				"migrate", "apply",
				"--url", "sqlite://"+dbPath,
				"--dir", "file://"+migrationsDir,
			)
			c.Assert(err, qt.IsNil)

			tc.setEnv(t)
			args := append([]string{
				"migrate", "down",
				"--url", "sqlite://" + dbPath,
				"--dir", "file://" + migrationsDir,
				"--to-version", "20260801000001",
				tc.flag,
			}, tc.pathArgs...)

			out, err := executeAtlasProjectCommand(args...)

			c.Assert(err, qt.IsNotNil, qt.Commentf("command output:\n%s", out))
			c.Assert(err.Error(), qt.Contains, downUnsupportedFlagRefusal)
			c.Assert(sqliteUsersEmailColumnCount(c.TB, dbPath), qt.Equals, 1)
		})
	}
}
