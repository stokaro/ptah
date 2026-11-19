package migrateup_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"
)

// writeDeclaredDrift writes an atlas.hcl whose env "local" has dev as its dev
// database, envExclude as its exclude list, and the drift block whose body is
// drift. It returns the file's path.
func writeDeclaredDrift(c *qt.C, dev, envExclude, drift string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "atlas.hcl")
	content := "env \"local\" {\n" +
		"  dev = " + strconv.Quote(dev) + "\n" +
		"  exclude = " + envExclude + "\n" +
		"  check \"migrate_apply\" {\n" +
		"    drift {\n" + drift + "    }\n" +
		"  }\n" +
		"}\n"
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}

// TestMigrateUpDeclaredDriftCheck_HappyPath runs the drift check the atlas.hcl
// env declares, without --check-drift. Each row's target holds version 2 and
// whatever its out-of-band statement built; the run applies version 3 when
// the row says it goes on.
func TestMigrateUpDeclaredDriftCheck_HappyPath(t *testing.T) {
	rows := []struct {
		name       string
		outOfBand  []string
		envExclude string
		drift      string
		want       string
	}{
		{
			name:       "a target that matches its history",
			envExclude: "[]",
			want:       "Drift check: the database matches the schema its 2 applied migrations produce\n",
		},
		{
			name:       "a drifted target with on_error = CONTINUE",
			outOfBand:  []string{"ALTER TABLE users ADD COLUMN nickname TEXT"},
			envExclude: "[]",
			drift:      "      on_error = CONTINUE\n",
			want: "  - extra column users.nickname\n" +
				"The pending migrations run anyway, because the check is set to continue on drift.\n",
		},
		{
			name:       "a drift exclude that leaves the drifted table out replaces the env's",
			outOfBand:  []string{"CREATE TABLE scratch (id INTEGER PRIMARY KEY)"},
			envExclude: `["orders_archive"]`,
			drift:      "      exclude = [\"scratch\"]\n",
			want:       "Drift check: the database matches the schema its 2 applied migrations produce\n",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir, dbPath, shadowURL := driftTarget(c, "2", row.outOfBand...)
			config := writeDeclaredDrift(c, shadowURL, row.envExclude, row.drift)

			out, code := runUpExit(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir,
				"--config", config, "--env", "local")

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, row.want)
			c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(3))
		})
	}
}

// TestMigrateUpDeclaredDriftCheckTurnedOff holds --check-drift=false over a
// declared check: a drifted target is applied to, and no check is reported.
func TestMigrateUpDeclaredDriftCheckTurnedOff(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath, shadowURL := driftTarget(c, "2", "ALTER TABLE users ADD COLUMN nickname TEXT")
	config := writeDeclaredDrift(c, shadowURL, "[]", "")

	out, code := runUpExit(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir,
		"--config", config, "--env", "local", "--check-drift=false")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "Drift check")
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(3))
}

// TestMigrateUpDeclaredDriftCheckTurnedOffByTheEnvironment holds
// PTAH_CHECK_DRIFT=false to the flag's word: a value the environment sets is
// as explicit as one typed, so it wins over the declared check too.
func TestMigrateUpDeclaredDriftCheckTurnedOffByTheEnvironment(t *testing.T) {
	t.Setenv("PTAH_CHECK_DRIFT", "false")
	c := qt.New(t)
	migrationsDir, dbPath, shadowURL := driftTarget(c, "2", "ALTER TABLE users ADD COLUMN nickname TEXT")
	config := writeDeclaredDrift(c, shadowURL, "[]", "")

	out, code := runUpExit(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir,
		"--config", config, "--env", "local")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "Drift check")
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(3))
}

// TestMigrateUpDeclaredDriftCheckContinuesPastACheckThatCannotDecide holds
// on_error = CONTINUE to its word when the check cannot compare at all: the
// shadow database is the target, so the replay is refused, and the run warns
// and goes on.
func TestMigrateUpDeclaredDriftCheckContinuesPastACheckThatCannotDecide(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath, _ := driftTarget(c, "2")
	config := writeDeclaredDrift(c, "sqlite://"+dbPath, "[]", "      on_error = CONTINUE\n")

	out, code := runUpExit(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir,
		"--config", config, "--env", "local")

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "Drift check: could not compare the database with its applied history: ")
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(3))
}

// TestMigrateUpDeclaredDriftCheck_FailurePath refuses a drifted target. The
// drift exclude row is the control for the replacing row above: the env's
// list names the drifted table and the drift block's does not, so the table
// is drift.
func TestMigrateUpDeclaredDriftCheck_FailurePath(t *testing.T) {
	rows := []struct {
		name       string
		outOfBand  string
		envExclude string
		drift      string
		extra      []string
		finding    string
	}{
		{
			name:       "an empty drift block",
			outOfBand:  "ALTER TABLE users ADD COLUMN nickname TEXT",
			envExclude: "[]",
			finding:    "  - extra column users.nickname\n",
		},
		{
			name:       "on_error = FAIL",
			outOfBand:  "ALTER TABLE users ADD COLUMN nickname TEXT",
			envExclude: "[]",
			drift:      "      on_error = FAIL\n",
			finding:    "  - extra column users.nickname\n",
		},
		{
			name:       "--check-drift over on_error = CONTINUE",
			outOfBand:  "ALTER TABLE users ADD COLUMN nickname TEXT",
			envExclude: "[]",
			drift:      "      on_error = CONTINUE\n",
			extra:      []string{"--check-drift"},
			finding:    "  - extra column users.nickname\n",
		},
		{
			name:       "a drift exclude replaces the env's that would leave the table out",
			outOfBand:  "CREATE TABLE scratch (id INTEGER PRIMARY KEY)",
			envExclude: `["scratch"]`,
			drift:      "      exclude = [\"orders_archive\"]\n",
			finding:    "  - extra table scratch\n",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir, dbPath, shadowURL := driftTarget(c, "2", row.outOfBand)
			config := writeDeclaredDrift(c, shadowURL, row.envExclude, row.drift)

			out, code := runUpExit(c, append([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", migrationsDir,
				"--config", config, "--env", "local"}, row.extra...)...)

			c.Assert(code, qt.Equals, 1, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, row.finding)
			c.Assert(out, qt.Contains, "No pending migration was run.")
			c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))
		})
	}
}

// TestMigrateUpDeclaredDriftCheckFailurePathNeedsADevDatabase refuses a
// declared check with nowhere to replay the history, naming the block, before
// the run opens the target.
func TestMigrateUpDeclaredDriftCheckFailurePathNeedsADevDatabase(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeThreeUpMigrations(c)
	dbPath := filepath.Join(c.TempDir(), "never-opened.db")
	config := filepath.Join(c.TempDir(), "atlas.hcl")
	c.Assert(os.WriteFile(config, []byte("env \"local\" {\n  check \"migrate_apply\" {\n    drift {}\n  }\n}\n"), 0o600), qt.IsNil)

	out, code := runUpExit(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir,
		"--config", config, "--env", "local")

	c.Assert(code, qt.Equals, 2)
	c.Assert(out, qt.Contains, `atlas.hcl check "migrate_apply" drift at `+config+`:3 needs the env's dev database, or --shadow-db: `+
		`the applied history is replayed on it`)
	c.Assert(countRevisionTables(c, dbPath), qt.Equals, 0)
}

// TestMigrateUpMalformedCheckDriftVariable_FailurePath refuses a
// PTAH_CHECK_DRIFT that is not a boolean before the run opens the target,
// with three migrations pending, whether or not the project declares a drift
// check: the variable is never read as the default, and no file of the
// target is created.
func TestMigrateUpMalformedCheckDriftVariable_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		project string
	}{
		{name: "no declared drift check", project: "env \"local\" {\n}\n"},
		{
			name:    "a declared drift check",
			project: "env \"local\" {\n  dev = \"sqlite://file?mode=memory\"\n  check \"migrate_apply\" {\n    drift {}\n  }\n}\n",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			t.Setenv("PTAH_CHECK_DRIFT", "maybe")
			c := qt.New(t)
			migrationsDir := writeThreeUpMigrations(c)
			dbPath := filepath.Join(c.TempDir(), "never-opened.db")
			config := filepath.Join(c.TempDir(), "atlas.hcl")
			c.Assert(os.WriteFile(config, []byte(row.project), 0o600), qt.IsNil)

			out, code := runUpExit(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir,
				"--config", config, "--env", "local")

			c.Assert(code, qt.Equals, 2, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, `invalid boolean value "maybe" for PTAH_CHECK_DRIFT`)
			_, statErr := os.Stat(dbPath)
			c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
		})
	}
}
