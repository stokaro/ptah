package migrateup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/replaytrust"
	"ptah.run/migration/migrator"
)

// driftTarget is a SQLite target migrated to version with the three-migration
// directory, with each statement in outOfBand run on it afterwards outside any
// migration. It returns the directory, the target's path and an empty shadow
// database's URL.
func driftTarget(c *qt.C, version string, outOfBand ...string) (migrationsDir, dbPath, shadowURL string) {
	c.Helper()
	migrationsDir = writeThreeUpMigrations(c)
	dbPath = filepath.Join(c.TempDir(), "target.db")
	_, code := runUpExit(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir, "--to-version", version)
	c.Assert(code, qt.Equals, 0)
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range outOfBand {
		_, err := conn.ExecContext(context.Background(), statement)
		c.Assert(err, qt.IsNil)
	}
	return migrationsDir, dbPath, "sqlite://" + filepath.Join(c.TempDir(), "shadow.db")
}

// runUpExit runs `ptah migrations up` through the shipped root and returns
// everything it wrote, both streams together, and its exit status.
func runUpExit(c *qt.C, args ...string) (string, int) {
	c.Helper()
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	code := root.RunContext(context.Background(), cmd, append([]string{"migrations", "up"}, args...)...)
	return out.String(), code
}

// runUpDocument runs with --json and decodes the run document from standard
// output.
func runUpDocument(c *qt.C, args ...string) (migrator.RunResult, string, int) {
	c.Helper()
	cmd := root.NewRootCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	code := root.RunContext(context.Background(), cmd, append([]string{"migrations", "up", "--json"}, args...)...)
	var result migrator.RunResult
	c.Assert(json.Unmarshal(stdout.Bytes(), &result), qt.IsNil, qt.Commentf("stdout: %s\nstderr: %s", stdout.String(), stderr.String()))
	return result, stderr.String(), code
}

func tableCount(c *qt.C, url string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&count), qt.IsNil)
	return count
}

func TestMigrateUpCheckDriftHappyPath(t *testing.T) {
	for _, test := range []struct {
		name      string
		version   string
		outOfBand []string
		extra     []string
		message   string
	}{
		{
			// Version 3 is pending, and what it creates is not drift.
			name: "a clean target with a pending migration", version: "2",
			message: "Drift check: the database matches the schema its 2 applied migrations produce",
		},
		{
			// The unlocked "already up to date" answer is not taken.
			name: "a clean target with nothing pending", version: "3",
			message: "Drift check: the database matches the schema its 3 applied migrations produce",
		},
		{
			name: "an out-of-band table the run excludes", version: "2",
			outOfBand: []string{"CREATE TABLE scratch (id INTEGER PRIMARY KEY)"},
			extra:     []string{"--drift-exclude", "scratch"},
			message:   "Drift check: the database matches the schema its 2 applied migrations produce",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir, dbPath, shadowURL := driftTarget(c, test.version, test.outOfBand...)

			out, code := runUpExit(c, append([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", migrationsDir,
				"--check-drift", "--shadow-db", shadowURL}, test.extra...)...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, test.message)
			c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(3))
			c.Assert(tableCount(c, shadowURL), qt.Equals, 0)
		})
	}
}

func TestMigrateUpCheckDriftHappyPathDocumentsTheCheck(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath, shadowURL := driftTarget(c, "2")

	result, _, code := runUpDocument(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir,
		"--check-drift", "--shadow-db", shadowURL)

	c.Assert(code, qt.Equals, 0)
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeApplied)
	c.Assert(result.DriftCheck, qt.DeepEquals, &migrator.DriftCheck{
		Outcome: migrator.DriftClean, Replayed: []int64{1, 2}, ReplayedKeys: []string{"1", "2"},
	})
}

// writeProject writes a project file holding content and returns its path,
// which is how --config spells it and how a warning names it.
func writeProject(c *qt.C, content string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	return path
}

// TestMigrateUpCheckDriftHappyPathWarnsAboutAnUnmatchedExclude holds the check
// to the warning `schema drift` gives for an exclude selector that names no
// object on either side: one line on standard error, naming where the selector
// was written, so the run document on standard output stays whole and the run
// goes on. The out-of-band table is left out by a selector that matches, and
// the warning does not name it. {config} stands for the project file's path.
func TestMigrateUpCheckDriftHappyPathWarnsAboutAnUnmatchedExclude(t *testing.T) {
	for _, test := range []struct {
		name    string
		project string
		// env holds NAME=value pairs set for the run.
		env   []string
		extra []string
		want  string
		// version is the target's version before the run, and after the
		// version the run leaves it at.
		version string
		after   int64
	}{
		{
			name: "the flag", project: "exclude: [scratch]\n",
			extra:   []string{"--drift-exclude", "nosuch_flag"},
			want:    `Warning: the --drift-exclude selection matched no objects: "nosuch_flag".`,
			version: "2", after: 3,
		},
		{
			name: "the variable that set the flag", project: "exclude: [scratch]\n",
			env:     []string{"PTAH_DRIFT_EXCLUDE=nosuch_env"},
			want:    `Warning: the exclude selection from PTAH_DRIFT_EXCLUDE matched no objects: "nosuch_env".`,
			version: "2", after: 3,
		},
		{
			name: "the project file", project: "exclude: [scratch, nosuch_file]\n",
			want:    `Warning: the exclude selection from {config} matched no objects: "nosuch_file".`,
			version: "2", after: 3,
		},
		{
			name: "the project file and the flag", project: "exclude: [nosuch_file]\n",
			extra: []string{"--drift-exclude", "scratch", "--drift-exclude", "nosuch_flag"},
			want: `Warning: the exclude selection from {config} matched no objects: "nosuch_file"; ` +
				`the --drift-exclude selection matched no objects: "nosuch_flag".`,
			version: "2", after: 3,
		},
		{
			// The unlocked "already up to date" answer is not taken, so the
			// check and its warning run.
			name: "with nothing pending", project: "exclude: [scratch]\n",
			extra:   []string{"--drift-exclude", "nosuch_flag"},
			want:    `Warning: the --drift-exclude selection matched no objects: "nosuch_flag".`,
			version: "3", after: 3,
		},
		{
			name: "in a dry run", project: "exclude: [scratch]\n",
			extra:   []string{"--drift-exclude", "nosuch_flag", "--dry-run"},
			want:    `Warning: the --drift-exclude selection matched no objects: "nosuch_flag".`,
			version: "2", after: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir, dbPath, shadowURL := driftTarget(c, test.version, "CREATE TABLE scratch (id INTEGER PRIMARY KEY)")
			config := writeProject(c, test.project)
			// After the setup run, which takes no --check-drift and would
			// refuse the variable.
			for _, pair := range test.env {
				name, value, _ := strings.Cut(pair, "=")
				t.Setenv(name, value)
			}

			result, stderr, code := runUpDocument(c, append([]string{"--db-url", "sqlite://" + dbPath,
				"--migrations-dir", migrationsDir, "--config", config, "--check-drift", "--shadow-db", shadowURL}, test.extra...)...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
			c.Assert(stderr, qt.Contains, strings.ReplaceAll(test.want, "{config}", config)+"\n")
			c.Assert(strings.Count(stderr, "matched no objects"), qt.Equals, strings.Count(test.want, "matched no objects"))
			c.Assert(result.DriftCheck.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, test.after)
		})
	}
}

// TestMigrateUpCheckDriftHappyPathIsSilentAboutAMatchingExclude is the
// control: a selector that leaves an object out, from the project file or the
// flag, draws no warning.
func TestMigrateUpCheckDriftHappyPathIsSilentAboutAMatchingExclude(t *testing.T) {
	for _, test := range []struct {
		name    string
		project string
		extra   []string
	}{
		{name: "the project file", project: "exclude: [scratch]\n"},
		{name: "the flag", project: "exclude: []\n", extra: []string{"--drift-exclude", "scratch"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir, dbPath, shadowURL := driftTarget(c, "2", "CREATE TABLE scratch (id INTEGER PRIMARY KEY)")

			out, code := runUpExit(c, append([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", migrationsDir,
				"--config", writeProject(c, test.project), "--check-drift", "--shadow-db", shadowURL}, test.extra...)...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Drift check: the database matches the schema its 2 applied migrations produce")
			c.Assert(out, qt.Not(qt.Contains), "matched no objects")
			c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(3))
		})
	}
}

// TestMigrateUpCheckDriftFailurePathWarnsAboutAMistypedExclude is the case
// the warning exists for: a selector meant to leave the out-of-band table out
// names nothing, so the table is drift and nothing runs, and the warning says
// which selector protected nothing.
func TestMigrateUpCheckDriftFailurePathWarnsAboutAMistypedExclude(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath, shadowURL := driftTarget(c, "2", "CREATE TABLE scratch (id INTEGER PRIMARY KEY)")

	out, code := runUpExit(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir,
		"--check-drift", "--shadow-db", shadowURL, "--drift-exclude", "scrach")

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `Warning: the --drift-exclude selection matched no objects: "scrach".`+"\n")
	c.Assert(out, qt.Contains, "  - extra table scratch\n")
	c.Assert(out, qt.Contains, "No pending migration was run.")
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))
}

func TestMigrateUpCheckDriftFailurePathRefusesADriftedTarget(t *testing.T) {
	for _, test := range []struct {
		name    string
		version string
		extra   []string
		// want is the applied version afterwards: nothing pending runs.
		want int64
	}{
		{name: "with a pending migration", version: "2", want: 2},
		{name: "with nothing pending", version: "3", want: 3},
		{name: "in a dry run", version: "2", extra: []string{"--dry-run"}, want: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir, dbPath, shadowURL := driftTarget(c, test.version, "ALTER TABLE users ADD COLUMN nickname TEXT")

			out, code := runUpExit(c, append([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", migrationsDir,
				"--check-drift", "--shadow-db", shadowURL}, test.extra...)...)

			c.Assert(code, qt.Equals, 1, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Drift check: the database differs from the schema its")
			c.Assert(out, qt.Contains, "  - extra column users.nickname\n")
			c.Assert(out, qt.Contains, "No pending migration was run.")
			c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, test.want)
			c.Assert(tableCount(c, shadowURL), qt.Equals, 0)
		})
	}
}

func TestMigrateUpCheckDriftFailurePathDocumentsTheDrift(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath, shadowURL := driftTarget(c, "2", "ALTER TABLE users ADD COLUMN nickname TEXT")

	result, stderr, code := runUpDocument(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir,
		"--check-drift", "--shadow-db", shadowURL)

	c.Assert(code, qt.Equals, 1)
	c.Assert(stderr, qt.Contains, "extra column users.nickname")
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(result.Applied, qt.HasLen, 0)
	c.Assert(result.DriftCheck, qt.DeepEquals, &migrator.DriftCheck{
		Outcome: migrator.DriftDrifted, Replayed: []int64{1, 2}, ReplayedKeys: []string{"1", "2"},
		Findings: []migrator.DriftFinding{{
			Kind: "extra_column", Object: "users.nickname", Table: "users", Column: "nickname", Message: "extra column users.nickname",
		}},
	})
}

func TestMigrateUpCheckDriftFailurePathCannotCheck(t *testing.T) {
	c := qt.New(t)
	migrationsDir, dbPath, _ := driftTarget(c, "2")

	result, stderr, code := runUpDocument(c, "--db-url", "sqlite://"+dbPath, "--migrations-dir", migrationsDir,
		"--check-drift", "--shadow-db", "sqlite://"+dbPath)

	c.Assert(code, qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "the drift check could not compare the database with its applied history")
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(2))
	c.Assert(result.DriftCheck.Outcome, qt.Equals, migrator.DriftUnable)
	c.Assert(result.DriftCheck.Stage, qt.Equals, "realm-check")
	c.Assert(result.DriftCheck.Reason, qt.Equals, "target_shadow_same_realm")
}

func TestMigrateUpCheckDriftFailurePathRefusesAnIgnoredSetting(t *testing.T) {
	for _, test := range []struct {
		name    string
		args    []string
		refusal string
	}{
		{name: "the check without a shadow database", args: []string{"--check-drift"}, refusal: "--check-drift requires --shadow-db"},
		{name: "a shadow database without the check", args: []string{"--shadow-db", "sqlite://shadow.db"}, refusal: "--shadow-db is used only by --check-drift"},
		{name: "an exclusion without the check", args: []string{"--drift-exclude", "scratch"}, refusal: "--drift-exclude is used only by --check-drift"},
		{
			name:    "trust in the replayed SQL without the check",
			args:    []string{"--trust-replay-sql"},
			refusal: "--trust-replay-sql is used only by --check-drift: no other step of the run replays SQL",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir := writeThreeUpMigrations(c)
			dbPath := filepath.Join(c.TempDir(), "never-opened.db")

			out, code := runUpExit(c, append([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", migrationsDir}, test.args...)...)

			c.Assert(code, qt.Equals, 2)
			c.Assert(out, qt.Contains, test.refusal)
			c.Assert(countRevisionTables(c, dbPath), qt.Equals, 0)
		})
	}
}

// Trust that names nothing to replay is refused only where the operator typed
// it: PTAH_TRUST_REPLAY_SQL, which a CI job sets for every command it runs,
// leaves a run without the check alone, and so does a typed false.
func TestMigrateUpCheckDriftHappyPathIgnoresTrustItWasNotGiven(t *testing.T) {
	for _, test := range []struct {
		name     string
		variable func(testing.TB)
		args     []string
	}{
		{name: "the variable", variable: envbooltest.Set(replaytrust.EnvVar, "1")},
		{name: "a typed false", variable: envbooltest.Unset(replaytrust.EnvVar), args: []string{"--trust-replay-sql=false"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.variable(t)
			c := qt.New(t)
			migrationsDir := writeThreeUpMigrations(c)
			dbPath := filepath.Join(c.TempDir(), "applied.db")

			out, code := runUpExit(c, append([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", migrationsDir}, test.args...)...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", out))
			c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(3))
		})
	}
}
