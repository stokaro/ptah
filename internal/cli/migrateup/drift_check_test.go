package migrateup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/root"
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
