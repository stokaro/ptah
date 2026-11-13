//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// driftCheckMigrations is a history whose second migration adds an index, so
// a target at version 1 has the index pending and must not read it as drift.
var driftCheckMigrations = map[string]string{
	"0000000001_users.up.sql":         "CREATE TABLE drift_users (id BIGINT PRIMARY KEY, email TEXT NOT NULL);\n",
	"0000000001_users.down.sql":       "DROP TABLE drift_users;\n",
	"0000000002_users_email.up.sql":   "CREATE INDEX drift_users_email ON drift_users (email);\n",
	"0000000002_users_email.down.sql": "DROP INDEX drift_users_email;\n",
}

type driftCheckRun struct {
	binary, migrationsDir, targetURL, shadowURL string
}

// newDriftCheckRun builds ptah once per test and gives it a target at
// version, holding one row, with each statement in outOfBand run on it
// afterwards, and an empty shadow database of its own.
func newDriftCheckRun(c *qt.C, t *testing.T, version string, outOfBand ...string) driftCheckRun {
	c.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	run := driftCheckRun{
		binary:        filepath.Join(t.TempDir(), "ptah"),
		migrationsDir: t.TempDir(),
		targetURL:     postgresScratchDevURL(c, "ptah_drift_target"),
		shadowURL:     postgresScratchDevURL(c, "ptah_drift_shadow"),
	}
	buildPtah(c, ctx, e2eRepoRoot(t), run.binary)
	for name, content := range driftCheckMigrations {
		c.Assert(os.WriteFile(filepath.Join(run.migrationsDir, name), []byte(content), 0o600), qt.IsNil)
	}
	_, stderr, code := run.up(c, "--to-version", version)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	run.exec(c, run.targetURL, "INSERT INTO drift_users (id, email) VALUES (1, 'kept@example.test')")
	for _, statement := range outOfBand {
		run.exec(c, run.targetURL, statement)
	}
	return run
}

// up runs `ptah migrations up` against the target and returns its standard
// output, its standard error and its exit status.
func (r driftCheckRun) up(c *qt.C, args ...string) (string, string, int) {
	c.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.binary,
		append([]string{"migrations", "up", "--db-url", r.targetURL, "--migrations-dir", r.migrationsDir}, args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return stdout.String(), stderr.String(), exit.ExitCode()
	}
	c.Assert(err, qt.IsNil)
	return stdout.String(), stderr.String(), 0
}

// check runs with --check-drift and --json and decodes the run document.
func (r driftCheckRun) check(c *qt.C, args ...string) (migrator.RunResult, string, int) {
	c.Helper()
	stdout, stderr, code := r.up(c, append([]string{"--check-drift", "--shadow-db", r.shadowURL, "--json"}, args...)...)
	var result migrator.RunResult
	c.Assert(json.Unmarshal([]byte(stdout), &result), qt.IsNil, qt.Commentf("stdout: %s\nstderr: %s", stdout, stderr))
	return result, stderr, code
}

func (r driftCheckRun) exec(c *qt.C, url, statement string) {
	c.Helper()
	db, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	_, err = db.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil)
}

func (r driftCheckRun) scalar(c *qt.C, url, query string) int {
	c.Helper()
	db, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var value int
	c.Assert(db.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil)
	return value
}

// shadowObjects counts what the shadow database holds in public afterwards.
func (r driftCheckRun) shadowObjects(c *qt.C) int {
	return r.scalar(c, r.shadowURL, "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public'")
}

func (r driftCheckRun) appliedVersion(c *qt.C) int {
	return r.scalar(c, r.targetURL, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations WHERE state = 'applied'")
}

func TestMigrateUpCheckDriftLiveHappyPath(t *testing.T) {
	for _, test := range []struct {
		name      string
		version   string
		outOfBand []string
		extra     []string
		replayed  []int64
	}{
		{name: "a clean target applies its pending index", version: "1", replayed: []int64{1}},
		{name: "an up-to-date clean target", version: "2", replayed: []int64{1, 2}},
		{
			name: "an out-of-band table the run excludes", version: "1",
			outOfBand: []string{"CREATE TABLE drift_scratch (id BIGINT PRIMARY KEY)"},
			extra:     []string{"--drift-exclude", "drift_scratch"},
			replayed:  []int64{1},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newDriftCheckRun(c, t, test.version, test.outOfBand...)

			result, stderr, code := run.check(c, test.extra...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
			c.Assert(result.DriftCheck.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(result.DriftCheck.Replayed, qt.DeepEquals, test.replayed)
			c.Assert(run.appliedVersion(c), qt.Equals, 2)
			c.Assert(run.scalar(c, run.targetURL, "SELECT COUNT(*) FROM drift_users"), qt.Equals, 1)
			c.Assert(run.shadowObjects(c), qt.Equals, 0)
		})
	}
}

func TestMigrateUpCheckDriftLiveFailurePathRefusesDrift(t *testing.T) {
	for _, test := range []struct {
		name      string
		version   string
		outOfBand []string
		extra     []string
		finding   migrator.DriftFinding
		want      int
	}{
		{
			name: "an out-of-band column blocks the pending index", version: "1",
			outOfBand: []string{"ALTER TABLE drift_users ADD COLUMN nickname TEXT"},
			finding: migrator.DriftFinding{Kind: "extra_column", Object: "drift_users.nickname", Table: "drift_users",
				Column: "nickname", Message: "extra column drift_users.nickname"},
			want: 1,
		},
		{
			name: "an out-of-band index on an up-to-date target", version: "2",
			outOfBand: []string{"CREATE INDEX drift_users_id_email ON drift_users (id, email)"},
			finding: migrator.DriftFinding{Kind: "extra_index", Object: "drift_users.drift_users_id_email", Table: "drift_users",
				Message: "extra index drift_users.drift_users_id_email"},
			want: 2,
		},
		{
			name: "a dry run reports the drift and changes nothing", version: "1",
			outOfBand: []string{"ALTER TABLE drift_users ADD COLUMN nickname TEXT"},
			extra:     []string{"--dry-run"},
			finding: migrator.DriftFinding{Kind: "extra_column", Object: "drift_users.nickname", Table: "drift_users",
				Column: "nickname", Message: "extra column drift_users.nickname"},
			want: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newDriftCheckRun(c, t, test.version, test.outOfBand...)

			result, stderr, code := run.check(c, test.extra...)

			c.Assert(code, qt.Equals, 1, qt.Commentf("%s", stderr))
			c.Assert(stderr, qt.Contains, "No pending migration was run.")
			c.Assert(result.DriftCheck.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(result.DriftCheck.Findings, qt.ContentEquals, []migrator.DriftFinding{test.finding})
			c.Assert(result.Applied, qt.HasLen, 0)
			c.Assert(run.appliedVersion(c), qt.Equals, test.want)
			c.Assert(run.scalar(c, run.targetURL, "SELECT COUNT(*) FROM drift_users"), qt.Equals, 1)
			c.Assert(run.shadowObjects(c), qt.Equals, 0)
		})
	}
}

func TestMigrateUpCheckDriftLiveFailurePathCannotCheck(t *testing.T) {
	for _, test := range []struct {
		name    string
		version string
		// prepare changes the run before the check.
		prepare func(c *qt.C, run *driftCheckRun)
		stage   string
		reason  string
		// shadowLeft is what the shadow database holds in public afterwards,
		// given what the target held before.
		shadowLeft func(before int) int
		want       int
	}{
		{
			name: "the target given as the shadow database", version: "1",
			prepare: func(_ *qt.C, run *driftCheckRun) { run.shadowURL = run.targetURL },
			stage:   "realm-check", reason: "target_shadow_same_realm",
			// The shadow is the target, which keeps everything it held.
			shadowLeft: func(before int) int { return before },
			want:       1,
		},
		{
			name: "a shadow database that holds a table", version: "1",
			prepare: func(c *qt.C, run *driftCheckRun) {
				run.exec(c, run.shadowURL, "CREATE TABLE keep_me (id BIGINT PRIMARY KEY)")
			},
			stage: "claim", reason: "not_clean",
			// keep_me and its primary key's index.
			shadowLeft: func(int) int { return 2 },
			want:       1,
		},
		{
			// A replay applies versions in order, so it cannot reproduce a
			// history that ran a lower version after a higher one.
			name: "a history applied out of order", version: "2",
			prepare: func(c *qt.C, run *driftCheckRun) {
				run.exec(c, run.targetURL, "UPDATE schema_migrations SET applied_at = applied_at + interval '1 hour' WHERE version = 1")
			},
			stage: "history", reason: migrator.UnreplayableOutOfOrder,
			shadowLeft: func(int) int { return 0 },
			want:       2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newDriftCheckRun(c, t, test.version)
			before := run.scalar(c, run.targetURL,
				"SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public'")
			test.prepare(c, &run)

			result, stderr, code := run.check(c)

			c.Assert(code, qt.Equals, 2, qt.Commentf("%s", stderr))
			c.Assert(stderr, qt.Contains, "the drift check could not compare the database with its applied history")
			c.Assert(result.DriftCheck.Outcome, qt.Equals, migrator.DriftUnable)
			c.Assert(result.DriftCheck.Stage, qt.Equals, test.stage)
			c.Assert(result.DriftCheck.Reason, qt.Equals, test.reason)
			c.Assert(run.scalar(c, run.targetURL,
				"SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public'"),
				qt.Equals, before)
			c.Assert(run.scalar(c, run.targetURL, "SELECT COUNT(*) FROM drift_users"), qt.Equals, 1)
			c.Assert(run.appliedVersion(c), qt.Equals, test.want)
			c.Assert(run.shadowObjects(c), qt.Equals, test.shadowLeft(before))
		})
	}
}
