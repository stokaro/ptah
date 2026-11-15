//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/migration/migrator"
)

// driftExcludeRun is a PostgreSQL target at version 1 of driftCheckMigrations,
// holding one row and the out-of-band table drift_scratch, an empty shadow
// database, and a project directory whose ptah.yaml the shipped ptah reads
// when it runs there.
type driftExcludeRun struct {
	migrationsDir, projectDir, target, shadow string
}

func newDriftExcludeRun(c *qt.C, project string) driftExcludeRun {
	c.Helper()
	run := driftExcludeRun{
		migrationsDir: c.TempDir(),
		projectDir:    c.TempDir(),
		target:        postgresScratchDevURL(c, "ptah_driftx_target"),
		shadow:        postgresScratchDevURL(c, "ptah_driftx_shadow"),
	}
	for name, content := range driftCheckMigrations {
		c.Assert(os.WriteFile(filepath.Join(run.migrationsDir, name), []byte(content), 0o600), qt.IsNil)
	}
	c.Assert(os.WriteFile(filepath.Join(run.projectDir, "ptah.yaml"), []byte(project), 0o600), qt.IsNil)
	// From a directory of its own, so the setup reads no project file.
	setup := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables()},
		"migrations", "up", "--db-url", run.target, "--migrations-dir", run.migrationsDir, "--to-version", "1")
	c.Assert(setup.ExitCode, qt.Equals, 0, historyComment(setup))
	execPostgresFamilySQL(c, c.Context(), run.target, "INSERT INTO drift_users (id, email) VALUES (1, 'kept@example.test')")
	// No key: a key would be a second finding, and the failure path is about
	// the table.
	execPostgresFamilySQL(c, c.Context(), run.target, "CREATE TABLE drift_scratch (id BIGINT)")
	return run
}

// check runs `ptah migrations up --check-drift --json` from the project
// directory, with no PTAH_* variable but env, and decodes the run document
// from standard output.
func (r driftExcludeRun) check(c *qt.C, env []string, args ...string) (migrator.RunResult, clirun.Result) {
	c.Helper()
	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: r.projectDir, Env: append(environmentWithoutPtahVariables(), env...)},
		append([]string{"migrations", "up", "--db-url", r.target, "--migrations-dir", r.migrationsDir,
			"--check-drift", "--shadow-db", r.shadow, "--json"}, args...)...)
	var result migrator.RunResult
	c.Assert(json.Unmarshal([]byte(got.Stdout), &result), qt.IsNil, historyComment(got))
	return result, got
}

// appliedVersion reads the target's highest applied version back from its
// revision table.
func (r driftExcludeRun) appliedVersion(c *qt.C) int {
	c.Helper()
	var version int
	query := "SELECT COALESCE(MAX(version), 0) FROM schema_migrations WHERE state = 'applied'"
	c.Assert(openReadScopeDatabase(c, r.target).QueryRowContext(c.Context(), query).Scan(&version), qt.IsNil)
	return version
}

// stderrWarnings is the lines a run wrote to standard error as warnings, in
// order, and nil when it wrote none.
func stderrWarnings(stderr string) []string {
	var warnings []string
	for line := range strings.Lines(stderr) {
		if strings.HasPrefix(line, "Warning: ") {
			warnings = append(warnings, strings.TrimSuffix(line, "\n"))
		}
	}
	return warnings
}

// TestMigrateUpCheckDriftExcludeUnmatchedE2E_HappyPath runs the shipped
// `ptah migrations up --check-drift` against PostgreSQL with exclude selectors
// from the flag, its variable and ptah.yaml. One that names no object on
// either side of the comparison draws the warning `schema drift` gives, naming
// where it was written; one that leaves drift_scratch out draws none. Each run
// leaves drift_scratch out, so the check is clean, the document on standard
// output parses, and the pending migration runs.
func TestMigrateUpCheckDriftExcludeUnmatchedE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		project string
		env     []string
		args    []string
		want    []string
	}{
		{
			name:    "an unmatched --drift-exclude",
			project: "exclude: [drift_scratch]\n",
			args:    []string{"--drift-exclude", "nosuch_flag"},
			want:    []string{`Warning: the --drift-exclude selection matched no objects: "nosuch_flag".`},
		},
		{
			name:    "an unmatched PTAH_DRIFT_EXCLUDE",
			project: "exclude: [drift_scratch]\n",
			env:     []string{"PTAH_DRIFT_EXCLUDE=nosuch_env"},
			want:    []string{`Warning: the exclude selection from PTAH_DRIFT_EXCLUDE matched no objects: "nosuch_env".`},
		},
		{
			name:    "an unmatched project file exclude",
			project: "exclude: [drift_scratch, nosuch_file]\n",
			want:    []string{`Warning: the exclude selection from ptah.yaml matched no objects: "nosuch_file".`},
		},
		{
			name:    "an unmatched project file exclude and an unmatched --drift-exclude",
			project: "exclude: [nosuch_file]\n",
			args:    []string{"--drift-exclude", "drift_scratch", "--drift-exclude", "nosuch_flag"},
			want: []string{`Warning: the exclude selection from ptah.yaml matched no objects: "nosuch_file"; ` +
				`the --drift-exclude selection matched no objects: "nosuch_flag".`},
		},
		{
			name:    "a matching project file exclude",
			project: "exclude: [drift_scratch]\n",
		},
		{
			name:    "a matching --drift-exclude",
			project: "exclude: []\n",
			args:    []string{"--drift-exclude", "drift_scratch"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newDriftExcludeRun(c, test.project)

			result, got := run.check(c, test.env, test.args...)

			c.Assert(got.ExitCode, qt.Equals, 0, historyComment(got))
			c.Assert(stderrWarnings(got.Stderr), qt.DeepEquals, test.want, historyComment(got))
			c.Assert(result.DriftCheck.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(result.Applied, qt.DeepEquals, []int64{2})
			c.Assert(run.appliedVersion(c), qt.Equals, 2)
		})
	}
}

// TestMigrateUpCheckDriftExcludeUnmatchedE2E_FailurePath is the mistake the
// warning exists for: the selector meant to leave drift_scratch out is
// mistyped, so it protects nothing, the table is drift and nothing runs. The
// warning names the selector that protected nothing next to the drift it let
// through.
func TestMigrateUpCheckDriftExcludeUnmatchedE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		project string
		args    []string
		want    []string
	}{
		{
			name:    "a mistyped --drift-exclude",
			project: "exclude: []\n",
			args:    []string{"--drift-exclude", "drift_scrach"},
			want:    []string{`Warning: the --drift-exclude selection matched no objects: "drift_scrach".`},
		},
		{
			name:    "a mistyped project file exclude",
			project: "exclude: [drift_scrach]\n",
			want:    []string{`Warning: the exclude selection from ptah.yaml matched no objects: "drift_scrach".`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newDriftExcludeRun(c, test.project)

			result, got := run.check(c, nil, test.args...)

			c.Assert(got.ExitCode, qt.Equals, 1, historyComment(got))
			c.Assert(stderrWarnings(got.Stderr), qt.DeepEquals, test.want, historyComment(got))
			c.Assert(got.Stderr, qt.Contains, "  - extra table drift_scratch\n")
			c.Assert(result.DriftCheck.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(result.DriftCheck.Findings, qt.DeepEquals, []migrator.DriftFinding{{
				Kind: "extra_table", Object: "drift_scratch", Table: "drift_scratch", Message: "extra table drift_scratch",
			}})
			c.Assert(result.Applied, qt.HasLen, 0)
			c.Assert(run.appliedVersion(c), qt.Equals, 1)
		})
	}
}
