//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// downPlanExcludeRun is a PostgreSQL target at version 2 of
// writeDownPlanMigrations that also holds audit_events, a table another tool
// created, an empty dev database, and a project directory whose project file
// the shipped binary reads when it runs there. The schema version 1 defines
// has no audit_events, so a derived rollback drops it unless a selector keeps
// it.
type downPlanExcludeRun struct {
	migrationsDir, projectDir, target, dev string
}

// newDownPlanExcludeRun writes project as file in the project directory and
// prepares the target with the native revision table.
func newDownPlanExcludeRun(c *qt.C, file, project string) downPlanExcludeRun {
	c.Helper()
	run := downPlanExcludeRun{
		migrationsDir: writeDownPlanMigrations(c, c.TempDir()),
		projectDir:    c.TempDir(),
		target:        postgresScratchDevURL(c, "ptah_downx_target"),
		dev:           postgresScratchDevURL(c, "ptah_downx_dev"),
	}
	c.Assert(os.WriteFile(filepath.Join(run.projectDir, file), []byte(project), 0o600), qt.IsNil)
	// From a directory of its own, so the setup reads no project file.
	setup := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Env: environmentWithoutPtahVariables()},
		"migrations", "up", "--db-url", run.target, "--migrations-dir", run.migrationsDir)
	c.Assert(setup.ExitCode, qt.Equals, 0, historyComment(setup))
	run.createAuditEvents(c)
	return run
}

// createAuditEvents is the other tool: a table no migration creates, holding
// a row a dropped table would lose.
func (r downPlanExcludeRun) createAuditEvents(c *qt.C) {
	c.Helper()
	execPostgresFamilySQL(c, c.Context(), r.target, "CREATE TABLE audit_events (id BIGINT PRIMARY KEY)")
	execPostgresFamilySQL(c, c.Context(), r.target, "INSERT INTO audit_events (id) VALUES (1)")
}

// down runs `ptah migrations down --plan` back to version 1 from the project
// directory, with no PTAH_* variable but env.
func (r downPlanExcludeRun) down(c *qt.C, env []string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: r.projectDir, Env: append(environmentWithoutPtahVariables(), env...)},
		append([]string{"migrations", "down", "--db-url", r.target, "--migrations-dir", r.migrationsDir,
			"--target", "1", "--plan", "--shadow-db", r.dev, "--confirm"}, args...)...)
}

// appliedVersion reads the target's highest applied version back from its
// revision table.
func (r downPlanExcludeRun) appliedVersion(c *qt.C) int {
	c.Helper()
	var version int
	query := "SELECT COALESCE(MAX(version), 0) FROM schema_migrations WHERE state = 'applied'"
	c.Assert(openReadScopeDatabase(c, r.target).QueryRowContext(c.Context(), query).Scan(&version), qt.IsNil)
	return version
}

// userTables is the target's public tables, without the revision bookkeeping.
func (r downPlanExcludeRun) userTables(c *qt.C) []string {
	c.Helper()
	return slices.DeleteFunc(postgresPublicTables(c, r.target), func(table string) bool {
		return strings.HasPrefix(table, "schema_migrations")
	})
}

// stderrErrors is the lines a run wrote to standard error as errors, in
// order, and nil when it wrote none.
func stderrErrors(stderr string) []string {
	var lines []string
	for line := range strings.Lines(stderr) {
		if strings.HasPrefix(line, "error: ") {
			lines = append(lines, strings.TrimSuffix(line, "\n"))
		}
	}
	return lines
}

// TestMigrationsDownPlanExcludeUnmatchedE2E_HappyPath is the control for the
// refusal below. A selector that names audit_events keeps it and draws no
// warning; a project with no selector drops it, so the planner's own
// selectors for the revision tables, which name nothing on this target, are
// not refused; and the opt-in `schema apply` reads restores the warning and
// the permissive rollback.
func TestMigrationsDownPlanExcludeUnmatchedE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		project      string
		env          []string
		wantWarnings []string
		wantTables   []string
	}{
		{
			name:       "a matching selector",
			project:    "exclude: [audit_events]\n",
			wantTables: []string{"audit_events", "users"},
		},
		{
			name:       "no selector",
			project:    "migration:\n  connect_timeout: 10s\n",
			wantTables: []string{"users"},
		},
		{
			name:         "the opt-in",
			project:      "exclude: [audit_event]\n",
			env:          []string{"PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1"},
			wantWarnings: []string{`Warning: the exclude selection from ptah.yaml matched no objects: "audit_event".`},
			wantTables:   []string{"users"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newDownPlanExcludeRun(c, "ptah.yaml", test.project)

			got := run.down(c, test.env)

			c.Assert(got.ExitCode, qt.Equals, 0, historyComment(got))
			c.Assert(stderrWarnings(got.Stderr), qt.DeepEquals, test.wantWarnings, historyComment(got))
			c.Assert(run.userTables(c), qt.DeepEquals, test.wantTables)
			c.Assert(run.appliedVersion(c), qt.Equals, 1)
		})
	}
}

// TestMigrationsDownPlanExcludeUnmatchedE2E_FailurePath mistypes the selector
// meant to keep audit_events, in each place a project writes it. The selector
// protects nothing, so the run refuses as `schema apply` does, before any
// statement reaches the target: the tables and the revision table read back as
// they were. A dry run refuses too, and a malformed opt-in is refused before
// anything is read, even beside a selector that matches.
func TestMigrationsDownPlanExcludeUnmatchedE2E_FailurePath(t *testing.T) {
	const refusal = `migrations down --plan refuses a selection that protects nothing, ` +
		`set PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1 to keep the permissive behavior`
	tests := []struct {
		name       string
		file       string
		project    string
		env        []string
		args       []string
		wantErrors []string
	}{
		{
			name:    "the ptah.yaml list",
			file:    "ptah.yaml",
			project: "exclude: [audit_event]\n",
			wantErrors: []string{`error: error running down migrations: ` +
				`the exclude selection from ptah.yaml matched no objects: "audit_event"; ` + refusal},
		},
		{
			name:    "the list of the selected ptah.yaml env",
			file:    "ptah.yaml",
			project: "exclude: [audit_events]\nenv:\n  local:\n    exclude: [audit_event]\n",
			args:    []string{"--env", "local"},
			wantErrors: []string{`error: error running down migrations: ` +
				`the exclude selection from ptah.yaml matched no objects: "audit_event"; ` + refusal},
		},
		{
			name:    "the list of the selected atlas.hcl env",
			file:    "atlas.hcl",
			project: "env \"local\" {\n  exclude = [\"audit_event\"]\n}\n",
			args:    []string{"--config", "atlas.hcl", "--env", "local"},
			wantErrors: []string{`error: error running down migrations: ` +
				`the exclude selection from atlas.hcl matched no objects: "audit_event"; ` + refusal},
		},
		{
			name:    "a dry run",
			file:    "ptah.yaml",
			project: "exclude: [audit_events, audit_event]\n",
			args:    []string{"--dry-run"},
			wantErrors: []string{`error: error running down migrations: ` +
				`the exclude selection from ptah.yaml matched no objects: "audit_event"; ` + refusal},
		},
		{
			name:       "a malformed opt-in",
			file:       "ptah.yaml",
			project:    "exclude: [audit_events]\n",
			env:        []string{"PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=yesplease"},
			wantErrors: []string{`error: invalid boolean value "yesplease" for PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newDownPlanExcludeRun(c, test.file, test.project)

			got := run.down(c, test.env, test.args...)

			c.Assert(got.ExitCode, qt.Equals, 2, historyComment(got))
			c.Assert(stderrErrors(got.Stderr), qt.DeepEquals, test.wantErrors, historyComment(got))
			c.Assert(got.Stdout, qt.Not(qt.Contains), "DROP TABLE")
			c.Assert(run.userTables(c), qt.DeepEquals, []string{"audit_events", "posts", "users"})
			c.Assert(run.appliedVersion(c), qt.Equals, 2)
		})
	}
}

// TestAtlasCompatMigrateDownPlanExcludeUnmatchedE2E_FailurePath runs the
// mistake through `ptah-compat migrate down --plan`, which hands the native
// verb the selected atlas.hcl env. It refuses with the compatibility
// surface's exit status, names the command the operator typed, and both
// revisions are still recorded.
func TestAtlasCompatMigrateDownPlanExcludeUnmatchedE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
	files := map[string]string{
		"1_users.sql": "CREATE TABLE users (id SERIAL PRIMARY KEY);\n",
		"2_posts.sql": "CREATE TABLE posts (id SERIAL PRIMARY KEY);\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(migrations, name), []byte(body), 0o600), qt.IsNil)
	}
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte("env \"local\" {\n  exclude = [\"audit_event\"]\n}\n"), 0o600), qt.IsNil)
	run := downPlanExcludeRun{
		projectDir: dir,
		target:     postgresScratchDevURL(c, "ptah_downx_target"),
		dev:        postgresScratchDevURL(c, "ptah_downx_dev"),
	}
	options := clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}
	hash := clirun.Run(c, clirun.Compat, options, "migrate", "hash", "--dir", "file://migrations")
	c.Assert(hash.ExitCode, qt.Equals, 0, historyComment(hash))
	apply := clirun.Run(c, clirun.Compat, options, "migrate", "apply", "--url", run.target, "--dir", "file://migrations")
	c.Assert(apply.ExitCode, qt.Equals, 0, historyComment(apply))
	run.createAuditEvents(c)

	got := clirun.Run(c, clirun.Compat, options, "migrate", "down", "--env", "local", "--url", run.target,
		"--dir", "file://migrations", "--to-version", "1", "--plan", "--dev-url", run.dev)

	c.Assert(got.ExitCode, qt.Equals, 1, historyComment(got))
	c.Assert(got.Stderr, qt.Equals, `Error: error running down migrations: `+
		`the exclude selection from atlas.hcl matched no objects: "audit_event"; `+
		`migrate down --plan refuses a selection that protects nothing, `+
		`set PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1 to keep the permissive behavior`+"\n")
	c.Assert(run.userTables(c), qt.DeepEquals, []string{"audit_events", "posts", "users"})
	var revisions int
	c.Assert(openReadScopeDatabase(c, run.target).QueryRowContext(c.Context(),
		"SELECT count(*) FROM atlas_schema_revisions.atlas_schema_revisions").Scan(&revisions), qt.IsNil)
	c.Assert(revisions, qt.Equals, 2)
}
