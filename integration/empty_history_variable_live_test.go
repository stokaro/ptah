//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// The tests in this file run the PostgreSQL half of
// empty_history_variable_e2e_test.go: a history recorded in another schema is
// what an empty PTAH_MIGRATIONS_SCHEMA or PTAH_REVISIONS_SCHEMA would miss, and
// only a server has schemas, so a server reads the result back.

// postgresTableCensus lists every table of the database at targetURL outside
// the catalogs, qualified by its schema.
func postgresTableCensus(c *qt.C, targetURL string) []string {
	c.Helper()
	return limitHistoryColumn(c, targetURL, "SELECT table_schema || '.' || table_name FROM information_schema.tables "+
		"WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY 1")
}

// histSchemaCensus is the database [postgresHistHistory] leaves: the tables in
// public, and the history in the schema hist.
var histSchemaCensus = []string{
	"hist.schema_migrations", "hist.schema_migrations_log", "public.a", "public.b", "public.c",
}

// postgresHistHistory applies the directory [nativeStopHistory] writes to a
// PostgreSQL database of this test's own with --migrations-schema hist.
func postgresHistHistory(c *qt.C) (dir, targetURL string) {
	c.Helper()
	dir, _ = nativeStopHistory(c)
	targetURL = postgresScratchURLOn(c, dbtarget.PostgreSQL, "ptah_empty_history")
	execOn(c, targetURL, "CREATE SCHEMA hist")
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables()},
		"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations", "--migrations-schema", "hist")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(postgresTableCensus(c, targetURL), qt.DeepEquals, histSchemaCensus)
	return dir, targetURL
}

func TestMigrationsSchemaVariableLive_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		args []string
	}{
		{name: "PTAH_MIGRATIONS_SCHEMA names the schema", env: []string{"PTAH_MIGRATIONS_SCHEMA=hist"}},
		{
			name: "a typed --migrations-schema beside an empty PTAH_MIGRATIONS_SCHEMA",
			env:  []string{"PTAH_MIGRATIONS_SCHEMA="},
			args: []string{"--migrations-schema", "hist"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir, targetURL := postgresHistHistory(c)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				append([]string{"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations"}, tc.args...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(postgresTableCensus(c, targetURL), qt.DeepEquals, histSchemaCensus)
		})
	}
}

func TestMigrationsSchemaVariableLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, targetURL := postgresHistHistory(c)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables("PTAH_MIGRATIONS_SCHEMA=")},
		"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations")

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Equals, "error: PTAH_MIGRATIONS_SCHEMA is set but empty: give it a value, or unset it and leave "+
		"--migrations-schema out to use the project file's schema or the default schema\n")
	c.Assert(postgresTableCensus(c, targetURL), qt.DeepEquals, histSchemaCensus)
}

// histRevisionsCensus is the database [postgresHistRevisions] leaves: the
// tables in public, and Atlas's revision table in the schema hist.
var histRevisionsCensus = []string{"hist.atlas_schema_revisions", "public.a", "public.b", "public.c"}

// postgresHistRevisions applies the directory [pendingLimitHistory] writes to
// a PostgreSQL database of this test's own with `ptah-compat migrate apply
// --revisions-schema hist`.
func postgresHistRevisions(c *qt.C) (dir, targetURL string) {
	c.Helper()
	dir, _ = pendingLimitHistory(c)
	targetURL = postgresScratchURLOn(c, dbtarget.PostgreSQL, "ptah_empty_revisions")
	execOn(c, targetURL, "CREATE SCHEMA hist")
	applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: withVariables()},
		"migrate", "apply", "--url", targetURL, "--dir", "file://migrations", "--revisions-schema", "hist")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(postgresTableCensus(c, targetURL), qt.DeepEquals, histRevisionsCensus)
	return dir, targetURL
}

func TestCompatRevisionsSchemaVariableLive_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir, targetURL := postgresHistRevisions(c)

	got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: withVariables("PTAH_REVISIONS_SCHEMA=hist")},
		"migrate", "apply", "--url", targetURL, "--dir", "file://migrations")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Equals, "No migration files to execute\n")
	c.Assert(postgresTableCensus(c, targetURL), qt.DeepEquals, histRevisionsCensus)
}

func TestCompatRevisionsSchemaVariableLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, targetURL := postgresHistRevisions(c)

	got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: withVariables("PTAH_REVISIONS_SCHEMA=")},
		"migrate", "apply", "--url", targetURL, "--dir", "file://migrations")

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Equals, "Error: PTAH_REVISIONS_SCHEMA is set but empty: give it a value, or unset it and leave "+
		"--revisions-schema out to use the project file's schema or the Atlas default\n")
	c.Assert(postgresTableCensus(c, targetURL), qt.DeepEquals, histRevisionsCensus)
}

// TestCompatMigrateDownReadsNoNativeHistoryVariableLive_HappyPath reverts the
// last migration of a history `ptah-compat migrate apply` recorded in Atlas's
// default place, atlas_schema_revisions.atlas_schema_revisions, under each
// variable `ptah migrations` reads for its own history. `migrate down` reads
// the revision table --revisions-schema places, as `migrate apply` and
// `migrate status` do, so none of them moves it or is refused when empty.
// Read by the forwarded native verb, PTAH_MIGRATIONS_SCHEMA=elsewhere reverts
// nothing and creates elsewhere.atlas_schema_revisions, and
// PTAH_MIGRATIONS_TABLE=elsewhere reverts nothing and creates public.elsewhere.
func TestCompatMigrateDownReadsNoNativeHistoryVariableLive_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		env  []string
	}{
		{name: "PTAH_MIGRATIONS_SCHEMA names another schema", env: []string{"PTAH_MIGRATIONS_SCHEMA=elsewhere"}},
		{name: "empty PTAH_MIGRATIONS_SCHEMA", env: []string{"PTAH_MIGRATIONS_SCHEMA="}},
		{name: "PTAH_MIGRATIONS_TABLE names another table", env: []string{"PTAH_MIGRATIONS_TABLE=elsewhere"}},
		{name: "empty PTAH_REVISION_FORMAT", env: []string{"PTAH_REVISION_FORMAT="}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir, _ := pendingLimitHistory(c)
			targetURL := postgresScratchURLOn(c, dbtarget.PostgreSQL, "ptah_compat_down_history")
			applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: withVariables()},
				"migrate", "apply", "--url", targetURL, "--dir", "file://migrations")
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				"migrate", "down", "--url", targetURL, "--dir", "file://migrations")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(postgresTableCensus(c, targetURL), qt.DeepEquals,
				[]string{"atlas_schema_revisions.atlas_schema_revisions", "public.a", "public.b"})
		})
	}
}
