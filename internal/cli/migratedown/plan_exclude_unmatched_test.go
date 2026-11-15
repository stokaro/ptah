package migratedown_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/shadow"
)

// A rollback derived with --plan executes a plan computed from a comparison,
// as `schema apply` does, so an exclude selector that names no object is
// refused there in the same words: it protects nothing, and the plan is free
// to drop the table it was written to keep.

// refusedUnmatchedAudit is the refusal for the mistyped selector below.
const refusedUnmatchedAudit = `error running down migrations: the exclude selection from ptah.yaml ` +
	`matched no objects: "external_audt"; migrations down --plan refuses a selection that protects nothing, ` +
	`set PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1 to keep the permissive behavior`

// planExcludeDir is noDownBodyDir with external_audit, a table no migration
// creates, added out of band. The run reads project from ./ptah.yaml: the
// test moves into a directory that holds it.
func planExcludeDir(c *qt.C, project string) (dir, dbPath string) {
	c.Helper()
	dir, dbPath = noDownBodyDir(c)
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE external_audit (id INTEGER PRIMARY KEY);")
	c.Assert(err, qt.IsNil)
	projectDir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(projectDir, "ptah.yaml"), []byte(project), 0o600), qt.IsNil)
	c.Chdir(projectDir)
	return dir, dbPath
}

// runPlanDown rolls the fixture back to version 1 with a derived plan.
func runPlanDown(c *qt.C, dir, dbPath string, args ...string) (string, error) {
	c.Helper()
	return runDown(append([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir,
		"--target", "1", "--plan", "--shadow-db", devURL(c), "--confirm"}, args...)...)
}

// TestMigrateDownPlanUnmatchedExclude_HappyPath is the control for the
// refusal: a selector that names external_audit keeps it, and a project with
// no selector at all rolls back. The planner leaves the revision tables out
// with selectors of its own that name nothing here, so the second row also
// shows they are not refused.
func TestMigrateDownPlanUnmatchedExclude_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		project    string
		wantTables []string
	}{
		{
			name:       "a matching selector",
			project:    "exclude: [external_audit]\n",
			wantTables: []string{"external_audit", "parent", "schema_migrations", "schema_migrations_log"},
		},
		{
			name:       "no selector",
			project:    "migration:\n  connect_timeout: 10s\n",
			wantTables: []string{"parent", "schema_migrations", "schema_migrations_log"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbPath := planExcludeDir(c, test.project)

			out, err := runPlanDown(c, dir, dbPath)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Not(qt.Contains), "matched no objects")
			c.Assert(tableCensus(c, dbPath), qt.DeepEquals, test.wantTables)
			c.Assert(recordedVersion(c, dbPath), qt.Equals, int64(1))
		})
	}
}

// TestMigrateDownPlanUnmatchedExclude_FailurePath mistypes the selector meant
// to keep external_audit. The run refuses before any statement reaches the
// database, and a dry run refuses too: the catalog and the revision table
// read back as they were.
func TestMigrateDownPlanUnmatchedExclude_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		project string
		args    []string
	}{
		{
			name:    "the top-level list, beside a selector that matches",
			project: "exclude: [external_audit, external_audt]\n",
		},
		{
			name:    "the list of the selected env",
			project: "exclude: [external_audit]\nenv:\n  local:\n    exclude: [external_audt]\n",
			args:    []string{"--env", "local"},
		},
		{
			name:    "a dry run",
			project: "exclude: [external_audt]\n",
			args:    []string{"--dry-run"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbPath := planExcludeDir(c, test.project)
			before := tableCensus(c, dbPath)

			out, err := runPlanDown(c, dir, dbPath, test.args...)

			c.Assert(err, qt.ErrorMatches, `.*`+regexp.QuoteMeta(`"external_audt"; migrations down --plan refuses`)+`.*`)
			c.Assert(out, qt.Not(qt.Contains), `DROP TABLE`)
			c.Assert(tableCensus(c, dbPath), qt.DeepEquals, before)
			c.Assert(before, qt.Contains, "external_audit")
			c.Assert(recordedVersion(c, dbPath), qt.Equals, int64(2))
		})
	}
}

// TestMigrateDownPlanUnmatchedExcludeMessage_FailurePath pins the refusal
// whole: it names the file the selector came from, the verb, and the opt-in,
// and it is the error an embedder of PlanDynamicRollback matches.
func TestMigrateDownPlanUnmatchedExcludeMessage_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := planExcludeDir(c, "exclude: [external_audt]\n")

	_, err := runPlanDown(c, dir, dbPath)

	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(refusedUnmatchedAudit))
	c.Assert(err, qt.ErrorIs, shadow.ErrUnmatchedExclude)
	c.Assert(recordedVersion(c, dbPath), qt.Equals, int64(2))
}

// TestMigrateDownPlanAllowUnmatchedExclude_HappyPath sets the opt-in
// `schema apply` reads. The selector that names nothing is a warning again,
// and the rollback drops the table it was meant to keep.
func TestMigrateDownPlanAllowUnmatchedExclude_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := planExcludeDir(c, "exclude: [external_audt]\n")
	c.Setenv("PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE", "1")

	out, err := runPlanDown(c, dir, dbPath)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `Warning: the exclude selection from ptah.yaml matched no objects: "external_audt".`+"\n")
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, []string{"parent", "schema_migrations", "schema_migrations_log"})
	c.Assert(recordedVersion(c, dbPath), qt.Equals, int64(1))
}

// TestMigrateDownPlanAllowUnmatchedExclude_FailurePath refuses a malformed
// opt-in before anything is read, even though the selector matches and the
// value would decide nothing on this run.
func TestMigrateDownPlanAllowUnmatchedExclude_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := planExcludeDir(c, "exclude: [external_audit]\n")
	c.Setenv("PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE", "yesplease")
	before := tableCensus(c, dbPath)

	_, err := runPlanDown(c, dir, dbPath)

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "yesplease" for PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE`)
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, before)
	c.Assert(recordedVersion(c, dbPath), qt.Equals, int64(2))
}
