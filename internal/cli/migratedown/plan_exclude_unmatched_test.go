package migratedown_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/migratedown"
	"ptah.run/internal/cli/migrateup"
	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/sqlitevirtual"
	"ptah.run/migration/shadow"
)

// A rollback derived with --plan executes a plan computed from a comparison,
// as `schema apply` does, so an exclude selector that names no object is
// refused there in the same words: it protects nothing, and the plan is free
// to drop the table it was written to keep.

// refusedUnmatchedAudit is the refusal for the mistyped selector below.
const refusedUnmatchedAudit = `the exclude selection from ptah.yaml ` +
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

// runPlanDownAnswering is runPlanDown without --confirm: the run asks, and
// answer is what the operator types.
func runPlanDownAnswering(c *qt.C, dir, dbPath, answer string) (string, error) {
	c.Helper()
	cmd := migratedown.NewMigrateDownCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader(answer + "\n"))
	cmd.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir,
		"--target", "1", "--plan", "--shadow-db", devURL(c)})
	err := cmd.Execute()
	return out.String(), err
}

// leavingReplayDir is planExcludeDir whose first migration also creates a
// TEMP view. Replayed, the view would live on the connection the replay ran
// on, and the cleanup of the dev database refuses a connection that holds one.
func leavingReplayDir(c *qt.C, project string) (dir, dbPath string) {
	c.Helper()
	root := c.TempDir()
	dir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	for name, body := range map[string]string{
		"1_parent.sql":      parentUpSQL + "CREATE TEMP VIEW replay_scratch AS SELECT 1;\n",
		"1_parent.down.sql": parentDownSQL,
		"2_orphan.sql":      orphanUpSQL,
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	dbPath = filepath.Join(root, "app.db")
	up := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	up.SetOut(&out)
	up.SetErr(&out)
	up.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir})
	c.Assert(up.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
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

// TestMigrateDownPlanUnmatchedExclude_HappyPath is the control for the
// refusal: a selector that names external_audit keeps it, and a project with
// no selector at all rolls back. The planner leaves the revision tables out
// with selectors of its own that name nothing here, so the second row also
// shows they are not refused. The list of the selected env replaces the
// top-level one, so a mistyped top-level selector is not read under it.
func TestMigrateDownPlanUnmatchedExclude_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		project    string
		args       []string
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
		{
			name:       "the env list in place of a mistyped top-level one",
			project:    "exclude: [external_audt]\nenv:\n  local:\n    exclude: [external_audit]\n",
			args:       []string{"--env", "local"},
			wantTables: []string{"external_audit", "parent", "schema_migrations", "schema_migrations_log"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbPath := planExcludeDir(c, test.project)

			out, err := runPlanDown(c, dir, dbPath, test.args...)

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

// TestMigrateDownPlanRefusesWhatTheCleanupCannotEmpty_FailurePath replays a
// history whose TEMP view the cleanup of the dev database could not take back.
// The dev replay guard refuses the view before the first statement of the
// replay runs, so the run stops on the refusal rather than on the selector
// that names nothing, and leaves the target where it was. Without the guard
// the replay runs, the comparison refuses the selector, and the cleanup fails
// on the view.
func TestMigrateDownPlanRefusesWhatTheCleanupCannotEmpty_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := leavingReplayDir(c, "exclude: [external_audt]\n")

	out, err := runPlanDown(c, dir, dbPath)

	c.Assert(err, qt.ErrorMatches, `dynamic rollback planning failed: replay migrations: `+
		`failed to validate migration SQL in 1_parent\.sql: sqlite migration replay rejects TEMP object `+
		`because its effects cannot be confined to the disposable database realm\nSQL: CREATE TEMP VIEW replay_scratch AS SELECT 1`,
		qt.Commentf("%s", out))
	c.Assert(err, qt.Not(qt.ErrorIs), shadow.ErrUnmatchedExclude)
	c.Assert(recordedVersion(c, dbPath), qt.Equals, int64(2))
}

// TestMigrateDownPlanUnmatchedExcludeAsksNothing_FailurePath runs without
// --confirm, and the operator would answer YES. The run refuses before it
// asks, so nobody is asked to approve a rollback that does not run.
func TestMigrateDownPlanUnmatchedExcludeAsksNothing_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := planExcludeDir(c, "exclude: [external_audt]\n")
	before := tableCensus(c, dbPath)

	out, err := runPlanDownAnswering(c, dir, dbPath, "YES")

	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(refusedUnmatchedAudit))
	c.Assert(out, qt.Not(qt.Contains), "Type 'YES' to confirm")
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, before)
	c.Assert(recordedVersion(c, dbPath), qt.Equals, int64(2))
}

// TestMigrateDownPlanAsksAfterTheDerivedPlan_HappyPath runs without
// --confirm. The derived plan is printed before the prompt, so the operator
// approves the statements that run, and a declined prompt leaves the target
// as it was.
func TestMigrateDownPlanAsksAfterTheDerivedPlan_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		answer      string
		wantTables  []string
		wantVersion int64
	}{
		{
			name:        "approved",
			answer:      "YES",
			wantTables:  []string{"external_audit", "parent", "schema_migrations", "schema_migrations_log"},
			wantVersion: 1,
		},
		{
			name:        "declined",
			answer:      "no",
			wantTables:  []string{"external_audit", "orphan", "parent", "schema_migrations", "schema_migrations_log"},
			wantVersion: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbPath := planExcludeDir(c, "exclude: [external_audit]\n")

			out, err := runPlanDownAnswering(c, dir, dbPath, test.answer)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Matches, `(?s).*Derived rollback plan: 1 statements\n  DROP TABLE "orphan"[^\n]*\n.*Type 'YES' to confirm: .*`)
			c.Assert(tableCensus(c, dbPath), qt.DeepEquals, test.wantTables)
			c.Assert(recordedVersion(c, dbPath), qt.Equals, test.wantVersion)
		})
	}
}

// TestMigrateDownAllowUnmatchedExcludeWithoutPlan_FailurePath refuses a
// malformed opt-in on a rollback that runs the down files and reads no
// exclude list: the command owns the variable, so every invocation resolves
// it.
func TestMigrateDownAllowUnmatchedExcludeWithoutPlan_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownSQL)
	c.Setenv("PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE", "yesplease")
	before := tableCensus(c, dbPath)

	_, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "1", "--confirm")

	c.Assert(err, qt.ErrorMatches, `invalid boolean value "yesplease" for PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE`)
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, before)
	c.Assert(recordedVersion(c, dbPath), qt.Equals, int64(2))
}

// TestMigrateDownAllowUnmatchedExcludeWithoutPlan_HappyPath is the control:
// a value that parses leaves the rollback by the down files to run.
func TestMigrateDownAllowUnmatchedExcludeWithoutPlan_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownSQL)
	c.Setenv("PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE", "1")

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "1", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(tableCensus(c, dbPath), qt.Not(qt.Contains), "child")
	c.Assert(recordedVersion(c, dbPath), qt.Equals, int64(1))
}

// createVirtualTable adds a SQLite virtual table to the target, out of band.
// No migration creates it, so a derived rollback would drop it, and the
// comparison refuses to plan that drop.
func createVirtualTable(c *qt.C, dbPath string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(), "CREATE VIRTUAL TABLE docs USING fts5(title, body);")
	c.Assert(err, qt.IsNil)
}

// TestMigrateDownPlanUnmatchedExcludeComesFirst_FailurePath gives the
// comparison two reasons to refuse: a mistyped selector, and a virtual table
// it will not plan a drop for. The selector is settled first, so the refusal
// names it. The control row keeps the selector that matches, and the
// comparison refuses at the virtual table, so the fixture does reach that
// refusal.
func TestMigrateDownPlanUnmatchedExcludeComesFirst_FailurePath(t *testing.T) {
	envbooltest.Unset(sqlitevirtual.AllowDropEnvVar)(t)
	tests := []struct {
		name    string
		project string
		wantErr string
	}{
		{
			name:    "a mistyped selector",
			project: "exclude: [external_audt]\n",
			wantErr: regexp.QuoteMeta(refusedUnmatchedAudit),
		},
		{
			name:    "a selector that matches",
			project: "exclude: [external_audit]\n",
			wantErr: `dynamic rollback planning failed: compare the schemas: unsupported feature: the database has virtual table "docs" .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbPath := planExcludeDir(c, test.project)
			createVirtualTable(c, dbPath)

			_, err := runPlanDown(c, dir, dbPath)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(recordedVersion(c, dbPath), qt.Equals, int64(2))
		})
	}
}
