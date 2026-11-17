//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// noteOnlyPlanTable holds a generated column. CockroachDB cannot change a
// generation expression in place, so a declaration that changes only the
// expression plans no statement, only the planner's note that the change needs
// a manual migration.
const noteOnlyPlanTable = "CREATE TABLE tasks (id INT8 PRIMARY KEY, title STRING, code STRING NOT NULL AS (upper(title)) STORED)"

// noteOnlyPlanWarning is the start of the note the planner writes for it.
const noteOnlyPlanWarning = "-- WARNING: Generated column public.tasks.code changed, but ALTER COLUMN SET EXPRESSION requires target capability alter_generated_column_expression"

// noteOnlyPlanDatabases creates a target holding the table and an empty dev
// database, both dropped when the test ends, and returns their URLs.
func noteOnlyPlanDatabases(c *qt.C) (target, dev string) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	stamp := time.Now().UnixNano()
	names := []string{fmt.Sprintf("ptah_note_only_%d", stamp), fmt.Sprintf("ptah_note_only_dev_%d", stamp)}
	for _, name := range names {
		createE2EDatabase(c, c.Context(), admin, name)
		c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	}
	target = replaceDatabaseName(c, adminURL, names[0])
	execPostgresFamilySQL(c, c.Context(), target, noteOnlyPlanTable)
	return target, replaceDatabaseName(c, adminURL, names[1])
}

// writeNoteOnlyPlanSchema writes statement as a schema file.
func writeNoteOnlyPlanSchema(c *qt.C, statement string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(statement+";\n"), 0o600), qt.IsNil)
	return path
}

// noteOnlyPlanExpression reads back the expression the database holds.
func noteOnlyPlanExpression(c *qt.C, target string) []string {
	c.Helper()
	return clauseReadBack(c, target, "SELECT generation_expression FROM information_schema.columns WHERE table_name = 'tasks' AND column_name = 'code'")
}

// noteOnlyPlanVerbs is schema apply on both surfaces and ptah-compat schema
// plan, as the shipped binaries run them, with the closing sentence each
// writes for a synced schema.
var noteOnlyPlanVerbs = []struct {
	name   string
	binary clirun.Target
	args   func(target, dev, schema string) []string
	synced string
}{
	{
		name:   "ptah schema apply",
		binary: clirun.Ptah,
		args: func(target, _, schema string) []string {
			return []string{"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve"}
		},
		synced: "Schema is synced, no changes to be made.\n",
	},
	{
		name:   "ptah-compat schema apply",
		binary: clirun.Compat,
		args: func(target, dev, schema string) []string {
			return []string{"schema", "apply", "--url", target, "--to", "file://" + filepath.ToSlash(schema), "--dev-url", dev, "--auto-approve"}
		},
		synced: "Schema is synced, no changes to be made\n",
	},
	{
		// A plan of notes alone has nothing to save, and the Atlas .plan.hcl
		// format has no field for the notes.
		name:   "ptah-compat schema plan",
		binary: clirun.Compat,
		args: func(target, dev, schema string) []string {
			return []string{"schema", "plan", "--from", target, "--to", "file://" + filepath.ToSlash(schema), "--dev-url", dev}
		},
		synced: "Schema is synced, no changes to be made.\n",
	},
}

// TestANoteOnlyPlanIsShownE2E plans and applies a declaration whose one change
// the target cannot make. Each verb shows the planner's note and says that the
// schema is not synced, and the database keeps its expression. Without the
// note kept in the plan, the plan reads as empty and both answer that the
// schema is synced (stokaro/ptah#6160).
func TestANoteOnlyPlanIsShownE2E(t *testing.T) {
	declared := "CREATE TABLE tasks (id INT8 PRIMARY KEY, title STRING, code STRING NOT NULL AS (lower(title)) STORED)"
	for _, verb := range noteOnlyPlanVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			target, dev := noteOnlyPlanDatabases(c)

			applied := clirun.Run(c, verb.binary, clirun.Options{Dir: c.TempDir()}, verb.args(target, dev, writeNoteOnlyPlanSchema(c, declared))...)

			out := applied.Stdout + applied.Stderr
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, noteOnlyPlanWarning)
			c.Assert(applied.Stdout, qt.Contains, "No statements planned, but the plan holds the notes above, so the schema is not synced")
			c.Assert(out, qt.Not(qt.Contains), "Schema is synced")
			c.Assert(noteOnlyPlanExpression(c, target), qt.DeepEquals, []string{"upper(title)"})
		})
	}
}

// TestASyncedGeneratedColumnIsReportedSyncedE2E is the control: the table
// declared as it is plans nothing and holds no note, so each verb still
// answers that the schema is synced, and says nothing else.
func TestASyncedGeneratedColumnIsReportedSyncedE2E(t *testing.T) {
	for _, verb := range noteOnlyPlanVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)
			target, dev := noteOnlyPlanDatabases(c)

			applied := clirun.Run(c, verb.binary, clirun.Options{Dir: c.TempDir()}, verb.args(target, dev, writeNoteOnlyPlanSchema(c, noteOnlyPlanTable))...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Equals, verb.synced)
		})
	}
}

// TestSchemaApplyRunsASavedNoteOnlyPlanE2E saves the note-only plan with ptah
// schema plan and applies the file. The apply shows the note and says that the
// schema is not synced; without the sentence it shows the note and says
// nothing about the schema at all.
func TestSchemaApplyRunsASavedNoteOnlyPlanE2E(t *testing.T) {
	c := qt.New(t)
	target, _ := noteOnlyPlanDatabases(c)
	declared := "CREATE TABLE tasks (id INT8 PRIMARY KEY, title STRING, code STRING NOT NULL AS (lower(title)) STORED)"
	planFile := filepath.Join(c.TempDir(), "note.plan.json")
	planned := runPtahIn(c, "schema", "plan", "--db-url", target, "--schema-file", writeNoteOnlyPlanSchema(c, declared), "--output", planFile)
	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))

	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--plan", planFile, "--auto-approve")

	out := applied.Stdout + applied.Stderr
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, noteOnlyPlanWarning)
	c.Assert(applied.Stdout, qt.Contains, "No statements planned, but the plan holds the notes above, so the schema is not synced.\n")
	c.Assert(out, qt.Not(qt.Contains), "Schema is synced")
	c.Assert(noteOnlyPlanExpression(c, target), qt.DeepEquals, []string{"upper(title)"})
}
