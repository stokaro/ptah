//go:build integration

package integration_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// realmDriftNotes creates notes in the role's own schema, the target's current
// schema, with a column the desired schema below does not declare.
const realmDriftNotes = "CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL, title text)"

// realmDriftSchema declares notes without a schema and without the column.
const realmDriftSchema = "CREATE TABLE notes (id bigint PRIMARY KEY, body text NOT NULL);\n"

// realmDriftReport is the part of the `schema drift --format json` document
// these tests read.
type realmDriftReport struct {
	Drift    bool              `json:"drift"`
	Findings []json.RawMessage `json:"findings"`
	Diff     struct {
		TablesModified []struct {
			TableName string `json:"table_name"`
		} `json:"tables_modified"`
	} `json:"diff"`
}

// runRealmDrift runs `ptah schema drift --format json` against the fixture's
// target with args added, and decodes the document it prints.
func runRealmDrift(c *qt.C, fixture realmCurrentSchemaFixture, args ...string) (clirun.Result, realmDriftReport) {
	c.Helper()
	root := realmCurrentSchemaRoot(c, realmDriftSchema)
	command := append([]string{
		"schema", "drift", "--schema-file", "schema.sql", "--db-url", fixture.targetURL, "--format", "json",
	}, args...)
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()}, command...)
	var report realmDriftReport
	c.Assert(json.Unmarshal([]byte(result.Stdout), &report), qt.IsNil,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result, report
}

// heldUndeclaredColumn lists the table holding the column realmDriftSchema
// leaves out, as schema.table, read from the target's catalog.
func (f realmCurrentSchemaFixture) heldUndeclaredColumn(c *qt.C) []string {
	c.Helper()
	return realmCurrentSchemaNames(c, f.target, `
		SELECT table_schema || '.' || table_name
		FROM information_schema.columns
		WHERE table_name = 'notes' AND column_name = 'title'`)
}

// TestSchemaDriftNamesATableOfTheCurrentSchemaByItsSchemaE2E reads drift on a
// realm-scoped URL whose current schema is the role's own, as `schema apply`
// and `schema compare` read it: the table the desired schema writes without a
// schema is placed in the current schema, and the report names the table the
// catalog holds the undeclared column in, schema included. Without the
// placement the report names it notes, a name apply's plan never writes.
func TestSchemaDriftNamesATableOfTheCurrentSchemaByItsSchemaE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newRealmCurrentSchemaFixture(c, realmDriftNotes)
	held := fixture.heldUndeclaredColumn(c)

	result, report := runRealmDrift(c, fixture)

	c.Assert(result.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	c.Assert(report.Drift, qt.IsTrue)
	modified := make([]string, 0, len(report.Diff.TablesModified))
	for _, table := range report.Diff.TablesModified {
		modified = append(modified, table.TableName)
	}
	c.Assert(modified, qt.DeepEquals, held)
}

// TestSchemaDriftIgnoresATableOfTheCurrentSchemaNamedByItsSchemaE2E ignores
// that table with --ignore tables=<schema>.notes, the schema read from the
// catalog, and the check passes. The control run without --ignore drifts, so
// the pass is the ignore's doing. Without the placement neither side of the
// comparison names the table by its schema, the entry matches nothing, and the
// check fails on the table the user asked it to ignore.
func TestSchemaDriftIgnoresATableOfTheCurrentSchemaNamedByItsSchemaE2E(t *testing.T) {
	c := qt.New(t)
	fixture := newRealmCurrentSchemaFixture(c, realmDriftNotes)
	held := fixture.heldUndeclaredColumn(c)
	c.Assert(held, qt.HasLen, 1)

	control, controlReport := runRealmDrift(c, fixture)
	ignored, ignoredReport := runRealmDrift(c, fixture, "--ignore", "tables="+held[0])

	c.Assert(control.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", control.Stdout, control.Stderr))
	c.Assert(controlReport.Drift, qt.IsTrue)
	c.Assert(ignored.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", ignored.Stdout, ignored.Stderr))
	c.Assert(ignoredReport.Drift, qt.IsFalse)
	c.Assert(ignoredReport.Findings, qt.HasLen, 0)
}
