package atlasschema_test

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
)

func TestCapturedAnnotationsDoNotExecuteOrChangeUnitBytes(t *testing.T) {
	for _, test := range []struct {
		name  string
		sql   string
		mode  migrator.MigrationTxMode
		count int
	}{
		{"mixed autocommit", "CREATE TABLE actual (id INTEGER);", migrator.MigrationTxModeNone, 1},
		{"mixed transaction", "CREATE TABLE actual (id INTEGER);", migrator.MigrationTxModeAll, 1},
		{"notes only", "", migrator.MigrationTxModeAll, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "target.db")
			conn := connectSQLite(c, path)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			first, last := "review\nCREATE TABLE injected (id INTEGER);", ""
			fragments := []renderer.Fragment{{Annotation: &first}, {}, {Annotation: &last}}
			fragments[1].SQL = test.sql
			rendered := planner.RenderedPlan{Request: renderer.Request{Target: "sqlite", Capabilities: conn.Info().Capabilities, Nodes: []ast.Node{ast.NewComment(first), ast.NewRawSQL(fragments[1].SQL), ast.NewComment(last)}}, Result: renderer.Result{Complete: true, ExecutableUnits: true, Fragments: fragments}, Dialect: "sqlite"}
			plan, err := atlasschema.CaptureApplyPlan(conn.Info(), rendered)
			c.Assert(err, qt.IsNil)
			units, err := plan.Units()
			c.Assert(err, qt.IsNil)
			c.Assert(units, qt.HasLen, test.count)
			c.Assert(plan.HasChanges(), qt.Equals, test.count > 0)
			notes := plan.Annotations()
			c.Assert(notes, qt.DeepEquals, []renderer.Annotation{{Before: 0, Text: first}, {Before: test.count, Text: last}})
			notes[0].Text = "changed"
			text, err := plan.SQL()
			c.Assert(err, qt.IsNil)
			c.Assert(text, qt.Contains, "-- review\n-- CREATE TABLE injected (id INTEGER);\n")
			c.Assert(plan.Preflight(c.Context(), conn, test.mode), qt.IsNil)
			c.Assert(sqliteTableExists(c, path, "actual"), qt.IsFalse)
			c.Assert(plan.Execute(c.Context(), conn, test.mode), qt.IsNil)
			c.Assert(sqliteTableExists(c, path, "actual"), qt.Equals, test.count > 0)
			c.Assert(sqliteTableExists(c, path, "injected"), qt.IsFalse)
		})
	}
}

func TestSavedAnnotationsRoundTripAndPreserveSchemaFingerprints(t *testing.T) {
	for _, test := range []struct {
		name  string
		count int
	}{
		{"mixed SQL and native", 2},
		{"only notes", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan := planWithNative(c)
			plan.Statements = plan.Statements[:test.count]
			plan.Annotations = []renderer.Annotation{{Before: 0, Text: "first\n*/ SELECT 1;"}, {Before: test.count, Text: ""}}
			encoded, err := atlasschema.MarshalPlanFile(plan)
			c.Assert(err, qt.IsNil)
			decoded, _, err := atlasschema.DecodePlanFile(encoded, "notes.plan.json")
			c.Assert(err, qt.IsNil)
			c.Assert(decoded.Annotations, qt.DeepEquals, plan.Annotations)
			c.Assert(decoded.FromFingerprint, qt.Equals, plan.FromFingerprint)
			c.Assert(decoded.ToFingerprint, qt.Equals, plan.ToFingerprint)
			c.Assert(decoded.HasChanges(), qt.Equals, test.count > 0)
			c.Assert(decoded.UnitReports(), qt.HasLen, test.count)
			var preview bytes.Buffer
			atlasschema.WriteUnitReports(&preview, decoded.UnitReports(), decoded.Annotations)
			c.Assert(preview.String(), qt.Matches, `(?s)-- first\n-- \*/ SELECT 1;\n.*--\n`)
			changed := plan
			changed.Annotations = []renderer.Annotation{{Before: 0, Text: "changed"}}
			other, err := atlasschema.MarshalPlanFile(changed)
			c.Assert(err, qt.IsNil)
			c.Assert(bytes.Equal(encoded, other), qt.IsFalse)
			c.Assert(changed.FromFingerprint, qt.Equals, plan.FromFingerprint)
			_, err = atlasschema.MarshalPlanFileHCL(plan)
			c.Assert(err, qt.ErrorMatches, ".*cannot preserve positioned annotations.*")
		})
	}
}

func TestAnnotationsPreserveExecutionInputPreview(t *testing.T) {
	c := qt.New(t)
	units := []atlasschema.ApplyUnitReport{{SQL: "SELECT 1;", Inputs: []renderer.InputReference{{Source: "environment", Name: "TOKEN"}}}}
	var out bytes.Buffer
	atlasschema.WriteUnitReports(&out, units, []renderer.Annotation{{Before: 0, Text: "review"}, {Before: 1, Text: "done"}})
	c.Assert(out.String(), qt.Equals, "-- review\nExecution input: environment TOKEN\nSELECT 1;\n-- done\n")
}

// A native unit is written as its owner's text form where the report carries
// one, as an SQL unit is, and by kind, subject and assessment where it does
// not.
func TestUnitReportsWriteANativeUnitAsItsTextForm(t *testing.T) {
	c := qt.New(t)
	units := []atlasschema.ApplyUnitReport{
		{Native: &atlasschema.NativeUnitReport{Kind: "example.org/fixture/table-operation", Subject: "items", Severity: "safe", Reason: "Adds a column",
			Access: "unchanged", AccessReason: "No grant changes", Text: `ALTER TABLE "items" ADD COLUMN "added" REAL;`}},
		{Native: &atlasschema.NativeUnitReport{Kind: "example.org/fixture/table-operation", Subject: "items", Severity: "destructive", Reason: "Rebuilds the table"}},
	}
	var out bytes.Buffer

	atlasschema.WriteUnitReports(&out, units, []renderer.Annotation{{Before: 1, Text: "rebuild"}})

	c.Assert(out.String(), qt.Equals, "ALTER TABLE \"items\" ADD COLUMN \"added\" REAL;\n-- rebuild\n"+
		"Native operation: example.org/fixture/table-operation\n  items: Rebuilds the table (destructive)\n")
}

// The compatibility surface writes the comment that names a unit's change above
// its statement, below any note the statement opens with. A native unit without
// a text form keeps its summary, which already states its kind and subject.
func TestNamedUnitReportsWriteTheChangeAboveTheStatement(t *testing.T) {
	c := qt.New(t)
	text := &atlasschema.NativeUnitReport{Kind: "example.org/fixture/table-operation", Subject: "items", Severity: "safe", Reason: "Adds a column", Text: `ALTER TABLE "items" ADD COLUMN "added" REAL;`}
	units := []atlasschema.ApplyUnitReport{
		{SQL: "-- a note\nCREATE TABLE \"t\" (\"id\" int);", Change: `Create "t" table`},
		{SQL: "CREATE INDEX \"i\" ON \"t\" (\"id\");"},
		{Native: text, Change: `Modify "items" table`},
		{Native: &atlasschema.NativeUnitReport{Kind: "example.org/fixture/table-operation", Subject: "items", Severity: "destructive", Reason: "Rebuilds the table"}, Change: `Modify "items" table`},
	}
	var out bytes.Buffer

	atlasschema.WriteNamedUnitReports(&out, units, []renderer.Annotation{{Before: 1, Text: "review"}})

	c.Assert(out.String(), qt.Equals, "-- a note\n-- Create \"t\" table\nCREATE TABLE \"t\" (\"id\" int);\n"+
		"-- review\nCREATE INDEX \"i\" ON \"t\" (\"id\");\n"+
		"-- Modify \"items\" table\nALTER TABLE \"items\" ADD COLUMN \"added\" REAL;\n"+
		"Native operation: example.org/fixture/table-operation\n  items: Rebuilds the table (destructive)\n")
	c.Assert(text.Text, qt.Equals, `ALTER TABLE "items" ADD COLUMN "added" REAL;`)
	c.Assert(units[0].SQL, qt.Equals, "-- a note\nCREATE TABLE \"t\" (\"id\" int);")
}

// The native surface lists the statements as they run, and its evidence document
// has no field for the name: the comment is the Atlas CLI's.
func TestUnitReportsLeaveTheChangeOutOfTheListingAndTheEvidence(t *testing.T) {
	c := qt.New(t)
	units := []atlasschema.ApplyUnitReport{{SQL: "CREATE TABLE \"t\" (\"id\" int);", Change: `Create "t" table`}}
	var out bytes.Buffer

	atlasschema.WriteUnitReports(&out, units, nil)
	document, err := json.Marshal(units)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "CREATE TABLE \"t\" (\"id\" int);\n")
	c.Assert(string(document), qt.Equals, `[{"sql":"CREATE TABLE \"t\" (\"id\" int);"}]`)
}

func TestSavedAnnotationsRefuseInvalidPositions(t *testing.T) {
	for _, test := range []struct {
		name  string
		notes []renderer.Annotation
	}{
		{"negative", []renderer.Annotation{{Before: -1}}},
		{"past end", []renderer.Annotation{{Before: 3}}},
		{"unordered", []renderer.Annotation{{Before: 1}, {Before: 0}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan := planWithNative(c)
			plan.Annotations = test.notes
			encoded, err := json.Marshal(plan)
			c.Assert(err, qt.IsNil)
			_, _, err = atlasschema.DecodePlanFile(encoded, "notes.plan.json")
			c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
			_, err = atlasschema.MarshalPlanFile(plan)
			c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
		})
	}
}
