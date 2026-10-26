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
