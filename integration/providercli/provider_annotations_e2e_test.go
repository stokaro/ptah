//go:build integration

package providercli_test

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/plangraph"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
)

func TestProviderAnnotationsStayOutsideExecutionAcrossProcess(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native"})
	conn, _ := transactionConnection(c, host, "")
	note := "review"
	executor := conn.Writer().(dbschema.UnitExecutor)
	c.Assert(executor.ExecuteUnits(c.Context(), []renderer.Fragment{{SQL: "CREATE TABLE rejected (id INTEGER);"}, {Annotation: &note}}), qt.ErrorIs, renderer.ErrInvalidResult)
	c.Assert(providerTablePresent(c, conn, "rejected"), qt.IsFalse)
	create := capturedOperation(c, runtime, "records", `{"action":"create"}`)
	common := &ast.CreateTableNode{Name: "common", Columns: []*ast.ColumnNode{{Name: "id", Type: "INTEGER", Primary: true}}}
	request := renderer.Request{Target: "fixture", Capabilities: conn.Info().Capabilities, Nodes: []ast.Node{
		ast.NewComment("before\nCREATE TABLE injected (id INTEGER);"), common, ast.NewComment(""),
		&ast.ExtensionStatement{Payload: create, Transaction: plangraph.TransactionForbidden},
		ast.NewComment("before duplicate"), &ast.ExtensionStatement{Payload: create, Transaction: plangraph.TransactionForbidden}, ast.NewComment("trailing"),
	}}
	result, err := runtime.Render(c.Context(), request)
	c.Assert(err, qt.IsNil)
	captured, err := atlasschema.CaptureApplyPlan(conn.Info(), planner.RenderedPlan{Request: request, Result: result, Dialect: "fixture"})
	c.Assert(err, qt.IsNil)
	units, err := captured.Units()
	c.Assert(err, qt.IsNil)
	c.Assert(units, qt.HasLen, 3)
	c.Assert(captured.Annotations(), qt.DeepEquals, []renderer.Annotation{{Before: 0, Text: "before\nCREATE TABLE injected (id INTEGER);"}, {Before: 1}, {Before: 2, Text: "before duplicate"}, {Before: 3, Text: "trailing"}})
	c.Assert(captured.Preflight(c.Context(), conn, migrator.MigrationTxModeNone), qt.IsNil)
	c.Assert(providerTablePresent(c, conn, "common"), qt.IsFalse)
	err = captured.Execute(c.Context(), conn, migrator.MigrationTxModeNone)
	failure, ok := errors.AsType[*dbschema.UnitExecutionError](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(failure.Total, qt.Equals, 3)
	c.Assert(failure.Applied, qt.Equals, 2)
	c.Assert(providerTablePresent(c, conn, "common"), qt.IsTrue)
	c.Assert(providerTablePresent(c, conn, "records"), qt.IsTrue)
	c.Assert(providerTablePresent(c, conn, "injected"), qt.IsFalse)
}

func TestProviderSavedAnnotationsSurvivePreviewAndApply(t *testing.T) {
	c := qt.New(t)
	f := nativeCommandFixture(c)
	plan := savedNativePlan(c, f)
	plan.Annotations = []renderer.Annotation{{Before: 0, Text: "review\n*/ SELECT 1;"}, {Before: len(plan.Statements), Text: "trailing"}}
	document, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(f.planPath, document, 0o600), qt.IsNil)
	preview := f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--dry-run", "--json")
	var report atlasschema.ApplyReport
	c.Assert(json.Unmarshal([]byte(preview), &report), qt.IsNil)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeDryRun)
	c.Assert(report.Units, qt.HasLen, 1)
	c.Assert(report.Annotations, qt.DeepEquals, plan.Annotations)
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), `"Name": "added"`)
	applied := f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve", "--json")
	c.Assert(json.Unmarshal([]byte(applied), &report), qt.IsNil)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
	c.Assert(report.Units, qt.HasLen, 1)
	c.Assert(report.Annotations, qt.DeepEquals, plan.Annotations)
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Contains, `"Name": "added"`)
}
