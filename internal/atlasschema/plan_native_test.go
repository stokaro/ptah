package atlasschema_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
	"ptah.run/migration/safety"
)

const nativeTestKind schemaext.Kind = "example.provider/table-change"

func nativeStatement() atlasschema.PlanStatement {
	return atlasschema.PlanStatement{
		Native: &atlasschema.PlanNative{
			Kind:    nativeTestKind,
			Subject: "items",
			Operation: schemaext.Envelope{
				Format: 1, Owner: "example.provider", Kind: nativeTestKind, Representation: schemaext.Operation,
				Version: 1, Definition: "sha256:" + strings.Repeat("e", 64), Payload: []byte(`{"action":"add"}`),
			},
		},
		Severity: safety.Safe, Reason: "adds a column", Owned: true,
	}
}

func planWithNative(c *qt.C) atlasschema.PlanFile {
	c.Helper()
	_, _, plan := prepareBoundPlan(c)
	plan.Statements = append(plan.Statements, nativeStatement())
	return plan
}

func TestSavedPlanKeepsNativeOperations_HappyPath(t *testing.T) {
	c := qt.New(t)
	plan := planWithNative(c)
	encoded, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	decoded, _, err := atlasschema.DecodePlanFile(encoded, "native.plan.json")
	c.Assert(err, qt.IsNil)
	c.Assert(decoded.HasNative(), qt.IsTrue)
	// The document indents the payload, so its bytes differ and its JSON does not.
	c.Assert(string(decoded.Statements[1].Native.Operation.Payload), qt.JSONEquals, map[string]string{"action": "add"})
	c.Assert(decoded.Statements[1].Native.Operation.Definition, qt.Equals, plan.Statements[1].Native.Operation.Definition)
	c.Assert(decoded.Statements[1].Native.Operation.Owner, qt.Equals, "example.provider")
	again, err := atlasschema.MarshalPlanFile(decoded)
	c.Assert(err, qt.IsNil)
	c.Assert(string(again), qt.Equals, string(encoded))

	units := decoded.UnitReports()
	c.Assert(units, qt.HasLen, 2)
	c.Assert(units[0].SQL, qt.Contains, "CREATE TABLE")
	c.Assert(units[0].Native, qt.IsNil)
	c.Assert(units[1].SQL, qt.Equals, "")
	c.Assert(units[1].Native, qt.DeepEquals, &atlasschema.NativeUnitReport{
		Kind: nativeTestKind, Subject: "items", Severity: safety.Safe, Reason: "adds a column",
	})
	c.Assert(decoded.StatementSQL(), qt.HasLen, 2)
	c.Assert(decoded.StatementSQL()[1], qt.Equals, "")
	c.Assert(decoded.SQL(), qt.Not(qt.Contains), "table-change")
}

func TestSavedPlanWithoutNativeOperationsIsUnchanged_HappyPath(t *testing.T) {
	c := qt.New(t)
	_, _, plan := prepareBoundPlan(c)
	c.Assert(plan.HasNative(), qt.IsFalse)
	encoded, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Not(qt.Contains), `"native"`)
}

func TestSavedPlanRefusesMalformedNativeOperations_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*atlasschema.PlanFile)
		want   string
	}{
		{"sql beside operation", func(p *atlasschema.PlanFile) { p.Statements[1].SQL = "SELECT 1" }, `plan statement 2 records both sql and a native operation`},
		{"no provider binding", func(p *atlasschema.PlanFile) { p.Provider = nil }, `plan statement 2 records a native operation, which needs the provider binding.*`},
		{"no owner verdict", func(p *atlasschema.PlanFile) { p.Statements[1].Owned = false }, `plan statement 2 records a native operation without its owner verdict`},
		{"kind not namespaced", func(p *atlasschema.PlanFile) { p.Statements[1].Native.Kind = "table-change" }, `plan statement 2 native operation kind "table-change" is not namespaced`},
		{"kind differs from envelope", func(p *atlasschema.PlanFile) { p.Statements[1].Native.Kind = "example.provider/other" }, `plan statement 2 native operation kind "example.provider/other" differs from its envelope kind.*`},
		{"wrong representation", func(p *atlasschema.PlanFile) { p.Statements[1].Native.Operation.Representation = schemaext.Desired }, `plan statement 2 native operation envelope has representation "desired".*`},
		{"empty statement", func(p *atlasschema.PlanFile) { p.Statements[1].Native = nil }, `plan statement 2 has empty sql`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan := planWithNative(c)
			test.change(&plan)
			encoded, err := atlasschema.MarshalPlanFile(plan)
			c.Assert(err, qt.IsNil)
			_, _, err = atlasschema.DecodePlanFile(encoded, "native.plan.json")
			c.Assert(err, qt.ErrorMatches, `invalid plan file native.plan.json: `+test.want)
		})
	}
}

func TestSavedPlanWithNativeOperationsRefusesSQLExecution_FailurePath(t *testing.T) {
	c := qt.New(t)
	_, conn, _ := prepareBoundPlan(c)
	plan := planWithNative(c)
	err := plan.Execute(c.Context(), conn, migrator.MigrationTxModeNone)
	c.Assert(err, qt.ErrorIs, renderer.ErrNativeSQL)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)

	encoded, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	path := filepath.Join(c.TempDir(), "native.plan.json")
	c.Assert(os.WriteFile(path, encoded, 0o600), qt.IsNil)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	_, _, err = atlasschema.ReadPlanDocument(c.Context(), runtime, path)
	c.Assert(err, qt.ErrorIs, renderer.ErrNativeSQL)
	complete, format, err := atlasschema.ReadExecutionPlanDocument(c.Context(), runtime, path)
	c.Assert(err, qt.IsNil)
	c.Assert(format, qt.Equals, atlasschema.PlanFormatJSON)
	c.Assert(complete.HasNative(), qt.IsTrue)
	c.Assert(complete.Statements, qt.HasLen, 2)
}
