package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/planapproval"
)

func TestSavedPlanBindsInputReferencesToApprovalAndReports(t *testing.T) {
	c := qt.New(t)
	_, conn, plan := prepareBoundPlan(c)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	refs := []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "DECLARED_INPUT"}}
	plan.Statements[0].Inputs = refs
	encoded, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	decoded, _, err := atlasschema.DecodePlanFile(encoded, "inputs.plan.json")
	c.Assert(err, qt.IsNil)
	c.Assert(decoded.HasInputReferences(), qt.IsTrue)
	c.Assert(decoded.UnitReports()[0].Inputs, qt.DeepEquals, refs)
	execution, err := decoded.ExecutionPlan(c.Context(), conn, runtime)
	c.Assert(err, qt.IsNil)
	units, err := execution.Units()
	c.Assert(err, qt.IsNil)
	c.Assert(units[0].Inputs, qt.DeepEquals, refs)
	decoded.Statements[0].Inputs[0].Name = "DIFFERENT_INPUT"
	changed, err := atlasschema.MarshalPlanFile(decoded)
	c.Assert(err, qt.IsNil)
	c.Assert(planapproval.Digest(changed), qt.Not(qt.Equals), planapproval.Digest(encoded))
	c.Assert(units[0].Inputs, qt.DeepEquals, refs)
}

func TestSavedPlanRefusesInputApprovalWithoutProviderOrValidNames(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*atlasschema.PlanFile)
		want   string
	}{
		{"no binding", func(p *atlasschema.PlanFile) { p.Provider = nil }, "records execution inputs without the provider binding"},
		{"invalid reference", func(p *atlasschema.PlanFile) { p.Statements[0].Inputs[0].Source = "file" }, "execution input needs an environment source"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, _, plan := prepareBoundPlan(c)
			plan.Statements[0].Inputs = []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "DECLARED_INPUT"}}
			test.change(&plan)
			encoded, err := atlasschema.MarshalPlanFile(plan)
			c.Assert(err, qt.IsNil)
			_, _, err = atlasschema.DecodePlanFile(encoded, "inputs.plan.json")
			c.Assert(err, qt.ErrorMatches, ".*"+test.want+".*")
		})
	}
}
