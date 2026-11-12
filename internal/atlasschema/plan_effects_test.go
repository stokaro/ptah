package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/planeffect"
	"ptah.run/migration/safety"
)

const (
	effectsReplaceSQL = "CREATE OR REPLACE VIEW \"order_totals\" WITH (security_invoker=true) AS\nSELECT id FROM orders WHERE total > 0"
	effectsTableSQL   = `CREATE TABLE "notes" ("id" integer)`
)

// restatedInvoker is the effect of a body change that restates
// security_invoker.
var restatedInvoker = []planeffect.Effect{{
	Version: planeffect.Version,
	Object:  planeffect.Object{Kind: planeffect.KindView, Schema: "public", Name: "order_totals"},
	Action:  planeffect.ActionReplace,
	Properties: []planeffect.Property{
		{Name: "security_barrier", Before: new(false), After: new(false)},
		{Name: "security_invoker", Before: new(true), After: new(true)},
	},
}}

// effectsPlan is a plan whose first statement carries an effect and whose
// second carries none.
func effectsPlan() atlasschema.PlanFile {
	return atlasschema.PlanFile{
		FormatVersion:   atlasschema.PlanFormatVersion,
		Name:            "views",
		Dialect:         "postgres",
		FromFingerprint: "sha256:" + fingerprintHex('a'),
		ToFingerprint:   "sha256:" + fingerprintHex('b'),
		Statements: []atlasschema.PlanStatement{
			{SQL: effectsReplaceSQL, Severity: safety.Safe, Reason: "does not remove data or tighten constraints", Effects: restatedInvoker},
			{SQL: effectsTableSQL, Severity: safety.Safe, Reason: "does not remove data or tighten constraints"},
		},
	}
}

// TestPlanFile_EffectsSurviveTheFile pins that the strict reader of a plan file
// accepts the effects the writer records, and returns them as they were.
func TestPlanFile_EffectsSurviveTheFile(t *testing.T) {
	c := qt.New(t)
	plan := effectsPlan()

	document, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	read, _, err := atlasschema.DecodePlanFile(document, "views.plan.json")

	c.Assert(err, qt.IsNil)
	c.Assert(read.Statements[0].Effects, qt.DeepEquals, restatedInvoker)
	c.Assert(read.Statements[1].Effects, qt.IsNil)
	c.Assert(string(document), qt.Contains, `"effects": [`)
	c.Assert(string(document), qt.Contains, `"changed": false`)
}

// TestPlanFileUnitReports_CarryEffects pins that `schema apply --plan` lists
// each unit with the effects its plan recorded.
func TestPlanFileUnitReports_CarryEffects(t *testing.T) {
	c := qt.New(t)

	units := effectsPlan().UnitReports()

	c.Assert(units, qt.HasLen, 2)
	c.Assert(units[0].Effects, qt.DeepEquals, restatedInvoker)
	c.Assert(units[1].Effects, qt.IsNil)
}

// TestPlanFileWithStatementsFromSQL_KeepsEffectsOfThePlannedSequence pins that
// an edit that leaves the statements as they were, comments aside, keeps what
// each statement does.
func TestPlanFileWithStatementsFromSQL_KeepsEffectsOfThePlannedSequence(t *testing.T) {
	c := qt.New(t)
	plan := effectsPlan()

	edited := withStatements(c, plan, "-- reviewed\n"+plan.SQL())

	c.Assert(edited.Statements, qt.HasLen, 2)
	c.Assert(edited.Statements[0].Effects, qt.DeepEquals, restatedInvoker)
	c.Assert(edited.Statements[1].Effects, qt.IsNil)
}

// TestPlanFileWithStatementsFromSQL_DropsEffectsAfterAnEdit pins that any other
// edit leaves no effect: a statement moved or added runs against a state the
// plan did not compute, so what the plan recorded for it may no longer hold.
func TestPlanFileWithStatementsFromSQL_DropsEffectsAfterAnEdit(t *testing.T) {
	tests := []struct {
		name   string
		edited string
	}{
		{name: "the statements in another order", edited: effectsTableSQL + ";\n" + effectsReplaceSQL + ";\n"},
		{name: "a statement added", edited: effectsReplaceSQL + ";\n" + effectsTableSQL + ";\nALTER VIEW \"order_totals\" RESET (security_invoker);\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			edited := withStatements(c, effectsPlan(), test.edited)

			c.Assert(edited.Statements, qt.Not(qt.HasLen), 0)
			c.Assert(effectsOf(edited), qt.DeepEquals, make([][]planeffect.Effect, len(edited.Statements)))
		})
	}
}

// effectsOf lists the effects of each statement of plan.
func effectsOf(plan atlasschema.PlanFile) [][]planeffect.Effect {
	effects := make([][]planeffect.Effect, len(plan.Statements))
	for i, statement := range plan.Statements {
		effects[i] = statement.Effects
	}
	return effects
}
