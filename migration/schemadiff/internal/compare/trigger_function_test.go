package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// declaredTrigger is the desired side: either an external function reference or
// an inline body, never both.
func declaredTrigger(executeFunction, body string) goschema.Trigger {
	return goschema.Trigger{
		Name: "trg_a", Table: "a", Timing: "AFTER", Event: "INSERT", ForEach: "ROW",
		ExecuteFunction: executeFunction, Body: body,
	}
}

// catalogTrigger is the same trigger as the reader reports it. The body is
// always the source of whatever function the trigger runs, because PostgreSQL
// has no inline body to report.
func catalogTrigger(executeFunction, body string) types.DBTrigger {
	return types.DBTrigger{
		Name: "trg_a", Table: "a", Timing: "AFTER", Event: "INSERT", ForEach: "ROW",
		ExecuteFunction: executeFunction, Body: body,
	}
}

const auditBody = "BEGIN INSERT INTO audit_log(msg) VALUES (TG_TABLE_NAME); RETURN NEW; END"

// assertChange holds the diff against one expected change key, where an empty
// key means the two sides agree.
func assertChange(c *qt.C, diff difftypes.TriggerDiff, key string) {
	c.Helper()
	if key == "" {
		c.Assert(diff.Changes, qt.HasLen, 0)
		return
	}
	c.Assert(diff.Changes[key], qt.Not(qt.Equals), "")
}

// TestTriggerDefinitions_AnExternalFunctionIsComparedByName pins that a
// declaration naming a function is held against the function the trigger runs,
// not against that function's source.
//
// The desired side of such a declaration has no body -- the function is
// somebody else's -- and the database side is always the running function's
// source. Comparing those made a declaration that names an existing function
// differ from a database that already matched it, so `CREATE OR REPLACE TRIGGER`
// was planned on every run and `--dry-run` never reported the schema as synced
// (stokaro/ptah#2210).
func TestTriggerDefinitions_AnExternalFunctionIsComparedByName(t *testing.T) {
	tests := []struct {
		name      string
		declared  string
		current   string
		changeKey string
	}{
		{name: "the same function", declared: "audit_fn", current: "audit_fn", changeKey: ""},
		{name: "case only", declared: "AUDIT_FN", current: "audit_fn", changeKey: ""},
		{name: "a different function", declared: "other_fn", current: "audit_fn", changeKey: "function"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.TriggerDefinitions(
				declaredTrigger(test.declared, ""),
				catalogTrigger(test.current, auditBody))

			assertChange(c, diff, test.changeKey)
		})
	}
}

// TestTriggerDefinitions_ATriggerPtahOwnsIsStillComparedByBody is the control.
//
// When the running function is the one Ptah generates for this trigger, the
// declaration owns the body and a change to it is the only thing worth
// reporting. Comparing by name there would compare a generated name against
// itself and never notice an edited body.
func TestTriggerDefinitions_ATriggerPtahOwnsIsStillComparedByBody(t *testing.T) {
	tests := []struct {
		name      string
		declared  string
		changeKey string
	}{
		{name: "an unchanged body", declared: auditBody, changeKey: ""},
		{name: "an edited body", declared: "BEGIN INSERT INTO audit_log(msg) VALUES (TG_OP); RETURN NEW; END", changeKey: "body"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.TriggerDefinitions(
				declaredTrigger("", test.declared),
				catalogTrigger("ptah_trigger_a_trg_a", auditBody))

			assertChange(c, diff, test.changeKey)
		})
	}
}

// TestTriggerDefinitions_ADeclarationCarryingOnlyABodyComparesByBody is the
// second control, and it is the reason the name comparison needs BOTH sides.
//
// The Atlas HCL surface cannot name the function a trigger runs -- it refuses a
// trigger without a body -- so a document inspected through it always describes
// the body, even for a trigger bound to somebody else's function. Reading that
// as a request to rebind would plan a change on every run for every such trigger,
// which is what a first attempt at this fix did: `schema inspect` dropped the
// trigger from the document and applying it planned a DROP of the trigger and
// its function.
func TestTriggerDefinitions_ADeclarationCarryingOnlyABodyComparesByBody(t *testing.T) {
	tests := []struct {
		name      string
		declared  string
		changeKey string
	}{
		{name: "the same body", declared: auditBody, changeKey: ""},
		{name: "an edited body", declared: "BEGIN INSERT INTO audit_log(msg) VALUES (TG_OP); RETURN NEW; END", changeKey: "body"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.TriggerDefinitions(
				declaredTrigger("", test.declared),
				catalogTrigger("audit_fn", auditBody))

			assertChange(c, diff, test.changeKey)
		})
	}
}
