package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
)

// triggerSchema is one trigger on table `a` running the named function.
func triggerSchema(executeFunction string) *catalog.Database {
	return &catalog.Database{
		Triggers: []catalog.Trigger{{
			Name:            "trg_a",
			Table:           "a",
			Timing:          "AFTER",
			Event:           "INSERT",
			ForEach:         "ROW",
			Body:            "BEGIN INSERT INTO audit_log(msg) VALUES (TG_TABLE_NAME); RETURN NEW; END",
			ExecuteFunction: executeFunction,
		}},
	}
}

// onlyTrigger returns the single converted trigger.
func onlyTrigger(c *qt.C, database *schemamodel.Database) schemamodel.Trigger {
	c.Helper()
	c.Assert(database.Triggers, qt.HasLen, 1)
	return database.Triggers[0]
}

// TestConvert_KeepsATriggerBoundToSomebodyElsesFunction pins the binding.
//
// PostgreSQL has no inline trigger body: a trigger always names a function, and
// Ptah writes one per trigger when the declaration carries a body. Reading only
// that function's source and discarding its NAME made every trigger look like
// one Ptah owns, so one audit function shared by ten tables was described ten
// times as ten inline bodies -- and replaying that built ten copies under
// ptah_trigger_* names, leaving the original defined and called by nothing
// (stokaro/ptah#2210).
func TestConvert_KeepsATriggerBoundToSomebodyElsesFunction(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertCatalogToSchema(triggerSchema("audit_fn"))

	trigger := onlyTrigger(c, database)
	c.Assert(trigger.ExecuteFunction, qt.Equals, "audit_fn")
	// The body is KEPT beside the reference. The native SQL description uses the
	// reference; the Atlas HCL surface cannot name a function a trigger runs and
	// refuses a trigger without a body, so clearing it made `schema inspect`
	// omit the trigger and the document then planned a DROP of what it had just
	// described.
	c.Assert(trigger.Body, qt.Contains, "INSERT INTO audit_log")
}

// TestConvert_LeavesATriggerPtahOwnsInline is the control, and it is what stops
// the fix from rewriting every trigger this repository already produces.
//
// A function named the way Ptah names the one it generates for this trigger is
// Ptah's own. Describing it as an external reference would make a schema Ptah
// wrote stop round-tripping: the body would vanish from the document and the
// function would have to be declared separately.
func TestConvert_LeavesATriggerPtahOwnsInline(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertCatalogToSchema(triggerSchema("ptah_trigger_a_trg_a"))

	trigger := onlyTrigger(c, database)
	c.Assert(trigger.ExecuteFunction, qt.Equals, "")
	c.Assert(trigger.Body, qt.Contains, "INSERT INTO audit_log")
}

// TestConvert_LeavesATriggerWithNoReportedFunctionInline covers a reader that
// does not report the name at all, which is every non-PostgreSQL reader today.
func TestConvert_LeavesATriggerWithNoReportedFunctionInline(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertCatalogToSchema(triggerSchema(""))

	trigger := onlyTrigger(c, database)
	c.Assert(trigger.ExecuteFunction, qt.Equals, "")
	c.Assert(trigger.Body, qt.Contains, "INSERT INTO audit_log")
}
