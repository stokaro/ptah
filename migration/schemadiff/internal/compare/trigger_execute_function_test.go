package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// The two sides qualify a trigger's function differently, so the comparison
// uses the more specific spelling both of them carry (stokaro/ptah#3113).
//
// A declaration names the function the way its author wrote it -- `app.touch`
// in a SQL file, or the qualified name an HCL reference resolves to -- while
// the catalog reports pg_proc.proname, which carries no schema. Compared as
// text, a trigger nobody had touched was planned for replacement on every run.
func TestTriggerDefinitions_AQualifiedFunctionMatchesItsBareName(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		observed string
	}{
		{name: "declared qualified against a bare catalog", declared: "app.touch", observed: "touch"},
		{name: "declared bare against a qualified catalog", declared: "touch", observed: "app.touch"},
		{name: "both bare", declared: "touch", observed: "touch"},
		{name: "both qualified and the same", declared: "app.touch", observed: "app.touch"},
		{name: "case differs", declared: "APP.Touch", observed: "app.touch"},
		{name: "a name holding a dot, qualified against bare", declared: `public."Dot.Fn"`, observed: `"Dot.Fn"`},
		{name: "a schema holding a dot", declared: `"Up.Case"."Fn"`, observed: `"Up.Case"."Fn"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.TriggerDefinitions(
				schemamodel.Trigger{Name: "t", Table: "app.t", ExecuteFunction: test.declared},
				catalog.Trigger{Name: "t", Table: "app.t", ExecuteFunction: test.observed},
				identifier.ForDialect(platform.Postgres),
			)

			c.Assert(diff.Changes["function"], qt.Equals, "")
		})
	}
}

// TestTriggerDefinitions_TwoSchemasAreTwoFunctions is the control.
//
// Folding the qualifier off both sides would bind a trigger to whichever
// `touch` the search path happened to find, so two qualified names stay two
// functions and a real rebinding is still reported.
func TestTriggerDefinitions_TwoSchemasAreTwoFunctions(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		observed string
	}{
		{name: "a different schema", declared: "app.touch", observed: "other.touch"},
		{name: "a different function", declared: "app.touch", observed: "app.stamp"},
		{name: "a different bare function", declared: "touch", observed: "stamp"},
		{name: "a different name holding a dot", declared: `public."Dot.Fn"`, observed: `"Dot.Gn"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compare.TriggerDefinitions(
				schemamodel.Trigger{Name: "t", Table: "app.t", ExecuteFunction: test.declared},
				catalog.Trigger{Name: "t", Table: "app.t", ExecuteFunction: test.observed},
				identifier.ForDialect(platform.Postgres),
			)

			c.Assert(diff.Changes["function"], qt.Not(qt.Equals), "")
		})
	}
}

func TestTriggerDefinitions_FunctionArgumentsAreCompared(t *testing.T) {
	c := qt.New(t)
	desired := schemamodel.Trigger{Name: "t", Table: "app.t", ExecuteFunction: "app.touch", FunctionArguments: []string{"updated_at"}}
	current := catalog.Trigger{Name: "t", Table: "app.t", ExecuteFunction: "touch", FunctionArguments: []string{"updated_at"}}
	semantics := identifier.ForDialect(platform.Postgres)
	c.Assert(compare.TriggerDefinitions(desired, current, semantics).Changes, qt.HasLen, 0)
	current.FunctionArguments = []string{"created_at"}
	c.Assert(compare.TriggerDefinitions(desired, current, semantics).Changes["arguments"], qt.Not(qt.Equals), "")
}

// TestTriggers_ARemovedTriggerDropsItsGeneratedFunctionInAnySchema removes a
// trigger whose read names the function Ptah generated for it with a schema.
// The removal carries no function of its own, so the function is dropped with
// the trigger, and it carries the read's name of the generated function, so the
// drop names its schema; a declared function in another schema stays named,
// and stays.
func TestTriggers_ARemovedTriggerDropsItsGeneratedFunctionInAnySchema(t *testing.T) {
	generated := schemamodel.Trigger{Name: "t2", Table: "app.items"}.FunctionName()
	tests := []struct {
		name          string
		executed      string
		wantDeclared  string
		wantGenerated string
	}{
		{name: "generated, in another schema", executed: "app." + generated, wantGenerated: "app." + generated},
		{name: "generated, in the read's schema", executed: generated},
		{name: "generated under the name another table spelling gave", executed: "ptah_trigger_public__items_t2", wantGenerated: "ptah_trigger_public__items_t2"},
		{name: "declared, in another schema", executed: "audit.set_updated_at", wantDeclared: "audit.set_updated_at"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.Triggers(&schemamodel.Database{}, &catalog.Database{
				Triggers: []catalog.Trigger{{
					Name: "t2", Schema: "app", Table: "items", Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW",
					ExecuteFunction: test.executed,
				}},
			}, diff)

			c.Assert(diff.TriggersRemoved, qt.HasLen, 1)
			c.Assert(diff.TriggersRemoved[0].ExecuteFunction, qt.Equals, test.wantDeclared)
			c.Assert(diff.TriggersRemoved[0].GeneratedFunction, qt.Equals, test.wantGenerated)
		})
	}
}

// TestTriggers_AReplacementRewritesTheGeneratedFunctionWhereItIs modifies a
// trigger whose generated function the database holds in a schema the read
// names. The replacement's declaration carries that schema, so CREATE OR
// REPLACE rewrites the function the trigger runs rather than creating one of
// the same name where a bare name resolves and leaving the old one behind.
func TestTriggers_AReplacementRewritesTheGeneratedFunctionWhereItIs(t *testing.T) {
	generated := schemamodel.Trigger{Name: "t2", Table: "app.items"}.FunctionName()
	tests := []struct {
		name     string
		executed string
		declared string
		want     string
	}{
		{name: "held in a schema the read names", executed: "app." + generated, want: "app." + generated},
		{name: "held where a bare name resolves", executed: generated, want: ""},
		{name: "held under the name another table spelling gave", executed: "fns.ptah_trigger_items_t2", want: "fns.ptah_trigger_items_t2"},
		{name: "the declaration says where", executed: "app." + generated, declared: "other." + generated, want: "other." + generated},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.Triggers(&schemamodel.Database{
				Triggers: []schemamodel.Trigger{{
					Name: "t2", Table: "app.items", Timing: "AFTER", Event: "UPDATE", ForEach: "ROW",
					Body: "RETURN NEW;", FunctionReference: test.declared,
				}},
			}, &catalog.Database{
				Triggers: []catalog.Trigger{{
					Name: "t2", Schema: "app", Table: "items", Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW",
					Body: "BEGIN RETURN NEW; END;", ExecuteFunction: test.executed,
				}},
			}, diff)

			c.Assert(diff.TriggersModified, qt.HasLen, 1)
			c.Assert(diff.TriggersModified[0].Desired.FunctionReference, qt.Equals, test.want)
		})
	}
}
