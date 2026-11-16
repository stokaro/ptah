package schemamodel_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// RunsDeclaredFunction tells the function Ptah generates for a trigger from
// one declared on its own. A read qualifies a function outside the schema it
// resolves names in, the generated function lands wherever the session that
// created it resolved a bare name, and its name follows the table's spelling
// at the time, so the trigger's generated name is its own in any schema, with
// the table written bare or in any schema, and however it is quoted. Any other
// name is a function of its own, one carrying Ptah's prefix included.
func TestTriggerRunsDeclaredFunction(t *testing.T) {
	trigger := schemamodel.Trigger{Name: "t", Table: "app.items"}
	generated := trigger.FunctionName()
	tests := []struct {
		name     string
		executed string
		want     bool
	}{
		{name: "nothing reported", executed: "", want: false},
		{name: "the generated name", executed: generated, want: false},
		{name: "the generated name in another schema", executed: "app." + generated, want: false},
		{name: "the generated name quoted", executed: `"app"."` + generated + `"`, want: false},
		{name: "the name a bare table spelling gives", executed: "ptah_trigger_items_t", want: false},
		{name: "the name another schema spelling gives", executed: "fns.ptah_trigger_public__items_t", want: false},
		{name: "a declared function", executed: "set_updated_at", want: true},
		{name: "a function of its own with Ptah's prefix", executed: "ptah_trigger_audit", want: true},
		{name: "another trigger's generated name", executed: "ptah_trigger_items_u", want: true},
		{name: "another table's generated name", executed: "ptah_trigger_orders_t", want: true},
		{name: "a declared function in another schema", executed: "audit.set_updated_at", want: true},
		{name: "a schema named like the generated function", executed: generated + ".touch", want: true},
		{name: "the prefix in another case", executed: `"PTAH_TRIGGER_items_t"`, want: true},
		{name: "the prefix with its underscores replaced", executed: "ptahxtriggery", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(trigger.RunsDeclaredFunction(test.executed), qt.Equals, test.want)
		})
	}
}

// GeneratedFunctionReference keeps the reference a read or a schema file gave
// the trigger's generated function, so a statement creating or dropping it
// reaches the function the database holds rather than whichever one a bare
// FunctionName resolves to. FunctionName itself, named bare, is what an empty
// reference already means.
func TestTriggerGeneratedFunctionReference(t *testing.T) {
	trigger := schemamodel.Trigger{Name: "t", Table: "app.items"}
	generated := trigger.FunctionName()
	tests := []struct {
		name     string
		executed string
		want     string
	}{
		{name: "nothing reported", executed: "", want: ""},
		{name: "the generated name, bare", executed: generated, want: ""},
		{name: "the generated name in a schema", executed: "app." + generated, want: "app." + generated},
		{name: "the generated name in a quoted schema", executed: `"App"."` + generated + `"`, want: "App." + generated},
		{name: "a schema holding a dot", executed: `"a.b".` + generated, want: `"a.b".` + generated},
		{name: "the name another table spelling gave, bare", executed: "ptah_trigger_items_t", want: "ptah_trigger_items_t"},
		{name: "the name another table spelling gave, in a schema", executed: "fns.ptah_trigger_items_t", want: "fns.ptah_trigger_items_t"},
		{name: "a declared function in a schema", executed: "audit.set_updated_at", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(trigger.GeneratedFunctionReference(test.executed), qt.Equals, test.want)
		})
	}
}

// GeneratedFunction names the function a statement creates, runs and drops:
// the reference when there is one, and FunctionName otherwise.
func TestTriggerGeneratedFunction(t *testing.T) {
	tests := []struct {
		name      string
		reference string
		want      string
	}{
		{name: "no reference", reference: "", want: "ptah_trigger_app__items_t"},
		{name: "a reference in a schema", reference: "app.ptah_trigger_app__items_t", want: "app.ptah_trigger_app__items_t"},
		{name: "a reference of another name", reference: "fns.ptah_trigger_items_t", want: "fns.ptah_trigger_items_t"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			trigger := schemamodel.Trigger{Name: "t", Table: "app.items", FunctionReference: test.reference}

			c.Assert(trigger.GeneratedFunction(), qt.Equals, test.want)
		})
	}
}

// A generated name long enough to be hashed is recognized as FunctionName
// spells it, in any schema.
func TestTriggerRunsDeclaredFunction_HashedName(t *testing.T) {
	c := qt.New(t)
	trigger := schemamodel.Trigger{Name: "t", Table: "app." + strings.Repeat("x", 60)}
	generated := trigger.FunctionName()

	c.Assert(len(generated) <= 63, qt.IsTrue)
	c.Assert(trigger.RunsDeclaredFunction(generated), qt.IsFalse)
	c.Assert(trigger.RunsDeclaredFunction("app."+generated), qt.IsFalse)
}
