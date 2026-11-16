package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
)

// TestRemoveTriggers_TheGeneratedFunctionDropNamesTheReadsSchema removes a
// trigger whose generated function the read named with its schema. The DROP
// FUNCTION names that schema, so it drops the function the trigger ran: a
// bare name resolves along the session's search path, which need not reach
// the schema, and the apply then fails with SQLSTATE 42883 after the trigger
// is gone. A removal the read named bare keeps the derived bare name.
func TestRemoveTriggers_TheGeneratedFunctionDropNamesTheReadsSchema(t *testing.T) {
	generated := schemamodel.Trigger{Name: "t2", Table: "app.items"}.FunctionName()
	tests := []struct {
		name      string
		generated string
		want      string
	}{
		{name: "named with its schema", generated: "app." + generated, want: `DROP FUNCTION IF EXISTS "app"."` + generated + `"();`},
		{name: "named bare", generated: "", want: `DROP FUNCTION IF EXISTS "` + generated + `"();`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql := droppedRoutineSQL(c, &difftypes.SchemaDiff{
				TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "t2", TableName: "app.items", GeneratedFunction: test.generated}},
			})

			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestAddTriggers_TheGeneratedFunctionIsCreatedInItsSchema renders a trigger
// whose generated function a read found in a schema it names: the function is
// created there and the trigger runs that one.
func TestAddTriggers_TheGeneratedFunctionIsCreatedInItsSchema(t *testing.T) {
	c := qt.New(t)
	trigger := schemamodel.Trigger{
		Name: "t2", Table: "app.items", Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW",
		Body: "RETURN NEW;", FunctionReference: "app.ptah_trigger_app__items_t2",
	}

	sql := droppedRoutineSQL(c, &difftypes.SchemaDiff{
		TriggersAdded: []difftypes.TriggerRef{{TriggerName: "t2", TableName: "app.items", Desired: trigger}},
	})

	c.Assert(sql, qt.Contains, `FUNCTION "app"."`+trigger.FunctionName()+`"()`)
	c.Assert(sql, qt.Contains, `EXECUTE FUNCTION "app"."`+trigger.FunctionName()+`"()`)
}
