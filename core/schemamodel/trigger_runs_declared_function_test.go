package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// RunsDeclaredFunction tells the function Ptah generates for a trigger from
// one declared on its own. A read qualifies a function outside the schema it
// resolves names in, and the generated function lands wherever the session
// that created it resolved a bare name, so the generated name is the trigger's
// own in any schema and however it is quoted.
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
		{name: "a declared function", executed: "set_updated_at", want: true},
		{name: "a declared function in another schema", executed: "audit.set_updated_at", want: true},
		{name: "a schema named like the generated function", executed: generated + ".touch", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(trigger.RunsDeclaredFunction(test.executed), qt.Equals, test.want)
		})
	}
}
