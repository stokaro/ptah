package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// triggerEventsDatabase is a table and one trigger on it firing on event.
func triggerEventsDatabase(event string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "t", StructName: "t"}},
		Fields: []schemamodel.Field{{StructName: "t", Name: "id", Type: "integer"}},
		Triggers: []schemamodel.Trigger{{
			Name: "t_touch", Table: "t", Timing: "BEFORE", Event: event, ForEach: "ROW",
			Body: "BEGIN RETURN NEW; END;",
		}},
	}
}

// TestRenderTriggerEvents_RoundTrip writes each event of a list as an
// attribute of the timing block and reads the same list back. The
// PostgreSQL reader reports `INSERT OR UPDATE` for an audit trigger, so a
// block that could hold one event left that trigger out of every inspected
// document (stokaro/ptah#3692).
func TestRenderTriggerEvents_RoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		event string
		want  string
	}{
		{name: "one event", event: "UPDATE", want: `    update = true`},
		{name: "two events", event: "INSERT OR UPDATE", want: "    insert = true\n    update = true"},
		{
			name:  "every event",
			event: "INSERT OR DELETE OR UPDATE OR TRUNCATE",
			want:  "    insert = true\n    delete = true\n    update = true\n    truncate = true",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rendered, err := atlashclrender.Render(triggerEventsDatabase(test.event))
			c.Assert(err, qt.IsNil)
			c.Assert(rendered.Diagnostics, qt.HasLen, 0)
			c.Assert(string(rendered.Data), qt.Contains, "  before {\n"+test.want+"\n  }\n")

			parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
			c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", string(rendered.Data)))
			c.Assert(parsed.Triggers, qt.HasLen, 1)
			c.Assert(parsed.Triggers[0].Event, qt.Equals, test.event)
		})
	}
}
