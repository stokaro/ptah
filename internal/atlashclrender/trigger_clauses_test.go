package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// TestRender_LeavesOutATriggerItCannotSpell reports a trigger whose clauses the
// HCL trigger block has no attribute for, and writes no block for it. Written
// without the clause, the block would describe a trigger that fires on every
// UPDATE rather than one of named columns, fires on other rows, or whose
// function reads transition tables that do not exist.
func TestRender_LeavesOutATriggerItCannotSpell(t *testing.T) {
	tests := []struct {
		name    string
		trigger schemamodel.Trigger
		want    atlashclrender.Diagnostic
	}{
		{
			name:    "no event",
			trigger: schemamodel.Trigger{Event: ""},
			want: atlashclrender.Diagnostic{
				Severity: atlashclrender.SeverityWarning, Path: `triggers["t"]["trg"]`,
				Message: "trigger event cannot be represented in HCL schema output",
			},
		},
		{
			name:    "an update column list",
			trigger: schemamodel.Trigger{Event: "INSERT OR UPDATE OF id"},
			want: atlashclrender.Diagnostic{
				Severity: atlashclrender.SeverityWarning, Path: `triggers["t"]["trg"]`,
				Message: "trigger event cannot be represented in HCL schema output",
			},
		},
		{
			name:    "a WHEN condition",
			trigger: schemamodel.Trigger{Event: "UPDATE", When: "NEW.a > 0"},
			want: atlashclrender.Diagnostic{
				Severity: atlashclrender.SeverityWarning, Path: `triggers["t"]["trg"]`,
				Message: "trigger WHEN condition cannot be represented in HCL schema output",
			},
		},
		{
			name:    "a transition table",
			trigger: schemamodel.Trigger{Event: "UPDATE", ForEach: "STATEMENT", NewTable: "n"},
			want: atlashclrender.Diagnostic{
				Severity: atlashclrender.SeverityWarning, Path: `triggers["t"]["trg"]`,
				Message: "trigger transition tables cannot be represented in HCL schema output",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			trigger := test.trigger
			trigger.Name, trigger.Table, trigger.Timing, trigger.Body = "trg", "t", "AFTER", "BEGIN RETURN NULL; END;"
			db := &schemamodel.Database{
				Tables:   []schemamodel.Table{{Name: "t", StructName: "T"}},
				Triggers: []schemamodel.Trigger{trigger},
			}

			result, err := atlashclrender.Render(db)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{test.want})
			c.Assert(string(result.Data), qt.Not(qt.Contains), `trigger "trg"`)
		})
	}
}
