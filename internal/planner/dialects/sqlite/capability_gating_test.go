package sqlite_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	sqliteplanner "ptah.run/internal/planner/dialects/sqlite"
	"ptah.run/migration/schemadiff/difftypes"
)

// viewFixture916 and triggerFixture916 are the two diffs the table below plans.
// Each names exactly one object kind so a refusal can only come from that
// kind's own gate.
func viewFixture916() (*difftypes.SchemaDiff, *schemamodel.Database) {
	return &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "active_notes"}}},
		&schemamodel.Database{Views: []schemamodel.View{{
			Name: "active_notes",
			Body: "SELECT id FROM notes WHERE body IS NOT NULL",
		}}}
}

func triggerFixture916() (*difftypes.SchemaDiff, *schemamodel.Database) {
	trigger := schemamodel.Trigger{
		StructName: "Note",
		Name:       "touch",
		Table:      "notes",
		Timing:     "AFTER",
		Event:      "UPDATE",
		Body:       "SELECT 1",
	}
	// The declaration travels with the entry (stokaro/ptah#2315); the schema is
	// returned alongside because the caller hands it to the planner.
	return &difftypes.SchemaDiff{TriggersAdded: []difftypes.TriggerRef{{
			TriggerName: "touch", TableName: "notes", Desired: trigger,
		}}},
		&schemamodel.Database{Triggers: []schemamodel.Trigger{trigger}}
}

// TestSQLitePlanner_RefusesObjectKindsTheTargetDeclines pins both arms of the
// gate stokaro/ptah#916 item 3 added. The SQLite planner used to be registered
// with a factory that threw the options away, so a set declining views or
// triggers still got statements creating them; now the set it was built with
// decides, and the shipped preset -- which allows both -- plans exactly as
// before.
func TestSQLitePlanner_RefusesObjectKindsTheTargetDeclines(t *testing.T) {
	viewDiff, viewSchema := viewFixture916()
	triggerDiff, triggerSchema := triggerFixture916()

	tests := []struct {
		name      string
		caps      capability.Capabilities
		diff      *difftypes.SchemaDiff
		desired   *schemamodel.Database
		wantError string
	}{
		{
			name:    "the shipped preset plans a view",
			caps:    capability.SQLite3(),
			diff:    viewDiff,
			desired: viewSchema,
		},
		{
			name:    "the shipped preset plans a trigger",
			caps:    capability.SQLite3(),
			diff:    triggerDiff,
			desired: triggerSchema,
		},
		{
			name:      "a set declining views refuses one",
			caps:      capability.SQLite3().With(capability.Views, false),
			diff:      viewDiff,
			desired:   viewSchema,
			wantError: "views are not supported by the target capability set",
		},
		{
			name:      "a set declining triggers refuses one",
			caps:      capability.SQLite3().With(capability.Triggers, false),
			diff:      triggerDiff,
			desired:   triggerSchema,
			wantError: "triggers are not supported by the target capability set",
		},
		{
			// The non-interference control: declining views must not reach the
			// trigger gate, or one gate would answer for both and reverting
			// either would still look measured.
			name:    "a set declining views still plans a trigger",
			caps:    capability.SQLite3().With(capability.Views, false),
			diff:    triggerDiff,
			desired: triggerSchema,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			p := sqliteplanner.NewWithCapabilities(test.caps)

			nodes, err := p.GenerateMigrationAST(withDeclaredTable(test.diff, test.desired))

			c.Assert(errorText916(err), qt.Contains, test.wantError)
			// The arithmetic half of the assertion, which the empty wantError
			// rows need: qt.Contains with "" passes on any string, so without
			// this a row that expected a plan and got a refusal would still be
			// green.
			c.Assert(len(nodes) > 0, qt.Equals, test.wantError == "",
				qt.Commentf("err=%v nodes=%d", err, len(nodes)))
		})
	}
}

// errorText916 keeps the loop body branch-free: a nil error reads as the empty
// string, which is what the rows expecting a plan carry in wantError.
func errorText916(err error) string {
	texts := map[bool]func() string{
		true:  func() string { return "" },
		false: func() string { return fmt.Sprint(err) },
	}
	return texts[err == nil]()
}
