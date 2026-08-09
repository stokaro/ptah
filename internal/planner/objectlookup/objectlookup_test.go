package objectlookup_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/planner/objectlookup"
)

// TestView_HappyPath pins the two spellings a diff and a schema legitimately use
// for the same view. The down direction plans against a schema converted back
// from an introspected database, which qualifies every name it read; the diff
// records the name the Go schema spells, which is normally bare.
func TestView_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		views    []goschema.View
		lookup   string
		wantBody string
	}{
		{
			name:     "exact name",
			views:    []goschema.View{{Name: "active_users", Body: "exact"}},
			lookup:   "active_users",
			wantBody: "exact",
		},
		{
			name:     "diff spells it bare, schema qualifies it",
			views:    []goschema.View{{Name: "reporting.active_users", Body: "qualified"}},
			lookup:   "active_users",
			wantBody: "qualified",
		},
		{
			name:     "diff qualifies it, schema spells it bare",
			views:    []goschema.View{{Name: "active_users", Body: "bare"}},
			lookup:   "reporting.active_users",
			wantBody: "bare",
		},
		{
			name: "an exact match wins over an unqualified one",
			views: []goschema.View{
				{Name: "reporting.active_users", Body: "qualified"},
				{Name: "active_users", Body: "bare"},
			},
			lookup:   "active_users",
			wantBody: "bare",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got := objectlookup.View(test.views, test.lookup)
			c.Assert(got, qt.IsNotNil)
			c.Assert(got.Body, qt.Equals, test.wantBody)
		})
	}
}

// TestView_FailurePath pins what the resolver refuses to guess at. Two views of
// the same name in different schemas name no one object, and rendering a
// statement for either of them would be a coin toss on which one the migration
// destroys.
func TestView_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		views  []goschema.View
		lookup string
	}{
		{
			name:   "no candidate at all",
			views:  []goschema.View{{Name: "other_view"}},
			lookup: "active_users",
		},
		{
			name: "the same name in two schemas",
			views: []goschema.View{
				{Name: "reporting.active_users", Body: "reporting"},
				{Name: "archive.active_users", Body: "archive"},
			},
			lookup: "active_users",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(objectlookup.View(test.views, test.lookup), qt.IsNil)
		})
	}
}

func TestMaterializedView_HappyPath(t *testing.T) {
	c := qt.New(t)

	views := []goschema.MaterializedView{{Name: "reporting.user_stats", Body: "qualified"}}
	got := objectlookup.MaterializedView(views, "user_stats")
	c.Assert(got, qt.IsNotNil)
	c.Assert(got.Body, qt.Equals, "qualified")
}

func TestMaterializedView_FailurePath(t *testing.T) {
	c := qt.New(t)

	views := []goschema.MaterializedView{
		{Name: "reporting.user_stats"},
		{Name: "archive.user_stats"},
	}
	c.Assert(objectlookup.MaterializedView(views, "user_stats"), qt.IsNil)
}

// TestTrigger_HappyPath covers the half of a trigger's identity that carries a
// schema: the table it hangs on.
func TestTrigger_HappyPath(t *testing.T) {
	c := qt.New(t)

	triggers := []goschema.Trigger{{Name: "touch", Table: "reporting.users", Body: "qualified"}}
	got := objectlookup.Trigger(triggers, "users", "touch")
	c.Assert(got, qt.IsNotNil)
	c.Assert(got.Body, qt.Equals, "qualified")
}

func TestTrigger_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		triggers []goschema.Trigger
		table    string
		trigger  string
	}{
		{
			name:     "the trigger name still has to match",
			triggers: []goschema.Trigger{{Name: "touch", Table: "reporting.users"}},
			table:    "users",
			trigger:  "other",
		},
		{
			name: "the same trigger name on the same table in two schemas",
			triggers: []goschema.Trigger{
				{Name: "touch", Table: "reporting.users"},
				{Name: "touch", Table: "archive.users"},
			},
			table:   "users",
			trigger: "touch",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(objectlookup.Trigger(test.triggers, test.table, test.trigger), qt.IsNil)
		})
	}
}
