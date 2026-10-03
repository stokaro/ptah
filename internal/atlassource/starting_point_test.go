package atlassource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlassource"
)

// replayedOnStartingPoint is a migration directory replayed on a dev database
// a docker block provisioned: the starting point's auth.users, and the
// directory's public.todos and the trigger it put on auth.users.
func replayedOnStartingPoint() atlassource.State {
	return atlassource.State{
		DB: &catalog.Database{
			Schemas: []catalog.Schema{{Name: "auth"}, {Name: "public"}},
			Tables:  []catalog.Table{{Schema: "auth", Name: "users"}, {Schema: "public", Name: "todos"}},
			Triggers: []catalog.Trigger{
				{Schema: "auth", Table: "users", Name: "on_signup"},
			},
		},
		Schema: &schemamodel.Database{},
		EnvironmentState: &catalog.Database{
			Schemas: []catalog.Schema{{Name: "auth"}, {Name: "public"}},
			Tables:  []catalog.Table{{Schema: "auth", Name: "users"}},
		},
	}
}

// TestStateWithoutStartingPointLeavesWhatTheSourceBuilt pins the comparison
// side of a docker block's starting point for every verb that reads a source
// through a dev database (stokaro/ptah#4056). The starting point leaves both
// representations of the state; what the directory built stays, the trigger
// it put on a starting-point table included.
func TestStateWithoutStartingPointLeavesWhatTheSourceBuilt(t *testing.T) {
	c := qt.New(t)
	other := atlassource.State{DB: &catalog.Database{
		Tables: []catalog.Table{{Schema: "public", Name: "todos"}},
	}}

	got := replayedOnStartingPoint().WithoutStartingPoint(other, "postgres")

	c.Assert(got.DB.Tables, qt.DeepEquals, []catalog.Table{{Schema: "public", Name: "todos"}})
	c.Assert(got.DB.Triggers, qt.HasLen, 1)
	c.Assert(got.DB.Schemas, qt.HasLen, 0)
	c.Assert(got.Schema.Tables, qt.HasLen, 1)
	c.Assert(got.Schema.Tables[0].Name, qt.Equals, "todos")
}

// TestStateWithoutStartingPointKeepsWhatADocumentDeclares pins that a
// starting-point object the other side declares stays and is compared, when
// that side is a document with no catalog read of its own.
func TestStateWithoutStartingPointKeepsWhatADocumentDeclares(t *testing.T) {
	c := qt.New(t)
	other := atlassource.State{Schema: &schemamodel.Database{
		Tables: []schemamodel.Table{{Schema: "auth", Name: "users"}},
	}}

	got := replayedOnStartingPoint().WithoutStartingPoint(other, "postgres")

	c.Assert(got.DB.Tables, qt.DeepEquals, []catalog.Table{
		{Schema: "auth", Name: "users"}, {Schema: "public", Name: "todos"},
	})
}

// TestStateWithoutStartingPointOfAnotherSource pins that a state read from no
// docker block's dev database is compared as it was read.
func TestStateWithoutStartingPointOfAnotherSource(t *testing.T) {
	c := qt.New(t)
	state := replayedOnStartingPoint()
	state.EnvironmentState = nil

	got := state.WithoutStartingPoint(atlassource.State{}, "postgres")

	c.Assert(got.DB, qt.Equals, state.DB)
	c.Assert(got.Schema, qt.Equals, state.Schema)
}
