package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
	pgplanner "ptah.run/internal/planner/dialects/postgres"
)

// appSession is a session whose search path is app alone, as
// `search_path=app` on the URL gives.
var appSession = identifier.Semantics{DefaultSchema: "app", SearchPath: []string{"app"}}

// TestViewLikesForCreateIn_ReadsTheSessionDefaultSchema pins the order a
// session's default schema decides. Each body names zbase as the session puts
// it, and the reader is declared first, so only the edge reorders them.
// Ordered by the dialect's default schema, public, `app.zbase` reads no zbase
// declared without a schema, the reader is created first, and the server
// answers `relation "app.zbase" does not exist`.
func TestViewLikesForCreateIn_ReadsTheSessionDefaultSchema(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		zbase     string
		semantics identifier.Semantics
	}{
		{name: "a reference in the default schema", body: "SELECT id FROM app.zbase", zbase: "zbase", semantics: appSession},
		{
			name: "a bare reference to a relation further along the path", body: "SELECT id FROM zbase", zbase: "public.zbase",
			semantics: identifier.Semantics{DefaultSchema: "app", SearchPath: []string{"app", "public"}},
		},
		{
			name: "a default schema with no path reported", body: "SELECT id FROM app.zbase", zbase: "zbase",
			semantics: identifier.Semantics{DefaultSchema: "app"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects := []deporder.ViewLike{
				{Name: "asummary", Body: test.body},
				{Name: "other.zbase", Body: "SELECT 2 AS id"},
				{Name: test.zbase, Body: "SELECT 1 AS id"},
			}

			ordered := deporder.ViewLikesForCreateIn(objects, platform.Postgres, test.semantics)

			c.Assert(viewNames(ordered), qt.DeepEquals, []string{"other.zbase", test.zbase, "asummary"})
		})
	}
}

// TestViewLikesForCreateIn_GuessesNoSchema is the control: a schema the
// session does not reach, or a session that reports nothing, adds no edge, so
// the declarations keep their order. The order never guesses; see
// [deporder.ViewLikesForCreateForDialect].
func TestViewLikesForCreateIn_GuessesNoSchema(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		semantics identifier.Semantics
	}{
		{name: "a reference into public, outside the session", body: "SELECT id FROM public.zbase", semantics: appSession},
		{name: "a reference into app with no session known", body: "SELECT id FROM app.zbase", semantics: identifier.Semantics{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects := []deporder.ViewLike{
				{Name: "asummary", Body: test.body},
				{Name: "zbase", Body: "SELECT 1 AS id"},
			}

			ordered := deporder.ViewLikesForCreateIn(objects, platform.Postgres, test.semantics)

			c.Assert(viewNames(ordered), qt.DeepEquals, []string{"asummary", "zbase"})
		})
	}
}

// TestViewLikesForDropIn_MirrorsTheCreationOrder pins the drop order under the
// same session: the reader of app.zbase is dropped before zbase, which a drop
// without CASCADE needs.
func TestViewLikesForDropIn_MirrorsTheCreationOrder(t *testing.T) {
	c := qt.New(t)
	objects := []deporder.ViewLike{
		{Name: "zbase", Body: "SELECT 1 AS id"},
		{Name: "asummary", Body: "SELECT id FROM app.zbase"},
	}

	ordered := deporder.ViewLikesForDropIn(objects, platform.Postgres, appSession)

	c.Assert(viewNames(ordered), qt.DeepEquals, []string{"asummary", "zbase"})
}

// TestPlaceRoutinesIn_ReadsTheSessionDefaultSchema pins routine placement
// under the same session: a function returning SETOF app.orders waits for an
// orders declared without a schema, and one returning SETOF public.orders does
// not.
func TestPlaceRoutinesIn_ReadsTheSessionDefaultSchema(t *testing.T) {
	tests := []struct {
		name    string
		returns string
		want    deporder.RoutinePlacement
	}{
		{name: "a result in the default schema", returns: "SETOF app.orders", want: deporder.RoutineWithRelations},
		{name: "a result outside the session", returns: "SETOF public.orders", want: deporder.RoutineBeforeTypes},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			routine := schemamodel.Function{Name: "recent", Language: "plpgsql", Returns: test.returns, Body: "BEGIN RETURN; END"}

			got := deporder.PlaceRoutinesIn([]schemamodel.Function{routine}, nil,
				deporder.RoutineCreation{Relations: []string{"orders"}}, platform.Postgres, pgplanner.ResolvesBodyAtCreate, appSession)

			c.Assert(got["recent"], qt.Equals, test.want)
		})
	}
}
