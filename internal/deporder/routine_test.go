package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
)

// createdOrdersAndMood is a plan that creates the table orders in schema app,
// the view recent_orders and the enum mood.
var createdOrdersAndMood = deporder.RoutineCreation{
	Types:     []string{"mood"},
	Relations: []string{"app.orders", "recent_orders"},
}

// TestPlaceRoutines_OneRoutine pins where one routine's own definition puts
// it. The rows vary what PostgreSQL resolves at creation: the signature always,
// a LANGUAGE sql body, and nothing else.
func TestPlaceRoutines_OneRoutine(t *testing.T) {
	tests := []struct {
		name    string
		routine schemamodel.Function
		dialect string
		want    deporder.RoutinePlacement
	}{
		{
			name:    "a sql body reading a created table waits for it",
			routine: schemamodel.Function{Name: "order_count", Language: "sql", Returns: "bigint", Body: "SELECT count(*) FROM app.orders"},
			dialect: platform.Postgres,
			want:    deporder.RoutineWithRelations,
		},
		{
			name:    "an unqualified reference names the qualified table",
			routine: schemamodel.Function{Name: "order_count", Language: "SQL", Returns: "bigint", Body: "SELECT count(*) FROM orders"},
			dialect: platform.Postgres,
			want:    deporder.RoutineWithRelations,
		},
		{
			name:    "a sql body reading a created view waits for it",
			routine: schemamodel.Function{Name: "recent_count", Language: "sql", Returns: "bigint", Body: "SELECT count(*) FROM recent_orders"},
			dialect: platform.Postgres,
			want:    deporder.RoutineWithRelations,
		},
		{
			name:    "a PL/pgSQL body is not read at creation",
			routine: schemamodel.Function{Name: "order_count", Language: "plpgsql", Returns: "bigint", Body: "BEGIN RETURN (SELECT count(*) FROM app.orders); END"},
			dialect: platform.Postgres,
			want:    deporder.RoutineBeforeTypes,
		},
		{
			name:    "a table row type in the signature waits for the table",
			routine: schemamodel.Function{Name: "big_orders", Language: "plpgsql", Returns: "SETOF app.orders", Body: "BEGIN RETURN; END"},
			dialect: platform.Postgres,
			want:    deporder.RoutineWithRelations,
		},
		{
			name:    "a created type in the signature waits for the type only",
			routine: schemamodel.Function{Name: "default_mood", Language: "plpgsql", Returns: "mood", Body: "BEGIN RETURN 'happy'; END"},
			dialect: platform.Postgres,
			want:    deporder.RoutineAfterTypes,
		},
		{
			name:    "a created type in a parameter waits for the type",
			routine: schemamodel.Function{Name: "is_happy", Language: "sql", Parameters: "m mood", Returns: "boolean", Body: "SELECT m = 'happy'"},
			dialect: platform.Postgres,
			want:    deporder.RoutineAfterTypes,
		},
		{
			name:    "a table the plan does not create names nothing",
			routine: schemamodel.Function{Name: "user_count", Language: "sql", Returns: "bigint", Body: "SELECT count(*) FROM users"},
			dialect: platform.Postgres,
			want:    deporder.RoutineBeforeTypes,
		},
		{
			name:    "a name inside a string literal names nothing",
			routine: schemamodel.Function{Name: "label", Language: "sql", Returns: "text", Body: "SELECT 'orders'::text"},
			dialect: platform.Postgres,
			want:    deporder.RoutineBeforeTypes,
		},
		{
			name:    "a name inside a comment names nothing",
			routine: schemamodel.Function{Name: "one", Language: "sql", Returns: "integer", Body: "SELECT 1 -- from orders"},
			dialect: platform.Postgres,
			want:    deporder.RoutineBeforeTypes,
		},
		{
			name:    "CockroachDB resolves a sql body too",
			routine: schemamodel.Function{Name: "order_count", Language: "sql", Returns: "bigint", Body: "SELECT count(*) FROM orders"},
			dialect: platform.CockroachDB,
			want:    deporder.RoutineWithRelations,
		},
		{
			name:    "MySQL does not resolve a body at creation",
			routine: schemamodel.Function{Name: "order_count", Language: "sql", Returns: "bigint", Body: "SELECT count(*) FROM orders"},
			dialect: platform.MySQL,
			want:    deporder.RoutineBeforeTypes,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := deporder.PlaceRoutines([]schemamodel.Function{test.routine}, nil, createdOrdersAndMood, test.dialect)

			c.Assert(got, qt.DeepEquals, map[string]deporder.RoutinePlacement{test.routine.Name: test.want})
		})
	}
}

// TestPlaceRoutines_ACallerFollowsItsCallee pins the propagation over the call
// graph: a LANGUAGE sql body calling a routine is resolved at creation like one
// reading a table, so the caller cannot be placed before its callee.
func TestPlaceRoutines_ACallerFollowsItsCallee(t *testing.T) {
	c := qt.New(t)
	routines := []schemamodel.Function{
		{Name: "top", Language: "sql", Returns: "bigint", Body: "SELECT middle()"},
		{Name: "middle", Language: "sql", Returns: "bigint", Body: "SELECT order_count()"},
		{Name: "order_count", Language: "sql", Returns: "bigint", Body: "SELECT count(*) FROM orders"},
		{Name: "unrelated", Language: "sql", Returns: "integer", Body: "SELECT 1"},
	}
	dependencies := map[string][]string{"top": {"middle"}, "middle": {"order_count"}}

	got := deporder.PlaceRoutines(routines, dependencies, createdOrdersAndMood, platform.Postgres)

	c.Assert(got, qt.DeepEquals, map[string]deporder.RoutinePlacement{
		"top":         deporder.RoutineWithRelations,
		"middle":      deporder.RoutineWithRelations,
		"order_count": deporder.RoutineWithRelations,
		"unrelated":   deporder.RoutineBeforeTypes,
	})
}

// TestPlaceRoutines_OverloadsShareTheLatestPlacement pins that the placement is
// by name: the statements of one name are emitted together, so one overload
// that has to wait makes the others wait with it.
func TestPlaceRoutines_OverloadsShareTheLatestPlacement(t *testing.T) {
	c := qt.New(t)
	routines := []schemamodel.Function{
		{Name: "total", Language: "sql", Parameters: "a integer", Returns: "integer", Body: "SELECT a"},
		{Name: "total", Language: "sql", Parameters: "a bigint", Returns: "bigint", Body: "SELECT sum(id) FROM orders"},
	}

	got := deporder.PlaceRoutines(routines, nil, createdOrdersAndMood, platform.Postgres)

	c.Assert(got, qt.DeepEquals, map[string]deporder.RoutinePlacement{"total": deporder.RoutineWithRelations})
}

// TestViewLikesForCreate_ARoutineSitsBetweenTheViewsItReadsAndCalls pins the
// ordering the relation-placed routines join: a view that calls a routine
// follows it, and a view the routine reads precedes it.
func TestViewLikesForCreate_ARoutineSitsBetweenTheViewsItReadsAndCalls(t *testing.T) {
	c := qt.New(t)
	routine := schemamodel.Function{Name: "big_count", Language: "sql", Returns: "bigint", Body: "SELECT count(*) FROM big"}
	objects := []deporder.ViewLike{
		{Name: "big_count", Body: deporder.RoutineOrderingBody(routine, platform.Postgres), Routine: true},
		{Name: "summary", Body: "SELECT big_count() AS n"},
		{Name: "big", Body: "SELECT * FROM orders WHERE total > 100"},
	}

	ordered := deporder.ViewLikesForCreateForDialect(objects, platform.Postgres)

	names := make([]string, 0, len(ordered))
	for _, object := range ordered {
		names = append(names, object.Name)
	}
	c.Assert(names, qt.DeepEquals, []string{"big", "big_count", "summary"})
}
