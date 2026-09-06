package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer/internal/dialects/postgres"
)

// TestVisitCreateHypertable_WritesTheCallTheExtensionPublishes pins the shape
// of the statement, which is a function call rather than DDL because
// TimescaleDB has no CREATE HYPERTABLE grammar.
//
// Every argument below is measured on TimescaleDB 2.29.2 / PostgreSQL 17:
//
//	create_hypertable('conditions', by_range('time'))                  -> (1,t)
//	the same call again                                                -> ERROR: already a hypertable
//	… if_not_exists => TRUE                                            -> (1,f), NOTICE
//	by_range('time', INTERVAL '1 day')                                 -> the catalog reports `1 day`
//	… create_default_indexes => FALSE                                  -> no index is created
func TestVisitCreateHypertable_WritesTheCallTheExtensionPublishes(t *testing.T) {
	tests := []struct {
		name string
		node *ast.CreateHypertableNode
		want string
	}{
		{
			name: "the default interval",
			node: ast.NewCreateHypertable("public.readings", "time"),
			want: "SELECT create_hypertable('public.readings', by_range('time'), " +
				"create_default_indexes => FALSE);",
		},
		{
			name: "a declared interval",
			node: ast.NewCreateHypertable("readings", "time").SetChunkInterval("1 day"),
			want: "SELECT create_hypertable('readings', by_range('time', INTERVAL '1 day'), " +
				"create_default_indexes => FALSE);",
		},
		{
			name: "guarded",
			node: ast.NewCreateHypertable("readings", "time").SetIfNotExists(true),
			want: "SELECT create_hypertable('readings', by_range('time'), if_not_exists => TRUE, " +
				"create_default_indexes => FALSE);",
		},
		{
			// An identifier with a quote in it is a string literal here, not an
			// identifier position: the extension takes a regclass and a text
			// argument, so escaping is the literal's rule.
			name: "a name carrying a quote",
			node: ast.NewCreateHypertable("odd'name", "ts"),
			want: "SELECT create_hypertable('odd''name', by_range('ts'), " +
				"create_default_indexes => FALSE);",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			renderer := postgres.NewWithCapabilities(capability.Postgres17().With(capability.Hypertables, true), platform.Postgres)

			out, err := renderer.Render(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, test.want)
		})
	}
}

// TestVisitCreateHypertable_SkipsWhereTheExtensionIsAbsent is the other half of
// the capability, and the reason the key exists at all.
//
// A PostgreSQL target without TimescaleDB has no such function, and a plan that
// called it would fail at apply time on
// `function create_hypertable(unknown, unknown) does not exist` instead of
// saying so in the plan an operator reviews.
func TestVisitCreateHypertable_SkipsWhereTheExtensionIsAbsent(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.NewWithCapabilities(capability.Postgres17(), platform.Postgres)

	out, err := renderer.Render(ast.NewCreateHypertable("readings", "time"))

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "hypertable readings is not supported by this target; skipped.")
	c.Assert(out, qt.Not(qt.Contains), "create_hypertable")
}
