package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
)

// matviewSplitRow is one target and what the shared materialized-view handler
// makes of a materialized view there.
type matviewSplitRow struct {
	name       string
	dialect    string
	caps       capability.Capabilities
	wantCreate string
	wantDrop   string
}

// TestMaterializedViewCapabilitySplit_HappyPath pins that one handler answers
// two ways.
//
// One postgres.Renderer serves PostgreSQL, CockroachDB and YugabyteDB, and a
// single VisitCreateMaterializedView body serves all three. It reads the
// target's capability set, so the same code writes DDL where the set holds
// [capability.MaterializedViews] and a skipped comment where it does not. A
// completeness table that records one verdict per handler records the wrong
// answer for one of these cells, whichever verdict it picks; only the set of
// targets states what the handler does.
//
// DROP MATERIALIZED VIEW is the second signal: a change that lost the
// capability read would flip both columns of the row without the key, and a
// change that hard-coded one target's answer would flip one column of every
// row.
func TestMaterializedViewCapabilitySplit_HappyPath(t *testing.T) {
	tests := []matviewSplitRow{
		{
			name:       "postgres",
			dialect:    platform.Postgres,
			caps:       capability.ForDialect(platform.Postgres),
			wantCreate: "CREATE MATERIALIZED VIEW \"mv1\" AS\nSELECT 1\n;\n",
			wantDrop:   "DROP MATERIALIZED VIEW \"mv1\";\n",
		},
		{
			name:       "cockroachdb",
			dialect:    platform.CockroachDB,
			caps:       capability.ForDialect(platform.CockroachDB),
			wantCreate: "CREATE MATERIALIZED VIEW \"mv1\" AS\nSELECT 1\n;\n",
			wantDrop:   "DROP MATERIALIZED VIEW \"mv1\";\n",
		},
		{
			name:       "yugabytedb",
			dialect:    platform.YugabyteDB,
			caps:       capability.ForDialect(platform.YugabyteDB),
			wantCreate: "CREATE MATERIALIZED VIEW \"mv1\" AS\nSELECT 1\n;\n",
			wantDrop:   "DROP MATERIALIZED VIEW \"mv1\";\n",
		},
		{
			name:    "a target without materialized views",
			dialect: platform.Postgres,
			caps: capability.Postgres17().With(capability.MaterializedViews, false).
				With(capability.MaterializedViewComments, false).With(capability.MaterializedViewOptions, false),
			wantCreate: "-- POSTGRES: materialized view mv1 is not supported by this target; skipped.\n",
			wantDrop:   "-- POSTGRES: materialized view mv1 is not supported by this target; skipped.\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			created, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, &ast.CreateMaterializedViewNode{
				Name: "mv1",
				Body: "SELECT 1",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(created, qt.Equals, test.wantCreate)

			dropped, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, ast.NewDropMaterializedView("mv1"))
			c.Assert(err, qt.IsNil)
			c.Assert(dropped, qt.Equals, test.wantDrop)
		})
	}
}
