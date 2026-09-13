package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
)

// matviewSplitRow is one target and what the shared materialized-view handler
// makes of a materialized view there.
type matviewSplitRow struct {
	dialect    string
	wantCreate string
	wantDrop   string
}

// TestMaterializedViewCapabilitySplit_HappyPath pins that one handler answers
// two ways.
//
// One postgres.Renderer serves PostgreSQL, CockroachDB, YugabyteDB and Spanner,
// and a single VisitCreateMaterializedView body serves all four. It reads the
// target's capability set, so the same code writes DDL on three of them and a
// skipped comment on Spanner. A completeness table that records one verdict per
// handler records the wrong answer for this cell, whichever verdict it picks;
// only the set of targets states what the handler does.
//
// DROP MATERIALIZED VIEW is the second signal: a change that lost the
// capability read would flip both columns of the Spanner row, and a change that
// hard-coded one target's answer would flip one column of every row.
func TestMaterializedViewCapabilitySplit_HappyPath(t *testing.T) {
	tests := []matviewSplitRow{
		{
			dialect:    platform.Postgres,
			wantCreate: "CREATE MATERIALIZED VIEW \"mv1\" AS\nSELECT 1\n;\n",
			wantDrop:   "DROP MATERIALIZED VIEW \"mv1\";\n",
		},
		{
			dialect:    platform.CockroachDB,
			wantCreate: "CREATE MATERIALIZED VIEW \"mv1\" AS\nSELECT 1\n;\n",
			wantDrop:   "DROP MATERIALIZED VIEW \"mv1\";\n",
		},
		{
			dialect:    platform.YugabyteDB,
			wantCreate: "CREATE MATERIALIZED VIEW \"mv1\" AS\nSELECT 1\n;\n",
			wantDrop:   "DROP MATERIALIZED VIEW \"mv1\";\n",
		},
		{
			dialect:    platform.Spanner,
			wantCreate: "-- SPANNER: materialized view mv1 is not supported by this target; skipped.\n",
			wantDrop:   "-- SPANNER: materialized view mv1 is not supported by this target; skipped.\n",
		},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			created, err := renderer.RenderSQL(test.dialect, &ast.CreateMaterializedViewNode{
				Name: "mv1",
				Body: "SELECT 1",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(created, qt.Equals, test.wantCreate)

			dropped, err := renderer.RenderSQL(test.dialect, ast.NewDropMaterializedView("mv1"))
			c.Assert(err, qt.IsNil)
			c.Assert(dropped, qt.Equals, test.wantDrop)
		})
	}
}
