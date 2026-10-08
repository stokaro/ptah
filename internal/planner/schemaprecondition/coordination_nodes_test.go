package schemaprecondition_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/clickhouse"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/internal/planner/dialects/sqlite"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestRefuseCoordinationNodes_FailurePath refuses a coordination node change
// for a planner of a dialect that has none, by the capability key, naming the
// node: the comparison records a declared node on any target, and planning
// nothing would report the database synced.
func TestRefuseCoordinationNodes_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{name: "a creation",
			diff:    &difftypes.SchemaDiff{CoordinationNodesAdded: []schemamodel.CoordinationNode{{Schema: "app", Name: "locks"}}},
			wantErr: `the diff creates coordination node app.locks, which requires target capability coordination_nodes, unavailable on this postgres target: a coordination node is a YDB object`},
		{name: "a change",
			diff:    &difftypes.SchemaDiff{CoordinationNodesModified: []difftypes.CoordinationNodeChange{{Name: "locks"}}},
			wantErr: `the diff changes coordination node locks, which requires target capability coordination_nodes, .*`},
		{name: "a drop",
			diff:    &difftypes.SchemaDiff{CoordinationNodesRemoved: []schemamodel.CoordinationNode{{Name: "locks"}}},
			wantErr: `the diff drops coordination node locks, which requires target capability coordination_nodes, .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := schemaprecondition.RefuseCoordinationNodes(platform.Postgres, test.diff)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestRefuseCoordinationNodes_HappyPath passes a diff that changes no node,
// and no diff at all.
func TestRefuseCoordinationNodes_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(schemaprecondition.RefuseCoordinationNodes(platform.Postgres, nil), qt.IsNil)
	c.Assert(schemaprecondition.RefuseCoordinationNodes(platform.Postgres, &difftypes.SchemaDiff{TablesRemoved: []string{"t"}}),
		qt.IsNil)
}

// Every planner other than YDB's refuses a diff that creates a node, rather
// than planning nothing for it.
func TestPlanners_RefuseCoordinationNodes(t *testing.T) {
	diff := &difftypes.SchemaDiff{CoordinationNodesAdded: []schemamodel.CoordinationNode{{Name: "locks"}}}
	tests := []struct {
		name string
		plan func(*difftypes.SchemaDiff) error
	}{
		{name: "postgres", plan: func(d *difftypes.SchemaDiff) error {
			_, err := postgres.New().GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				d,
			)
			return err
		}},
		{name: "mysql", plan: func(d *difftypes.SchemaDiff) error {
			_, err := mysql.New().GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				d,
			)
			return err
		}},
		{name: "sqlite", plan: func(d *difftypes.SchemaDiff) error {
			_, err := sqlite.New().GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				d,
			)
			return err
		}},
		{name: "clickhouse", plan: func(d *difftypes.SchemaDiff) error {
			_, err := clickhouse.New().GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				d,
			)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := test.plan(diff)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `the diff creates coordination node locks, which requires target capability coordination_nodes, .*`)
		})
	}
}
