package schemaprecondition_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/clickhouse"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/internal/planner/dialects/sqlite"
	"ptah.run/migration/schemadiff/difftypes"
)

// Every planner other than YDB's refuses a diff that creates a node, rather
// than planning nothing for it.
func TestPlanners_RefuseCoordinationNodes(t *testing.T) {
	diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbcoordination.Ref("", "locks"), Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}}}}
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
			c.Assert(err.Error(), qt.Contains, string(ydbcoordination.Kind))
		})
	}
}
