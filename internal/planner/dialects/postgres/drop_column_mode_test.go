package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// planColumnRemoval plans the removal of events.created_at on dialect and
// returns the ALTER TABLE statements it renders.
func planColumnRemoval(c *qt.C, dialect string, caps capability.Capabilities) []string {
	c.Helper()
	diff := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:      "events",
			ColumnsRemoved: difftypes.ColumnChanges{{Name: "created_at"}},
		}},
	}
	nodes, err := postgres.NewForDialect(dialect, caps).
		GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	return renderedStatements(c, nodes, caps, dialect)
}

// TestPlanner_DropsASpannerColumnWithoutCascade pins the drop mode of a
// column removal on Spanner.
//
// The planner wrote every removal as DROP COLUMN ... CASCADE, which Spanner's
// PostgreSQL interface refuses with "Only <RESTRICT> drop mode is supported in
// <ALTER> statement operations", so no plan that removed a column could be
// applied there (stokaro/ptah#4280).
func TestPlanner_DropsASpannerColumnWithoutCascade(t *testing.T) {
	c := qt.New(t)

	statements := planColumnRemoval(c, platform.Spanner, capability.SpannerPostgres())

	c.Assert(statements, qt.DeepEquals, []string{`ALTER TABLE "events" DROP COLUMN "created_at";`})
}

// TestPlanner_DropsAPostgresColumnWithCascade is the control: PostgreSQL keeps
// CASCADE, which drops the row-level security policies that name the column.
func TestPlanner_DropsAPostgresColumnWithCascade(t *testing.T) {
	c := qt.New(t)

	statements := planColumnRemoval(c, platform.Postgres, capability.Postgres16())

	c.Assert(statements, qt.DeepEquals, []string{`ALTER TABLE "events" DROP COLUMN "created_at" CASCADE;`})
}
