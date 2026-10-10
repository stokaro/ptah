package postgres_test

import (
	"context"
	"strings"
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

// planRemovalStatements plans diff on dialect and returns every statement it
// renders, comments left out.
func planRemovalStatements(c *qt.C, dialect string, caps capability.Capabilities, diff *difftypes.SchemaDiff) []string {
	c.Helper()
	nodes, err := postgres.NewForDialect(dialect, caps).
		GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	var statements []string
	for _, node := range nodes {
		sql, err := builtin.RenderSQLWithCapabilities(dialect, caps, node)
		c.Assert(err, qt.IsNil)
		for line := range strings.SplitSeq(sql, "\n") {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "--") {
				statements = append(statements, line)
			}
		}
	}
	return statements
}

// removedTableWithIndexAndView removes a table, its index and a view reading it.
func removedTableWithIndexAndView() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesRemoved:  difftypes.TableRemovals{{Name: "events"}},
		IndexesRemoved: []difftypes.IndexRef{{Name: "events_kind_idx", TableName: "events"}},
		ViewsRemoved:   difftypes.ViewChanges{{Name: "recent_events", Body: "SELECT id FROM events"}},
	}
}

// TestPlanner_DropsSpannerTablesAndViewsWithoutCascade pins the drop mode of
// a table and a view removal on Spanner, and that the table's index goes
// first.
//
// The planner wrote DROP TABLE and DROP VIEW with CASCADE, which Spanner's
// PostgreSQL interface refuses with "Only <RESTRICT> behavior is supported by
// <DROP> statement", so no plan that removed a table or a view could be
// applied there. Without CASCADE, Spanner refuses to drop a table that still
// has an index, so the order is part of the fix (stokaro/ptah#4362).
func TestPlanner_DropsSpannerTablesAndViewsWithoutCascade(t *testing.T) {
	c := qt.New(t)

	statements := planRemovalStatements(c, platform.Spanner, capability.SpannerPostgres(), removedTableWithIndexAndView())

	c.Assert(statements, qt.DeepEquals, []string{
		`DROP INDEX IF EXISTS "events_kind_idx";`,
		`DROP VIEW IF EXISTS "recent_events";`,
		`DROP TABLE IF EXISTS "events";`,
	})
}

// TestPlanner_DropsPostgresTablesAndViewsWithCascade is the control:
// PostgreSQL keeps CASCADE on both.
func TestPlanner_DropsPostgresTablesAndViewsWithCascade(t *testing.T) {
	c := qt.New(t)

	statements := planRemovalStatements(c, platform.Postgres, capability.Postgres16(), removedTableWithIndexAndView())

	c.Assert(statements, qt.DeepEquals, []string{
		`DROP INDEX IF EXISTS "events_kind_idx";`,
		`DROP VIEW IF EXISTS "recent_events" CASCADE;`,
		`DROP TABLE IF EXISTS "events" CASCADE;`,
	})
}
