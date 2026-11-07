package postgres_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
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

// TestPlanner_DropsAPostgresColumnWithCascade pins that a column removal on
// PostgreSQL carries CASCADE, which drops the row-level security policies that
// name the column.
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

// TestPlanner_DropsPostgresTablesAndViewsWithCascade pins that a table and a
// view removal on PostgreSQL carry CASCADE, and that the table's index goes
// first.
func TestPlanner_DropsPostgresTablesAndViewsWithCascade(t *testing.T) {
	c := qt.New(t)

	statements := planRemovalStatements(c, platform.Postgres, capability.Postgres16(), removedTableWithIndexAndView())

	c.Assert(statements, qt.DeepEquals, []string{
		`DROP INDEX IF EXISTS "events_kind_idx";`,
		`DROP VIEW IF EXISTS "recent_events" CASCADE;`,
		`DROP TABLE IF EXISTS "events" CASCADE;`,
	})
}
