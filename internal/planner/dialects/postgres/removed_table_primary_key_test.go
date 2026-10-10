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

// cockroachRemovalSQL plans diff on CockroachDB and returns the rendered SQL.
func cockroachRemovalSQL(c *qt.C, diff *difftypes.SchemaDiff) string {
	c.Helper()
	planner := postgres.NewForDialect(platform.CockroachDB, capability.CockroachDB26())
	nodes, err := planner.GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	var sql strings.Builder
	for _, node := range nodes {
		rendered, err := builtin.RenderSQLWithCapabilities(platform.CockroachDB, capability.CockroachDB26(), node)
		c.Assert(err, qt.IsNil)
		sql.WriteString(rendered)
	}
	return sql.String()
}

// TestPlanner_DropsARemovedTableWithItsPrimaryKey pins that a table the schema
// no longer declares is dropped without a separate drop of its primary key.
//
// The comparator reports the key of a removed table as a removed constraint,
// and the plan dropped it before the table. CockroachDB refuses that statement
// -- a primary key may only be dropped beside the ADD of its replacement, and
// v26 tables are schema_locked -- so the apply failed and the table stayed
// (stokaro/ptah#4277).
func TestPlanner_DropsARemovedTableWithItsPrimaryKey(t *testing.T) {
	c := qt.New(t)

	sql := cockroachRemovalSQL(c, &difftypes.SchemaDiff{
		TablesRemoved: difftypes.TableRemovals{{Name: "sessions"}},
		ConstraintsRemoved: difftypes.ConstraintRemovals{
			{Name: "sessions_pkey", TableName: "sessions", Type: "PRIMARY KEY"},
		},
	})

	c.Assert(sql, qt.Not(qt.Contains), "DROP CONSTRAINT")
	c.Assert(sql, qt.Contains, `DROP TABLE IF EXISTS "sessions" CASCADE`)
}

// TestPlanner_DropsOtherConstraintsOfARemovedTable is the control: only the
// primary key is left to the DROP TABLE, and a kept table's removed key is
// still dropped.
func TestPlanner_DropsOtherConstraintsOfARemovedTable(t *testing.T) {
	c := qt.New(t)

	sql := cockroachRemovalSQL(c, &difftypes.SchemaDiff{
		TablesRemoved: difftypes.TableRemovals{{Name: "sessions"}},
		ConstraintsRemoved: difftypes.ConstraintRemovals{
			{Name: "sessions_pkey", TableName: "sessions", Type: "PRIMARY KEY"},
			{Name: "sessions_user_fk", TableName: "sessions", Type: "FOREIGN KEY"},
			{Name: "accounts_pkey", TableName: "accounts", Type: "PRIMARY KEY"},
		},
	})

	c.Assert(sql, qt.Not(qt.Contains), `"sessions_pkey"`)
	c.Assert(sql, qt.Contains, `DROP CONSTRAINT IF EXISTS "sessions_user_fk"`)
	c.Assert(sql, qt.Contains, `DROP CONSTRAINT IF EXISTS "accounts_pkey"`)
}
