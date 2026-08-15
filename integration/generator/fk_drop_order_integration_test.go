//go:build integration

package generator_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
)

func TestFKDropOrder_DownRoundTrip_Integration(t *testing.T) {
	cases := []struct {
		dialect string
		envKey  string
	}{
		{"postgres", "POSTGRES_URL"},
		{"mysql", "MYSQL_URL"},
		{"mariadb", "MARIADB_URL"},
	}

	for _, tc := range cases {
		t.Run(tc.dialect, func(t *testing.T) {
			c := qt.New(t)
			conn := requireGeneratorDatabaseConnection(t, tc.envKey)

			dialect := conn.Info().Dialect
			dropFKOrderTables(conn, dialect)
			t.Cleanup(func() { dropFKOrderTables(conn, dialect) })

			target := fkOrderSchema()
			goschema.Finalize(target)
			upSQL, downSQL := generateLiveMigrationSQL(c, conn, target)
			execScript(c, conn, upSQL, "UP")

			t.Logf("[%s] generated DOWN:\n%s", dialect, downSQL)
			execScript(c, conn, downSQL, "DOWN")

			dbAfterDown, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			c.Assert(hasFKOrderTables(dbAfterDown), qt.IsFalse)
		})
	}
}

func TestFKDropOrder_MutualCycleDownRoundTrip_Integration(t *testing.T) {
	cases := []struct {
		dialect string
		envKey  string
	}{
		{"postgres", "POSTGRES_URL"},
		{"mysql", "MYSQL_URL"},
		{"mariadb", "MARIADB_URL"},
	}

	for _, tc := range cases {
		t.Run(tc.dialect, func(t *testing.T) {
			c := qt.New(t)
			conn := requireGeneratorDatabaseConnection(t, tc.envKey)

			dialect := conn.Info().Dialect
			dropMutualFKCycleTables(conn, dialect)
			t.Cleanup(func() { dropMutualFKCycleTables(conn, dialect) })

			target := mutualFKCycleSchema()
			goschema.Finalize(target)
			upSQL, downSQL := generateLiveMigrationSQL(c, conn, target)
			execScript(c, conn, upSQL, "UP")

			t.Logf("[%s] generated mutual-cycle DOWN:\n%s", dialect, downSQL)
			execScript(c, conn, downSQL, "DOWN")

			dbAfterDown, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			c.Assert(hasMutualFKCycleTables(dbAfterDown), qt.IsFalse)
		})
	}
}

func dropFKOrderTables(conn *dbschema.DatabaseConnection, dialect string) {
	for _, tableName := range []string{
		"ptah_fk_order_tasks",
		"ptah_fk_order_memberships",
		"ptah_fk_order_projects",
		"ptah_fk_order_accounts",
	} {
		_, _ = conn.Exec(dropTableSQL(dialect, tableName))
	}
}

func dropMutualFKCycleTables(conn *dbschema.DatabaseConnection, dialect string) {
	if dialect == "mysql" || dialect == "mariadb" {
		_, _ = conn.Exec("SET FOREIGN_KEY_CHECKS=0")
		defer func() { _, _ = conn.Exec("SET FOREIGN_KEY_CHECKS=1") }()
	}
	for _, tableName := range []string{"left_nodes", "right_nodes"} {
		_, _ = conn.Exec(dropTableSQL(dialect, tableName))
	}
}

func hasFKOrderTables(schema *dbschematypes.DBSchema) bool {
	for _, table := range schema.Tables {
		if strings.HasPrefix(table.Name, "ptah_fk_order_") {
			return true
		}
	}
	return false
}

func hasMutualFKCycleTables(schema *dbschematypes.DBSchema) bool {
	for _, table := range schema.Tables {
		if table.Name == "left_nodes" || table.Name == "right_nodes" {
			return true
		}
	}
	return false
}
