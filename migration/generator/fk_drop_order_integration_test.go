//go:build integration

package generator

import (
	"context"
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
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
			url := os.Getenv(tc.envKey)
			if url == "" {
				t.Skipf("skipping %s: %s not set", tc.dialect, tc.envKey)
			}

			c := qt.New(t)
			ctx := context.Background()
			conn, err := dbschema.ConnectToDatabase(ctx, url)
			if err != nil {
				t.Skipf("skipping %s: cannot connect: %v", tc.dialect, err)
			}
			t.Cleanup(func() { _ = conn.Close() })

			dialect := conn.Info().Dialect
			dropFKOrderTables(conn, dialect)
			t.Cleanup(func() { dropFKOrderTables(conn, dialect) })

			target := fkOrderSchema()
			goschema.Finalize(target)
			empty := &dbschematypes.DBSchema{}
			upDiff := schemadiff.CompareWithDialect(target, empty, dialect)
			c.Assert(upDiff.HasChanges(), qt.IsTrue)

			upSQL, err := generateUpMigrationSQL(upDiff, target, dialect)
			c.Assert(err, qt.IsNil)
			execScript(c, conn, upSQL, "UP")

			downSQL, err := generateDownMigrationSQL(upDiff, target, empty, dialect)
			c.Assert(err, qt.IsNil)
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
			url := os.Getenv(tc.envKey)
			if url == "" {
				t.Skipf("skipping %s: %s not set", tc.dialect, tc.envKey)
			}

			c := qt.New(t)
			ctx := context.Background()
			conn, err := dbschema.ConnectToDatabase(ctx, url)
			if err != nil {
				t.Skipf("skipping %s: cannot connect: %v", tc.dialect, err)
			}
			t.Cleanup(func() { _ = conn.Close() })

			dialect := conn.Info().Dialect
			dropMutualFKCycleTables(conn, dialect)
			t.Cleanup(func() { dropMutualFKCycleTables(conn, dialect) })

			target := mutualFKCycleSchema()
			goschema.Finalize(target)
			empty := &dbschematypes.DBSchema{}
			upDiff := schemadiff.CompareWithDialect(target, empty, dialect)
			c.Assert(upDiff.HasChanges(), qt.IsTrue)

			upSQL, err := generateUpMigrationSQL(upDiff, target, dialect)
			c.Assert(err, qt.IsNil)
			execScript(c, conn, upSQL, "UP")

			downSQL, err := generateDownMigrationSQL(upDiff, target, empty, dialect)
			c.Assert(err, qt.IsNil)
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
