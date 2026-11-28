//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// Removing a view also asks the generator to restore it in the down file.
// Storage that the read cannot describe must not become default storage in
// that file, even when the forward operation is an intentional removal.
func TestClickHouseRemovedUnknownViewStorageRefusesMigrationFilesE2E(t *testing.T) {
	for _, view := range []string{"keyed", "routed"} {
		t.Run(view, func(t *testing.T) {
			c := qt.New(t)
			url, _ := clickHouseViewStorageFixture(c)
			conn := connectDevDialect(c, url)
			rebuildExec(c, conn, "INSERT INTO src VALUES (1, 17)")
			stored := materializedStorageStatement(c, conn, view)
			schema := inspectedWithoutMaterializedView(c, url, view)
			migrations := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)

			generated := clirun.Run(c, clirun.Ptah, clirun.Options{}, "migrations", "generate",
				"--db-url", url, "--schema-file", schema, "--migrations-dir", migrations, "--name", "remove_view",
			)

			c.Assert(generated.ExitCode, qt.Not(qt.Equals), 0,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr),
			)
			c.Assert(generated.Stderr, qt.Contains, "cannot plan the rollback of removing materialized view")
			c.Assert(generated.Stderr, qt.Contains, "ptah.run/clickhouse/view-storage")
			files, err := filepath.Glob(filepath.Join(migrations, "*.sql"))
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 0)
			c.Assert(materializedStorageStatement(c, conn, view), qt.Equals, stored)
			c.Assert(materializedStorageRows(c, conn, view), qt.Equals, uint64(17))
			rebuildExec(c, conn, "INSERT INTO src VALUES (2, 23)")
			c.Assert(materializedStorageRows(c, conn, view), qt.Equals, uint64(40))
		})
	}
}

// A view whose storage the model carries can still be removed and recreated.
// Unknown storage on retained siblings does not prevent that migration.
func TestClickHouseRemovedKnownViewStorageRunsBothDirectionsE2E(t *testing.T) {
	c := qt.New(t)
	url, _ := clickHouseViewStorageFixture(c)
	conn := connectDevDialect(c, url)
	rebuildExec(c, conn, "INSERT INTO src VALUES (1, 17)")
	keyed := materializedStorageStatement(c, conn, "keyed")
	routed := materializedStorageStatement(c, conn, "routed")
	schema := inspectedWithoutMaterializedView(c, url, "plain")
	migrations := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)

	generated := clirun.Run(c, clirun.Ptah, clirun.Options{}, "migrations", "generate",
		"--db-url", url, "--schema-file", schema, "--migrations-dir", migrations, "--name", "remove_plain_view",
	)

	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	downs, err := filepath.Glob(filepath.Join(migrations, "*.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(downs, qt.HasLen, 1)
	down, err := os.ReadFile(downs[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Contains, "CREATE MATERIALIZED VIEW `plain` ENGINE = MergeTree ORDER BY tuple() AS")
	runPtahNative(c, "migrations", "up", "--db-url", url, "--migrations-dir", migrations, "--allow-destructive")
	var remaining uint64
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'plain'").Scan(&remaining), qt.IsNil)
	c.Assert(remaining, qt.Equals, uint64(0))
	runPtahNative(c, "migrations", "down", "--db-url", url, "--migrations-dir", migrations, "--target", "0", "--confirm")

	c.Assert(materializedStorageStatement(c, conn, "plain"), qt.Contains, "ENGINE = MergeTree ORDER BY tuple()")
	c.Assert(materializedStorageStatement(c, conn, "keyed"), qt.Equals, keyed)
	c.Assert(materializedStorageStatement(c, conn, "routed"), qt.Equals, routed)
	c.Assert(materializedStorageRows(c, conn, "plain"), qt.Equals, uint64(0))
	rebuildExec(c, conn, "INSERT INTO src VALUES (2, 23)")
	c.Assert(materializedStorageRows(c, conn, "plain"), qt.Equals, uint64(23))
	c.Assert(materializedStorageRows(c, conn, "keyed"), qt.Equals, uint64(40))
	c.Assert(materializedStorageRows(c, conn, "routed"), qt.Equals, uint64(40))
}

// The SQL schema reader applies DROP VIEW to the description, leaving the
// source table and the other materialized views unchanged.
func inspectedWithoutMaterializedView(c *qt.C, url, view string) string {
	c.Helper()
	inspected := clirun.Run(c, clirun.Ptah, clirun.Options{}, "schema", "inspect", "--db-url", url, "--format", "sql")
	c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspected.Stderr))
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(inspected.Stdout+"\nDROP VIEW `"+view+"`;\n"), 0o600), qt.IsNil)
	return schema
}

func materializedStorageStatement(c *qt.C, conn *dbschema.DatabaseConnection, view string) string {
	c.Helper()
	var statement string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = ?", view,
	).Scan(&statement), qt.IsNil)
	return statement
}

func materializedStorageRows(c *qt.C, conn *dbschema.DatabaseConnection, view string) uint64 {
	c.Helper()
	var sum uint64
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT sum(c) FROM `"+view+"`").Scan(&sum), qt.IsNil)
	return sum
}
