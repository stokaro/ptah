//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// These tests write a versioned migration that drops a relation holding a row
// policy, apply it with `ptah migrations up`, roll it back with `migrations
// down`, and read the database back. ClickHouse binds a policy to a name and
// keeps it after DROP TABLE and DROP VIEW, so the up migration drops the
// policies with the relation, and the down migration has to create them again:
// a view or table that comes back without its policy shows every row. Each
// runs on the current server and on 24.10.

// rollbackBase declares the table the views read and nothing else.
const rollbackBase = "tables:\n  base:\n" + handoverColumns

// rollbackDown rolls back every migration in migrations.
func rollbackDown(c *qt.C, url, migrations string) {
	c.Helper()
	runPtahNative(c, "migrations", "down", "--db-url", url, "--migrations-dir", migrations, "--target", "0", "--confirm")
}

// rollbackVisible counts the rows of v the connection's account sees, which
// every policy on v filters.
func rollbackVisible(c *qt.C, conn *dbschema.DatabaseConnection) uint64 {
	c.Helper()
	var visible uint64
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count() FROM v").Scan(&visible), qt.IsNil)
	return visible
}

// TestClickHouseRowPolicyRollback_RestoresAViewsPolicy creates the view v over
// the ids 1, 2 and 200 with the policy vf, which shows the ids above 100, and
// writes a migration that drops v: for a table the declaration creates under
// the name, and because the declaration leaves v out. After up and down, v is
// a view again, vf is on it, and the account sees 1 of the 3 rows.
func TestClickHouseRowPolicyRollback_RestoresAViewsPolicy(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
		wantAfterUp []string
	}{
		{name: "a view a table takes the name of", declaration: handoverTenant, wantAfterUp: []string{"v tenant permissive id > 100"}},
		{name: "a view the declaration leaves out", declaration: rollbackBase},
	}
	for _, server := range clickHouseServers(qt.New(t)) {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				url, conn := handoverDatabase(c, server.url, "CREATE VIEW v AS SELECT id FROM base")
				dropPoliciesAtCleanup(c, conn, "vf ON v", "tenant ON v")
				rebuildExec(c, conn, "INSERT INTO base VALUES (1), (2), (200)", "CREATE ROW POLICY vf ON v USING id > 100 TO ALL")
				dir := c.TempDir()

				down := rollbackMigrationsIn(c, dir, url, test.declaration)
				afterUp := outlivedPolicies(c, conn)
				rollbackDown(c, url, filepath.Join(dir, "migrations"))

				c.Assert(afterUp, qt.DeepEquals, test.wantAfterUp)
				c.Assert(handoverEngine(c, conn), qt.Equals, "View")
				c.Assert(outlivedPolicies(c, conn), qt.DeepEquals, []string{"v vf permissive id > 100"})
				c.Assert(rollbackVisible(c, conn), qt.Equals, uint64(1))
				// The read-back above shows the policy is back; this pins the
				// order that keeps a failed policy from leaving v unfiltered.
				c.Assert(down, qt.Matches, "(?s).*CREATE ROW POLICY `vf` ON `v` USING \\(id > 100\\) AS PERMISSIVE TO ALL;\nCREATE VIEW .*")
			})
		}
	}
}

// rollbackRole creates a role named for the connection's database and suffix,
// and drops it at cleanup.
func rollbackRole(c *qt.C, conn *dbschema.DatabaseConnection, suffix string) string {
	c.Helper()
	var database string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT currentDatabase()").Scan(&database), qt.IsNil)
	role := database + "_" + suffix
	rebuildExec(c, conn, "CREATE ROLE "+role)
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	return role
}

// rollbackRelations counts the relations named v in the connection's
// database, of any engine.
func rollbackRelations(c *qt.C, conn *dbschema.DatabaseConnection) uint64 {
	c.Helper()
	var relations uint64
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'v'").Scan(&relations), qt.IsNil)
	return relations
}

// TestClickHouseRowPolicyRollback_LeavesNoUnfilteredViewWhenAPolicyFails
// gives the view v the policy vf for two roles, writes a migration that drops
// v and applies it, and then drops one of the roles. The rollback creates vf
// again, which ClickHouse refuses for the missing role, and stops there. The
// down migration creates the policy ahead of the view, so it leaves no
// relation named v, and nothing shows the rows vf hid.
func TestClickHouseRowPolicyRollback_LeavesNoUnfilteredViewWhenAPolicyFails(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
	}{
		{name: "a view a table takes the name of", declaration: handoverTenant},
		{name: "a view the declaration leaves out", declaration: rollbackBase},
	}
	for _, server := range clickHouseServers(qt.New(t)) {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				url, conn := handoverDatabase(c, server.url, "CREATE VIEW v AS SELECT id FROM base")
				kept, gone := rollbackRole(c, conn, "kept"), rollbackRole(c, conn, "gone")
				dropPoliciesAtCleanup(c, conn, "vf ON v", "tenant ON v")
				rebuildExec(c, conn, "INSERT INTO base VALUES (1), (2), (200)", "CREATE ROW POLICY vf ON v USING id > 100 TO "+kept+", "+gone)
				dir := c.TempDir()
				rollbackMigrationsIn(c, dir, url, test.declaration)
				rebuildExec(c, conn, "DROP ROLE "+gone)

				got := runPtahIn(c, "migrations", "down", "--db-url", url, "--migrations-dir", filepath.Join(dir, "migrations"), "--target", "0", "--confirm")

				c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stderr, qt.Contains, gone)
				c.Assert(rollbackRelations(c, conn), qt.Equals, uint64(0))
				c.Assert(outlivedPolicies(c, conn), qt.IsNil)
			})
		}
	}
}

// TestClickHouseRowPolicyRollback_RestoresATablesPolicy creates the table v
// with the policy p, which shows the ids above 100, and writes a migration
// that drops v: because the declaration leaves it out, and for a view the
// declaration creates under the name. The rollback is written, and after up
// and down v is a table again with p on it, so of the ids 1, 2 and 200
// inserted after the rollback the account sees 1.
func TestClickHouseRowPolicyRollback_RestoresATablesPolicy(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
	}{
		{name: "a table the declaration leaves out", declaration: rollbackBase},
		{name: "a table a view takes the name of", declaration: handoverBase + "views:\n  v:\n    body: SELECT id FROM base\n"},
	}
	for _, server := range clickHouseServers(qt.New(t)) {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				url, conn := handoverDatabase(c, server.url, "CREATE TABLE v (id UInt64) ENGINE = MergeTree ORDER BY id")
				dropPoliciesAtCleanup(c, conn, "p ON v")
				rebuildExec(c, conn, "CREATE ROW POLICY p ON v USING id > 100 TO ALL")
				dir := c.TempDir()

				rollbackMigrationsIn(c, dir, url, test.declaration)
				afterUp := outlivedPolicies(c, conn)
				rollbackDown(c, url, filepath.Join(dir, "migrations"))

				c.Assert(afterUp, qt.IsNil)
				c.Assert(handoverEngine(c, conn), qt.Equals, "MergeTree")
				c.Assert(outlivedPolicies(c, conn), qt.DeepEquals, []string{"v p permissive id > 100"})
				c.Assert(handoverVisibleRows(c, conn), qt.Equals, uint64(1))
			})
		}
	}
}

// rollbackMigrationsIn writes the migration from the database url holds to
// declaration into dir and applies it, drops included. It returns the down
// file.
func rollbackMigrationsIn(c *qt.C, dir, url, declaration string) string {
	c.Helper()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
	schema := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(schema, []byte(declaration), 0o600), qt.IsNil)
	runPtahNative(c, "migrations", "generate", "--db-url", url, "--migrations-dir", migrations, "--schema-file", schema, "--name", "step")
	downs, err := filepath.Glob(filepath.Join(migrations, "*.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(downs, qt.HasLen, 1)
	down, err := os.ReadFile(downs[0])
	c.Assert(err, qt.IsNil)
	runPtahNative(c, "migrations", "up", "--db-url", url, "--migrations-dir", migrations, "--allow-destructive")
	return string(down)
}
