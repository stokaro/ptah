//go:build integration

package integration_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// ClickHouse keeps one namespace for tables, views and materialized views,
// and refuses to create a relation while another holds its name: "Table d.v
// already exists" (code 57). These tests replace each kind with another under
// the name v through `ptah schema apply`, so the server judges the order the
// plan drops and creates in, and read back the engine the server reports for
// v. Each runs on the current server and on 24.10.

// handoverColumns is the column list of every table below.
const handoverColumns = "    columns:\n      id: {type: UInt64, primary: true, not_null: true}\n"

// handoverBase declares the table the views read.
const handoverBase = "tables:\n  base:\n" + handoverColumns

// handoverDatabase is a scratch database on the server serverURL names that
// holds the table base and the relation current, and a connection to it.
func handoverDatabase(c *qt.C, serverURL, current string) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	url := createdDevDialectDatabase(c, serverURL, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	conn := connectDevDialect(c, url)
	rebuildExec(c, conn, "CREATE TABLE base (id UInt64) ENGINE = MergeTree ORDER BY id", current)
	return url, conn
}

// handoverEngine reads the engine the server reports for the relation v in
// the connection's database, or an empty string when it holds none.
func handoverEngine(c *qt.C, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	var engine string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT ifNull(any(engine), '') FROM system.tables WHERE database = currentDatabase() AND name = 'v'").Scan(&engine), qt.IsNil)
	return engine
}

// TestClickHouseRelationHandover_HappyPath creates v as one kind and applies a
// declaration of v as another. One apply converges: the plan drops the old
// relation before it creates the new one, and the next apply plans nothing.
func TestClickHouseRelationHandover_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		current     string
		declaration string
		wantEngine  string
	}{
		{
			name:        "a view to a table",
			current:     "CREATE VIEW v AS SELECT id FROM base",
			declaration: handoverBase + "  v:\n" + handoverColumns,
			wantEngine:  "MergeTree",
		},
		{
			name:        "a materialized view to a table",
			current:     "CREATE MATERIALIZED VIEW v ENGINE = MergeTree ORDER BY tuple() AS SELECT id FROM base",
			declaration: handoverBase + "  v:\n" + handoverColumns,
			wantEngine:  "MergeTree",
		},
		{
			name:        "a table to a view",
			current:     "CREATE TABLE v (id UInt64) ENGINE = MergeTree ORDER BY id",
			declaration: handoverBase + "views:\n  v:\n    body: SELECT id FROM base\n",
			wantEngine:  "View",
		},
		{
			name:        "a table to a materialized view",
			current:     "CREATE TABLE v (id UInt64) ENGINE = MergeTree ORDER BY id",
			declaration: handoverBase + "matviews:\n  v:\n    body: SELECT id FROM base\n",
			wantEngine:  "MaterializedView",
		},
	}
	for _, server := range clickHouseServers(qt.New(t)) {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				url, conn := handoverDatabase(c, server.url, test.current)

				applied := rebuildApply(c, url, test.declaration)
				again := rebuildApply(c, url, test.declaration)

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(handoverEngine(c, conn), qt.Equals, test.wantEngine)
				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

// handoverTenant declares the table v with the policy tenant, which shows the
// ids above 100 to every account.
const handoverTenant = handoverBase + "  v:\n" + handoverColumns +
	"row_policies:\n  tenant:\n    table: v\n    using: id > 100\n    to: ALL\n"

// handoverVisibleRows inserts the ids 1, 2 and 200 into v and counts the rows
// the connection's account sees, which every policy on v filters.
func handoverVisibleRows(c *qt.C, conn *dbschema.DatabaseConnection) uint64 {
	c.Helper()
	rebuildExec(c, conn, "INSERT INTO v VALUES (1), (2), (200)")
	var visible uint64
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count() FROM v").Scan(&visible), qt.IsNil)
	return visible
}

// TestClickHouseRelationHandover_DropsTheOldRelationsRowPolicies gives the
// name of a view or materialized view holding the policy leak, which shows
// every row, to a table declared with the policy tenant. ClickHouse keeps a
// policy bound to the name after DROP VIEW and applies it to the table at
// once, so the plan drops leak ahead of the view. Read back after one apply,
// tenant alone is in force and the account sees 1 of the 3 rows. The next
// apply plans nothing; a leak left on the table would make it plan the drop.
func TestClickHouseRelationHandover_DropsTheOldRelationsRowPolicies(t *testing.T) {
	tests := []struct {
		name    string
		current string
	}{
		{name: "a view", current: "CREATE VIEW v AS SELECT id FROM base"},
		{name: "a materialized view", current: "CREATE MATERIALIZED VIEW v ENGINE = MergeTree ORDER BY tuple() AS SELECT id FROM base"},
	}
	for _, server := range clickHouseServers(qt.New(t)) {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				url, conn := handoverDatabase(c, server.url, test.current)
				dropPoliciesAtCleanup(c, conn, "leak ON v", "tenant ON v")
				rebuildExec(c, conn, "CREATE ROW POLICY leak ON v USING 1 TO ALL")

				applied := rebuildApply(c, url, handoverTenant)

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(handoverEngine(c, conn), qt.Equals, "MergeTree")
				c.Assert(handoverVisibleRows(c, conn), qt.Equals, uint64(1))
				c.Assert(outlivedPolicies(c, conn), qt.DeepEquals, []string{"v tenant permissive id > 100"})
				c.Assert(applied.Stdout, qt.Contains, "DROP ROW POLICY `leak` ON `v`;\nDROP VIEW ")
				again := rebuildApply(c, url, handoverTenant)
				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

// dropPoliciesAtCleanup drops the row policies in policies, written `name ON
// table`, when the test ends, because ClickHouse keeps a policy after its
// database is dropped.
func dropPoliciesAtCleanup(c *qt.C, conn *dbschema.DatabaseConnection, policies ...string) {
	c.Helper()
	c.Cleanup(func() {
		for _, policy := range policies {
			_, err := conn.ExecContext(context.Background(), "DROP ROW POLICY IF EXISTS "+policy)
			c.Check(err, qt.IsNil)
		}
	})
}
