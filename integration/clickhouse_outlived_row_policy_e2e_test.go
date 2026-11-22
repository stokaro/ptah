//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// ClickHouse binds a row policy to a table name, not to a table: it keeps the
// policy when the table is dropped, and applies it at once to the next table
// created under the name. These tests leave such a policy behind, apply a
// declaration with `ptah schema apply`, and read back the policies the server
// holds and the rows the connection's account sees, since a plan that reads
// correctly can still leave a filter in force or be refused by the server.
// Each runs on the current server and on 24.10.

// outlivedTable is the column list of every table below.
const outlivedTable = "    columns:\n      id: {type: UInt64, primary: true, not_null: true}\n"

// outlivedDeclaration declares the table t and the row policies in policies,
// a row_policies block or nothing.
func outlivedDeclaration(policies string) string {
	return "tables:\n  t:\n" + outlivedTable + policies
}

// outlivedPolicy is a row_policies block declaring the permissive policy name
// on t with the filter using, for every account.
func outlivedPolicy(name, using string) string {
	return "row_policies:\n  " + name + ":\n    table: t\n    using: " + using + "\n    to: ALL\n"
}

// outlivedDatabase is a scratch database on the server serverURL names, and a
// connection to it. The row policies in cleanup, written `name ON table`, are
// dropped when the test ends, because a policy outlives its database too.
func outlivedDatabase(c *qt.C, serverURL string, cleanup ...string) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	url := createdDevDialectDatabase(c, serverURL, "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	conn := connectDevDialect(c, url)
	c.Cleanup(func() {
		for _, policy := range cleanup {
			_, err := conn.ExecContext(context.Background(), "DROP ROW POLICY IF EXISTS "+policy)
			c.Check(err, qt.IsNil)
		}
	})
	return url, conn
}

// outlivedScratch is a scratch database on the server serverURL names that
// holds no table t, and the policy leak, created on t with the clauses orphan
// and kept when t was dropped.
func outlivedScratch(c *qt.C, serverURL, orphan string) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	url, conn := outlivedDatabase(c, serverURL, "leak ON t", "tenant ON t")
	rebuildExec(c, conn,
		"CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id",
		"CREATE ROW POLICY leak ON t "+orphan,
		"DROP TABLE t SYNC",
	)
	return url, conn
}

// outlivedPolicies reads every row policy the server holds in the
// connection's database as `table name composition filter`, with the
// parentheses left out of the filter: 26.10 stores `(id > 100)` where 24.10
// stores `id > 100`.
func outlivedPolicies(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(),
		"SELECT concat(table, ' ', short_name, ' ', if(is_restrictive, 'restrictive', 'permissive'), ' ', "+
			"replaceRegexpAll(ifNull(select_filter, ''), '[()]', '')) "+
			"FROM system.row_policies WHERE database = currentDatabase() ORDER BY table, short_name")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var policies []string
	for rows.Next() {
		var policy string
		c.Assert(rows.Scan(&policy), qt.IsNil)
		policies = append(policies, policy)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return policies
}

// outlivedVisibleRows inserts the ids 1, 2 and 200 into t and counts the rows
// the connection's account sees, which every policy on t filters.
func outlivedVisibleRows(c *qt.C, conn *dbschema.DatabaseConnection) uint64 {
	c.Helper()
	rebuildExec(c, conn, "INSERT INTO t VALUES (1), (2), (200)")
	var visible uint64
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT count() FROM t").Scan(&visible), qt.IsNil)
	return visible
}

// TestClickHouseOutlivedRowPolicy_HappyPath leaves the policy leak on the name
// t and applies a declaration that creates t. One apply converges: leak is
// dropped when the declaration leaves it out, so neither a permissive policy
// that shows every row nor a restrictive one that hides some stays in force,
// and it is altered to the declaration when the declaration states a policy
// of its name, which a CREATE ROW POLICY would fail on with code 493. Only the
// declared policies then filter the rows.
func TestClickHouseOutlivedRowPolicy_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		orphan       string
		policies     string
		wantPolicies []string
		wantVisible  uint64
	}{
		{
			name:         "a permissive policy the declaration leaves out",
			orphan:       "USING 1 TO ALL",
			policies:     outlivedPolicy("tenant", "id > 100"),
			wantPolicies: []string{"t tenant permissive id > 100"},
			wantVisible:  1,
		},
		{
			name:         "a restrictive policy the declaration leaves out",
			orphan:       "AS RESTRICTIVE USING id < 50 TO ALL",
			policies:     outlivedPolicy("tenant", "id > 0"),
			wantPolicies: []string{"t tenant permissive id > 0"},
			wantVisible:  3,
		},
		{
			name:        "a declaration of the table alone",
			orphan:      "USING id < 50 TO ALL",
			wantVisible: 3,
		},
		{
			name:         "a declared policy of the same name",
			orphan:       "USING 1 TO ALL",
			policies:     outlivedPolicy("leak", "id > 100"),
			wantPolicies: []string{"t leak permissive id > 100"},
			wantVisible:  1,
		},
	}
	for _, server := range clickHouseServers(qt.New(t)) {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				url, conn := outlivedScratch(c, server.url, test.orphan)
				declaration := outlivedDeclaration(test.policies)

				applied := rebuildApply(c, url, declaration)
				again := rebuildApply(c, url, declaration)

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(outlivedPolicies(c, conn), qt.DeepEquals, test.wantPolicies)
				c.Assert(outlivedVisibleRows(c, conn), qt.Equals, test.wantVisible)
				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

// TestClickHouseOutlivedRowPolicy_KeepsPoliciesOnRelationsItDoesNotModel
// applies a declaration to a database whose policies are on a view, on a
// Distributed table, relations the read does not describe as tables, and on
// every table of the database. The apply succeeds, reports each policy as one
// the comparison could not decide, plans nothing for it, and the server still
// holds all three.
func TestClickHouseOutlivedRowPolicy_KeepsPoliciesOnRelationsItDoesNotModel(t *testing.T) {
	for _, server := range clickHouseServers(qt.New(t)) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := outlivedDatabase(c, server.url, "on_view ON v", "on_dist ON dist")
			keepsPoliciesOnUnmodeledRelations(c, url, conn)
		})
	}
}

// keepsPoliciesOnUnmodeledRelations is
// [TestClickHouseOutlivedRowPolicy_KeepsPoliciesOnRelationsItDoesNotModel] on
// the scratch database url names.
func keepsPoliciesOnUnmodeledRelations(c *qt.C, url string, conn *dbschema.DatabaseConnection) {
	c.Helper()
	database := conn.Info().Schema
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP ROW POLICY IF EXISTS on_all ON "+database+".*")
		c.Check(err, qt.IsNil)
	})
	rebuildExec(c, conn,
		"CREATE TABLE base (id UInt64) ENGINE = MergeTree ORDER BY id",
		"CREATE VIEW v AS SELECT id FROM base",
		"CREATE TABLE dist (id UInt64) ENGINE = Distributed('default', '"+database+"', 'base')",
		"CREATE ROW POLICY on_view ON v USING id > 1 TO ALL",
		"CREATE ROW POLICY on_dist ON dist USING id > 1 TO ALL",
		"CREATE ROW POLICY on_all ON "+database+".* USING id > 0 TO ALL",
	)
	declaration := "tables:\n  base:\n" + outlivedTable + "views:\n  v:\n    body: SELECT id FROM base\n"

	applied := rebuildApply(c, url, declaration)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Contains, "could not decide them: ")
	c.Assert(applied.Stderr, qt.Contains, `dist.on_dist (the policy is on "dist" (engine Distributed), which the read does not describe as a table; the plan keeps the policy)`)
	c.Assert(applied.Stderr, qt.Contains, `v.on_view (the policy is on "v" (engine View), which the read does not describe as a table; the plan keeps the policy)`)
	c.Assert(applied.Stderr, qt.Contains, "*.on_all (the policy is written ON "+database+".*, for every table of the database, which Ptah does not model; "+
		"the plan keeps the policy)")
	c.Assert(applied.Stdout, qt.Not(qt.Contains), "ROW POLICY")
	c.Assert(outlivedPolicies(c, conn), qt.DeepEquals, []string{" on_all permissive id > 0", "dist on_dist permissive id > 1", "v on_view permissive id > 1"})
}

// outlivedExportScratch is a scratch database on the server serverURL names
// that holds the table keep, the policy leak ClickHouse kept on the name t
// when t was dropped, and the policy on_all written ON the database.
func outlivedExportScratch(c *qt.C, serverURL string) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	url, conn := outlivedScratch(c, serverURL, "USING 1 TO ALL")
	database := conn.Info().Schema
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP ROW POLICY IF EXISTS on_all ON "+database+".*")
		c.Check(err, qt.IsNil)
	})
	rebuildExec(c, conn,
		"CREATE TABLE keep (id UInt64) ENGINE = MergeTree ORDER BY id",
		"CREATE ROW POLICY on_all ON "+database+".* USING id > 0 TO ALL",
	)
	return url, conn
}

// outlivedRun runs ptah with args in dir.
func outlivedRun(c *qt.C, dir string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, args...)
}

// TestClickHouseOutlivedRowPolicy_ExportsLeaveOutWhatNoSourceDeclares writes
// a database holding a policy whose table is gone, and one written ON the
// database, with every verb that writes a schema from a database. Each
// succeeds and writes the table. The policy whose table is gone is named on
// standard error with the statement that drops it; the database-wide one is
// named where the format allows, in a comment of the document or, for Go
// annotations, on standard error.
func TestClickHouseOutlivedRowPolicy_ExportsLeaveOutWhatNoSourceDeclares(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		output     string
		wantWide   string
		wantOutput string
	}{
		{name: "schema inspect as SQL", args: []string{"schema", "inspect", "--format", "sql"},
			wantWide: "ptah.run/clickhouse/row-policy *.on_all was not described", wantOutput: "CREATE TABLE keep ("},
		{name: "schema inspect as HCL", args: []string{"schema", "inspect", "--format", "hcl"},
			wantWide: "// ptah.run/clickhouse/row-policy *.on_all was not described", wantOutput: `table "keep" {`},
		{name: "db read", args: []string{"db", "read"},
			wantWide: "ptah.run/clickhouse/row-policy *.on_all was not described", wantOutput: "CREATE TABLE keep ("},
		{name: "introspect", args: []string{"introspect", "--out", "models"}, output: "models/keep.go",
			wantWide: `warning: ClickHouse row policy "on_all" on every table of the database is left out`, wantOutput: "type Keep struct"},
	}
	for _, server := range clickHouseServers(qt.New(t)) {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				url, _ := outlivedExportScratch(c, server.url)
				dir := c.TempDir()

				written := outlivedRun(c, dir, append(test.args, "--db-url", url)...)

				c.Assert(written.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", written.Stdout, written.Stderr))
				c.Assert(written.Stderr, qt.Contains, `warning: ClickHouse row policy "leak" on table "t" is left out: the database holds it for a table `+
					"it does not hold, and applies it to the next table created under that name; no schema can declare it, so applying a schema "+
					"written from this read drops it with "+
					"DROP ROW POLICY `leak` ON `t`;\n")
				document := written.Stdout + outlivedFile(c, dir, test.output)
				c.Assert(written.Stdout+written.Stderr, qt.Contains, test.wantWide)
				c.Assert(document, qt.Contains, test.wantOutput)
				c.Assert(document, qt.Not(qt.Contains), "leak")
			})
		}
	}
}

// outlivedFile reads the file name in dir, or nothing when name is empty.
func outlivedFile(c *qt.C, dir, name string) string {
	c.Helper()
	if name == "" {
		return ""
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	c.Assert(err, qt.IsNil)
	return string(data)
}

// TestClickHouseOutlivedRowPolicy_AppliedExportDropsThePolicy applies the SQL
// a database was written to back to it. The policy whose table is gone is
// dropped, since the source describes every row policy; the one written ON
// the database is kept, and the next apply plans nothing.
func TestClickHouseOutlivedRowPolicy_AppliedExportDropsThePolicy(t *testing.T) {
	for _, server := range clickHouseServers(qt.New(t)) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := outlivedExportScratch(c, server.url)
			dir := c.TempDir()
			written := outlivedRun(c, dir, "schema", "inspect", "--format", "sql", "--db-url", url)
			c.Assert(written.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", written.Stderr))
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(written.Stdout), 0o600), qt.IsNil)

			applied := outlivedRun(c, dir, "schema", "apply", "--db-url", url, "--schema-file", "schema.sql", "--auto-approve")
			again := outlivedRun(c, dir, "schema", "apply", "--db-url", url, "--schema-file", "schema.sql", "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "DROP ROW POLICY `leak` ON `t`;")
			c.Assert(outlivedPolicies(c, conn), qt.DeepEquals, []string{" on_all permissive id > 0"})
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Not(qt.Contains), "ROW POLICY")
		})
	}
}

// TestClickHouseOutlivedRowPolicy_ViewDropTakesItsPolicy applies a
// declaration that leaves out a view holding a row policy. The plan drops the
// policy with the view, ahead of it, so the server keeps no policy for the
// next relation created under the name, and the next apply plans nothing.
func TestClickHouseOutlivedRowPolicy_ViewDropTakesItsPolicy(t *testing.T) {
	for _, server := range clickHouseServers(qt.New(t)) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			url, conn := outlivedDatabase(c, server.url, "on_view ON v")
			rebuildExec(c, conn,
				"CREATE TABLE base (id UInt64) ENGINE = MergeTree ORDER BY id",
				"CREATE VIEW v AS SELECT id FROM base",
				"CREATE ROW POLICY on_view ON v USING id > 1 TO ALL",
			)
			declaration := "tables:\n  base:\n" + outlivedTable

			applied := rebuildApply(c, url, declaration)
			again := rebuildApply(c, url, declaration)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "DROP ROW POLICY `on_view` ON `v`;\nDROP VIEW ")
			c.Assert(applied.Stderr, qt.Not(qt.Contains), "on_view")
			c.Assert(outlivedPolicies(c, conn), qt.HasLen, 0)
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// TestClickHouseOutlivedRowPolicy_DatabaseAsEitherSide compares a database
// holding a policy whose table is gone, and one written ON the database, with
// an empty one, each way round. As the desired side, the database stands for
// a schema written from it: both policies are left out, each named, and the
// plan creates neither. As the current side, the read keeps both: the plan
// drops the policy whose table is gone and keeps the database-wide one.
func TestClickHouseOutlivedRowPolicy_DatabaseAsEitherSide(t *testing.T) {
	for _, server := range clickHouseServers(qt.New(t)) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			orphaned, _ := outlivedExportScratch(c, server.url)
			empty, _ := outlivedDatabase(c, server.url)
			dir := c.TempDir()

			toOrphaned := outlivedRun(c, dir, "schema", "diff", "--from", empty, "--to", orphaned)
			fromOrphaned := outlivedRun(c, dir, "schema", "diff", "--from", orphaned, "--to", empty)

			c.Assert(toOrphaned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", toOrphaned.Stdout, toOrphaned.Stderr))
			c.Assert(toOrphaned.Stderr, qt.Contains, `warning: ClickHouse row policy "leak" on table "t" is left out:`)
			c.Assert(toOrphaned.Stdout, qt.Contains, "CREATE TABLE keep (")
			c.Assert(toOrphaned.Stderr, qt.Contains, `warning: ClickHouse row policy "on_all" on every table of the database is left out:`)
			c.Assert(toOrphaned.Stdout, qt.Not(qt.Contains), "ROW POLICY")
			c.Assert(fromOrphaned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", fromOrphaned.Stdout, fromOrphaned.Stderr))
			c.Assert(fromOrphaned.Stdout, qt.Contains, "DROP ROW POLICY `leak` ON `t`;")
			c.Assert(fromOrphaned.Stdout, qt.Not(qt.Contains), "on_all")
		})
	}
}
