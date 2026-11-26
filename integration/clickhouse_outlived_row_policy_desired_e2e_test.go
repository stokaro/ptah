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
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// A database read is also the desired side of a comparison: a database named
// by --to, a migration directory replayed on the dev database, and the
// target's state written onto the dev database before a rehearsal. A source
// cannot declare a row policy whose table is gone, or one written ON the
// database, so each of these leaves both out and names them on standard
// error. These tests drive each path on the current server, and on 24.10
// where a replay can run there.

// outlivedRunWith runs ptah with args in dir, with extra added to an
// environment that holds no other PTAH_ variable.
func outlivedRunWith(c *qt.C, dir string, extra []string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), extra...)}, args...)
}

// outlivedMigrations writes files to a migration directory, hashes it in the
// Atlas format, and returns its file URL.
func outlivedMigrations(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return "file://" + filepath.ToSlash(dir)
}

// outlivedDropOnAll drops the policy on_all written ON the connection's
// database when the test ends, since ClickHouse keeps it after the database
// is dropped.
func outlivedDropOnAll(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	database := conn.Info().Schema
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP ROW POLICY IF EXISTS on_all ON "+database+".*")
		c.Check(err, qt.IsNil)
	})
}

// outlivedColumns reads the columns of the table keep in the connection's
// database, in their order.
func outlivedColumns(c *qt.C, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	var columns string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT arrayStringConcat(groupArray(name), ',') FROM (SELECT name FROM system.columns "+
			"WHERE database = currentDatabase() AND table = 'keep' ORDER BY position)").Scan(&columns), qt.IsNil)
	return columns
}

// TestClickHouseOutlivedRowPolicy_SelfDiffDropsThePolicy compares a database
// holding a policy whose table is gone with itself. The plan drops the
// policy: as the desired side the database stands for a schema written from
// it, and no schema can declare the policy. This is deliberate, and it is why
// a drift check between two environments that both hold such a policy
// reports its drop. The policy written ON the database is kept, and both are
// named.
func TestClickHouseOutlivedRowPolicy_SelfDiffDropsThePolicy(t *testing.T) {
	for _, server := range clickHouseServers(qt.New(t)) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			orphaned, _ := outlivedExportScratch(c, server.url)

			got := outlivedRun(c, c.TempDir(), "schema", "diff", "--from", orphaned, "--to", orphaned)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Equals, "DROP ROW POLICY `leak` ON `t`;\n")
			c.Assert(got.Stderr, qt.Contains, `warning: ClickHouse row policy "leak" on table "t" is left out:`)
			c.Assert(got.Stderr, qt.Contains, `warning: ClickHouse row policy "on_all" on every table of the database is left out:`)
		})
	}
}

// TestClickHouseOutlivedRowPolicy_DriftReportsTheDrop checks a database
// holding a policy whose table is gone for drift against a schema written from
// it. The written schema cannot declare the policy, so the check reports its
// drop as a destructive change and fails.
func TestClickHouseOutlivedRowPolicy_DriftReportsTheDrop(t *testing.T) {
	for _, server := range clickHouseServers(qt.New(t)) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			orphaned, _ := outlivedExportScratch(c, server.url)
			dir := c.TempDir()
			written := outlivedRun(c, dir, "schema", "inspect", "--format", "sql", "--db-url", orphaned)
			c.Assert(written.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", written.Stderr))
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(written.Stdout), 0o600), qt.IsNil)

			got := outlivedRun(c, dir, "schema", "drift", "--schema-file", "schema.sql", "--db-url", orphaned)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, "- feature_changes:ptah.run/clickhouse/row-policy-change: 1 (destructive)")
		})
	}
}

// TestClickHouseOutlivedRowPolicy_ReplayedDirectoryLeavesThePoliciesOut
// replays a migration directory that leaves a policy behind on a dropped
// table and writes one ON the database. The replay writes row policies, which
// outlive the dev database, so it runs only with PTAH_TRUST_REPLAY_SQL=1. The
// directory's read leaves both out and names them, the plan drops the policy
// the target keeps on its own dropped table, and nothing is planned for the
// database-wide one. It runs on the current server alone: the dev database's
// cleanup proves what it can see with CHECK GRANT, which 24.10 lacks, so a
// replay there is refused before it starts.
func TestClickHouseOutlivedRowPolicy_ReplayedDirectoryLeavesThePoliciesOut(t *testing.T) {
	c := qt.New(t)
	server := dbtarget.URL(c, dbtarget.ClickHouse)
	target, _ := outlivedScratch(c, server, "USING 1 TO ALL")
	dev, devConn := outlivedDatabase(c, server, "leak ON t")
	outlivedDropOnAll(c, devConn)
	migrations := outlivedMigrations(c, map[string]string{
		"20260101000001_t.sql":    "CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id;\nCREATE ROW POLICY leak ON t USING 1 TO ALL;\n",
		"20260101000002_drop.sql": "DROP TABLE t;\n",
		"20260101000003_keep.sql": "CREATE TABLE keep (id UInt64) ENGINE = MergeTree ORDER BY id;\nCREATE ROW POLICY on_all ON * USING id > 0 TO ALL;\n",
	})

	got := outlivedRunWith(c, c.TempDir(), []string{"PTAH_TRUST_REPLAY_SQL=1"},
		"schema", "diff", "--from", target, "--to", migrations, "--dev-url", dev)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, `warning: ClickHouse row policy "leak" on table "t" is left out:`)
	c.Assert(got.Stderr, qt.Contains, `warning: ClickHouse row policy "on_all" on every table of the database is left out:`)
	c.Assert(got.Stdout, qt.Contains, "DROP ROW POLICY `leak` ON `t`;")
	c.Assert(got.Stdout, qt.Contains, "CREATE TABLE `keep` (")
	c.Assert(got.Stdout, qt.Not(qt.Contains), "on_all")
}

// TestClickHouseOutlivedRowPolicy_DevLeftoversStayOutOfThePlan replays a
// directory on a dev database that already holds a policy whose table is gone
// and one written ON the database. ClickHouse keeps both through the reset,
// so the replayed read holds them. It leaves both out and names them, and the
// plan writes no row policy statement for the target. Like the replay above,
// it runs on the current server alone.
func TestClickHouseOutlivedRowPolicy_DevLeftoversStayOutOfThePlan(t *testing.T) {
	c := qt.New(t)
	server := dbtarget.URL(c, dbtarget.ClickHouse)
	target, _ := outlivedDatabase(c, server)
	dev, devConn := outlivedScratch(c, server, "USING 1 TO ALL")
	outlivedDropOnAll(c, devConn)
	rebuildExec(c, devConn, "CREATE ROW POLICY on_all ON "+devConn.Info().Schema+".* USING id > 0 TO ALL")
	migrations := outlivedMigrations(c, map[string]string{
		"20260101000001_keep.sql": "CREATE TABLE keep (id UInt64) ENGINE = MergeTree ORDER BY id;\n",
	})

	got := outlivedRun(c, c.TempDir(), "schema", "diff", "--from", target, "--to", migrations, "--dev-url", dev)

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, `warning: ClickHouse row policy "leak" on table "t" is left out:`)
	c.Assert(got.Stderr, qt.Contains, `warning: ClickHouse row policy "on_all" on every table of the database is left out:`)
	c.Assert(got.Stdout, qt.Contains, "CREATE TABLE `keep` (")
	c.Assert(got.Stdout, qt.Not(qt.Contains), "ROW POLICY")
	c.Assert(outlivedPolicies(c, devConn), qt.DeepEquals, []string{" on_all permissive id > 0", "t leak permissive 1"})
}

// TestClickHouseOutlivedRowPolicy_EmptySelectionPlansNothingOutsideIt
// compares a database holding a policy whose table is gone, and one written
// ON the database, with one holding the table other, under --include other.
// The selection matches nothing on the first database, whichever side it is,
// and the plan touches the selected table alone.
func TestClickHouseOutlivedRowPolicy_EmptySelectionPlansNothingOutsideIt(t *testing.T) {
	tests := []struct {
		name     string
		from, to int
		want     string
	}{
		{name: "as the current side", from: 0, to: 1, want: "CREATE TABLE `other` ("},
		{name: "as the desired side", from: 1, to: 0, want: "DROP TABLE `other`;"},
	}
	for _, server := range clickHouseServers(qt.New(t)) {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				orphaned, _ := outlivedExportScratch(c, server.url)
				other, otherConn := outlivedDatabase(c, server.url)
				rebuildExec(c, otherConn, "CREATE TABLE other (id UInt64) ENGINE = MergeTree ORDER BY id")
				urls := [2]string{orphaned, other}

				got := outlivedRun(c, c.TempDir(), "schema", "diff", "--from", urls[test.from], "--to", urls[test.to], "--include", "other")

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stdout, qt.Contains, test.want)
				c.Assert(got.Stdout, qt.Not(qt.Contains), "ROW POLICY")
				c.Assert(got.Stdout, qt.Not(qt.Contains), "keep")
			})
		}
	}
}

// outlivedKeepWithNote declares the table keep with the column note added.
const outlivedKeepWithNote = "tables:\n  keep:\n" + outlivedTable + "      note: {type: String, not_null: true}\n"

// TestClickHouseOutlivedRowPolicy_RehearsalRefusesTheDrop applies a
// declaration with --dev-url to a database holding a policy whose table is
// gone. The plan drops the policy, and the rehearsal cannot run the drop: the
// dev database would have to hold the policy, and a row policy outlives the
// dev database, so the guard refuses the statement before any runs. With
// PTAH_TRUST_REPLAY_SQL=1 the guard lets it run and the dev database refuses
// it, since no source can declare the policy for the baseline to write there.
// Either way the apply stops before the target, which keeps its policies and
// its columns, and the dev database is left empty.
func TestClickHouseOutlivedRowPolicy_RehearsalRefusesTheDrop(t *testing.T) {
	tests := []struct {
		name    string
		env     []string
		wantErr string
	}{
		{name: "untrusted", wantErr: "error: statement 1 cannot be rehearsed on the dev database: clickhouse migration replay rejects DROP ROW " +
			"because its effects cannot be confined to the disposable database realm"},
		{name: "trusted", env: []string{"PTAH_TRUST_REPLAY_SQL=1"}, wantErr: "error: dev database simulation failed during plan: "},
	}
	for _, server := range clickHouseServers(qt.New(t)) {
		for _, test := range tests {
			t.Run(server.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target, targetConn := outlivedExportScratch(c, server.url)
				dev, devConn := outlivedDatabase(c, server.url, "leak ON t")
				dir := c.TempDir()
				c.Assert(os.WriteFile(filepath.Join(dir, "schema.yaml"), []byte(outlivedKeepWithNote), 0o600), qt.IsNil)

				got := outlivedRunWith(c, dir, test.env,
					"schema", "apply", "--db-url", target, "--schema-file", "schema.yaml", "--dev-url", dev, "--auto-approve")

				c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stdout, qt.Contains, "DROP ROW POLICY `leak` ON `t`;")
				c.Assert(got.Stderr, qt.Contains, test.wantErr)
				c.Assert(outlivedPolicies(c, targetConn), qt.DeepEquals, []string{" on_all permissive id > 0", "t leak permissive 1"})
				c.Assert(outlivedColumns(c, targetConn), qt.Equals, "id")
				c.Assert(rebuildTables(c, devConn), qt.HasLen, 0)
				c.Assert(outlivedPolicies(c, devConn), qt.HasLen, 0)
			})
		}
	}
}

// TestClickHouseOutlivedRowPolicy_RehearsalLeavesADatabaseWidePolicyOut
// applies a declaration with --dev-url to a database holding a policy written
// ON the database. The baseline written onto the dev database leaves the
// policy out, as a source cannot declare it, so the rehearsal runs and the
// apply adds the column. The target keeps the policy, the dev database holds
// none, and the next apply plans nothing and counts the policy it could not
// compare.
func TestClickHouseOutlivedRowPolicy_RehearsalLeavesADatabaseWidePolicyOut(t *testing.T) {
	for _, server := range clickHouseServers(qt.New(t)) {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			target, targetConn := outlivedDatabase(c, server.url)
			outlivedDropOnAll(c, targetConn)
			rebuildExec(c, targetConn,
				"CREATE TABLE keep (id UInt64) ENGINE = MergeTree ORDER BY id",
				"CREATE ROW POLICY on_all ON "+targetConn.Info().Schema+".* USING id > 0 TO ALL",
			)
			dev, devConn := outlivedDatabase(c, server.url)
			outlivedDropOnAll(c, devConn)

			applied := rebuildApply(c, target, outlivedKeepWithNote, "--dev-url", dev)
			again := rebuildApply(c, target, outlivedKeepWithNote, "--dev-url", dev)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Not(qt.Contains), "ROW POLICY")
			c.Assert(outlivedColumns(c, targetConn), qt.Equals, "id,note")
			c.Assert(outlivedPolicies(c, targetConn), qt.DeepEquals, []string{" on_all permissive id > 0"})
			c.Assert(outlivedPolicies(c, devConn), qt.HasLen, 0)
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Equals, "No changes planned, but 1 comparison limit remains unresolved.\n")
		})
	}
}
