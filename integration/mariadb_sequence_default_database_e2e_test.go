//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// sequenceDefaultMigrations are, one version each, a sequence and a table
// whose key defaults to it, then the table's removal. MariaDB stores the
// default as nextval(`<database>`.`s`), with the database that ran the
// statement.
var sequenceDefaultMigrations = []map[string]string{
	{
		"0000000001_base.up.sql":   "CREATE SEQUENCE s;\nCREATE TABLE t2 (id int PRIMARY KEY DEFAULT NEXTVAL(s), n int);\n",
		"0000000001_base.down.sql": "DROP TABLE t2;\nDROP SEQUENCE s;\n",
	},
	{
		"0000000002_drop.up.sql":   "DROP TABLE t2;\n",
		"0000000002_drop.down.sql": "SELECT 1;\n",
	},
}

// writeSequenceDefaultMigrations writes the first versions of
// sequenceDefaultMigrations.
func writeSequenceDefaultMigrations(c *qt.C, versions int) string {
	c.Helper()
	dir := c.TempDir()
	for _, version := range sequenceDefaultMigrations[:versions] {
		for name, body := range version {
			c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
		}
	}
	return dir
}

// drawnKey inserts a row into t2 of database without its key and answers the
// key the default drew.
func (s mysqlScratch) drawnKey(c *qt.C, database string) int {
	c.Helper()
	conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, s.adminDSN, database))
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	_, err = conn.ExecContext(c.Context(), "INSERT INTO t2 (n) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var id int
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT MAX(id) FROM t2").Scan(&id), qt.IsNil)
	return id
}

// TestMigrationsDownPlanNamesNoShadowDatabaseInASequenceDefaultE2E rolls the
// target back to the version that holds t2, with a rollback derived on a
// shadow database of another server. The target's server holds a database of
// the shadow's name with a sequence s that starts at 1000. The rollback creates
// t2 drawing from the target's own s, so the first row's key is 1. Written
// with the shadow's name, the rollback creates t2 drawing from the namesake's
// sequence, and the key is 1000; without the namesake it fails with error
// 1146 (stokaro/ptah#6710).
func TestMigrationsDownPlanNamesNoShadowDatabaseInASequenceDefaultE2E(t *testing.T) {
	c := qt.New(t)
	scratch := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin)
	targetName, target := scratch.database(c, "seq_target")
	shadowName, shadow := newMySQLFamilyScratch(c, dbtarget.MariaDBDevServer).database(c, "seq_shadow")
	scratch.namesake(c, shadowName)
	_, err := scratch.admin.ExecContext(c.Context(), "CREATE SEQUENCE `"+shadowName+"`.s START WITH 1000")
	c.Assert(err, qt.IsNil)
	dir := writeSequenceDefaultMigrations(c, 2)
	options := clirun.Options{
		Dir: c.TempDir(), Timeout: 2 * time.Minute,
		Env: append(environmentWithoutPtahVariables(), "PTAH_TRUST_REPLAY_SQL=1"),
	}
	up := clirun.Run(c, clirun.Ptah, options, "migrations", "up", "--db-url", target, "--migrations-dir", dir,
		"--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))

	got := clirun.Run(c, clirun.Ptah, options, "migrations", "down", "--db-url", target, "--migrations-dir", dir,
		"--shadow-db", shadow, "--target", "1", "--plan", "--confirm")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "DEFAULT (nextval(`s`))")
	c.Assert(got.Stdout, qt.Not(qt.Contains), shadowName)
	c.Assert(scratch.drawnKey(c, targetName), qt.Equals, 1)
}

// TestMigrationsCheckpointNamesNoShadowDatabaseInASequenceDefaultE2E squashes
// the sequence and t2 into a checkpoint replayed on a shadow database. The
// checkpoint creates the sequence and the default that reads it without the
// shadow's name, so the directory applied anywhere creates both in the
// database it runs in (stokaro/ptah#6710).
func TestMigrationsCheckpointNamesNoShadowDatabaseInASequenceDefaultE2E(t *testing.T) {
	c := qt.New(t)
	shadowName, shadow := newMySQLFamilyScratch(c, dbtarget.MariaDBDevServer).database(c, "seq_ckp")
	dir := writeSequenceDefaultMigrations(c, 1)
	options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}

	got := clirun.Run(c, clirun.Ptah, options, "migrations", "checkpoint", "--migrations-dir", dir,
		"--shadow-db", shadow, "--trust-replay-sql", "--dry-run")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "CREATE SEQUENCE `s`")
	c.Assert(got.Stdout, qt.Contains, "DEFAULT (nextval(`s`))")
	c.Assert(got.Stdout, qt.Not(qt.Contains), shadowName)
}

// TestSchemaApplySequenceDefaultConvergesE2E applies a sequence and a table
// whose key defaults to it, then applies the same file again. The read takes
// the database off the stored nextval(`<database>`.`s`), so the second apply
// plans nothing; compared with the stored text, it plans the default again on
// every run.
func TestSchemaApplySequenceDefaultConvergesE2E(t *testing.T) {
	c := qt.New(t)
	scratch := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin)
	targetName, target := scratch.database(c, "seq_apply")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE SEQUENCE s;\nCREATE TABLE t2 (id int PRIMARY KEY DEFAULT NEXTVAL(s), n int);\n"),
		0o600), qt.IsNil)
	options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}
	apply := []string{"schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve"}
	first := clirun.Run(c, clirun.Ptah, options, apply...)
	c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))

	again := clirun.Run(c, clirun.Ptah, options, apply...)

	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	c.Assert(scratch.drawnKey(c, targetName), qt.Equals, 1)
}
