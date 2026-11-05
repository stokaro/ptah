package schema_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// skippedColumnAndIndex are the lines a plan that skips the drop of t.b and of
// t.t_a writes, in the words a generated migration carries.
const skippedColumnAndIndex = "SKIP: DROP COLUMN of t.b omitted by diff policy (skip: drop_column)\n" +
	"SKIP: DROP INDEX of t.t_a omitted by diff policy (skip: drop_index)\n"

// diffSkipProject is a SQLite database holding t(id, a, b) and an index on a,
// a desired schema of t(id, a) plus whatever extra declares, and a ptah.yaml
// carrying config. Dropping b is a table rebuild on SQLite, and the rebuild
// loses every index the desired table does not declare.
func diffSkipProject(c *qt.C, extra, config string) (dbPath, schemaPath, configPath string) {
	c.Helper()
	dir := c.TempDir()
	dbPath = filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT);\nCREATE INDEX t_a ON t(a);")
	schemaPath = writeSchemaSQLFile(c, dir, "schema.sql", "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT"+extra+");\n")
	configPath = filepath.Join(dir, "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte(config), 0o600), qt.IsNil)
	return dbPath, schemaPath, configPath
}

// runSchemaSucceeding executes a native `ptah schema ...` subcommand that has
// to succeed and returns what it wrote to each stream.
func runSchemaSucceeding(c *qt.C, args ...string) (stdout, stderr string) {
	c.Helper()
	stdout, stderr, err := runSchemaStreams(args...)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	return stdout, stderr
}

// sqliteCount answers a counting query against the SQLite database at dbPath.
func sqliteCount(c *qt.C, dbPath, query string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(context.Background(), query).Scan(&count), qt.IsNil)
	return count
}

const (
	columnBCount = "SELECT count(*) FROM pragma_table_info('t') WHERE name = 'b'"
	columnCCount = "SELECT count(*) FROM pragma_table_info('t') WHERE name = 'c'"
	indexTACount = "SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = 't_a'"
)

// TestSchemaApplyHonorsDiffSkipForColumnsAndIndexes_HappyPath pins the native
// apply against the plan `ptah migrations generate` writes for the same
// project: nothing to do, and a line for each drop the policy withheld beside
// the synced result.
func TestSchemaApplyHonorsDiffSkipForColumnsAndIndexes_HappyPath(t *testing.T) {
	c := qt.New(t)
	dbPath, schemaPath, configPath := diffSkipProject(c, "", "diff:\n  skip: [drop_index, drop_column]\n")

	stdout, stderr := runSchemaSucceeding(c, "apply", "--db-url", "sqlite://"+dbPath, "--schema-file", schemaPath,
		"--config", configPath, "--auto-approve")

	c.Assert(stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
	c.Assert(stderr, qt.Equals, skippedColumnAndIndex)
	c.Assert(sqliteCount(c, dbPath, columnBCount), qt.Equals, 1)
	c.Assert(sqliteCount(c, dbPath, indexTACount), qt.Equals, 1)
}

// TestSchemaApplyHonorsDiffSkipBesideAPlannedChange_HappyPath pins a plan the
// policy leaves something in: the added column is applied, b and t_a stay,
// and the plan opens with the comment for each change it left out.
func TestSchemaApplyHonorsDiffSkipBesideAPlannedChange_HappyPath(t *testing.T) {
	c := qt.New(t)
	dbPath, schemaPath, configPath := diffSkipProject(c, ", c TEXT", "diff:\n  skip: [drop_index, drop_column]\n")

	stdout, _ := runSchemaSucceeding(c, "apply", "--db-url", "sqlite://"+dbPath, "--schema-file", schemaPath,
		"--config", configPath, "--auto-approve")

	c.Assert(stdout, qt.Contains, "Planned schema changes:\n-- "+
		"SKIP: DROP COLUMN of t.b omitted by diff policy (skip: drop_column)\n-- "+
		"SKIP: DROP INDEX of t.t_a omitted by diff policy (skip: drop_index)\n")
	c.Assert(stdout, qt.Contains, `ALTER TABLE "t" ADD COLUMN "c" TEXT`)
	c.Assert(stdout, qt.Not(qt.Contains), "DROP TABLE")
	c.Assert(sqliteCount(c, dbPath, columnBCount), qt.Equals, 1)
	c.Assert(sqliteCount(c, dbPath, columnCCount), qt.Equals, 1)
	c.Assert(sqliteCount(c, dbPath, indexTACount), qt.Equals, 1)
}

// TestSchemaApplyWithoutDiffSkipDropsColumnAndIndex_HappyPath is the control:
// the project without the policy rebuilds t, so the rows above measure the
// policy and not the fixture.
func TestSchemaApplyWithoutDiffSkipDropsColumnAndIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	dbPath, schemaPath, configPath := diffSkipProject(c, "", "diff:\n  skip: []\n")

	stdout, stderr := runSchemaSucceeding(c, "apply", "--db-url", "sqlite://"+dbPath, "--schema-file", schemaPath,
		"--config", configPath, "--auto-approve")

	c.Assert(stdout, qt.Contains, `DROP TABLE "t"`)
	c.Assert(stdout+stderr, qt.Not(qt.Contains), "SKIP:")
	c.Assert(sqliteCount(c, dbPath, columnBCount), qt.Equals, 0)
	c.Assert(sqliteCount(c, dbPath, indexTACount), qt.Equals, 0)
}

// TestSchemaDiffHonorsDiffSkipForColumnsAndIndexes_HappyPath pins the preview
// verb on the same policy: it plans what apply would, and says what it left
// out beside the synced result.
func TestSchemaDiffHonorsDiffSkipForColumnsAndIndexes_HappyPath(t *testing.T) {
	c := qt.New(t)
	dbPath, schemaPath, configPath := diffSkipProject(c, "", "diff:\n  skip: [drop_index, drop_column]\n")

	stdout, stderr := runSchemaSucceeding(c, "diff", "--from", "sqlite://"+dbPath, "--to", schemaPath,
		"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"), "--config", configPath)

	c.Assert(stdout, qt.Not(qt.Contains), "DROP")
	c.Assert(stderr, qt.Equals, skippedColumnAndIndex)
}

// TestSchemaApplyKeepsASkippedColumnThroughARebuild_HappyPath pins a column the
// policy keeps on a table SQLite rebuilds for another change: retyping a
// rebuilds t, and the rebuilt table carries b and its rows. The community
// binary v1.3.0 rebuilds t without b under the same policy, measured on
// SQLite, which loses the column the project asked to keep.
func TestSchemaApplyKeepsASkippedColumnThroughARebuild_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	seedSQLite(c, dbPath, "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT);\nINSERT INTO t VALUES (1, '7', 'kept');")
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql", "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER);\n")
	configPath := filepath.Join(dir, "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("diff:\n  skip: [drop_column]\n"), 0o600), qt.IsNil)

	stdout, _ := runSchemaSucceeding(c, "apply", "--db-url", "sqlite://"+dbPath, "--schema-file", schemaPath,
		"--config", configPath, "--auto-approve")

	c.Assert(stdout, qt.Contains, "-- SKIP: DROP COLUMN of t.b omitted by diff policy (skip: drop_column)\n")
	c.Assert(stdout, qt.Contains, `INSERT INTO "new_t" ("id", "a", "b") SELECT "id", "a", "b" FROM "t";`)
	c.Assert(sqliteCount(c, dbPath, "SELECT count(*) FROM t WHERE b = 'kept' AND a = 7"), qt.Equals, 1)
}
