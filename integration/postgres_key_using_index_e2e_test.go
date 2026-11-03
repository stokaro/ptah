//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// keysUsingIndexes each make a key of a unique index, with ALTER TABLE ...
// ADD CONSTRAINT ... USING INDEX. PostgreSQL 18.6 runs each file: the key
// takes the index's columns, INCLUDE columns and NULLS treatment, and the
// index is renamed to the constraint's name.
var keysUsingIndexes = []struct {
	name string
	sql  string
}{
	{
		name: "a named unique key",
		sql:  "CREATE TABLE t (a int, b int);\nCREATE UNIQUE INDEX t_ab ON t (a, b);\nALTER TABLE t ADD CONSTRAINT t_ab_key UNIQUE USING INDEX t_ab;",
	},
	{
		name: "an unnamed unique key",
		sql:  "CREATE TABLE t (a int);\nCREATE UNIQUE INDEX t_a ON t (a);\nALTER TABLE t ADD UNIQUE USING INDEX t_a;",
	},
	{
		name: "a key over an index with INCLUDE columns whose NULLs are not distinct",
		sql: "CREATE TABLE t (a int, b int);\nCREATE UNIQUE INDEX t_a ON t (a) INCLUDE (b) NULLS NOT DISTINCT;\n" +
			"ALTER TABLE t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a;",
	},
	{
		name: "a deferrable key",
		sql: "CREATE TABLE t (a int);\nCREATE UNIQUE INDEX t_a ON t (a);\n" +
			"ALTER TABLE t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a DEFERRABLE INITIALLY DEFERRED;",
	},
	{
		name: "a primary key over a nullable column",
		sql:  "CREATE TABLE t (id int, b int);\nCREATE UNIQUE INDEX t_id ON t (id);\nALTER TABLE t ADD CONSTRAINT t_pkey PRIMARY KEY USING INDEX t_id;",
	},
	{
		name: "a key named as its index",
		sql:  "CREATE TABLE t (a int);\nCREATE UNIQUE INDEX t_a ON t (a);\nALTER TABLE t ADD CONSTRAINT t_a UNIQUE USING INDEX t_a;",
	},
	{
		name: "an index under the name the key freed, and a guarded one under the key's",
		sql: "CREATE TABLE t (a int, b int);\nCREATE UNIQUE INDEX t_a ON t (a);\n" +
			"ALTER TABLE t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a;\n" +
			"CREATE INDEX t_a ON t (b);\nCREATE INDEX IF NOT EXISTS t_a_key ON t (b);",
	},
	{
		name: "an unnamed key, then a guarded index under a name derived from no columns",
		sql: "CREATE TABLE t (a int, b int);\nCREATE UNIQUE INDEX t_a ON t (a);\nALTER TABLE t ADD UNIQUE USING INDEX t_a;\n" +
			"CREATE INDEX IF NOT EXISTS t_key ON t (b);",
	},
	{
		name: "a key of a table in another schema",
		sql: "CREATE SCHEMA app;\nCREATE TABLE app.t (a int);\nCREATE UNIQUE INDEX t_a ON app.t (a);\n" +
			"ALTER TABLE app.t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a;",
	},
}

// TestPostgresKeyUsingIndexDiffEmptyE2E builds a database from each file and
// diffs it with the file: nothing changes, because the reader makes the key of
// the index as the server did.
func TestPostgresKeyUsingIndexDiffEmptyE2E(t *testing.T) {
	for _, test := range keysUsingIndexes {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.sql+"\n"), 0o600), qt.IsNil)

			code, stdout, stderr := runAsOneRun(c, nativeTree, "schema", "diff",
				"--from", databaseBuiltFrom(c, test.sql), "--to", "file://"+filepath.ToSlash(schema))

			c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
		})
	}
}

// TestPostgresKeyUsingIndexAcrossFilesDiffEmptyE2E is the same with the index
// declared by an earlier file of a schema directory and made a key by a later
// one, which the server runs in order.
func TestPostgresKeyUsingIndexAcrossFilesDiffEmptyE2E(t *testing.T) {
	c := qt.New(t)
	dir, _, script := writeGuardedFiles(c, []string{
		"CREATE TABLE t (a int);\nCREATE UNIQUE INDEX t_a ON t (a);",
		"ALTER TABLE t ADD CONSTRAINT t_a_key UNIQUE USING INDEX t_a;",
	})

	code, stdout, stderr := runAsOneRun(c, nativeTree, "schema", "diff",
		"--from", databaseBuiltFrom(c, script), "--to", "file://"+filepath.ToSlash(dir))

	c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Equals, "Schemas are synced, no changes to be made.\n")
}

// TestPostgresKeyUsingIndexApplyConvergesLive applies each file to an empty
// database and compares again: the server accepts the key the plan writes, and
// the second comparison finds nothing to do.
func TestPostgresKeyUsingIndexApplyConvergesLive(t *testing.T) {
	for _, test := range keysUsingIndexes {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.sql+"\n"), 0o600), qt.IsNil)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}
