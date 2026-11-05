//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// removedSequenceReadDefaults counts the column defaults public.t holds. On
// CockroachDB information_schema reports each as a string, and NULL for none.
const removedSequenceReadDefaults = "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 't' AND column_default IS NOT NULL"

// removedSequenceApply runs the native binary's `schema apply` of desired over
// the public schema of the database at target.
func removedSequenceApply(c *qt.C, target, desired string) clirun.Result {
	c.Helper()
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	return runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")
}

// TestSchemaApplyRemovedSequenceE2E_OnCockroachDB removes a sequence a column of a
// kept table drew from. pg_get_serial_sequence names such a sequence on CockroachDB,
// so the default read as the column's own and the plan left it there: DROP SEQUENCE
// stopped at `cannot drop sequence s because other objects depend on it`. A row and
// its value survive, and a second run finding nothing proves the plan ended in the
// declared state.
func TestSchemaApplyRemovedSequenceE2E_OnCockroachDB(t *testing.T) {
	const desired = "CREATE TABLE t (id INT8 PRIMARY KEY, n INT8, m INT8);\n"
	tests := []struct{ name, built string }{
		{
			name: "a column draws from a sequence no column owns",
			built: "CREATE SEQUENCE s;\nCREATE TABLE t (id INT8 PRIMARY KEY, n INT8 DEFAULT nextval('s'), m INT8);\n" +
				"INSERT INTO t (id, m) VALUES (1, 3);\n",
		},
		{
			name: "the column owns the sequence it draws from",
			built: "CREATE SEQUENCE s;\nCREATE TABLE t (id INT8 PRIMARY KEY, n INT8 DEFAULT nextval('s'), m INT8);\n" +
				"ALTER SEQUENCE s OWNED BY t.n;\nINSERT INTO t (id, m) VALUES (1, 3);\n",
		},
		{
			name: "two columns draw from it and one owns it",
			built: "CREATE SEQUENCE s;\nCREATE TABLE t (id INT8 PRIMARY KEY, n INT8 DEFAULT nextval('s'), m INT8 DEFAULT 3);\n" +
				"ALTER TABLE t ALTER COLUMN m SET DEFAULT nextval('s');\nALTER SEQUENCE s OWNED BY t.n;\nINSERT INTO t (id, m) VALUES (1, 3);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := cockroachVisibilityDatabase(c, c.Context(), test.built)

			run := removedSequenceApply(c, target, desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(readInt(c, target, removedSequenceReadDefaults), qt.Equals, 0)
			c.Assert(readInt(c, target, "SELECT count(*) FROM information_schema.sequences WHERE sequence_schema = 'public'"), qt.Equals, 0)
			c.Assert(readInt(c, target, "SELECT m FROM t WHERE id = 1"), qt.Equals, 3)
			again := removedSequenceApply(c, target, desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyRemovedSequenceE2E_ADefaultOnAnotherSchemasSequenceIsADefault is the
// sequence the run does not remove. The column drew from a sequence of another
// schema and read as its own, so the plan reported the schema synced while the
// declaration held no default. The sequence stays, since the run leaves its schema
// out.
func TestSchemaApplyRemovedSequenceE2E_ADefaultOnAnotherSchemasSequenceIsADefault(t *testing.T) {
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(), "CREATE SCHEMA other;\nCREATE SEQUENCE other.s;\n"+
		"CREATE TABLE t (id INT8 PRIMARY KEY, n INT8 DEFAULT nextval('other.s'));\n")

	run := removedSequenceApply(c, target, "CREATE TABLE t (id INT8 PRIMARY KEY, n INT8);\n")

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Contains, `ALTER COLUMN "n" DROP DEFAULT`)
	c.Assert(readInt(c, target, removedSequenceReadDefaults), qt.Equals, 0)
	c.Assert(readInt(c, target, "SELECT count(*) FROM information_schema.sequences WHERE sequence_schema = 'other'"), qt.Equals, 1)
}

// TestSchemaApplyRemovedSequenceE2E_AnUndeclaredReaderStopsTheDrop gives the sequence
// a column in a schema the run leaves out. The server refuses the drop and the
// other schema's column keeps its default. CockroachDB commits each schema change
// by itself, so the kept column's default stays dropped, as any statement ahead of a
// refusal does there.
func TestSchemaApplyRemovedSequenceE2E_AnUndeclaredReaderStopsTheDrop(t *testing.T) {
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(), "CREATE SEQUENCE s;\n"+
		"CREATE TABLE t (id INT8 PRIMARY KEY, n INT8 DEFAULT nextval('s'));\n"+
		"CREATE SCHEMA reporting;\nCREATE TABLE reporting.u (id INT8 PRIMARY KEY, n INT8 DEFAULT nextval('public.s'));\n")

	run := removedSequenceApply(c, target, "CREATE TABLE t (id INT8 PRIMARY KEY, n INT8);\n")

	c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Contains, `ALTER COLUMN "n" DROP DEFAULT`)
	c.Assert(run.Stderr, qt.Contains, "cannot drop sequence s because other objects depend on it")
	c.Assert(readInt(c, target, "SELECT count(*) FROM information_schema.sequences WHERE sequence_schema = 'public'"), qt.Equals, 1)
	c.Assert(readInt(c, target, "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'reporting' AND table_name = 'u' AND column_default IS NOT NULL"), qt.Equals, 1)
}

// TestSchemaApplyRemovedSequenceE2E_APostgreSQLDomainColumnsDefault is the shape
// PostgreSQL shares: a domain column that owns the sequence it draws from has a
// sequence described on its own, and read as the column's own, its default was left
// in place under the drop.
func TestSchemaApplyRemovedSequenceE2E_APostgreSQLDomainColumnsDefault(t *testing.T) {
	c := qt.New(t)
	const domain = "CREATE DOMAIN pos AS bigint CHECK (VALUE > 0);\n"

	target, run := runExactApply(c,
		domain+"CREATE SEQUENCE s;\nCREATE TABLE t (id bigint PRIMARY KEY, n pos DEFAULT nextval('s'));\nALTER SEQUENCE s OWNED BY t.n;\n",
		domain+"CREATE TABLE t (id bigint PRIMARY KEY, n pos);\n")

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(readInt(c, target, removedSequenceReadDefaults), qt.Equals, 0)
	c.Assert(readInt(c, target, "SELECT count(*) FROM pg_class WHERE relname = 's'"), qt.Equals, 0)
}
