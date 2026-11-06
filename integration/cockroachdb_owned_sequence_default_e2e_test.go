//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

const (
	// ownedSequenceBuilt is a column that draws from a sequence it owns.
	ownedSequenceBuilt = "CREATE TABLE t (id INT8 PRIMARY KEY, a INT8);\nCREATE SEQUENCE s OWNED BY t.a;\nALTER TABLE t ALTER COLUMN a SET DEFAULT nextval('s');\n"
	// ownedSequenceCount counts the sequences public holds.
	ownedSequenceCount = "SELECT count(*) FROM information_schema.sequences WHERE sequence_schema = 'public'"
)

// TestSchemaApplyCockroachDBOwnedSequenceDefaultE2E_HappyPath changes the default of a
// column that owns the sequence it draws from, and keeps the sequence. CockroachDB
// 26.3.5 answers the bare ALTER COLUMN with `internal error: executing declarative
// schema change` (SQLSTATE XX000). Each row reads the default back and finds the
// second run synced. Dropping the column then takes the sequence with it, which
// only a sequence that has that owner does, so the row proves the owner came back.
func TestSchemaApplyCockroachDBOwnedSequenceDefaultE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name, built, desired, plan, reads string
		sequencesLeft                     int
	}{
		{
			name:    "the default leaves, and the declaration keeps the owner",
			built:   ownedSequenceBuilt,
			desired: "CREATE TABLE t (id INT8 PRIMARY KEY, a INT8);\nCREATE SEQUENCE s OWNED BY t.a;\n",
			plan:    "ALTER SEQUENCE \"s\" OWNED BY NONE;\nALTER TABLE \"t\" ALTER COLUMN \"a\" DROP DEFAULT;\nALTER SEQUENCE \"s\" OWNED BY \"t\".\"a\";",
		},
		{
			name:    "the default leaves, and the declaration names no owner",
			built:   ownedSequenceBuilt,
			desired: "CREATE TABLE t (id INT8 PRIMARY KEY, a INT8);\nCREATE SEQUENCE s;\n",
			plan:    "ALTER SEQUENCE \"s\" OWNED BY NONE;\nALTER TABLE \"t\" ALTER COLUMN \"a\" DROP DEFAULT;\nALTER SEQUENCE \"s\" OWNED BY \"t\".\"a\";",
		},
		{
			name:    "the default moves to another sequence",
			built:   ownedSequenceBuilt + "CREATE SEQUENCE s2;\n",
			desired: "CREATE SEQUENCE s2;\nCREATE TABLE t (id INT8 PRIMARY KEY, a INT8 DEFAULT nextval('s2'));\nCREATE SEQUENCE s OWNED BY t.a;\n",
			plan:    "ALTER SEQUENCE \"s\" OWNED BY NONE;\nALTER TABLE \"t\" ALTER COLUMN \"a\" SET DEFAULT nextval('s2');\nALTER SEQUENCE \"s\" OWNED BY \"t\".\"a\";",
			reads:   "nextval('public.s2'::REGCLASS)", sequencesLeft: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := cockroachVisibilityDatabase(c, c.Context(), test.built)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(test.desired), 0o600), qt.IsNil)

			run := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, test.plan)
			c.Assert(ownedSequenceDefault(c, target), qt.Equals, test.reads)
			again := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")

			ownedSequenceDropColumn(c, target)
			c.Assert(readInt(c, target, ownedSequenceCount), qt.Equals, test.sequencesLeft)
		})
	}
}

// TestSchemaApplyCockroachDBOwnedSequenceDefaultE2E_AnotherOwner is the control: a
// default on a sequence another column owns changes without a release, and the
// owner stays where it was.
func TestSchemaApplyCockroachDBOwnedSequenceDefaultE2E_AnotherOwner(t *testing.T) {
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(),
		"CREATE TABLE t (id INT8 PRIMARY KEY, a INT8, b INT8);\nCREATE SEQUENCE s OWNED BY t.b;\nALTER TABLE t ALTER COLUMN a SET DEFAULT nextval('s');\n")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t (id INT8 PRIMARY KEY, a INT8, b INT8);\nCREATE SEQUENCE s OWNED BY t.b;\n"), 0o600), qt.IsNil)

	run := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Not(qt.Contains), "ALTER SEQUENCE")
	c.Assert(ownedSequenceDefault(c, target), qt.Equals, "")
}

// ownedSequenceDefault reads the default of t.a back, and "" where it has none.
func ownedSequenceDefault(c *qt.C, target string) string {
	c.Helper()
	return columnKeyString(c, target, "SELECT coalesce(column_default, '') FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 't' AND column_name = 'a'")
}

// ownedSequenceDropColumn drops t.a, which takes the sequences it owns.
func ownedSequenceDropColumn(c *qt.C, target string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), `ALTER TABLE t SET (schema_locked = false)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), `ALTER TABLE t DROP COLUMN a`)
	c.Assert(err, qt.IsNil)
}
