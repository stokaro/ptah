//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	// serialPlainDefault reads back the default of parent's column, and "" for none.
	serialPlainDefault = "SELECT coalesce((SELECT column_default FROM information_schema.columns " +
		"WHERE table_schema = 'public' AND table_name = 'parent' AND column_name = 'n'), '')"
	// serialPlainSequences counts the sequences public holds.
	serialPlainSequences = "SELECT count(*) FROM pg_class WHERE relkind = 'S' AND relnamespace = 'public'::regnamespace"
)

// TestSchemaApplySerialAgainstPlainTypeE2E_HappyPath declares a plain type over a
// column the database holds as serial. The comparison read them as synced, so a
// declared column with no default went on drawing from a sequence. Each row reads the
// column and the sequences back, and a second run finds nothing to do.
func TestSchemaApplySerialAgainstPlainTypeE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name, built, desired, wantDefault string
		wantSequences                     int
	}{
		{
			name:    "a key column",
			built:   "CREATE TABLE parent (n bigserial PRIMARY KEY);\nINSERT INTO parent DEFAULT VALUES;\n",
			desired: "CREATE TABLE parent (n bigint PRIMARY KEY);\n",
		},
		{
			name:    "a column beside the key",
			built:   "CREATE TABLE parent (id bigint PRIMARY KEY, n serial);\n",
			desired: "CREATE TABLE parent (id bigint PRIMARY KEY, n int NOT NULL);\n",
		},
		{
			name:    "the declaration keeps the serial type, control",
			built:   "CREATE TABLE parent (n bigserial PRIMARY KEY);\n",
			desired: "CREATE TABLE parent (n bigserial PRIMARY KEY);\n",

			wantDefault: "nextval('parent_n_seq'::regclass)", wantSequences: 1,
		},
		{
			name:    "the declaration moves the default to a sequence of its own",
			built:   "CREATE TABLE parent (id bigint PRIMARY KEY, n bigserial);\nCREATE SEQUENCE other;\n",
			desired: "CREATE SEQUENCE other;\nCREATE TABLE parent (id bigint PRIMARY KEY, n bigint NOT NULL DEFAULT nextval('other'));\nCREATE SEQUENCE parent_n_seq OWNED BY parent.n;\n",

			wantDefault: "nextval('other'::regclass)", wantSequences: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, test.built)

			run := keyedApply(c, target, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, serialPlainDefault), qt.Equals, test.wantDefault)
			c.Assert(readInt(c, target, serialPlainSequences), qt.Equals, test.wantSequences)
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplySerialAgainstPlainTypeE2E_AnUndeclaredReaderStopsThePlan draws from
// the sequence in a schema the run leaves out. The server refuses DROP SEQUENCE and
// the plan's one transaction gives the column its default back.
func TestSchemaApplySerialAgainstPlainTypeE2E_AnUndeclaredReaderStopsThePlan(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, "CREATE TABLE parent (n bigserial PRIMARY KEY);\nCREATE SCHEMA reporting;\n"+
		"CREATE TABLE reporting.u (id bigint DEFAULT nextval('public.parent_n_seq'));\n")

	run := keyedApply(c, target, "CREATE TABLE parent (n bigint PRIMARY KEY);\n")

	c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stderr, qt.Contains, "cannot drop sequence parent_n_seq because other objects depend on it")
	c.Assert(columnKeyString(c, target, serialPlainDefault), qt.Equals, "nextval('parent_n_seq'::regclass)")
	c.Assert(readInt(c, target, serialPlainSequences), qt.Equals, 1)
}

// TestMigrationsSerialAgainstPlainTypeE2E_TheRollbackRestoresTheColumn runs the plan
// through a migration and back. The rollback creates the sequence, gives the column
// its default and the sequence its owner, and the serial declaration then reads as
// synced.
func TestMigrationsSerialAgainstPlainTypeE2E_TheRollbackRestoresTheColumn(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, "CREATE TABLE parent (n bigserial PRIMARY KEY);\n")
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	schema := serialPlainSchemaFile(c, "CREATE TABLE parent (n bigint PRIMARY KEY);\n")

	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--migrations-dir", dir, "--name", "plain")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(columnKeyString(c, target, serialPlainDefault), qt.Equals, "")
	c.Assert(readInt(c, target, serialPlainSequences), qt.Equals, 0)

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(columnKeyString(c, target, serialPlainDefault), qt.Equals, "nextval('parent_n_seq'::regclass)")
	again := keyedApply(c, target, "CREATE TABLE parent (n bigserial PRIMARY KEY);\n")
	c.Assert(again.Stdout, qt.Contains, "Schema is synced")
}

// serialPlainSchemaFile writes desired to a schema file and returns its path.
func serialPlainSchemaFile(c *qt.C, desired string) string {
	c.Helper()
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	return schema
}
