//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// cockroachKeyRemovedRead prints the primary key of public.parent as the server does.
const cockroachKeyRemovedRead = "SELECT conname || ': ' || pg_get_constraintdef(oid) FROM pg_constraint " +
	"WHERE conrelid = 'public.parent'::regclass AND contype = 'p'"

// TestSchemaApplyCockroachDBKeyRemovedE2E_HappyPath removes a primary key on CockroachDB
// 26.3.5, where every table holds one. A DROP CONSTRAINT alone stopped at the server.
// Each row reads the key back and counts the rows, and a second run finds nothing to do.
func TestSchemaApplyCockroachDBKeyRemovedE2E_HappyPath(t *testing.T) {
	const rows = "INSERT INTO parent VALUES (1, 10), (2, 20);\n"
	tests := []struct{ name, built, desired string }{
		{
			name:    "the key leaves",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8);\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL, code INT8);\n",
		},
		{
			name:    "its column leaves after it",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8);\n" + rows,
			desired: "CREATE TABLE parent (code INT8);\n",
		},
		{
			name:    "a key of another name",
			built:   "CREATE TABLE parent (id INT8, code INT8, CONSTRAINT custom PRIMARY KEY (id));\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL, code INT8);\n",
		},
		{
			name: "a foreign key takes a UNIQUE over the columns",
			built: "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8);\n" +
				"CREATE TABLE child (id INT8 PRIMARY KEY, pid INT8 REFERENCES parent (id));\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL UNIQUE, code INT8);\n" +
				"CREATE TABLE child (id INT8 PRIMARY KEY, pid INT8 REFERENCES parent (id));\n",
		},
		{
			name:    "a table without a key, control",
			built:   "CREATE TABLE parent (id INT8 NOT NULL, code INT8);\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL, code INT8);\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := cockroachVisibilityDatabase(c, c.Context(), test.built)

			run := keyedApply(c, target, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, cockroachKeyRemovedRead), qt.Equals, "parent_pkey: PRIMARY KEY (rowid ASC)")
			c.Assert(readInt(c, target, "SELECT count(*) FROM parent"), qt.Equals, 2)
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestMigrationsCockroachDBKeyRemovedE2E_TheRollbackRestoresTheKey rolls the removal back.
// The server takes the key off the hidden column and drops the column with it.
func TestMigrationsCockroachDBKeyRemovedE2E_TheRollbackRestoresTheKey(t *testing.T) {
	c := qt.New(t)
	target, _ := cockroachVisibilityDatabase(c, c.Context(), "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8);\nINSERT INTO parent VALUES (1, 10);\n")
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE parent (id INT8 NOT NULL, code INT8);\n"), 0o600), qt.IsNil)

	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "key")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(columnKeyString(c, target, cockroachKeyRemovedRead), qt.Equals, "parent_pkey: PRIMARY KEY (rowid ASC)")

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(columnKeyString(c, target, cockroachKeyRemovedRead), qt.Equals, "parent_pkey: PRIMARY KEY (id ASC)")
	c.Assert(readInt(c, target, "SELECT count(*) FROM information_schema.columns WHERE table_name = 'parent' AND column_name = 'rowid'"), qt.Equals, 0)
}
