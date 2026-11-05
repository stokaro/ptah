//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// removedForeignKeyEngines are the servers that keep the index.
var removedForeignKeyEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "mysql", engine: dbtarget.MySQLAdmin},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin},
}

const (
	// removedForeignKeyParent is the table the foreign keys reference.
	removedForeignKeyParent = "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL UNIQUE);\n"
	// removedForeignKeyIndexes reads the indexes of child back, the key left out.
	removedForeignKeyIndexes = "SELECT COALESCE((SELECT GROUP_CONCAT(DISTINCT INDEX_NAME ORDER BY INDEX_NAME) FROM information_schema.STATISTICS " +
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'child' AND INDEX_NAME <> 'PRIMARY'), '')"
)

// removedForeignKeyApply runs `schema apply` of desired over the database at target.
func removedForeignKeyApply(c *qt.C, target, desired string) (int, string, string) {
	c.Helper()
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	run := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")
	return run.ExitCode, run.Stdout, run.Stderr
}

// TestSchemaApplyRemovedForeignKeyE2E_HappyPath removes a foreign key on MySQL and
// MariaDB. The server keeps the index it built for the key, the comparison then finds
// an index nobody declares, and the plan stopped short: only the next run planned
// DROP INDEX. Each row reads the indexes back from the catalog, and a second run finds
// nothing to do.
func TestSchemaApplyRemovedForeignKeyE2E_HappyPath(t *testing.T) {
	const withKey = "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parent (id));\n"
	tests := []struct{ name, built, desired, want string }{
		{
			name:    "the index leaves with the key",
			built:   withKey,
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint);\n",
		},
		{
			name:    "an unnamed key, whose index the server names after the column",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint);\n",
		},
		{
			name:    "the declaration holds an index of that name",
			built:   withKey,
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint);\nCREATE INDEX child_fk ON child (pid);\n",
			want:    "child_fk",
		},
		{
			name: "a foreign key the plan keeps needs the index",
			built: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id), " +
				"CONSTRAINT b_fk FOREIGN KEY (pid) REFERENCES parent (code));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT b_fk FOREIGN KEY (pid) REFERENCES parent (code));\n",
			want:    "a_fk",
		},
		{
			name:    "a foreign key the plan adds takes the index over",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT old_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT new_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			want:    "new_fk",
		},
		{
			name:    "the plan changes the key under its name",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (code));\n",
			want:    "c_fk",
		},
		{
			name:    "the index leaves with its column",
			built:   withKey,
			desired: "CREATE TABLE child (id bigint PRIMARY KEY);\n",
		},
		{
			name:    "the declaration holds the index the key was built on",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, KEY i (pid), CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint);\nCREATE INDEX i ON child (pid);\n",
			want:    "i",
		},
	}
	for _, engine := range removedForeignKeyEngines {
		for _, test := range tests {
			t.Run(engine.name+" "+test.name, func(t *testing.T) {
				c := qt.New(t)
				_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, removedForeignKeyParent+test.built)
				desired := removedForeignKeyParent + test.desired

				exit, stdout, stderr := removedForeignKeyApply(c, target, desired)

				c.Assert(exit, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
				c.Assert(columnKeyString(c, target, removedForeignKeyIndexes), qt.Equals, test.want)
				_, again, _ := removedForeignKeyApply(c, target, desired)
				c.Assert(again, qt.Contains, "Schema is synced")
			})
		}
	}
}

// TestMigrationsRemovedForeignKeyE2E_TheRollbackRestoresTheKey runs the removal through
// a migration and back. The key and its index return, and the first declaration reads
// as synced.
func TestMigrationsRemovedForeignKeyE2E_TheRollbackRestoresTheKey(t *testing.T) {
	const withKey = removedForeignKeyParent + "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT child_fk FOREIGN KEY (pid) REFERENCES parent (id));\n"
	for _, engine := range removedForeignKeyEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, withKey)
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(removedForeignKeyParent+"CREATE TABLE child (id bigint PRIMARY KEY, pid bigint);\n"), 0o600), qt.IsNil)

			generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "fk")
			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(columnKeyString(c, target, removedForeignKeyIndexes), qt.Equals, "")

			down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

			c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
			c.Assert(columnKeyString(c, target, removedForeignKeyIndexes), qt.Equals, "child_fk")
			_, again, _ := removedForeignKeyApply(c, target, withKey)
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}
