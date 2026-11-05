//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	// keptKeyParent is the table the foreign keys reference, with a key over two columns.
	keptKeyParent = "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL UNIQUE, UNIQUE KEY parent_pair (id, code));\n"
	// keptKeyRead prints the indexes of child with their columns and whether they are
	// unique, and the foreign keys it holds, as the server does.
	keptKeyRead = "SELECT concat(" +
		"coalesce((SELECT group_concat(concat(index_name, '(', cols, ')', IF(non_unique = 0, '!', '')) ORDER BY index_name SEPARATOR ',') FROM " +
		"(SELECT index_name, non_unique, group_concat(column_name ORDER BY seq_in_index) cols FROM information_schema.statistics " +
		"WHERE table_schema = database() AND table_name = 'child' AND index_name <> 'PRIMARY' GROUP BY index_name, non_unique) i), ''), ' | ', " +
		"coalesce((SELECT group_concat(constraint_name ORDER BY constraint_name) FROM information_schema.table_constraints " +
		"WHERE table_schema = database() AND table_name = 'child' AND constraint_type = 'FOREIGN KEY'), ''))"
)

// TestSchemaApplyKeptForeignKeyIndexE2E_HappyPath removes every index a foreign key the
// plan keeps stands on. The plan dropped the index alone, and the server stopped it with
// Error 1553, on this run or on the next. Each row reads the indexes and the keys back,
// and a second run finds nothing to do.
func TestSchemaApplyKeptForeignKeyIndexE2E_HappyPath(t *testing.T) {
	const keeps = "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id));\n"
	tests := []struct{ name, built, desired, want string }{
		{
			name: "the removed key shared its index with the kept key",
			built: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id), " +
				"CONSTRAINT b_fk FOREIGN KEY (pid) REFERENCES parent (code));\n",
			desired: keeps,
			want:    "a_fk(pid) | a_fk",
		},
		{
			name: "the removed key's wider index carried the kept key",
			built: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id), " +
				"CONSTRAINT b_fk FOREIGN KEY (pid, qid) REFERENCES parent (id, code));\n",
			desired: keeps,
			want:    "a_fk(pid) | a_fk",
		},
		{
			name:    "an index of the author's own name",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, KEY cover (pid), CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: keeps,
			want:    "a_fk(pid) | a_fk",
		},
		{
			name:    "two indexes of the author's own names",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, KEY x (pid), KEY y (pid), CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: keeps,
			want:    "a_fk(pid) | a_fk",
		},
		{
			name:    "a wider index of the author's own name",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, KEY w (pid, qid), CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: keeps,
			want:    "a_fk(pid) | a_fk",
		},
		{
			name:    "a unique index",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, UNIQUE KEY u (pid), CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: keeps,
			want:    "a_fk(pid) | a_fk",
		},
		{
			name: "a wider kept key carries a narrower one",
			built: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, KEY w (pid, qid), CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id), " +
				"CONSTRAINT p_fk FOREIGN KEY (pid, qid) REFERENCES parent (id, code));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id), " +
				"CONSTRAINT p_fk FOREIGN KEY (pid, qid) REFERENCES parent (id, code));\n",
			want: "p_fk(pid,qid) | a_fk,p_fk",
		},
		{
			name:    "the server names the kept key's index after its column",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, KEY cover (pid), FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, FOREIGN KEY (pid) REFERENCES parent (id));\n",
			want:    "pid(pid) | child_ibfk_1",
		},
		{
			name:    "another index stays under the key, control",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, KEY x (pid), KEY y (pid), CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: keeps + "CREATE INDEX y ON child (pid);\n",
			want:    "y(pid) | a_fk",
		},
		{
			name:    "the removed index is on another column, control",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, KEY other (qid), CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: keeps,
			want:    "a_fk(pid) | a_fk",
		},
	}
	for _, engine := range removedForeignKeyEngines {
		for _, test := range tests {
			t.Run(engine.name+" "+test.name, func(t *testing.T) {
				c := qt.New(t)
				_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, keptKeyParent+test.built)
				desired := keptKeyParent + test.desired

				run := removedForeignKeyApply(c, target, desired)

				c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
				c.Assert(keptKeyNumbered(columnKeyString(c, target, keptKeyRead)), qt.Equals, test.want)
				again := removedForeignKeyApply(c, target, desired)
				c.Assert(again.Stdout, qt.Contains, "Schema is synced")
			})
		}
	}
}

// keptKeyNumbered spells the name MariaDB 12.1 and later give an unnamed key as the
// name MySQL gives it, so one row reads both.
func keptKeyNumbered(read string) string {
	return strings.Replace(read, "| 1", "| child_ibfk_1", 1)
}

// TestMigrationsKeptForeignKeyIndexE2E_TheRollbackRestoresTheIndexes runs the plan through
// a migration and rolls it back. The kept key stands on an index throughout.
func TestMigrationsKeptForeignKeyIndexE2E_TheRollbackRestoresTheIndexes(t *testing.T) {
	const built = keptKeyParent + "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, KEY cover (pid), CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id));\n"
	for _, engine := range removedForeignKeyEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, built)
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(keptKeyParent+"CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT a_fk FOREIGN KEY (pid) REFERENCES parent (id));\n"), 0o600), qt.IsNil)

			generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "fk")
			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(columnKeyString(c, target, keptKeyRead), qt.Equals, "a_fk(pid) | a_fk")

			down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

			c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
			c.Assert(columnKeyString(c, target, keptKeyRead), qt.Equals, "cover(pid) | a_fk")
		})
	}
}
