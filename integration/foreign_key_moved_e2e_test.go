//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// foreignKeyMovedRead prints the indexes of child with their columns and its foreign keys
// with theirs, as the server does.
const foreignKeyMovedRead = "SELECT concat(" +
	"coalesce((SELECT group_concat(concat(index_name, '(', cols, ')') ORDER BY index_name SEPARATOR ',') FROM " +
	"(SELECT index_name, group_concat(column_name ORDER BY seq_in_index) cols FROM information_schema.statistics " +
	"WHERE table_schema = database() AND table_name = 'child' AND index_name <> 'PRIMARY' GROUP BY index_name) i), ''), ' | ', " +
	"coalesce((SELECT group_concat(concat(constraint_name, '(', column_name, ')') ORDER BY constraint_name, ordinal_position SEPARATOR ',') " +
	"FROM information_schema.key_column_usage WHERE table_schema = database() AND table_name = 'child' AND referenced_table_name IS NOT NULL), ''))"

// TestSchemaApplyForeignKeyMovedE2E_HappyPath moves a foreign key to another column under
// its own name, and gives a new key the name of an index that leaves. The plan added the
// key while the old index held the name, and the server stopped it at Error 1061. Each
// row reads the indexes and the keys back, and a second run finds nothing to do.
func TestSchemaApplyForeignKeyMovedE2E_HappyPath(t *testing.T) {
	tests := []struct{ name, built, desired, want string }{
		{
			name:    "the key moves under its name",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id));\n",
			want:    "c_fk(qid) | c_fk(qid)",
		},
		{
			name:    "the old column leaves as well",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, qid bigint, CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id));\n",
			want:    "c_fk(qid) | c_fk(qid)",
		},
		{
			name:    "a new key takes the name of an index that leaves",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, KEY c_fk (pid));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id));\n",
			want:    "c_fk(qid) | c_fk(qid)",
		},
		{
			name:    "the key moves to another name, control",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT d_fk FOREIGN KEY (qid) REFERENCES parent (id));\n",
			want:    "d_fk(qid) | d_fk(qid)",
		},
		{
			name:    "the key keeps its column, control",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (code));\n",
			want:    "c_fk(pid) | c_fk(pid)",
		},
	}
	for _, engine := range removedForeignKeyEngines {
		for _, test := range tests {
			t.Run(engine.name+" "+test.name, func(t *testing.T) {
				c := qt.New(t)
				_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, removedForeignKeyParent+test.built)
				desired := removedForeignKeyParent + test.desired

				run := removedForeignKeyApply(c, target, desired)

				c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
				c.Assert(columnKeyString(c, target, foreignKeyMovedRead), qt.Equals, test.want)
				again := removedForeignKeyApply(c, target, desired)
				c.Assert(again.Stdout, qt.Contains, "Schema is synced")
			})
		}
	}
}

// TestMigrationsForeignKeyMovedE2E_TheRollbackMovesTheKeyBack rolls the move back: the
// rollback meets the same name, on the other column.
func TestMigrationsForeignKeyMovedE2E_TheRollbackMovesTheKeyBack(t *testing.T) {
	const built = removedForeignKeyParent + "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id));\n"
	for _, engine := range removedForeignKeyEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, built)
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(removedForeignKeyParent+"CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id));\n"), 0o600), qt.IsNil)

			generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "fk")
			c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
			up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
			c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
			c.Assert(columnKeyString(c, target, foreignKeyMovedRead), qt.Equals, "c_fk(qid) | c_fk(qid)")

			down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

			c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
			c.Assert(columnKeyString(c, target, foreignKeyMovedRead), qt.Equals, "c_fk(pid) | c_fk(pid)")
		})
	}
}
