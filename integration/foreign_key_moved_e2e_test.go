//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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
			name:    "another column of the table leaves as well",
			built:   "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, other bigint, CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id));\n",
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

// TestMigrationsForeignKeyMovedE2E_AKeptKeyStandsOnTheOldIndex moves a foreign key under its
// name, or gives a new key the name of an index, while another key the plan keeps stands on
// that index. The old index cannot leave alone, as the server answers Error 1553, and it
// holds the name the moved key's new index takes, so it has to leave ahead of the key's
// addition and in the statement that gives the kept key an index of its own. The rollback
// rebuilds the index with the key gone. Each row holds a row of data through both.
func TestMigrationsForeignKeyMovedE2E_AKeptKeyStandsOnTheOldIndex(t *testing.T) {
	const (
		rows  = "INSERT INTO parent VALUES (1, 1), (2, 2);\nINSERT INTO child (id, pid, qid) VALUES (1, 1, 1);\n"
		old   = "CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id)"
		moved = "CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id)"
		kept  = "CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code)"
		table = "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, "
		after = table + moved + ", " + kept + ");\n"
		ended = "c_fk(qid),d_fk(pid) | c_fk(qid),d_fk(pid)"
		// keyMovedParent also holds the pair a key over two columns references.
		keyMovedParent = "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL UNIQUE, UNIQUE KEY pair (id, code));\n"
		back           = "c_fk(pid) | c_fk(pid),d_fk(pid)"
	)
	tests := []struct{ name, built, desired, afterUp, afterDown string }{
		{
			name:      "the shared index holds the moved key's name",
			built:     table + kept + ", " + old + ");\n",
			desired:   after,
			afterUp:   ended,
			afterDown: back,
		},
		{
			name:      "the shared index holds the kept key's name",
			built:     table + old + ", " + kept + ");\n",
			desired:   after,
			afterUp:   ended,
			afterDown: back,
		},
		{
			name:      "the author's index holds the moved key's name",
			built:     table + "KEY c_fk (pid), " + old + ", " + kept + ");\n",
			desired:   after,
			afterUp:   ended,
			afterDown: back,
		},
		{
			name:      "a new key takes the name of the index the kept key stands on",
			built:     table + "KEY c_fk (pid), " + kept + ");\n",
			desired:   after,
			afterUp:   ended,
			afterDown: "c_fk(pid) | d_fk(pid)",
		},
		{
			name:      "another column of the table leaves as well",
			built:     "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, other bigint, " + kept + ", " + old + ");\n",
			desired:   after,
			afterUp:   ended,
			afterDown: back,
		},
		{
			name:      "a removed key stands on the index too",
			built:     table + "KEY c_fk (pid, qid), CONSTRAINT b_fk FOREIGN KEY (pid, qid) REFERENCES parent (id, code), " + old + ", " + kept + ");\n",
			desired:   after,
			afterUp:   ended,
			afterDown: "c_fk(pid,qid) | b_fk(pid),b_fk(qid),c_fk(pid),d_fk(pid)",
		},
		{
			name:      "the kept key has an index that stays, control",
			built:     table + "KEY z_cover (pid), " + old + ", " + kept + ");\n",
			desired:   table + "KEY z_cover (pid), " + moved + ", " + kept + ");\n",
			afterUp:   "c_fk(qid),z_cover(pid) | c_fk(qid),d_fk(pid)",
			afterDown: "z_cover(pid) | c_fk(pid),d_fk(pid)",
		},
	}
	for _, engine := range removedForeignKeyEngines {
		for _, test := range tests {
			t.Run(engine.name+" "+test.name, func(t *testing.T) {
				c := qt.New(t)
				_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, keyMovedParent+test.built+rows)
				dir := filepath.Join(c.TempDir(), "migrations")
				c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
				schema := filepath.Join(c.TempDir(), "schema.sql")
				c.Assert(os.WriteFile(schema, []byte(keyMovedParent+test.desired), 0o600), qt.IsNil)

				generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "fk")
				c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
				up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
				c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
				c.Assert(columnKeyString(c, target, foreignKeyMovedRead), qt.Equals, test.afterUp)
				c.Assert(readInt(c, target, "SELECT count(*) FROM child"), qt.Equals, 1)
				again := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
				c.Assert(again.Stdout, qt.Contains, "Schema is synced")

				down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

				c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
				c.Assert(columnKeyString(c, target, foreignKeyMovedRead), qt.Equals, test.afterDown)
				c.Assert(readInt(c, target, "SELECT count(*) FROM child"), qt.Equals, 1)
			})
		}
	}
}

// TestSchemaApplyForeignKeyMovedE2E_AKeptKeyStandsOnTheOldIndex runs the same plan through
// schema apply, which writes no rollback and asks no simulation of the plan.
func TestSchemaApplyForeignKeyMovedE2E_AKeptKeyStandsOnTheOldIndex(t *testing.T) {
	const desired = removedForeignKeyParent + "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, " +
		"CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id), CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code));\n"
	const built = removedForeignKeyParent + "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, " +
		"CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code), CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id));\n"
	for _, engine := range removedForeignKeyEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, built)

			run := removedForeignKeyApply(c, target, desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, foreignKeyMovedRead), qt.Equals, "c_fk(qid),d_fk(pid) | c_fk(qid),d_fk(pid)")
			again := removedForeignKeyApply(c, target, desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestCompatMigrateDiffForeignKeyMovedE2E_AKeptKeyStandsOnTheOldIndex writes the plan and its
// rollback through the compatibility surface, whose statements come in the community
// binary's order, and runs the files on the server. The statement that frees the name has
// to stay ahead of the foreign key that takes it.
func TestCompatMigrateDiffForeignKeyMovedE2E_AKeptKeyStandsOnTheOldIndex(t *testing.T) {
	const desired = removedForeignKeyParent + "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, " +
		"CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id), CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code));\n"
	const built = removedForeignKeyParent + "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, KEY c_fk (pid), " +
		"CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code));\n"
	for _, engine := range removedForeignKeyEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			_, dev := scratch.database(c, "fk_dev")
			root := c.TempDir()
			dir := filepath.Join(root, "migrations")
			c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
			diff := func(name, schema string) []byte {
				c.Helper()
				file := filepath.Join(root, name+".sql")
				c.Assert(os.WriteFile(file, []byte(schema), 0o600), qt.IsNil)
				stdout, stderr, err := runCompat(c.Context(), "migrate", "diff", name, "--dir", "file://"+filepath.ToSlash(dir)+"?format=golang-migrate",
					"--to", "file://"+filepath.ToSlash(file), "--dev-url", dev)
				c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
				ups, err := filepath.Glob(filepath.Join(dir, "*_"+name+".up.sql"))
				c.Assert(err, qt.IsNil)
				c.Assert(ups, qt.HasLen, 1)
				up, err := os.ReadFile(ups[0])
				c.Assert(err, qt.IsNil)
				return up
			}
			first := diff("first", built)
			time.Sleep(time.Second) // the version is a timestamp in seconds
			second := diff("second", desired)
			downs, err := filepath.Glob(filepath.Join(dir, "*_second.down.sql"))
			c.Assert(err, qt.IsNil)
			c.Assert(downs, qt.HasLen, 1)
			down, err := os.ReadFile(downs[0])
			c.Assert(err, qt.IsNil)

			_, applied := scratch.builtFrom(c, string(first)+string(second))
			_, rolledBack := scratch.builtFrom(c, string(first)+string(second)+string(down))

			c.Assert(columnKeyString(c, applied, foreignKeyMovedRead), qt.Equals, "c_fk(qid),d_fk(pid) | c_fk(qid),d_fk(pid)")
			c.Assert(columnKeyString(c, rolledBack, foreignKeyMovedRead), qt.Equals, "c_fk(pid) | d_fk(pid)")
		})
	}
}

// TestMigrationsForeignKeyMovedE2E_TwoKeysTakeTheNamesOfTheIndexesAKeptKeyStandsOn removes
// both indexes a kept key stands on, each under the name of a foreign key the plan moves
// or adds. One statement gives the kept key an index of its own and drops both, and the
// plan wrote it again at the second key, which the server stopped with Error 1091. Each
// row runs the plan through schema apply, and through a migration and its rollback.
func TestMigrationsForeignKeyMovedE2E_TwoKeysTakeTheNamesOfTheIndexesAKeptKeyStandsOn(t *testing.T) {
	const (
		parent  = "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL UNIQUE, alt bigint NOT NULL UNIQUE);\n"
		indexes = "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, rid bigint, KEY x_fk (pid), KEY y_fk (pid), " +
			"CONSTRAINT k_fk FOREIGN KEY (pid) REFERENCES parent (alt)"
		desired = parent + "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, rid bigint, " +
			"CONSTRAINT k_fk FOREIGN KEY (pid) REFERENCES parent (alt), CONSTRAINT x_fk FOREIGN KEY (qid) REFERENCES parent (id), " +
			"CONSTRAINT y_fk FOREIGN KEY (rid) REFERENCES parent (code));\n"
		rows  = "INSERT INTO parent VALUES (1, 1, 1);\nINSERT INTO child (id, pid) VALUES (1, 1);\n"
		ended = "k_fk(pid),x_fk(qid),y_fk(rid) | k_fk(pid),x_fk(qid),y_fk(rid)"
	)
	tests := []struct{ name, built, afterDown string }{
		{
			name:      "two keys move under their names",
			built:     indexes + ", CONSTRAINT x_fk FOREIGN KEY (pid) REFERENCES parent (id), CONSTRAINT y_fk FOREIGN KEY (pid) REFERENCES parent (code));\n",
			afterDown: "x_fk(pid),y_fk(pid) | k_fk(pid),x_fk(pid),y_fk(pid)",
		},
		{
			name:      "two new keys take the names",
			built:     indexes + ");\n",
			afterDown: "x_fk(pid),y_fk(pid) | k_fk(pid)",
		},
	}
	for _, engine := range removedForeignKeyEngines {
		for _, test := range tests {
			t.Run(engine.name+" "+test.name+" through schema apply", func(t *testing.T) {
				c := qt.New(t)
				_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, parent+test.built+rows)

				run := removedForeignKeyApply(c, target, desired)

				c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
				c.Assert(columnKeyString(c, target, foreignKeyMovedRead), qt.Equals, ended)
				again := removedForeignKeyApply(c, target, desired)
				c.Assert(again.Stdout, qt.Contains, "Schema is synced")
			})
			t.Run(engine.name+" "+test.name+" through a migration", func(t *testing.T) {
				c := qt.New(t)
				_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, parent+test.built+rows)
				dir := filepath.Join(c.TempDir(), "migrations")
				c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
				schema := filepath.Join(c.TempDir(), "schema.sql")
				c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)

				generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "fk")
				c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
				up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
				c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
				c.Assert(columnKeyString(c, target, foreignKeyMovedRead), qt.Equals, ended)

				down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

				c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
				c.Assert(columnKeyString(c, target, foreignKeyMovedRead), qt.Equals, test.afterDown)
				c.Assert(readInt(c, target, "SELECT count(*) FROM child"), qt.Equals, 1)
			})
		}
	}
}
