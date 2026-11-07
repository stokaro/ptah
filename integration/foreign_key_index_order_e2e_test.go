//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

const (
	// indexOrderParent is the table the foreign keys reference, with a key over two
	// columns.
	indexOrderParent = "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL UNIQUE, alt bigint NOT NULL UNIQUE, UNIQUE KEY pair (id, code));\n"
	// indexOrderChild opens the table that holds the foreign keys.
	indexOrderChild = "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, rid bigint"
	// indexOrderRows is one row in each table, which every plan has to keep.
	indexOrderRows = "INSERT INTO parent VALUES (1, 1, 1);\nINSERT INTO child VALUES (1, 1, 1, 1);\n"
	// indexOrderRead prints the indexes of child with their columns, a unique one
	// marked, and its foreign keys with theirs, as the server does.
	indexOrderRead = "SELECT concat(" +
		"coalesce((SELECT group_concat(concat(index_name, '(', cols, ')', IF(non_unique = 0, '!', '')) ORDER BY index_name SEPARATOR ',') FROM " +
		"(SELECT index_name, non_unique, group_concat(column_name ORDER BY seq_in_index) cols FROM information_schema.statistics " +
		"WHERE table_schema = database() AND table_name = 'child' AND index_name <> 'PRIMARY' GROUP BY index_name, non_unique) i), ''), ' | ', " +
		"coalesce((SELECT group_concat(concat(constraint_name, '(', column_name, ')') ORDER BY constraint_name, ordinal_position SEPARATOR ',') " +
		"FROM information_schema.key_column_usage WHERE table_schema = database() AND table_name = 'child' AND referenced_table_name IS NOT NULL), ''))"
)

// indexOrderRow is one plan over child: the table as it is built, as it is declared,
// and as the plan leaves it, read with indexOrderRead.
type indexOrderRow struct{ name, built, desired, want string }

// appliedOnce runs schema apply of row's declaration over a database built from row,
// reads child back, and runs the plan again, which finds nothing to do.
func appliedOnce(c *qt.C, engine dbtarget.Engine, row indexOrderRow) string {
	c.Helper()
	_, target := newMySQLFamilyScratch(c, engine).builtFrom(c, indexOrderParent+row.built+indexOrderRows)
	desired := indexOrderParent + row.desired
	run := removedForeignKeyApply(c, target, desired)
	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	applied := columnKeyString(c, target, indexOrderRead)
	again := removedForeignKeyApply(c, target, desired)
	c.Assert(again.Stdout, qt.Contains, "Schema is synced")
	return applied
}

// migratedAndBack runs row's declaration over a database built from row through a
// migration and its rollback. It answers child as the build left it, as the migration
// left it, and as the rollback left it, and holds the row of data through both.
func migratedAndBack(c *qt.C, engine dbtarget.Engine, row indexOrderRow) (built, up, down string) {
	c.Helper()
	_, target := newMySQLFamilyScratch(c, engine).builtFrom(c, indexOrderParent+row.built+indexOrderRows)
	built = columnKeyString(c, target, indexOrderRead)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(indexOrderParent+row.desired), 0o600), qt.IsNil)
	generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "fk")
	c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
	migrated := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
	c.Assert(migrated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", migrated.Stdout, migrated.Stderr))
	up = columnKeyString(c, target, indexOrderRead)
	c.Assert(readInt(c, target, "SELECT count(*) FROM child"), qt.Equals, 1)
	rolledBack := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")
	c.Assert(rolledBack.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", rolledBack.Stdout, rolledBack.Stderr))
	down = columnKeyString(c, target, indexOrderRead)
	c.Assert(readInt(c, target, "SELECT count(*) FROM child"), qt.Equals, 1)
	return built, up, down
}

// runIndexOrderRows runs each row through schema apply, and through a migration and
// its rollback, on MySQL and MariaDB.
func runIndexOrderRows(t *testing.T, rows []indexOrderRow) {
	t.Helper()
	for _, engine := range removedForeignKeyEngines {
		for _, row := range rows {
			t.Run(engine.name+" "+row.name+" through schema apply", func(t *testing.T) {
				c := qt.New(t)

				applied := appliedOnce(c, engine.engine, row)

				c.Assert(applied, qt.Equals, row.want)
			})
			t.Run(engine.name+" "+row.name+" through a migration", func(t *testing.T) {
				c := qt.New(t)

				built, up, down := migratedAndBack(c, engine.engine, row)

				c.Assert(up, qt.Equals, row.want)
				c.Assert(down, qt.Equals, built)
			})
		}
	}
}

// TestForeignKeyIndexOrderE2E_AnIndexThePlanAddsTakesOverARemovedKeysIndex removes a
// foreign key and adds an index over its columns. The server drops the index it built
// for the key once the new index is there, also after DROP FOREIGN KEY, and the plan's
// DROP INDEX after the addition stopped at Error 1091. The rollback puts the key back.
func TestForeignKeyIndexOrderE2E_AnIndexThePlanAddsTakesOverARemovedKeysIndex(t *testing.T) {
	const key = ", CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id)"
	runIndexOrderRows(t, []indexOrderRow{
		{
			name:    "an index over the key's column",
			built:   indexOrderChild + key + ");\n",
			desired: indexOrderChild + ", KEY idx_pid (pid));\n",
			want:    "idx_pid(pid) | ",
		},
		{
			name:    "a wider index",
			built:   indexOrderChild + key + ");\n",
			desired: indexOrderChild + ", KEY w (pid, qid));\n",
			want:    "w(pid,qid) | ",
		},
		{
			name:    "a descending index",
			built:   indexOrderChild + key + ");\n",
			desired: indexOrderChild + ", KEY idx_pid (pid DESC));\n",
			want:    "idx_pid(pid) | ",
		},
		{
			name:    "two keys on one index",
			built:   indexOrderChild + key + ", CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code));\n",
			desired: indexOrderChild + ", KEY idx_pid (pid));\n",
			want:    "idx_pid(pid) | ",
		},
		{
			name:    "the key moves to another column under its name",
			built:   indexOrderChild + key + ");\n",
			desired: indexOrderChild + ", KEY idx_pid (pid), CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id));\n",
			want:    "c_fk(qid),idx_pid(pid) | c_fk(qid)",
		},
		{
			name:    "the author's index under the key's name, control",
			built:   indexOrderChild + ", KEY c_fk (pid)" + key + ");\n",
			desired: indexOrderChild + ", KEY idx_pid (pid));\n",
			want:    "idx_pid(pid) | ",
		},
	})
}

// TestForeignKeyIndexOrderE2E_TheRollbackOfAnIndexAKeptKeyComesToStandOn adds an index
// over a foreign key the plan keeps. The server dropped the index it built for the key,
// the key stood on the new index alone, and the rollback's DROP INDEX stopped at Error
// 1553; an index the author had named that way stayed, and only the next run dropped it.
// The plan drops the old index in the statement that adds the new one, and the rollback
// puts it back first. Two keys moved onto one column end on one index under the second
// key's name, which the rollback met with Error 1061.
func TestForeignKeyIndexOrderE2E_TheRollbackOfAnIndexAKeptKeyComesToStandOn(t *testing.T) {
	const kept = ", CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id)"
	runIndexOrderRows(t, []indexOrderRow{
		{
			name:    "an index over the key's column",
			built:   indexOrderChild + kept + ");\n",
			desired: indexOrderChild + ", KEY idx_pid (pid)" + kept + ");\n",
			want:    "idx_pid(pid) | c_fk(pid)",
		},
		{
			name:    "a wider index",
			built:   indexOrderChild + kept + ");\n",
			desired: indexOrderChild + ", KEY w (pid, qid)" + kept + ");\n",
			want:    "w(pid,qid) | c_fk(pid)",
		},
		{
			name:    "a removed key shared the index",
			built:   indexOrderChild + ", CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code)" + kept + ");\n",
			desired: indexOrderChild + ", KEY idx_pid (pid)" + kept + ");\n",
			want:    "idx_pid(pid) | c_fk(pid)",
		},
		{
			name:    "the author's index under the key's name",
			built:   indexOrderChild + ", KEY c_fk (pid)" + kept + ");\n",
			desired: indexOrderChild + ", KEY idx_pid (pid)" + kept + ");\n",
			want:    "idx_pid(pid) | c_fk(pid)",
		},
		{
			name:    "two keys move onto one column under their names",
			built:   indexOrderChild + kept + ", CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code));\n",
			desired: indexOrderChild + ", CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id), CONSTRAINT d_fk FOREIGN KEY (qid) REFERENCES parent (code));\n",
			want:    "d_fk(qid) | c_fk(qid),d_fk(qid)",
		},
	})
}

// TestForeignKeyIndexOrderE2E_AnotherChangedKeyStandsOnAMovedKeysIndex moves a foreign
// key under its name while another key the plan moves or removes stands on its old index.
// The old index left between the moved key's drop and its addition, and the server
// stopped the drop at Error 1553.
func TestForeignKeyIndexOrderE2E_AnotherChangedKeyStandsOnAMovedKeysIndex(t *testing.T) {
	runIndexOrderRows(t, []indexOrderRow{
		{
			name: "both keys move under their names",
			built: indexOrderChild + ", CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code), " +
				"CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: indexOrderChild + ", CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id), CONSTRAINT d_fk FOREIGN KEY (rid) REFERENCES parent (code));\n",
			want:    "c_fk(qid),d_fk(rid) | c_fk(qid),d_fk(rid)",
		},
		{
			name: "a removed key stands on it too",
			built: indexOrderChild + ", CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id), " +
				"CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code));\n",
			desired: indexOrderChild + ", CONSTRAINT d_fk FOREIGN KEY (qid) REFERENCES parent (code));\n",
			want:    "d_fk(qid) | d_fk(qid)",
		},
		{
			name: "two keys move and a third leaves",
			built: indexOrderChild + ", CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code), " +
				"CONSTRAINT e_fk FOREIGN KEY (pid) REFERENCES parent (alt), CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id));\n",
			desired: indexOrderChild + ", CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id), CONSTRAINT d_fk FOREIGN KEY (rid) REFERENCES parent (code));\n",
			want:    "c_fk(qid),d_fk(rid) | c_fk(qid),d_fk(rid)",
		},
	})
}

// TestForeignKeyIndexOrderE2E_AColumnTakesTheNameOfTheIndexAKeyStandsOn gives a column its
// own UNIQUE while the only index a foreign key stands on holds the name that UNIQUE
// takes. The plan dropped the index ahead of the column change, and the server stopped it
// at Error 1553.
func TestForeignKeyIndexOrderE2E_AColumnTakesTheNameOfTheIndexAKeyStandsOn(t *testing.T) {
	const (
		unique = "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint UNIQUE, qid bigint, rid bigint"
		k      = ", CONSTRAINT k_fk FOREIGN KEY (pid) REFERENCES parent (alt)"
		j      = ", CONSTRAINT j_fk FOREIGN KEY (pid) REFERENCES parent (id)"
		pair   = ", CONSTRAINT k_fk FOREIGN KEY (pid, qid) REFERENCES parent (id, code)"
	)
	runIndexOrderRows(t, []indexOrderRow{
		{
			name:    "one kept key",
			built:   indexOrderChild + ", KEY pid (pid)" + k + ");\n",
			desired: unique + k + ");\n",
			want:    "pid(pid)! | k_fk(pid)",
		},
		{
			name:    "two kept keys",
			built:   indexOrderChild + ", KEY pid (pid)" + j + k + ");\n",
			desired: unique + j + k + ");\n",
			want:    "pid(pid)! | j_fk(pid),k_fk(pid)",
		},
		{
			name:    "a key the plan removes",
			built:   indexOrderChild + ", KEY pid (pid)" + k + ");\n",
			desired: unique + ");\n",
			want:    "pid(pid)! | ",
		},
		{
			name:    "a kept key over two columns",
			built:   indexOrderChild + ", KEY pid (pid, qid)" + pair + ");\n",
			desired: unique + pair + ");\n",
			want:    "k_fk(pid,qid),pid(pid)! | k_fk(pid),k_fk(qid)",
		},
	})
}

// TestForeignKeyIndexOrderE2E_AKeyThePlanAddsTakesOverABuiltIndex adds a UNIQUE, a
// column's own UNIQUE or a primary key over the column of a foreign key whose index the
// server built. The server drops that index once the key is in, also after DROP FOREIGN
// KEY. The plan's DROP INDEX after the addition stopped at Error 1091 where the foreign
// key left, and the rollback's removal of the key stopped at Error 1553 where it stayed.
// A second addition over the column made the server drop the index too, and the
// statement that took it over then stopped at Error 1091. On a table that loses a column,
// a removed index can leave ahead of the column changes; one a key takes over waits for
// the key's statement, since the kept key stands on it until then.
func TestForeignKeyIndexOrderE2E_AKeyThePlanAddsTakesOverABuiltIndex(t *testing.T) {
	const (
		key     = ", CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id)"
		unique  = "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint UNIQUE, qid bigint, rid bigint"
		keyless = "CREATE TABLE child (id bigint NOT NULL, pid bigint NOT NULL, qid bigint, rid bigint"
	)
	runIndexOrderRows(t, []indexOrderRow{
		{
			name:    "a UNIQUE, the key removed",
			built:   indexOrderChild + key + ");\n",
			desired: indexOrderChild + ", CONSTRAINT u UNIQUE (pid));\n",
			want:    "u(pid)! | ",
		},
		{
			name:    "the column's own UNIQUE, the key removed",
			built:   indexOrderChild + key + ");\n",
			desired: unique + ");\n",
			want:    "pid(pid)! | ",
		},
		{
			name:    "a UNIQUE, the key kept",
			built:   indexOrderChild + key + ");\n",
			desired: indexOrderChild + ", CONSTRAINT u UNIQUE (pid)" + key + ");\n",
			want:    "u(pid)! | c_fk(pid)",
		},
		{
			name:    "the column's own UNIQUE, the key kept",
			built:   indexOrderChild + key + ");\n",
			desired: unique + key + ");\n",
			want:    "pid(pid)! | c_fk(pid)",
		},
		{
			name:    "a UNIQUE, the key kept, on a table that loses a column",
			built:   indexOrderChild + key + ");\n",
			desired: "CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint, CONSTRAINT u UNIQUE (pid)" + key + ");\n",
			want:    "u(pid)! | c_fk(pid)",
		},
		{
			name:    "a wider UNIQUE, the key kept",
			built:   indexOrderChild + key + ");\n",
			desired: indexOrderChild + ", CONSTRAINT u UNIQUE (pid, qid)" + key + ");\n",
			want:    "u(pid,qid)! | c_fk(pid)",
		},
		{
			name:    "the author's index under the key's name",
			built:   indexOrderChild + ", KEY c_fk (pid)" + key + ");\n",
			desired: indexOrderChild + ", CONSTRAINT u UNIQUE (pid)" + key + ");\n",
			want:    "u(pid)! | c_fk(pid)",
		},
		{
			name:    "a primary key, the key kept",
			built:   keyless + key + ");\n",
			desired: keyless + ", PRIMARY KEY (pid, id)" + key + ");\n",
			want:    " | c_fk(pid)",
		},
		{
			name:    "the key moves to another column under its name",
			built:   indexOrderChild + key + ");\n",
			desired: indexOrderChild + ", CONSTRAINT u UNIQUE (pid), CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id));\n",
			want:    "c_fk(qid),u(pid)! | c_fk(qid)",
		},
		{
			name:    "a UNIQUE and an index, the key kept",
			built:   indexOrderChild + key + ");\n",
			desired: indexOrderChild + ", CONSTRAINT u UNIQUE (pid), KEY w (pid, qid)" + key + ");\n",
			want:    "u(pid)!,w(pid,qid) | c_fk(pid)",
		},
		{
			name:    "two indexes, the key kept",
			built:   indexOrderChild + key + ");\n",
			desired: indexOrderChild + ", KEY a (pid), KEY w (pid, qid)" + key + ");\n",
			want:    "a(pid),w(pid,qid) | c_fk(pid)",
		},
	})
}
