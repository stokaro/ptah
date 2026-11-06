//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// columnsAndKeysRead prints the columns of table t and every index and constraint of the
// database other than a primary key, as the server holds them.
const columnsAndKeysRead = "SELECT concat(" +
	"(SELECT group_concat(column_name ORDER BY column_name) FROM information_schema.columns " +
	"WHERE table_schema = database() AND table_name = 't'), ' | ', " +
	"coalesce((SELECT group_concat(n ORDER BY n) FROM (" +
	"SELECT DISTINCT concat(table_name, '.', index_name) n FROM information_schema.statistics " +
	"WHERE table_schema = database() AND index_name <> 'PRIMARY' " +
	"UNION SELECT concat(table_name, '.', constraint_name) FROM information_schema.table_constraints " +
	"WHERE table_schema = database() AND constraint_type <> 'PRIMARY KEY') k), ''))"

// TestMigrationsKeysAheadOfColumnDropsE2E runs a plan in which a table loses a column and
// the keys and indexes over it, in each direction, and reads them back. Written after the
// column, the key's own drop answers Error 1091 on MySQL, because the key has left with
// the column. A second run finds nothing to do.
func TestMigrationsKeysAheadOfColumnDropsE2E(t *testing.T) {
	tests := []struct{ name, built, desired, afterUp, afterDown string }{
		{
			name:      "forward, an index over the column",
			built:     "CREATE TABLE t (id int PRIMARY KEY, a int, b int, KEY i (b));\n",
			desired:   "CREATE TABLE t (id int PRIMARY KEY, a int);\n",
			afterUp:   "a,id | ",
			afterDown: "a,b,id | t.i",
		},
		{
			name:      "forward, a UNIQUE of its own name over the column",
			built:     "CREATE TABLE t (id int PRIMARY KEY, a int, b int, CONSTRAINT uq_b UNIQUE (b));\n",
			desired:   "CREATE TABLE t (id int PRIMARY KEY, a int);\n",
			afterUp:   "a,id | ",
			afterDown: "a,b,id | t.uq_b",
		},
		{
			name:      "forward, a CHECK over the column",
			built:     "CREATE TABLE t (id int PRIMARY KEY, a int, b int, CONSTRAINT k CHECK (b > 0));\n",
			desired:   "CREATE TABLE t (id int PRIMARY KEY, a int);\n",
			afterUp:   "a,id | ",
			afterDown: "a,b,id | t.k",
		},
		{
			name:      "forward, a UNIQUE over the column and another, with rows it would collide on",
			built:     "CREATE TABLE t (id int PRIMARY KEY, a int, b int, CONSTRAINT u UNIQUE (a, b));\nINSERT INTO t VALUES (1, 1, 1), (2, 1, 2);\n",
			desired:   "CREATE TABLE t (id int PRIMARY KEY, a int);\n",
			afterUp:   "a,id | ",
			afterDown: "a,b,id | t.u",
		},
		{
			name:  "rollback, an index and a foreign key over a new column",
			built: "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE t (id int PRIMARY KEY);\n",
			desired: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				"CREATE TABLE t (id int PRIMARY KEY, p_id int, KEY t_p_idx (p_id), CONSTRAINT t_p_fk FOREIGN KEY (p_id) REFERENCES p (id));\n",
			afterUp:   "id,p_id | t.t_p_fk,t.t_p_idx",
			afterDown: "id | ",
		},
		{
			name: "rollback, a foreign key moved back from a column that leaves",
			built: "CREATE TABLE t (id int PRIMARY KEY);\n" +
				"CREATE TABLE a (id int PRIMARY KEY, t_ref int, KEY a_t_ref (t_ref), CONSTRAINT a_t_fk FOREIGN KEY (t_ref) REFERENCES t (id));\n",
			desired: "CREATE TABLE t (id int PRIMARY KEY, code int, CONSTRAINT t_code_key UNIQUE (code));\n" +
				"CREATE TABLE a (id int PRIMARY KEY, t_ref int, KEY a_t_ref (t_ref), CONSTRAINT a_t_fk FOREIGN KEY (t_ref) REFERENCES t (code));\n",
			afterUp:   "code,id | a.a_t_fk,a.a_t_ref,t.t_code_key",
			afterDown: "id | a.a_t_fk,a.a_t_ref",
		},
		{
			name: "both ways, every kind of change on one table",
			built: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				"CREATE TABLE t (id int PRIMARY KEY, a int, b int, c int, CONSTRAINT ck_a CHECK (a > 0), CONSTRAINT uq_b UNIQUE (b), KEY t_c_idx (c));\n",
			desired: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				"CREATE TABLE t (id int PRIMARY KEY, a bigint, c int, e int, f int, g int, CONSTRAINT ck_e CHECK (e > 0), CONSTRAINT uq_f UNIQUE (f), " +
				"KEY t_g_idx (g), CONSTRAINT t_g_fk FOREIGN KEY (g) REFERENCES p (id), KEY t_c_idx2 (c, e));\n",
			afterUp:   "a,c,e,f,g,id | t.ck_e,t.t_c_idx2,t.t_g_fk,t.t_g_idx,t.uq_f",
			afterDown: "a,b,c,id | t.ck_a,t.t_c_idx,t.uq_b",
		},
		{
			name:      "a kept index stays, control",
			built:     "CREATE TABLE t (id int PRIMARY KEY, a int, b int, KEY i (a));\n",
			desired:   "CREATE TABLE t (id int PRIMARY KEY, a int, KEY i (a));\n",
			afterUp:   "a,id | t.i",
			afterDown: "a,b,id | t.i",
		},
	}
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{"mysql", dbtarget.MySQLAdmin},
		{"mariadb", dbtarget.MariaDBAdmin},
	}
	for _, test := range tests {
		for _, engine := range engines {
			t.Run(test.name+" on "+engine.name, func(t *testing.T) {
				c := qt.New(t)
				_, target := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, test.built)
				dir := filepath.Join(c.TempDir(), "migrations")
				c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
				schema := filepath.Join(c.TempDir(), "schema.sql")
				c.Assert(os.WriteFile(schema, []byte(test.desired), 0o600), qt.IsNil)

				generated := runPtahIn(c, "migrations", "generate", "--db-url", target, "--schema-file", schema, "--migrations-dir", dir, "--name", "keys")
				c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", generated.Stdout, generated.Stderr))
				up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")
				c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
				c.Assert(columnKeyString(c, target, columnsAndKeysRead), qt.Equals, test.afterUp)
				again := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
				c.Assert(again.Stdout, qt.Contains, "Schema is synced")

				down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

				c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
				c.Assert(columnKeyString(c, target, columnsAndKeysRead), qt.Equals, test.afterDown)
			})
		}
	}
}
