//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// keysHeldRead prints every foreign key and UNIQUE key of the database as the server
// holds it, by table.
const keysHeldRead = "SELECT coalesce(group_concat(concat(table_name, '.', constraint_name) ORDER BY table_name, constraint_name), '') " +
	"FROM information_schema.table_constraints " +
	"WHERE table_schema = database() AND constraint_type IN ('FOREIGN KEY', 'UNIQUE')"

// TestMigrationsRemovedForeignKeyAheadOfItsKeyE2E runs a removal of a foreign key and of
// the UNIQUE key it stands on, in each direction, and reads the keys back. With the key
// ahead of the foreign key the server stops the plan with Error 1553, because the diff
// lists removals by table. A second run finds nothing to do.
func TestMigrationsRemovedForeignKeyAheadOfItsKeyE2E(t *testing.T) {
	tests := []struct{ name, built, desired, afterUp, afterDown string }{
		{
			name: "the plan removes both, the key on the earlier table",
			built: "CREATE TABLE a (id int PRIMARY KEY, code int, CONSTRAINT a_code_uq UNIQUE (code));\n" +
				"CREATE TABLE z (id int PRIMARY KEY, a_code int, KEY z_code (a_code), CONSTRAINT z_a_fk FOREIGN KEY (a_code) REFERENCES a (code));\n",
			desired: "CREATE TABLE a (id int PRIMARY KEY, code int);\n" +
				"CREATE TABLE z (id int PRIMARY KEY, a_code int, KEY z_code (a_code));\n",
			afterUp:   "",
			afterDown: "a.a_code_uq,z.z_a_fk",
		},
		{
			name:  "the rollback removes both, the key and the foreign key on one table",
			built: "CREATE TABLE t (id int PRIMARY KEY, code int, parent_code int);\n",
			desired: "CREATE TABLE t (id int PRIMARY KEY, code int, parent_code int, CONSTRAINT t_code_key UNIQUE (code), " +
				"CONSTRAINT t_parent_fk FOREIGN KEY (parent_code) REFERENCES t (code));\n",
			afterUp:   "t.t_code_key,t.t_parent_fk",
			afterDown: "",
		},
		{
			name:  "the rollback removes both, the foreign key on a table it drops",
			built: "CREATE TABLE a (id int PRIMARY KEY, code int);\n",
			desired: "CREATE TABLE a (id int PRIMARY KEY, code int, n_id int, CONSTRAINT a_code_key UNIQUE (code));\n" +
				"CREATE TABLE n (id int PRIMARY KEY, a_code int, CONSTRAINT n_a_fk FOREIGN KEY (a_code) REFERENCES a (code));\n" +
				"ALTER TABLE a ADD CONSTRAINT a_n_fk FOREIGN KEY (n_id) REFERENCES n (id);\n",
			afterUp:   "a.a_code_key,a.a_n_fk,n.n_a_fk",
			afterDown: "",
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
				c.Assert(columnKeyString(c, target, keysHeldRead), qt.Equals, test.afterUp)
				again := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
				c.Assert(again.Stdout, qt.Contains, "Schema is synced")

				down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

				c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
				c.Assert(columnKeyString(c, target, keysHeldRead), qt.Equals, test.afterDown)
			})
		}
	}
}
