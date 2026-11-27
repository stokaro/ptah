//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// TestSchemaApplyViewWithAJoinConvergesE2E applies views that join tables,
// in the spellings a declaration writes, then applies the same file again.
// The server stores each join in a spelling of its own, with parentheses
// around the joins and around each condition, and the comparison reads the
// two alike, so the second apply plans nothing. Read as written, every view
// with a join is replaced on every apply (stokaro/ptah#6757).
func TestSchemaApplyViewWithAJoinConvergesE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			crm, _ := scratch.database(c, "view_join_crm")
			_, err := scratch.admin.ExecContext(c.Context(), "CREATE TABLE `"+crm+"`.accounts (id int PRIMARY KEY, name varchar(20))")
			c.Assert(err, qt.IsNil)
			_, target := scratch.database(c, "view_join")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(`CREATE TABLE t (id int PRIMARY KEY, x int);
CREATE TABLE u (id int PRIMARY KEY, name varchar(20));
CREATE TABLE w (id int PRIMARY KEY);
CREATE SQL SECURITY INVOKER VIEW v1 AS SELECT t.id, a.name FROM t JOIN u a ON a.id = t.id;
CREATE SQL SECURITY INVOKER VIEW v2 AS SELECT t.id FROM t INNER JOIN u AS a ON a.id = t.id AND a.name = 'x';
CREATE SQL SECURITY INVOKER VIEW v3 AS SELECT t.id FROM t LEFT OUTER JOIN u ON u.id = t.id LEFT JOIN w ON w.id = u.id;
CREATE SQL SECURITY INVOKER VIEW v4 AS SELECT t.id FROM t JOIN u ON (u.id = t.id OR u.id = t.x) AND t.x > 1;
CREATE SQL SECURITY INVOKER VIEW v5 AS SELECT t.id FROM t CROSS JOIN u;
CREATE SQL SECURITY INVOKER VIEW v6 AS SELECT t.id FROM t JOIN (u JOIN w ON w.id = u.id) ON u.id = t.id;
CREATE SQL SECURITY INVOKER VIEW v7 AS SELECT t.id FROM t JOIN u ON u.id = t.id WHERE t.x = 1;
CREATE SQL SECURITY INVOKER VIEW v8 AS SELECT t.id, a.name FROM t JOIN `+crm+`.accounts a ON a.id = t.id;
`), 0o600), qt.IsNil)
			options := clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute, Env: environmentWithoutPtahVariables()}
			first := clirun.Run(c, clirun.Ptah, options, "schema", "apply", "--db-url", target, "--schema-file", schema,
				"--auto-approve")
			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))

			again := clirun.Run(c, clirun.Ptah, options, "schema", "apply", "--db-url", target, "--schema-file", schema,
				"--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}
