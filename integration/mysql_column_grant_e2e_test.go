//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// These tests declare privileges limited to columns on MySQL and MariaDB, run
// `ptah schema apply`, and read back what the server holds. A grant planned
// without its column list is a grant on the whole table: the role reads every
// column, the secret one the declaration leaves out included, and the next
// plan revokes the table grant and grants it again.

// columnGrantSchema declares t1 and a view over two of its columns, and gives
// %[1]s SELECT on id and name, UPDATE on name, and SELECT on the view's name.
// The secret column is the role's on no statement.
const columnGrantSchema = `CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));
CREATE VIEW v1 AS SELECT id, name FROM t1;
CREATE ROLE %[1]s;
GRANT SELECT (id, name) ON t1 TO %[1]s;
GRANT UPDATE (name) ON t1 TO %[1]s;
GRANT SELECT (name) ON v1 TO %[1]s;
`

// columnGrantsDeclared is what the server holds for the role after an apply of
// [columnGrantSchema]: column privileges only, none grantable, and no row on a
// whole table.
var columnGrantsDeclared = []string{
	"column t1.id SELECT NO",
	"column t1.name SELECT NO",
	"column t1.name UPDATE NO",
	"column v1.name SELECT NO",
}

// columnGrantRun is a database holding t1 and v1, and a role the test owns.
type columnGrantRun struct {
	scratch  mysqlScratch
	database string
	url      string
	role     string
}

// newColumnGrantRun creates the database and the role on engine's server, and
// gives the role held, each statement a format whose %[1]s is the database and
// %[2]s the role. Both are dropped when the test ends.
func newColumnGrantRun(c *qt.C, engine dbtarget.Engine, held []string) columnGrantRun {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	run := columnGrantRun{scratch: scratch, role: fmt.Sprintf("ptah_cg_%d", time.Now().UnixNano()%1_000_000_000)}
	_, err := scratch.admin.ExecContext(c.Context(), "CREATE ROLE "+run.role)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := scratch.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+run.role)
		c.Check(dropErr, qt.IsNil)
	})
	run.database, run.url = scratch.builtFrom(c,
		"CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\n"+
			"CREATE VIEW v1 AS SELECT id, name FROM t1;\n")
	for _, statement := range held {
		_, err := scratch.admin.ExecContext(c.Context(), fmt.Sprintf(statement, run.database, run.role))
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return run
}

// apply runs `ptah schema apply` with the desired schema, a format whose %[1]s
// is the role, and the extra arguments.
func (r columnGrantRun) apply(c *qt.C, desired string, args ...string) clirun.Result {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(fmt.Sprintf(desired, r.role)), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		append([]string{"schema", "apply", "--db-url", r.url, "--schema-file", file}, args...)...)
}

// privileges is what the server holds for the role in the database: one
// `table <name> <privilege> <grantable>` per privilege on a whole table or
// view, then one `column <table>.<column> <privilege> <grantable>` per
// privilege on a column, each in order.
func (r columnGrantRun) privileges(c *qt.C) []string {
	c.Helper()
	rows, err := r.scratch.admin.QueryContext(c.Context(), `
		SELECT CONCAT('table ', TABLE_NAME, ' ', PRIVILEGE_TYPE, ' ', IS_GRANTABLE) AS privilege, 0 AS kind
		FROM information_schema.TABLE_PRIVILEGES
		WHERE TABLE_SCHEMA = ? AND SUBSTRING_INDEX(GRANTEE, '@', 1) = CONCAT('''', ?, '''')
		UNION ALL
		SELECT CONCAT('column ', TABLE_NAME, '.', COLUMN_NAME, ' ', PRIVILEGE_TYPE, ' ', IS_GRANTABLE), 1
		FROM information_schema.COLUMN_PRIVILEGES
		WHERE TABLE_SCHEMA = ? AND SUBSTRING_INDEX(GRANTEE, '@', 1) = CONCAT('''', ?, '''')
		ORDER BY kind, privilege`, r.database, r.role, r.database, r.role)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var held []string
	for rows.Next() {
		var privilege string
		var kind int
		c.Assert(rows.Scan(&privilege, &kind), qt.IsNil)
		held = append(held, privilege)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// An apply gives the role the declared column privileges and nothing on a
// whole table, whatever the role held before: nothing, SELECT on all of t1 with
// the grant option beside SELECT on its id column, or the secret column with
// the grant option. A revoke on t1 takes the column privileges of the same kind
// and a revoke of the grant option takes it from every privilege on t1, so the
// plan runs the revokes first and grants the column privileges a revoke took.
// The next plan finds nothing to change.
func TestSchemaApplyMySQLFamilyColumnGrantsE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		held []string
	}{
		{name: "nothing held"},
		{
			name: "the whole table with the grant option",
			held: []string{
				"GRANT SELECT ON `%[1]s`.t1 TO %[2]s WITH GRANT OPTION",
				"GRANT SELECT (id) ON `%[1]s`.t1 TO %[2]s",
			},
		},
		{
			name: "a column the declaration leaves out, with the grant option",
			held: []string{"GRANT SELECT (secret) ON `%[1]s`.t1 TO %[2]s WITH GRANT OPTION"},
		},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRun(c, engine.admin, test.held)

				applied := run.apply(c, columnGrantSchema, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(run.privileges(c), qt.DeepEquals, columnGrantsDeclared)

				again := run.apply(c, columnGrantSchema, "--dry-run")

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

// A column privilege the engine has no column form for is refused by name
// before anything runs: the role holds nothing afterwards.
func TestSchemaApplyMySQLFamilyColumnGrantsE2E_FailurePath(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRun(c, engine.admin, nil)

			applied := run.apply(c, "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\n"+
				"CREATE VIEW v1 AS SELECT id, name FROM t1;\n"+
				"CREATE ROLE %[1]s;\n"+
				"GRANT DELETE (name) ON t1 TO %[1]s;\n", "--auto-approve")

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", applied.Stdout))
			c.Assert(applied.Stderr, qt.Contains, "grants only SELECT, INSERT, UPDATE and REFERENCES on columns")
			c.Assert(run.privileges(c), qt.IsNil)
		})
	}
}
