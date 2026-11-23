//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// These tests connect to a database on a MySQL or MariaDB server started with
// --lower-case-table-names=1 through a URL that spells its name in upper case.
// The server folds the name, so the session selects the lower-case database
// and every catalog row names it that way. A description keyed by the URL's
// spelling names no object the read found, and a comparison reports each view
// and grant twice, missing under one name and extra under the other.

// foldedServerEngines are the servers that fold names, each reached with an
// account that may create databases.
var foldedServerEngines = []struct {
	name   string
	server dbtarget.Engine
}{
	{name: "MySQL", server: dbtarget.MySQLFoldedServer},
	{name: "MariaDB", server: dbtarget.MariaDBFoldedServer},
}

// foldedRun is a database holding items, a view over it and a role holding
// SELECT on items, and the URL naming the database in upper case.
type foldedRun struct {
	scratch  mysqlScratch
	database string
	url      string
	role     string
}

// newFoldedRun creates the database and the role on server. Both are dropped
// when the test ends.
func newFoldedRun(c *qt.C, server dbtarget.Engine) foldedRun {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, server)
	var folding int
	c.Assert(scratch.admin.QueryRowContext(c.Context(), "SELECT @@lower_case_table_names").Scan(&folding), qt.IsNil)
	c.Assert(folding, qt.Equals, 1)

	run := foldedRun{scratch: scratch, role: fmt.Sprintf("ptah_fold_%d", time.Now().UnixNano()%1_000_000_000)}
	_, err := scratch.admin.ExecContext(c.Context(), "CREATE ROLE "+run.role)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := scratch.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+run.role)
		c.Check(dropErr, qt.IsNil)
	})
	run.database, _ = scratch.builtFrom(c, foldedRunHistory)
	// #nosec G202 -- the database and the role are names this test generates.
	_, err = scratch.admin.ExecContext(c.Context(), "GRANT SELECT ON `"+run.database+"`.items TO "+run.role)
	c.Assert(err, qt.IsNil)
	run.url, err = atlasurl.WithDatabaseName(dbtarget.URL(c, server), strings.ToUpper(run.database))
	c.Assert(err, qt.IsNil)
	return run
}

// foldedRunHistory builds the database's table and view.
const foldedRunHistory = "CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL);\n" +
	"CREATE VIEW item_ids AS SELECT id FROM items;\n"

// foldedRunSchema is the database as it stands; %[1]s is the role.
const foldedRunSchema = foldedRunHistory + "CREATE ROLE %[1]s;\nGRANT SELECT ON items TO %[1]s;\n"

// apply runs `ptah schema apply` with the desired schema, a format whose %[1]s
// is the role, and the extra arguments.
func (r foldedRun) apply(c *qt.C, desired string, args ...string) clirun.Result {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(fmt.Sprintf(desired, r.role)), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		append([]string{"schema", "apply", "--db-url", r.url, "--schema-file", file}, args...)...)
}

// tablePrivileges is what the server holds for the role on the database's
// tables, one `<table> <privilege>` each, in order.
func (r foldedRun) tablePrivileges(c *qt.C) []string {
	c.Helper()
	rows, err := r.scratch.admin.QueryContext(c.Context(), `
		SELECT CONCAT(TABLE_NAME, ' ', PRIVILEGE_TYPE) AS privilege
		FROM information_schema.TABLE_PRIVILEGES
		WHERE TABLE_SCHEMA = ? AND SUBSTRING_INDEX(GRANTEE, '@', 1) = CONCAT('''', ?, '''')
		ORDER BY privilege`, r.database, r.role)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var held []string
	for rows.Next() {
		var privilege string
		c.Assert(rows.Scan(&privilege), qt.IsNil)
		held = append(held, privilege)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// The database matches its declaration through a URL that spells its name in
// another case.
func TestSchemaApplyMySQLFamilyFoldedDatabaseNameE2E_HappyPath(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newFoldedRun(c, engine.server)

			planned := run.apply(c, foldedRunSchema, "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// A declared grant the role lacks is the one statement planned, the server
// holds it after the apply, and the next plan finds nothing to change. The
// read describes the database the session selected, not an empty one under
// the URL's spelling.
func TestSchemaApplyMySQLFamilyFoldedDatabaseNameE2E_AddsAGrant(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newFoldedRun(c, engine.server)
			widened := foldedRunSchema + "GRANT INSERT ON items TO %[1]s;\n"

			applied := run.apply(c, widened, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "GRANT INSERT ON `items` TO `"+run.role+"`;")
			c.Assert(applied.Stdout, qt.Not(qt.Contains), "VIEW")
			c.Assert(applied.Stdout, qt.Not(qt.Contains), "REVOKE")
			c.Assert(run.tablePrivileges(c), qt.DeepEquals, []string{"items INSERT", "items SELECT"})

			again := run.apply(c, widened, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// A grant on the database that the schema declares under the URL's spelling
// keeps a declared revoke on one of its tables: the server folds the name, so
// the grant is on the database the session selected. The apply refuses the
// revoke and changes nothing, whether the revoke names the table alone or with
// the database as the server stores it. Measured on MySQL 26.10.0 and MariaDB
// 12.3.3 with the database compared by its bytes: the apply exits 0 with
// GRANT SELECT on the database under the URL's spelling and REVOKE SELECT on
// items, and the role keeps SELECT on items through the database grant. The
// table named with its database fails that way on master too.
func TestSchemaApplyMySQLFamilyFoldedDatabaseNameE2E_RefusesARevokeUnderADeclaredGrant(t *testing.T) {
	tests := []struct {
		name  string
		table string // <db> is the database as the server stores it
	}{
		{name: "the table alone", table: "items"},
		{name: "the table with its database", table: "<db>.items"},
	}
	for _, engine := range foldedServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newFoldedRun(c, engine.server)
				spelled := strings.ToUpper(run.database)
				table := strings.ReplaceAll(test.table, "<db>", run.database)
				grants := filepath.Join(c.TempDir(), "grants.yaml")
				c.Assert(os.WriteFile(grants, fmt.Appendf(nil,
					"grants:\n  everything:\n    role: %s\n    privileges: [SELECT]\n    on_schema: %s\n", run.role, spelled), 0o600), qt.IsNil)

				applied := run.apply(c, foldedRunHistory+"CREATE ROLE %[1]s;\nREVOKE SELECT ON "+table+" FROM %[1]s;\n",
					"--auto-approve", "--schema-file", grants)

				c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(applied.Stderr, qt.Matches, fmt.Sprintf(
					`(?s).*the schema grants SELECT ON %s\.\* to role "%s", which covers table %s, so a revoke on the table cannot take it.*`,
					spelled, run.role, regexp.QuoteMeta(table)))
				c.Assert(run.tablePrivileges(c), qt.DeepEquals, []string{"items SELECT"})
				c.Assert(run.databasePrivileges(c), qt.IsNil)
			})
		}
	}
}

// databasePrivileges is what the server holds for the role on the whole
// database, one privilege each, in order.
func (r foldedRun) databasePrivileges(c *qt.C) []string {
	c.Helper()
	rows, err := r.scratch.admin.QueryContext(c.Context(), `
		SELECT PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES
		WHERE TABLE_SCHEMA = ? AND SUBSTRING_INDEX(GRANTEE, '@', 1) = CONCAT('''', ?, '''')
		ORDER BY PRIVILEGE_TYPE`, r.database, r.role)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var held []string
	for rows.Next() {
		var privilege string
		c.Assert(rows.Scan(&privilege), qt.IsNil)
		held = append(held, privilege)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// `migrations drift` replays the history in a shadow database and compares it
// with the database the URL selects: the view the history builds is the
// database's own view.
func TestMigrationsDriftFoldedDatabaseNameE2E(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.server)
			target, _ := scratch.database(c, "fold")
			_, shadow := scratch.database(c, "fold_shadow")
			url, err := atlasurl.WithDatabaseName(dbtarget.URL(c, engine.server), strings.ToUpper(target))
			c.Assert(err, qt.IsNil)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_items.up.sql"),
				[]byte("-- atlas:txmode none\n"+foldedRunHistory), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_items.down.sql"),
				[]byte("DROP VIEW item_ids;\nDROP TABLE items;\n"), 0o600), qt.IsNil)
			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
				"migrations", "up", "--db-url", url, "--migrations-dir", dir)
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				"migrations", "drift", "--db-url", url, "--migrations-dir", dir, "--shadow-db", shadow)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(result.Stdout, qt.Not(qt.Contains), "item_ids")
		})
	}
}
