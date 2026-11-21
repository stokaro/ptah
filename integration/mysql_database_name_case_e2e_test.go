//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// These tests read one database on a MySQL or MariaDB server that keeps the
// case of a database name (lower_case_table_names=0, the Linux default), beside
// a second database whose name differs from it in case alone. information_schema
// compares a database name without regard to case in its privilege views on
// both engines, and in TRIGGERS and EVENTS on MariaDB, so a filter on the name
// the read selects matches the rows of the twin as well. A plan built on that
// read revokes the twin's grants, and on MariaDB plans drops of its triggers.

// caseTwinRun is a database holding items, its twin holding objects of every
// kind a read describes, and a role holding privileges on both.
type caseTwinRun struct {
	scratch  mysqlScratch
	database string
	twin     string
	url      string
	role     string
}

// newCaseTwinRun creates both databases and the role on engine's server. The
// role holds SELECT on the database's items, and on the twin INSERT on items,
// UPDATE on a column of items, and SHOW VIEW on the whole twin. All three are
// dropped when the test ends.
func newCaseTwinRun(c *qt.C, engine dbtarget.Engine) caseTwinRun {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	var folding int
	c.Assert(scratch.admin.QueryRowContext(c.Context(), "SELECT @@lower_case_table_names").Scan(&folding), qt.IsNil)
	c.Assert(folding, qt.Equals, 0, qt.Commentf("a server that folds names holds no two databases that differ in case"))

	run := caseTwinRun{scratch: scratch, role: fmt.Sprintf("ptah_case_%d", time.Now().UnixNano()%1_000_000_000)}
	run.database, run.url = scratch.builtFrom(c, "CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL);")
	run.twin = strings.ToUpper(run.database)
	createMySQLDatabase(c, c.Context(), scratch.admin, run.twin)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), scratch.admin, run.twin) })
	_, err := scratch.admin.ExecContext(c.Context(), "CREATE ROLE "+run.role)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := scratch.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+run.role)
		c.Check(dropErr, qt.IsNil)
	})
	twin := "`" + run.twin + "`"
	for _, statement := range []string{
		"CREATE TABLE " + twin + ".items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, owner INT)",
		"CREATE TABLE " + twin + ".twin_only (id INT PRIMARY KEY)",
		"CREATE VIEW " + twin + ".twin_ids AS SELECT id FROM " + twin + ".items",
		"CREATE PROCEDURE " + twin + ".twin_proc() SELECT 1",
		"CREATE TRIGGER " + twin + ".twin_owner BEFORE INSERT ON " + twin + ".items FOR EACH ROW SET NEW.owner = 1",
		"GRANT SELECT ON `" + run.database + "`.items TO " + run.role,
		"GRANT INSERT ON " + twin + ".items TO " + run.role,
		"GRANT UPDATE (name) ON " + twin + ".items TO " + run.role,
		"GRANT SHOW VIEW ON " + twin + ".* TO " + run.role,
	} {
		_, err := scratch.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return run
}

// apply runs `ptah schema apply` against the database with the desired
// schema, a format whose %[1]s is the role, and the extra arguments.
func (r caseTwinRun) apply(c *qt.C, desired string, args ...string) clirun.Result {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(fmt.Sprintf(desired, r.role)), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		append([]string{"schema", "apply", "--db-url", r.url, "--schema-file", file}, args...)...)
}

// twinPrivileges is what the role holds on the twin: one `<scope>
// <privilege>` per privilege on the database, a table or a column, in order.
// The twin is matched byte for byte, as the views would match the database
// too.
func (r caseTwinRun) twinPrivileges(c *qt.C) []string {
	c.Helper()
	const grantee = "SUBSTRING_INDEX(GRANTEE, '@', 1) = CONCAT('''', ?, '''')"
	const twin = "CONVERT(TABLE_SCHEMA USING utf8mb4) COLLATE utf8mb4_bin = CONVERT(? USING utf8mb4)"
	rows, err := r.scratch.admin.QueryContext(c.Context(), `
		SELECT CONCAT('database ', PRIVILEGE_TYPE) AS privilege
		FROM information_schema.SCHEMA_PRIVILEGES WHERE `+grantee+` AND `+twin+`
		UNION ALL
		SELECT CONCAT('table ', TABLE_NAME, ' ', PRIVILEGE_TYPE)
		FROM information_schema.TABLE_PRIVILEGES WHERE `+grantee+` AND `+twin+`
		UNION ALL
		SELECT CONCAT('column ', TABLE_NAME, '.', COLUMN_NAME, ' ', PRIVILEGE_TYPE)
		FROM information_schema.COLUMN_PRIVILEGES WHERE `+grantee+` AND `+twin+`
		ORDER BY privilege`, r.role, r.twin, r.role, r.twin, r.role, r.twin)
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

// caseTwinSchema is the database as it stands: items, the role, and its SELECT
// on items.
const caseTwinSchema = `CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL);
CREATE ROLE %[1]s;
GRANT SELECT ON items TO %[1]s;
`

// The database matches its declaration, and nothing of the twin's is planned.
func TestSchemaApplyMySQLFamilyIgnoresADatabaseDifferingInCaseE2E_HappyPath(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newCaseTwinRun(c, engine.admin)

			planned := run.apply(c, caseTwinSchema, "--dry-run")

			c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
			c.Assert(planned.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// A declaration that takes the role's SELECT away plans the one revoke, on the
// database it names, and the apply leaves every privilege on the twin in
// place: the read sees its own database's grant, and only that one.
func TestSchemaApplyMySQLFamilyRevokesOnlyInTheDatabaseItNamesE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newCaseTwinRun(c, engine.admin)
			before := run.twinPrivileges(c)
			c.Assert(before, qt.DeepEquals, []string{
				"column items.name UPDATE", "database SHOW VIEW", "table items INSERT",
			})
			revoked := "CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL);\nCREATE ROLE %[1]s;\n"

			applied := run.apply(c, revoked, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "REVOKE SELECT ON `"+run.database+"`.`items` FROM `"+run.role+"`;")
			c.Assert(applied.Stdout, qt.Not(qt.Contains), run.twin)
			c.Assert(applied.Stdout, qt.Not(qt.Contains), "twin_")
			c.Assert(run.twinPrivileges(c), qt.DeepEquals, before)

			again := run.apply(c, revoked, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// A migration that writes to items runs under tx-mode file beside a twin whose
// items carries a trigger. The trigger is the twin's: read by name without the
// byte comparison, MariaDB's TRIGGERS reports it for the database too, and the
// run is refused as one writing to a table with indirect behavior.
func TestMigrationsUpMySQLFamilyIgnoresATriggerOfADatabaseDifferingInCaseE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newCaseTwinRun(c, engine.admin)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "1_fill.up.sql"), []byte("INSERT INTO items VALUES (1, 'a');\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "1_fill.down.sql"), []byte("DELETE FROM items;\n"), 0o600), qt.IsNil)

			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Timeout: time.Minute},
				"migrations", "up", "--db-url", run.url, "--migrations-dir", dir)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			var rows int
			c.Assert(run.scratch.admin.QueryRowContext(c.Context(),
				"SELECT COUNT(*) FROM `"+run.database+"`.items").Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 1)
		})
	}
}

// A cleanup of the database leaves a twin that holds a view of its own items
// as it was. MariaDB writes the view's reference as `R_T`.`items`, and a
// search for `r_t`. that ignores case counts the view as one of another
// database reading this one, and refuses the cleanup. The twin holds nothing
// that runs, since a cleanup refuses beside any routine, event or trigger of
// another database.
func TestDBDropAllMySQLFamilyIgnoresAViewOfADatabaseDifferingInCaseE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			database, url := scratch.builtFrom(c, "CREATE TABLE items (id INT PRIMARY KEY);")
			twin := strings.ToUpper(database)
			createMySQLDatabase(c, c.Context(), scratch.admin, twin)
			c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), scratch.admin, twin) })
			for _, statement := range []string{
				"CREATE TABLE `" + twin + "`.items (id INT PRIMARY KEY)",
				"CREATE VIEW `" + twin + "`.twin_ids AS SELECT id FROM `" + twin + "`.items",
			} {
				_, err := scratch.admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}

			dropped := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
				"db", "drop-all", "--db-url", url, "--auto-approve")

			c.Assert(dropped.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", dropped.Stdout, dropped.Stderr))
			c.Assert(scratch.tableCount(c, database), qt.Equals, 0)
			c.Assert(scratch.tableCount(c, twin), qt.Equals, 2)
		})
	}
}
