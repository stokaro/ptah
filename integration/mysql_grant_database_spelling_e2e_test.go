//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// leavesTheRole is a comment for a desired schema that declares nothing about
// the run's role, which the apply helpers format into every schema.
const leavesTheRole = "-- The role %[1]s is left as it is.\n"

// spelledDatabase replaces <DB> in text with database in upper case and <db>
// with database as the server stores it.
func spelledDatabase(text, database string) string {
	return strings.NewReplacer("<DB>", strings.ToUpper(database), "<db>", database).Replace(text)
}

// A revoke that names the table's database in another case than the server
// stores it takes the grant on a server that folds names: the server reads
// both spellings as one database. The apply revokes SELECT on items, and the
// next plan is empty.
func TestSchemaApplyMySQLFamilyFoldedDatabaseNameE2E_RevokesUnderEverySpelling(t *testing.T) {
	tests := []struct {
		name string
		url  string // the database the URL names
		on   string // the table the revoke names
	}{
		{name: "the database in upper case, through a URL in upper case", url: "<DB>", on: "<DB>.items"},
		{name: "the database in upper case, through a URL as stored", url: "<db>", on: "<DB>.items"},
		{name: "the database as stored, through a URL in upper case", url: "<DB>", on: "<db>.items"},
	}
	for _, engine := range foldedServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newFoldedRun(c, engine.server)
				url, err := atlasurl.WithDatabaseName(dbtarget.URL(c, engine.server), spelledDatabase(test.url, run.database))
				c.Assert(err, qt.IsNil)
				run.url = url
				declared := foldedRunHistory + "REVOKE SELECT ON " + spelledDatabase(test.on, run.database) + " FROM %[1]s;\n"

				applied := run.apply(c, declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(run.tablePrivileges(c), qt.IsNil)
				again := run.apply(c, declared, "--dry-run")
				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

// A grant that names the table's database in another case than the server
// stores it is the grant the role holds on a server that folds names. A role
// holding SELECT on items, declared with SELECT and INSERT there, gets INSERT
// alone, keeps SELECT, and the next plan is empty.
func TestSchemaApplyMySQLFamilyFoldedDatabaseNameE2E_GrantsUnderEverySpelling(t *testing.T) {
	tests := []struct {
		name string
		on   string // the table the grant names
	}{
		{name: "the database in upper case", on: "<DB>.items"},
		{name: "the database as stored", on: "<db>.items"},
	}
	for _, engine := range foldedServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newFoldedRun(c, engine.server)
				declared := foldedRunHistory + "CREATE ROLE %[1]s;\nGRANT SELECT, INSERT ON " + spelledDatabase(test.on, run.database) + " TO %[1]s;\n"

				applied := run.apply(c, declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(applied.Stdout, qt.Not(qt.Contains), "REVOKE")
				c.Assert(run.tablePrivileges(c), qt.DeepEquals, []string{"items INSERT", "items SELECT"})
				again := run.apply(c, declared, "--dry-run")
				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

// A revoke of a database-level privilege, `REVOKE SELECT ON SCHEMA <DB>`, that
// names the database in upper case takes the role's grant on the database. The
// apply revokes it, and the next plan is empty.
func TestSchemaApplyMySQLFamilyFoldedDatabaseNameE2E_RevokesADatabaseGrantUnderAnotherSpelling(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newFoldedRun(c, engine.server)
			// #nosec G202 -- the database and the role are names this test generates.
			_, err := run.scratch.admin.ExecContext(c.Context(), "GRANT SELECT ON `"+run.database+"`.* TO "+run.role)
			c.Assert(err, qt.IsNil)
			c.Assert(foldedSchemaPrivileges(c, run), qt.DeepEquals, []string{"SELECT"})
			declared := foldedRunHistory + "REVOKE SELECT ON SCHEMA " + spelledDatabase("<DB>", run.database) + " FROM %[1]s;\n"

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(foldedSchemaPrivileges(c, run), qt.IsNil)
			again := run.apply(c, declared, "--dry-run")
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// foldedSchemaPrivileges is what the server holds for the role on the whole
// database, one privilege each, in order.
func foldedSchemaPrivileges(c *qt.C, run foldedRun) []string {
	c.Helper()
	rows, err := run.scratch.admin.QueryContext(c.Context(), `
		SELECT PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES
		WHERE TABLE_SCHEMA = ? AND SUBSTRING_INDEX(GRANTEE, '@', 1) = CONCAT('''', ?, '''')
		ORDER BY PRIVILEGE_TYPE`, run.database, run.role)
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

// A foreign key that names the referenced table's database in upper case
// references the table of the selected database. The apply creates it there,
// and the next plan is empty rather than dropping and adding the key again.
func TestSchemaApplyMySQLFamilyFoldedDatabaseNameE2E_KeepsAForeignKeyUnderAnotherSpelling(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newFoldedRun(c, engine.server)
			declared := foldedRunHistory + leavesTheRole + "CREATE TABLE notes (id INT PRIMARY KEY, item_id INT, " +
				"CONSTRAINT fk_item FOREIGN KEY (item_id) REFERENCES " + spelledDatabase("<DB>", run.database) + ".items (id));\n"

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			var schema, table string
			c.Assert(run.scratch.admin.QueryRowContext(c.Context(), `
				SELECT REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE
				WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'notes' AND CONSTRAINT_NAME = 'fk_item'`, run.database,
			).Scan(&schema, &table), qt.IsNil)
			c.Assert(schema+"."+table, qt.Equals, run.database+".items")
			again := run.apply(c, declared, "--dry-run")
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// A table that the document qualifies with the database in upper case is a
// table of the selected database. The apply creates it there rather than
// refusing it as outside the URL's database, and the next plan is empty.
func TestSchemaApplyMySQLFamilyFoldedDatabaseNameE2E_PlacesATableUnderAnotherSpelling(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newFoldedRun(c, engine.server)
			declared := foldedRunHistory + leavesTheRole + "CREATE TABLE " + spelledDatabase("<DB>", run.database) + ".notes (id INT PRIMARY KEY);\n"

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			var tables int
			c.Assert(run.scratch.admin.QueryRowContext(c.Context(),
				"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'notes'", run.database,
			).Scan(&tables), qt.IsNil)
			c.Assert(tables, qt.Equals, 1)
			again := run.apply(c, declared, "--dry-run")
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// On a server that keeps names apart by case, a database whose name differs
// only in case is another database. A revoke on a table there leaves the
// role's grant on the table of the selected database, and the plan is empty.
func TestSchemaApplyMySQLDatabaseNameByItsBytesE2E_KeepsTheGrantOfAnotherDatabase(t *testing.T) {
	c := qt.New(t)
	run := newColumnGrantRunFrom(c, dbtarget.MySQLAdmin, "CREATE TABLE t3 (id INT PRIMARY KEY);\n",
		[]string{"GRANT SELECT ON `%[1]s`.t3 TO %[2]s"})
	var folding int
	c.Assert(run.scratch.admin.QueryRowContext(c.Context(), "SELECT @@lower_case_table_names").Scan(&folding), qt.IsNil)
	c.Assert(folding, qt.Equals, 0)
	declared := fmt.Sprintf("CREATE TABLE t3 (id INT PRIMARY KEY);\nREVOKE SELECT ON %s.t3 FROM %%[1]s;\n", strings.ToUpper(run.database))

	planned := run.apply(c, declared, "--dry-run")

	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	c.Assert(planned.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
	c.Assert(run.privileges(c), qt.DeepEquals, []string{"table t3 SELECT NO"})
}

// On a server that keeps names apart by case, a table the document qualifies
// with the selected database in upper case is in another database, and the
// URL naming one database refuses it before anything is planned.
func TestSchemaApplyMySQLDatabaseNameByItsBytesE2E_RefusesATableInAnotherCase(t *testing.T) {
	c := qt.New(t)
	run := newColumnGrantRunFrom(c, dbtarget.MySQLAdmin, "CREATE TABLE t3 (id INT PRIMARY KEY);\n", nil)
	upper := strings.ToUpper(run.database)
	declared := leavesTheRole + fmt.Sprintf("CREATE TABLE t3 (id INT PRIMARY KEY);\nCREATE TABLE %s.notes (id INT PRIMARY KEY);\n", upper)

	planned := run.apply(c, declared, "--auto-approve")

	c.Assert(planned.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", planned.Stdout))
	c.Assert(planned.Stderr, qt.Contains, fmt.Sprintf(
		"table %q is in database %q, but the target URL is limited to database %q", upper+".notes", upper, run.database))
	var tables int
	c.Assert(run.scratch.admin.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_NAME = 'notes' AND TABLE_SCHEMA IN (?, ?)", run.database, upper,
	).Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, 0)
}

// applyServerSchema runs `ptah schema apply` against the whole server url with
// the desired schema and the extra arguments.
func applyServerSchema(c *qt.C, url, desired string, args ...string) clirun.Result {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(desired), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		append([]string{"schema", "apply", "--db-url", url, "--schema-file", file}, args...)...)
}

// On a whole server that folds names, a database the document declares in
// upper case is the database the server holds under its own spelling. The
// apply keeps it rather than dropping it, and the plan is empty.
func TestSchemaApplyMySQLFamilyFoldedServerE2E_KeepsADatabaseUnderAnotherSpelling(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			account := newMySQLServerAccount(c, engine.server, []string{"kept"}, []string{"kept"})
			declared := "CREATE DATABASE " + strings.ToUpper(account.names["kept"]) + ";\n"

			applied := applyServerSchema(c, account.url, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Not(qt.Contains), "DROP DATABASE")
			c.Assert(account.exists(c, "kept"), qt.IsTrue)
			again := applyServerSchema(c, account.url, declared, "--dry-run")
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// On a whole server that folds names, a table the document qualifies with its
// database in upper case is the table the server holds. The apply creates and
// drops nothing, and the table keeps its row.
func TestSchemaApplyMySQLFamilyFoldedServerE2E_KeepsATableUnderAnotherSpelling(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			account := newMySQLServerAccount(c, engine.server, []string{"kept"}, []string{"kept"})
			kept := account.names["kept"]
			// #nosec G202 -- the database is a name this test generates.
			_, err := account.admin.ExecContext(c.Context(), "CREATE TABLE `"+kept+"`.items (id INT PRIMARY KEY)")
			c.Assert(err, qt.IsNil)
			// #nosec G202 -- the database is a name this test generates.
			_, err = account.admin.ExecContext(c.Context(), "INSERT INTO `"+kept+"`.items VALUES (7)")
			c.Assert(err, qt.IsNil)
			upper := strings.ToUpper(kept)
			declared := "CREATE DATABASE " + upper + ";\nCREATE TABLE " + upper + ".items (id INT PRIMARY KEY);\n"

			applied := applyServerSchema(c, account.url, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			var rows int
			// #nosec G202 -- the database is a name this test generates.
			c.Assert(account.admin.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+kept+"`.items").Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 1)
		})
	}
}

// On a whole server that keeps names apart by case, a database the document
// declares in upper case is another database: the plan creates it and drops
// the one the document does not declare.
func TestSchemaApplyMySQLServerDatabaseNameByItsBytesE2E_PlansAnotherDatabase(t *testing.T) {
	c := qt.New(t)
	account := newMySQLServerAccount(c, dbtarget.MySQLAdmin, []string{"kept"}, []string{"kept"})
	var folding int
	c.Assert(account.admin.QueryRowContext(c.Context(), "SELECT @@lower_case_table_names").Scan(&folding), qt.IsNil)
	c.Assert(folding, qt.Equals, 0)
	kept := account.names["kept"]
	upper := strings.ToUpper(kept)

	planned := applyServerSchema(c, account.url, "CREATE DATABASE "+upper+";\n", "--dry-run")

	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	c.Assert(planned.Stdout, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `"+upper+"`;")
	c.Assert(planned.Stdout, qt.Contains, "DROP DATABASE `"+kept+"`;")
	c.Assert(account.exists(c, "kept"), qt.IsTrue)
}

// A revoke that names the table in another case than the server stores it
// takes the grant on a server that folds names, with or without the database.
// The apply revokes SELECT on items, and the next plan is empty.
func TestSchemaApplyMySQLFamilyFoldedTableNameE2E_RevokesUnderEverySpelling(t *testing.T) {
	tests := []struct {
		name string
		on   string // the table the revoke names
	}{
		{name: "the table in another case", on: "Items"},
		{name: "the table in another case, in the database as stored", on: "<db>.Items"},
		{name: "the table and the database in upper case", on: "<DB>.ITEMS"},
	}
	for _, engine := range foldedServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newFoldedRun(c, engine.server)
				declared := foldedRunHistory + "REVOKE SELECT ON " + spelledDatabase(test.on, run.database) + " FROM %[1]s;\n"

				applied := run.apply(c, declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(run.tablePrivileges(c), qt.IsNil)
				again := run.apply(c, declared, "--dry-run")
				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

// A grant that names the table in another case is the grant the role holds on
// a server that folds names. A role holding SELECT on items, declared with
// SELECT and INSERT on Items, gets INSERT alone and keeps SELECT: no statement
// takes SELECT away between two others. The next plan is empty.
func TestSchemaApplyMySQLFamilyFoldedTableNameE2E_GrantsUnderAnotherSpelling(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newFoldedRun(c, engine.server)
			declared := foldedRunHistory + "CREATE ROLE %[1]s;\nGRANT SELECT, INSERT ON Items TO %[1]s;\n"

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Not(qt.Contains), "REVOKE")
			c.Assert(run.tablePrivileges(c), qt.DeepEquals, []string{"items INSERT", "items SELECT"})
			again := run.apply(c, declared, "--dry-run")
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// A table the document declares in another case than the server stores it is
// that table on a server that folds names. The apply creates and drops
// nothing, and the table keeps its row.
func TestSchemaApplyMySQLFamilyFoldedTableNameE2E_KeepsADeclaredTable(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newFoldedRun(c, engine.server)
			// #nosec G202 -- the database is a name this test generates.
			_, err := run.scratch.admin.ExecContext(c.Context(), "INSERT INTO `"+run.database+"`.items VALUES (7, 'kept')")
			c.Assert(err, qt.IsNil)
			declared := "CREATE TABLE Items (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL);\n" +
				"CREATE VIEW item_ids AS SELECT id FROM items;\nCREATE ROLE %[1]s;\nGRANT SELECT ON Items TO %[1]s;\n"

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			var rows int
			// #nosec G202 -- the database is a name this test generates.
			c.Assert(run.scratch.admin.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM `"+run.database+"`.items").Scan(&rows), qt.IsNil)
			c.Assert(rows, qt.Equals, 1)
		})
	}
}

// A foreign key that names the referenced table in another case references
// that table on a server that folds names. The apply creates the key on
// items, and the next plan is empty.
func TestSchemaApplyMySQLFamilyFoldedTableNameE2E_KeepsAForeignKey(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newFoldedRun(c, engine.server)
			declared := foldedRunHistory + leavesTheRole + "CREATE TABLE notes (id INT PRIMARY KEY, item_id INT, " +
				"CONSTRAINT fk_item FOREIGN KEY (item_id) REFERENCES Items (id));\n"

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			var table string
			c.Assert(run.scratch.admin.QueryRowContext(c.Context(), `
				SELECT REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE
				WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'notes' AND CONSTRAINT_NAME = 'fk_item'`, run.database,
			).Scan(&table), qt.IsNil)
			c.Assert(table, qt.Equals, "items")
			again := run.apply(c, declared, "--dry-run")
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// On a server that keeps names apart by case, Items and items are two tables.
// Nothing is respelled: the apply creates Items beside items, grants on each
// what the document declares there, and the next plan is empty.
func TestSchemaApplyMySQLTableNameByItsBytesE2E_KeepsTwoTables(t *testing.T) {
	c := qt.New(t)
	run := newColumnGrantRunFrom(c, dbtarget.MySQLAdmin, "CREATE TABLE items (id INT PRIMARY KEY);\n",
		[]string{"GRANT SELECT ON `%[1]s`.items TO %[2]s"})
	var folding int
	c.Assert(run.scratch.admin.QueryRowContext(c.Context(), "SELECT @@lower_case_table_names").Scan(&folding), qt.IsNil)
	c.Assert(folding, qt.Equals, 0)
	declared := "CREATE TABLE items (id INT PRIMARY KEY);\nCREATE TABLE Items (id INT PRIMARY KEY);\n" +
		"CREATE ROLE %[1]s;\nGRANT SELECT ON items TO %[1]s;\nGRANT INSERT ON Items TO %[1]s;\n"

	applied := run.apply(c, declared, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	var tables string
	c.Assert(run.scratch.admin.QueryRowContext(c.Context(), `
		SELECT GROUP_CONCAT(TABLE_NAME ORDER BY CAST(TABLE_NAME AS BINARY))
		FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND LOWER(TABLE_NAME) = 'items'`, run.database,
	).Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, "Items,items")
	c.Assert(run.privileges(c), qt.DeepEquals, []string{"table Items INSERT NO", "table items SELECT NO"})
	again := run.apply(c, declared, "--dry-run")
	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}

// A foreign key that names a table the same document declares, spelled in
// another case, references that table on a server that folds names, before the
// server holds it. The first apply creates both tables and the key on parts,
// and the next plan is empty.
func TestSchemaApplyMySQLFamilyFoldedTableNameE2E_CreatesAForeignKeyToADeclaredTable(t *testing.T) {
	for _, engine := range foldedServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newFoldedRun(c, engine.server)
			declared := foldedRunHistory + leavesTheRole + "CREATE TABLE parts (id INT PRIMARY KEY);\n" +
				"CREATE TABLE notes (id INT PRIMARY KEY, part_id INT, CONSTRAINT fk_part FOREIGN KEY (part_id) REFERENCES Parts (id));\n"

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			var table string
			c.Assert(run.scratch.admin.QueryRowContext(c.Context(), `
				SELECT REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE
				WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'notes' AND CONSTRAINT_NAME = 'fk_part'`, run.database,
			).Scan(&table), qt.IsNil)
			c.Assert(table, qt.Equals, "parts")
			again := run.apply(c, declared, "--dry-run")
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}
