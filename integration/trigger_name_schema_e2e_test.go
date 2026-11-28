//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dbtarget"
)

// A schema on a trigger's name is the trigger's schema, which is its table's.
// Applied, the trigger takes the name alone, and a second apply finds it
// synced. Read as one name, the qualified spelling created a trigger whose
// name holds a dot, and the next run reported nothing (stokaro/ptah#6850).

// writeQualifiedTriggerSchema writes contents, with <database> replaced by database,
// to a schema file and returns its path.
func writeQualifiedTriggerSchema(c *qt.C, database, contents string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(strings.ReplaceAll(contents, "<database>", database)), 0o600), qt.IsNil)
	return path
}

func TestMySQLFamilyQualifiedTriggerNameE2E(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "MySQL", engine: dbtarget.MySQLAdmin},
		{name: "MariaDB", engine: dbtarget.MariaDBAdmin},
	}
	tests := []struct{ name, table string }{
		{name: "qualified table", table: "`<database>`.`u`"},
		{name: "unqualified table", table: "`u`"},
	}
	for _, engine := range engines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				database, target := scratch.database(c, "trg_name")
				file := writeQualifiedTriggerSchema(c, database, "CREATE TABLE `u` (`id` INT NOT NULL, `c` INT NULL, PRIMARY KEY (`id`));\n"+
					"CREATE TRIGGER `<database>`.`trg_q` BEFORE INSERT ON "+test.table+" FOR EACH ROW SET NEW.c = 1;\n")

				first := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")
				second := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

				c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", first.Stdout, first.Stderr))
				c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", second.Stdout, second.Stderr))
				c.Assert(second.Stdout, qt.Contains, "no changes")
				var names string
				c.Assert(scratch.admin.QueryRowContext(c.Context(),
					"SELECT COALESCE(GROUP_CONCAT(TRIGGER_NAME), '') FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ?",
					database).Scan(&names), qt.IsNil)
				c.Assert(names, qt.Equals, "trg_q")
			})
		}
	}
}

// On a whole server the trigger's schema names the database, and the table
// written without one is read there, though the database read first holds a
// table of the same name.
func TestMySQLFamilyQualifiedTriggerNameOnAWholeServerE2E(t *testing.T) {
	for _, engine := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "MySQL", engine: dbtarget.MySQLAdmin},
		{name: "MariaDB", engine: dbtarget.MariaDBAdmin},
	} {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			first, _ := scratch.database(c, "trg_server_a")
			second, _ := scratch.database(c, "trg_server_b")
			server, err := atlasurl.WithDatabaseName(scratch.adminURL, "")
			c.Assert(err, qt.IsNil)
			contents := strings.NewReplacer("<first>", first, "<second>", second).Replace(
				"CREATE DATABASE `<first>`;\n" +
					"CREATE TABLE `<first>`.`u` (`id` INT NOT NULL, `c` INT NULL, PRIMARY KEY (`id`));\n" +
					"CREATE DATABASE `<second>`;\n" +
					"CREATE TABLE `<second>`.`u` (`id` INT NOT NULL, `c` INT NULL, PRIMARY KEY (`id`));\n" +
					"CREATE TRIGGER `<second>`.`trg_q` BEFORE INSERT ON `u` FOR EACH ROW SET NEW.c = 1;\n")
			file := writeQualifiedTriggerSchema(c, "", contents)
			schemas := first + "," + second

			apply := runPtahIn(c, "schema", "apply", "--db-url", server, "--schema-file", file, "--schemas", schemas, "--auto-approve")
			again := runPtahIn(c, "schema", "apply", "--db-url", server, "--schema-file", file, "--schemas", schemas, "--dry-run")

			c.Assert(apply.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", apply.Stdout, apply.Stderr))
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "no changes")
			var triggers string
			c.Assert(scratch.admin.QueryRowContext(c.Context(),
				"SELECT COALESCE(GROUP_CONCAT(CONCAT(TRIGGER_SCHEMA, '.', TRIGGER_NAME, ' ON ', EVENT_OBJECT_SCHEMA, '.', EVENT_OBJECT_TABLE)), '') "+
					"FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA IN (?, ?)",
				first, second).Scan(&triggers), qt.IsNil)
			c.Assert(triggers, qt.Equals, second+".trg_q ON "+second+".u")
		})
	}
}

// SQLite's main schema is the database file's own.
func TestSQLiteQualifiedTriggerNameE2E(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "trigger.db")
	target := atlasurl.SQLiteURLFromPath(path)
	file := writeQualifiedTriggerSchema(c, "", "CREATE TABLE u (id INTEGER PRIMARY KEY, c INTEGER);\n"+
		"CREATE TRIGGER main.trg_q AFTER INSERT ON u BEGIN UPDATE u SET c = 1 WHERE id = NEW.id; END;\n")

	first := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")
	second := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

	c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", first.Stdout, first.Stderr))
	c.Assert(second.ExitCode, qt.Equals, 0, qt.Commentf("%s\n%s", second.Stdout, second.Stderr))
	c.Assert(second.Stdout, qt.Contains, "no changes")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var names string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT COALESCE(group_concat(name), '') FROM sqlite_master WHERE type = 'trigger'").Scan(&names), qt.IsNil)
	c.Assert(names, qt.Equals, "trg_q")
}
