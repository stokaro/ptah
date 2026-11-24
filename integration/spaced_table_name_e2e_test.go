//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A table name may begin with a space on PostgreSQL and MySQL, so ` t` and `t`
// are two tables. Each test below runs `ptah schema apply` on a database that
// holds both, each with a row, and a desired schema that changes one of them.
// The catalog is read back to say which table the server changed.

// spacedTableEngine is a server the tests build the two tables on.
type spacedTableEngine struct {
	name string
	// built creates a database, dropped when the test ends, runs the SQL on
	// it and returns its URL.
	built func(c *qt.C, sql string) string
	// create builds ` t` and t with one row each.
	create string
	// tables lists ` t` and t as [table](columns), in name order. The
	// brackets show the space.
	tables string
	// rows counts the rows of ` t` and of t, as "1,1".
	rows string
}

var spacedTableEngines = []spacedTableEngine{
	{
		name:  "PostgreSQL",
		built: databaseBuiltFrom,
		create: `CREATE TABLE " t" (id INTEGER PRIMARY KEY NOT NULL);
CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL);
INSERT INTO " t" VALUES (1);
INSERT INTO t VALUES (2);`,
		tables: `SELECT string_agg('[' || t.table_name || '](' || (
			SELECT string_agg(column_name, ',' ORDER BY ordinal_position)
			FROM information_schema.columns k
			WHERE k.table_schema = t.table_schema AND k.table_name = t.table_name) || ')', ' ' ORDER BY t.table_name)
		FROM information_schema.tables t
		WHERE t.table_schema = current_schema() AND t.table_name IN (' t', 't')`,
		rows: `SELECT (SELECT count(*) FROM " t") || ',' || (SELECT count(*) FROM t)`,
	},
	{
		name:  "MySQL",
		built: func(c *qt.C, sql string) string { return mysqlFamilyBuiltDatabase(c, dbtarget.MySQLAdmin, sql) },
		create: "CREATE TABLE ` t` (id INTEGER PRIMARY KEY NOT NULL);\n" +
			"CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL);\n" +
			"INSERT INTO ` t` VALUES (1);\n" +
			"INSERT INTO t VALUES (2);",
		tables: "SELECT GROUP_CONCAT(CONCAT('[', t.TABLE_NAME, '](', (" +
			"SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) FROM information_schema.COLUMNS k " +
			"WHERE k.TABLE_SCHEMA = t.TABLE_SCHEMA AND k.TABLE_NAME = t.TABLE_NAME), ')') ORDER BY t.TABLE_NAME SEPARATOR ' ') " +
			"FROM information_schema.TABLES t WHERE t.TABLE_SCHEMA = DATABASE() AND t.TABLE_NAME IN (' t', 't')",
		rows: "SELECT CONCAT((SELECT COUNT(*) FROM ` t`), ',', (SELECT COUNT(*) FROM t))",
	},
}

// spacedTableSources declare ` t` with a new column and t as it is. The quoted
// spelling names ` t`: a document is text an author typed, and the space around
// a bare name is removed, so `name: " t"` names t.
var spacedTableSources = []struct {
	name string
	flag string
	file string
	// target is what the flag names, relative to the directory holding
	// the file: the file itself, or the directory for a Go source.
	target string
	body   string
}{
	{
		name:   "YAML",
		flag:   "--schema-file",
		file:   "schema.yaml",
		target: "schema.yaml",
		body: `tables:
  spaced:
    name: '" t"'
    columns:
      id:
        type: INTEGER
        primary: true
        not_null: true
      extra:
        type: TEXT
  plain:
    name: t
    columns:
      id:
        type: INTEGER
        primary: true
        not_null: true
`,
	},
	{
		name:   "Go",
		flag:   "--root-dir",
		file:   "models.go",
		target: "",
		body: `package models

//ptah:schema:table name="\" t\""
type Spaced struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" not_null="true"
	ID int
	//ptah:schema:field name="extra" type="TEXT"
	Extra string
}

//ptah:schema:table name="t"
type Plain struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true" not_null="true"
	ID int
}
`,
	},
}

// spacedTableQuery runs a query of the engine's on the database and returns
// the one text value it answers.
func spacedTableQuery(c *qt.C, database, query string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var value string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil, qt.Commentf("%s", query))
	return value
}

// spacedTableOnlyPlain declares t alone, so ` t` is a table the schema does not
// hold.
const spacedTableOnlyPlain = `tables:
  plain:
    name: t
    columns:
      id:
        type: INTEGER
        primary: true
        not_null: true
`

// TestSpacedTableNameApplyE2E applies each desired schema to a database holding
// ` t` and t on each engine. The catalog then holds the new column on ` t`
// alone, and a second run plans nothing, because the read pairs each table
// with the declaration of its own name.
func TestSpacedTableNameApplyE2E(t *testing.T) {
	for _, engine := range spacedTableEngines {
		for _, source := range spacedTableSources {
			t.Run(engine.name+"/"+source.name, func(t *testing.T) {
				c := qt.New(t)
				database := engine.built(c, engine.create)
				c.Assert(spacedTableQuery(c, database, engine.tables), qt.Equals, "[ t](id) [t](id)")
				dir := c.TempDir()
				c.Assert(os.WriteFile(filepath.Join(dir, source.file), []byte(source.body), 0o600), qt.IsNil)
				args := []string{"schema", "apply", "--db-url", database, source.flag, filepath.Join(dir, source.target), "--auto-approve"}

				applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(spacedTableQuery(c, database, engine.tables), qt.Equals, "[ t](id,extra) [t](id)")
				c.Assert(spacedTableQuery(c, database, engine.rows), qt.Equals, "1,1")

				again := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
				c.Assert(spacedTableQuery(c, database, engine.tables), qt.Equals, "[ t](id,extra) [t](id)")
			})
		}
	}
}

// TestSpacedTableNameDropE2E applies a desired schema that declares t alone to
// a database holding ` t` and t. The table the schema does not hold is ` t`,
// and the DROP names it: t keeps its row.
func TestSpacedTableNameDropE2E(t *testing.T) {
	for _, engine := range spacedTableEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			database := engine.built(c, engine.create)
			schema := filepath.Join(c.TempDir(), "schema.yaml")
			c.Assert(os.WriteFile(schema, []byte(spacedTableOnlyPlain), 0o600), qt.IsNil)
			args := []string{"schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve"}

			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(spacedTableQuery(c, database, engine.tables), qt.Equals, "[t](id)")
			c.Assert(spacedTableQuery(c, database, "SELECT CONCAT('', COUNT(*)) FROM t"), qt.Equals, "1")

			again := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}
