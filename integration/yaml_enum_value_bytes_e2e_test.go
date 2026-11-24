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

// An enum value may begin or end with a space, and the server holds ' sad' and
// 'sad' as two values. Each test below declares in YAML the enum a database
// already holds, with a row storing ' sad', and then adds a value. The catalog
// is read back to say which values the server holds.

// enumBytesEngine is a server the enum is built on.
type enumBytesEngine struct {
	name string
	// built creates a database, dropped when the test ends, runs the SQL on
	// it and returns its URL.
	built func(c *qt.C, sql string) string
	// create builds the enum and a row storing ' sad'.
	create string
	// declared is the YAML document of what create builds, and added is the
	// same document with the value ' new' added.
	declared, added string
	// statement is what the plan of added writes.
	statement string
	// values lists the values the server holds, in their order, with each
	// in brackets to show its space.
	values string
	// before and after are what values answers before the apply and after.
	before, after string
	// rows reads the stored values the same way, and stored is what it
	// answers before the apply and after.
	rows, stored string
}

var enumBytesEngines = []enumBytesEngine{
	{
		name:  "PostgreSQL",
		built: databaseBuiltFrom,
		create: `CREATE TYPE mood AS ENUM ('happy', ' sad', 'ok ');
CREATE TYPE enum_c_n AS ENUM ('happy', ' sad', 'ok ');
CREATE TABLE c (id INTEGER PRIMARY KEY NOT NULL, m mood, n enum_c_n);
INSERT INTO c VALUES (1, ' sad', ' sad');`,
		declared: enumBytesPostgresDocument("[happy, ' sad', 'ok ']"),
		added:    enumBytesPostgresDocument("[happy, ' sad', 'ok ', ' new']"),
		statement: `ALTER TYPE "public"."enum_c_n" ADD VALUE ' new';
ALTER TYPE "public"."mood" ADD VALUE ' new';`,
		values: `SELECT string_agg(t.typname || ':' || (
			SELECT string_agg('[' || e.enumlabel || ']', '' ORDER BY e.enumsortorder)
			FROM pg_enum e WHERE e.enumtypid = t.oid), ' ' ORDER BY t.typname)
		FROM pg_type t WHERE t.typname IN ('mood', 'enum_c_n')`,
		before: "enum_c_n:[happy][ sad][ok ] mood:[happy][ sad][ok ]",
		after:  "enum_c_n:[happy][ sad][ok ][ new] mood:[happy][ sad][ok ][ new]",
		rows:   `SELECT '[' || m || '][' || n || ']' FROM c`,
		stored: "[ sad][ sad]",
	},
	{
		// MySQL drops the trailing space of a value when it builds the
		// column, so the value stored as 'ok' is declared as 'ok'.
		name:  "MySQL",
		built: func(c *qt.C, sql string) string { return mysqlFamilyBuiltDatabase(c, dbtarget.MySQLAdmin, sql) },
		create: "CREATE TABLE c (id INTEGER PRIMARY KEY NOT NULL, n ENUM('happy', ' sad', 'ok'));\n" +
			"INSERT INTO c VALUES (1, ' sad');",
		declared:  enumBytesMySQLDocument("[happy, ' sad', ok]"),
		added:     enumBytesMySQLDocument("[happy, ' sad', ok, ' new']"),
		statement: "ALTER TABLE `c` MODIFY COLUMN `n` enum('happy',' sad','ok',' new');",
		values: "SELECT COLUMN_TYPE FROM information_schema.COLUMNS " +
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'c' AND COLUMN_NAME = 'n'",
		before: "enum('happy',' sad','ok')",
		after:  "enum('happy',' sad','ok',' new')",
		rows:   "SELECT CONCAT('[', n, ']') FROM c",
		stored: "[ sad]",
	},
}

// enumBytesPostgresDocument declares the enum mood, a column of it, and a
// column whose enum holds the same values.
func enumBytesPostgresDocument(values string) string {
	return `enums:
  mood: ` + values + `
tables:
  c:
    columns:
      id:
        type: INTEGER
        primary: true
        not_null: true
      m:
        type: mood
      n:
        enum: ` + values + `
`
}

// enumBytesMySQLDocument declares a column whose enum holds the values.
func enumBytesMySQLDocument(values string) string {
	return `tables:
  c:
    columns:
      id:
        type: INTEGER
        primary: true
        not_null: true
      n:
        enum: ` + values + `
`
}

// enumBytesQuery runs a query on the database and returns the one text value
// it answers.
func enumBytesQuery(c *qt.C, database, query string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var value string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil, qt.Commentf("%s", query))
	return value
}

// enumBytesApply runs `ptah schema apply` with the document and asserts it
// exits 0.
func enumBytesApply(c *qt.C, database, document, mode string) clirun.Result {
	c.Helper()
	schema := filepath.Join(c.TempDir(), "schema.yaml")
	c.Assert(os.WriteFile(schema, []byte(document), 0o600), qt.IsNil)
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"schema", "apply", "--db-url", database, "--schema-file", schema, mode)
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result
}

// TestYAMLEnumValueBytesE2E declares the enum the database holds: the plan is
// synced, because each declared value is the stored one. The added value is
// planned as an added value, the type keeps the stored row, and a second plan
// is synced. Trimmed, the declared values would differ from the stored ones,
// and the plan would rebuild the enum with a cast ' sad' cannot pass.
func TestYAMLEnumValueBytesE2E(t *testing.T) {
	for _, engine := range enumBytesEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			database := engine.built(c, engine.create)
			c.Assert(enumBytesQuery(c, database, engine.values), qt.Equals, engine.before)

			synced := enumBytesApply(c, database, engine.declared, "--dry-run")

			c.Assert(synced.Stdout, qt.Contains, "Schema is synced, no changes to be made.")

			applied := enumBytesApply(c, database, engine.added, "--auto-approve")

			c.Assert(applied.Stdout, qt.Contains, engine.statement)
			c.Assert(enumBytesQuery(c, database, engine.values), qt.Equals, engine.after)
			c.Assert(enumBytesQuery(c, database, engine.rows), qt.Equals, engine.stored)

			again := enumBytesApply(c, database, engine.added, "--dry-run")

			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}
