//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
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

// enumTrailingSpaceHCLDocument declares, in Atlas HCL, the column n of the
// table c in the database schema with the values of the type text.
func enumTrailingSpaceHCLDocument(schema, values string) string {
	return trailingSpaceHCLDocument(schema, "enum("+values+")")
}

// trailingSpaceHCLDocument declares, in Atlas HCL, the column n of the table c
// in the database schema, typed columnType.
func trailingSpaceHCLDocument(schema, columnType string) string {
	return `schema "` + schema + `" {}
table "c" {
  schema = schema.` + schema + `
  column "id" {
    type = int
  }
  column "n" {
    type = ` + columnType + `
    null = true
  }
  primary_key {
    columns = [column.id]
  }
}
`
}

// keepsTheSpace is the end of every trailing-space refusal: where the binary
// character set counts, which keeps the space.
const keepsTheSpace = `, or name the binary character set on the column or its table; ` +
	`a binary schema default does not count (stokaro/ptah#6665)`

// TestEnumTrailingSpaceE2E_FailurePath declares 'ok ' beside the column the
// server built from ENUM('happy', ' sad', 'ok ') and holds as
// enum('happy',' sad','ok'): measured on MySQL 8.4.13 and 26.10.0 and MariaDB
// 11.8.9, the server removes the trailing space of a value and keeps a
// leading one. Built as declared, the column would never match its
// declaration, so the apply is refused, and the column and its row are left
// as they were.
func TestEnumTrailingSpaceE2E_FailurePath(t *testing.T) {
	engines := []struct {
		name    string
		admin   dbtarget.Engine
		dialect string
	}{
		{name: "MySQL", admin: dbtarget.MySQLAdmin, dialect: "mysql"},
		{name: "MariaDB", admin: dbtarget.MariaDBAdmin, dialect: "mariadb"},
	}
	// document and table take the name of the database the test builds:
	// an HCL document states it as the schema, and the refusal then names
	// the table with it.
	documents := []struct {
		name, file      string
		document, table func(schema string) string
	}{
		{
			name: "YAML", file: "schema.yaml",
			document: func(string) string { return enumBytesMySQLDocument("[happy, ' sad', 'ok ']") },
			table:    func(string) string { return "c" },
		},
		{
			name: "HCL", file: "schema.hcl",
			document: func(schema string) string {
				return enumTrailingSpaceHCLDocument(schema, `"happy", " sad", "ok "`)
			},
			table: func(schema string) string { return schema + ".c" },
		},
		{
			// A plan does not change the default of an existing schema
			// (stokaro/ptah#6665), so the column keeps its table's utf8mb4
			// and the server removes the space.
			name: "HCL in a binary schema", file: "schema.hcl",
			document: func(schema string) string {
				return strings.Replace(enumTrailingSpaceHCLDocument(schema, `"happy", " sad", "ok "`),
					`schema "`+schema+`" {}`, `schema "`+schema+`" {
  charset = "binary"
  collate = "binary"
}`, 1)
			},
			table: func(schema string) string { return schema + ".c" },
		},
		{
			// A character set after the list is not part of the last value,
			// and utf8mb4 removes the space as the default does.
			name: "YAML type naming a character set", file: "schema.yaml",
			document: func(string) string {
				return "tables:\n  c:\n    columns:\n      id:\n        type: INTEGER\n        primary: true\n" +
					"        not_null: true\n      n:\n        type: \"ENUM('happy', ' sad', 'ok ') CHARACTER SET utf8mb4\"\n"
			},
			table: func(string) string { return "c" },
		},
	}
	for _, engine := range engines {
		for _, document := range documents {
			t.Run(engine.name+"/"+document.name, func(t *testing.T) {
				c := qt.New(t)
				name, database := newMySQLFamilyScratch(c, engine.admin).builtFrom(c,
					"CREATE TABLE c (id INTEGER PRIMARY KEY NOT NULL, n ENUM('happy', ' sad', 'ok '));\n"+
						"INSERT INTO c VALUES (1, ' sad');")
				schema := filepath.Join(c.TempDir(), document.file)
				c.Assert(os.WriteFile(schema, []byte(document.document(name)), 0o600), qt.IsNil)

				got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
					"schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")

				c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", got.Stdout))
				c.Assert(got.Stderr, qt.Contains, `column "n" of table "`+document.table(name)+`" declares the enum value "ok ", and `+
					engine.dialect+` removes the trailing spaces of an ENUM value, so the column would never `+
					`match its declaration: declare "ok"`+keepsTheSpace)
				c.Assert(enumBytesQuery(c, database, "SELECT COLUMN_TYPE FROM information_schema.COLUMNS "+
					"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'c' AND COLUMN_NAME = 'n'"),
					qt.Equals, "enum('happy',' sad','ok')")
				c.Assert(enumBytesQuery(c, database, "SELECT CONCAT('[', n, ']') FROM c"), qt.Equals, "[ sad]")
			})
		}
	}
}

// TestSetTrailingSpaceE2E_FailurePath declares 'c ' beside the column the
// server built from SET('a', ' b', 'c ') and holds as set('a',' b','c'):
// measured on MySQL 8.4.13 and MariaDB 11.8.9, the server removes the trailing
// space of a member and keeps a leading one, as it does for an ENUM value.
// Built as declared, the column would never match its declaration, and every
// apply would run the same MODIFY COLUMN, so the apply is refused, and the
// column and its row are left as they were.
func TestSetTrailingSpaceE2E_FailurePath(t *testing.T) {
	engines := []struct {
		name    string
		admin   dbtarget.Engine
		dialect string
	}{
		{name: "MySQL", admin: dbtarget.MySQLAdmin, dialect: "mysql"},
		{name: "MariaDB", admin: dbtarget.MariaDBAdmin, dialect: "mariadb"},
	}
	documents := []struct {
		name, file      string
		document, table func(schema string) string
	}{
		{
			name: "YAML", file: "schema.yaml",
			document: func(string) string {
				return "tables:\n  c:\n    columns:\n      id:\n        type: INTEGER\n        primary: true\n" +
					"        not_null: true\n      n:\n        type: \"SET('a',' b','c ')\"\n"
			},
			table: func(string) string { return "c" },
		},
		{
			name: "HCL", file: "schema.hcl",
			document: func(schema string) string {
				return trailingSpaceHCLDocument(schema, `set("a", " b", "c ")`)
			},
			table: func(schema string) string { return schema + ".c" },
		},
	}
	for _, engine := range engines {
		for _, document := range documents {
			t.Run(engine.name+"/"+document.name, func(t *testing.T) {
				c := qt.New(t)
				name, database := newMySQLFamilyScratch(c, engine.admin).builtFrom(c,
					"CREATE TABLE c (id INTEGER PRIMARY KEY NOT NULL, n SET('a', ' b', 'c '));\n"+
						"INSERT INTO c VALUES (1, ' b,c');")
				schema := filepath.Join(c.TempDir(), document.file)
				c.Assert(os.WriteFile(schema, []byte(document.document(name)), 0o600), qt.IsNil)

				got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
					"schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")

				c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", got.Stdout))
				c.Assert(got.Stderr, qt.Contains, `column "n" of table "`+document.table(name)+`" declares the SET member "c ", and `+
					engine.dialect+` removes the trailing spaces of a SET member, so the column would never `+
					`match its declaration: declare "c"`+keepsTheSpace)
				c.Assert(enumBytesQuery(c, database, "SELECT COLUMN_TYPE FROM information_schema.COLUMNS "+
					"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'c' AND COLUMN_NAME = 'n'"),
					qt.Equals, "set('a',' b','c')")
				c.Assert(enumBytesQuery(c, database, "SELECT CONCAT('[', n, ']') FROM c"), qt.Equals, "[ b,c]")
			})
		}
	}
}

// binaryEnumEngines are the servers a binary ENUM or SET column is built on.
var binaryEnumEngines = []struct {
	name  string
	admin dbtarget.Engine
}{
	{name: "MySQL", admin: dbtarget.MySQLAdmin},
	{name: "MariaDB", admin: dbtarget.MariaDBAdmin},
}

// TestEnumTrailingSpaceE2E_ReadsABinaryColumn reads a database holding 'ok '
// and 'm ' in an ENUM and a SET column of the binary character set, which
// keeps the trailing space of a value: measured on MySQL 8.4 and MariaDB 11.8,
// the column type holds 'ok ' and the row stores 6F6B20. Each read verb
// describes the column with its character set, and the HCL description,
// applied back, plans nothing. Refused as a declaration the server would not
// keep, the read verbs could not describe this database at all, and without
// the character set the description would build a column that removes the
// space.
func TestEnumTrailingSpaceE2E_ReadsABinaryColumn(t *testing.T) {
	const (
		enumColumn = "`b` enum('ok ','x') CHARACTER SET binary COLLATE binary"
		setColumn  = "`s` set('m ','n') CHARACTER SET binary COLLATE binary"
	)
	for _, engine := range binaryEnumEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			_, database := newMySQLFamilyScratch(c, engine.admin).builtFrom(c,
				"CREATE TABLE c (id INT PRIMARY KEY NOT NULL, b ENUM('ok ','x') CHARACTER SET binary, "+
					"s SET('m ','n') CHARACTER SET binary);\nINSERT INTO c VALUES (1, 'ok ', 'm ');")
			dir := c.TempDir()

			read := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, "db", "read", "--db-url", database)
			sql := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				"schema", "inspect", "--db-url", database, "--format", "sql")
			compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
				"schema", "inspect", "--url", database, "--format", "{{ sql . }}")
			hcl := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, "schema", "inspect", "--db-url", database)

			c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", read.Stderr))
			c.Assert(read.Stdout, qt.Contains, enumColumn)
			c.Assert(read.Stdout, qt.Contains, setColumn)
			c.Assert(sql.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", sql.Stderr))
			c.Assert(sql.Stdout, qt.Contains, enumColumn)
			c.Assert(sql.Stdout, qt.Contains, setColumn)
			c.Assert(compat.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", compat.Stderr))
			c.Assert(compat.Stdout, qt.Contains, "`b` enum('ok ','x') CHARACTER SET binary COLLATE binary NULL")
			c.Assert(hcl.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hcl.Stderr))
			c.Assert(hcl.Stdout, qt.Contains, `charset = "binary"`)

			schema := filepath.Join(dir, "inspected.hcl")
			c.Assert(os.WriteFile(schema, []byte(hcl.Stdout), 0o600), qt.IsNil)
			synced := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				"schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")

			c.Assert(synced.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", synced.Stderr))
			c.Assert(synced.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(enumBytesQuery(c, database, "SELECT CONCAT(HEX(b), ' ', HEX(s)) FROM c"), qt.Equals, "6F6B20 6D20")
		})
	}
}

// TestEnumTrailingSpaceE2E_AppliesABinaryColumn declares 'ok ' in a column of
// the binary character set, which keeps the space, so the apply builds the
// column with it and a second plan is synced. A refusal here would name a
// space the server does not remove.
func TestEnumTrailingSpaceE2E_AppliesABinaryColumn(t *testing.T) {
	documents := []struct {
		name, file string
		document   func(schema string) string
	}{
		{
			name: "HCL", file: "schema.hcl",
			document: func(schema string) string {
				return strings.Replace(trailingSpaceHCLDocument(schema, `"enum('ok ','x')"`),
					"    null = true\n", "    null = true\n    charset = \"binary\"\n", 1)
			},
		},
		{
			name: "YAML", file: "schema.yaml",
			document: func(string) string {
				return enumBytesMySQLDocument("['ok ', x]") +
					"        platform:\n          mysql: {charset: binary}\n          mariadb: {charset: binary}\n"
			},
		},
	}
	for _, engine := range binaryEnumEngines {
		for _, document := range documents {
			t.Run(engine.name+"/"+document.name, func(t *testing.T) {
				c := qt.New(t)
				name, database := newMySQLFamilyScratch(c, engine.admin).database(c, "enum_binary")
				schema := filepath.Join(c.TempDir(), document.file)
				c.Assert(os.WriteFile(schema, []byte(document.document(name)), 0o600), qt.IsNil)

				applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
					"schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve")
				synced := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
					"schema", "apply", "--db-url", database, "--schema-file", schema, "--dry-run")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
				c.Assert(enumBytesQuery(c, database, "SELECT COLUMN_TYPE FROM information_schema.COLUMNS "+
					"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'c' AND COLUMN_NAME = 'n'"), qt.Equals, "enum('ok ','x')")
				c.Assert(synced.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", synced.Stderr))
				c.Assert(synced.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}
