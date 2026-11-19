//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// MySQL and MariaDB store a CHECK condition, a generated column's expression
// and an expression default in a spelling of their own: MySQL groups every
// operand in parentheses, writes NOT into the operation it negates, writes
// X'41' as 0x41 and introduces each string, and MariaDB writes NOT the same
// way. MySQL's information_schema also reports these expressions encoded once
// more, with each byte of a character beyond ASCII as a character of its own.
// The reader decodes the text and the comparison reads both sides by the
// operator precedence the servers share, so a second run plans nothing and a
// changed value is still a change.

// mysqlFamilyEngines are the MySQL-family servers a row runs on.
var mysqlFamilyEngines = []dbtarget.Engine{dbtarget.MySQLAdmin, dbtarget.MariaDBAdmin}

// expressionConverging are tables each server stores in a spelling other
// than the declared one.
var expressionConverging = []struct {
	name   string
	schema string
}{
	{name: "CHECK with BETWEEN inside AND", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (id between 1 and 10 and c is not null));`},
	{name: "CHECK with a string beyond ASCII", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'héllo ☃'));`},
	{name: "CHECK with NOT LIKE", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c NOT LIKE 'x%'));`},
	{name: "CHECK with X'41'", schema: `CREATE TABLE t (id int PRIMARY KEY, b varbinary(10), CONSTRAINT k CHECK (b <> X'41'));`},
	{name: "CHECK with an introducer apart from its string", schema: `CREATE TABLE t (id int PRIMARY KEY, b varbinary(10), CONSTRAINT k CHECK (b <> _binary 'ab'));`},
	{name: "CHECK with NOT over a column", schema: `CREATE TABLE t (id int PRIMARY KEY, a int, b int, CONSTRAINT k CHECK (NOT a OR b > 0));`},
	{name: "CHECK with NOT over AND", schema: `CREATE TABLE t (id int PRIMARY KEY, a int, b int, CONSTRAINT k CHECK (NOT (a = 1 AND b = 2)));`},
	{name: "CHECK with IN over one member", schema: `CREATE TABLE t (id int PRIMARY KEY, a int, CONSTRAINT k CHECK (a IN (1)));`},
	{name: "CHECK with NOT REGEXP", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c NOT REGEXP '^x'));`},
	{name: "generated column with a quote", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), d varchar(40) GENERATED ALWAYS AS (concat(c, 'it''s')) VIRTUAL);`},
	{name: "generated column with a string beyond ASCII", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), d varchar(40) GENERATED ALWAYS AS (concat(c, 'é☃')) VIRTUAL);`},
	{name: "generated column with arithmetic", schema: `CREATE TABLE t (id int PRIMARY KEY, a int, d int GENERATED ALWAYS AS (a * 2 + 1) VIRTUAL);`},
	{name: "expression default with a quote", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20) DEFAULT (concat('it''s', 'x')));`},
	{name: "expression default with a string beyond ASCII", schema: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20) DEFAULT (concat('é☃', 'x')));`},
}

// TestMySQLFamilyExpressionsConvergeE2E applies each table to an empty
// database and holds the next run to no change.
func TestMySQLFamilyExpressionsConvergeE2E(t *testing.T) {
	for _, row := range expressionConverging {
		for _, engine := range mysqlFamilyEngines {
			t.Run(row.name+"/"+engine.String(), func(t *testing.T) {
				c := qt.New(t)
				_, url := newMySQLFamilyScratch(c, engine).database(c, "expr_apply")
				file := quotedTextSchemaFile(c, row.schema)

				applied := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", file, "--auto-approve")
				c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
				again := runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", file, "--dry-run")

				c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
				c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			})
		}
	}
}

// expressionChanged are the tables of [expressionConverging] built by the
// server, and declarations with one value changed. Each is a change the
// reading by grouping must not fold away.
var expressionChanged = []quotedTextRow{
	{
		name:    "CHECK with BETWEEN inside AND",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (id between 1 and 10 and c is not null));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (id between 1 and 11 and c is not null));`,
		planned: `CHECK (id between 1 and 11 and c is not null);`,
	},
	{
		name:    "CHECK with a string beyond ASCII",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'héllo ☃'));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'héllo ☂'));`,
		planned: `CHECK (c <> 'héllo ☂');`,
	},
	{
		name:    "CHECK with NOT LIKE against LIKE",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c NOT LIKE 'x%'));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c LIKE 'x%'));`,
		planned: `CHECK (c LIKE 'x%');`,
	},
	{
		name:    "CHECK with X'41' against X'42'",
		live:    `CREATE TABLE t (id int PRIMARY KEY, b varbinary(10), CONSTRAINT k CHECK (b <> X'41'));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, b varbinary(10), CONSTRAINT k CHECK (b <> X'42'));`,
		planned: `CHECK (b <> X'42');`,
	},
	{
		name:    "CHECK with an introducer apart from another string",
		live:    `CREATE TABLE t (id int PRIMARY KEY, b varbinary(10), CONSTRAINT k CHECK (b <> _binary 'ab'));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, b varbinary(10), CONSTRAINT k CHECK (b <> _binary 'ac'));`,
		planned: `CHECK (b <> _binary 'ac');`,
	},
	{
		name:    "CHECK with NOT over a column against the column",
		live:    `CREATE TABLE t (id int PRIMARY KEY, a int, b int, CONSTRAINT k CHECK (NOT a OR b > 0));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, a int, b int, CONSTRAINT k CHECK (a OR b > 0));`,
		planned: `CHECK (a OR b > 0);`,
	},
	{
		name:    "generated column with another quoted string",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), d varchar(40) GENERATED ALWAYS AS (concat(c, 'it''s')) VIRTUAL);`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), d varchar(40) GENERATED ALWAYS AS (concat(c, 'its')) VIRTUAL);`,
		planned: `concat(c, 'its')`,
	},
	{
		name:    "generated column with another string beyond ASCII",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), d varchar(40) GENERATED ALWAYS AS (concat(c, 'é☃')) VIRTUAL);`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), d varchar(40) GENERATED ALWAYS AS (concat(c, 'é☂')) VIRTUAL);`,
		planned: `concat(c, 'é☂')`,
	},
	{
		name:    "expression default with another quoted string",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20) DEFAULT (concat('it''s', 'x')));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20) DEFAULT (concat('its', 'x')));`,
		planned: `concat('its', 'x')`,
	},
	{
		name:    "expression default with another string beyond ASCII",
		live:    `CREATE TABLE t (id int PRIMARY KEY, c varchar(20) DEFAULT (concat('é☃', 'x')));`,
		desired: `CREATE TABLE t (id int PRIMARY KEY, c varchar(20) DEFAULT (concat('é☂', 'x')));`,
		planned: `concat('é☂', 'x')`,
	},
}

// TestMySQLFamilyChangedExpressionsPlanE2E holds each changed expression to a
// planned change.
func TestMySQLFamilyChangedExpressionsPlanE2E(t *testing.T) {
	for _, row := range expressionChanged {
		for _, engine := range mysqlFamilyEngines {
			t.Run(row.name+"/"+engine.String(), func(t *testing.T) {
				c := qt.New(t)
				_, url := newMySQLFamilyScratch(c, engine).builtFrom(c, row.live)

				planned := quotedTextDryRun(c, url, row.desired)

				c.Assert(planned.ExitCode, qt.Equals, 0, historyComment(planned))
				c.Assert(planned.Stdout, qt.Contains, row.planned)
			})
		}
	}
}

// expressionsReadTable holds an expression of each kind MySQL's
// information_schema encodes, once with a quote and once with characters
// beyond ASCII. It is created over the Go driver, whose connection character
// set is utf8mb4, so MySQL introduces each string with _utf8mb4.
const expressionsReadTable = `CREATE TABLE t (id int PRIMARY KEY, c varchar(20),
	dq varchar(40) DEFAULT (concat('it''s', 'x')),
	du varchar(40) DEFAULT (concat('é☃', 'x')),
	gq varchar(40) GENERATED ALWAYS AS (concat(c, 'it''s')) VIRTUAL,
	gu varchar(40) GENERATED ALWAYS AS (concat(c, 'é☃')) VIRTUAL,
	CONSTRAINT kq CHECK (c <> 'it''s'),
	CONSTRAINT ku CHECK (c <> 'é☃'))`

// expressionsIndexes adds an index on an expression holding a quote and one on
// an expression holding characters beyond ASCII. MariaDB refuses an expression
// as a key part, so these run on MySQL alone, and Ptah's parser declines a key
// expression beyond ASCII, so the table is built here and read back rather
// than applied from a schema file.
const expressionsIndexes = `;
	CREATE INDEX iq ON t ((concat(c, 'it''s')));
	CREATE INDEX iu ON t ((concat(c, 'é☃')))`

// TestMySQLFamilyReaderDecodesExpressionsE2E reads each expression back
// through the reader every command uses, and holds it to the SQL the server
// was given: on MySQL with the extra escaping and encoding taken off, on
// MariaDB as the server reports it.
func TestMySQLFamilyReaderDecodesExpressionsE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		schema string
		want   map[string]string
	}{
		{
			name:   "mysql",
			engine: dbtarget.MySQLAdmin,
			schema: expressionsReadTable + expressionsIndexes,
			want: map[string]string{
				"default dq":    "(concat(_utf8mb4'it\\'s',_utf8mb4'x'))",
				"default du":    "(concat(_utf8mb4'é☃',_utf8mb4'x'))",
				"generated gq":  "concat(`c`,_utf8mb4'it\\'s')",
				"generated gu":  "concat(`c`,_utf8mb4'é☃')",
				"check kq":      "(`c` <> _utf8mb4'it\\'s')",
				"check ku":      "(`c` <> _utf8mb4'é☃')",
				"index iq part": "concat(`c`,_utf8mb4'it\\'s')",
				"index iu part": "concat(`c`,_utf8mb4'é☃')",
			},
		},
		{
			name:   "mariadb",
			engine: dbtarget.MariaDBAdmin,
			schema: expressionsReadTable,
			want: map[string]string{
				"default dq":   "concat('it\\'s','x')",
				"default du":   "concat('é☃','x')",
				"generated gq": "concat(`c`,'it\\'s')",
				"generated gu": "concat(`c`,'é☃')",
				"check kq":     "`c` <> 'it\\'s'",
				"check ku":     "`c` <> 'é☃'",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, url := newMySQLFamilyScratch(c, test.engine).builtFrom(c, test.schema)
			conn, err := dbschema.ConnectToDatabase(c.Context(), url)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

			read, err := conn.Reader().ReadSchemaContext(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(readExpressions(read), qt.DeepEquals, test.want)
		})
	}
}

// readExpressions answers the expression defaults of dq and du, the generated
// expressions of gq and gu, and every CHECK condition and index expression of
// a read, keyed by kind and name.
func readExpressions(read *catalog.Database) map[string]string {
	got := make(map[string]string)
	for _, table := range read.Tables {
		for _, column := range table.Columns {
			if column.ColumnDefault != nil && (column.Name == "dq" || column.Name == "du") {
				got["default "+column.Name] = *column.ColumnDefault
			}
			if column.GeneratedExpression != nil {
				got["generated "+column.Name] = *column.GeneratedExpression
			}
		}
	}
	for _, check := range read.Constraints {
		if check.CheckClause != nil {
			got["check "+check.Name] = *check.CheckClause
		}
	}
	for _, index := range read.Indexes {
		for _, part := range index.Parts {
			if part.Expr != "" {
				got["index "+index.Name+" part"] = part.Expr
			}
		}
	}
	return got
}

// droppedCheckTable holds a CHECK whose string holds a quote, which MySQL's
// information_schema escapes once more, and characters beyond ASCII, which it
// encodes once more.
const droppedCheckTable = `CREATE TABLE t (id int PRIMARY KEY, c varchar(20), CONSTRAINT k CHECK (c <> 'it''s é☃'))`

// TestCompatMigrateDownRestoresADroppedCheckE2E rolls back a migration that
// drops a CHECK and holds the restored CHECK to the one the declaration
// creates.
//
// `ptah migrations generate` writes the drop and its down file from a database
// holding the CHECK, under the names it gives them, and `ptah-compat migrate
// hash`, `migrate apply --allow-dirty` and `migrate down` drive that directory
// against the same database. Two defects redden it. Restored from a clause
// read with the extra encoding left on, the CHECK compares the column with
// other characters than the declaration names. Read as a Ptah directory, as
// the native default reads a directory of .up.sql files, the rollback compares
// the atlas.sum hash `migrate apply` recorded with the file's hex digest and
// refuses with `checksum mismatch`.
func TestCompatMigrateDownRestoresADroppedCheckE2E(t *testing.T) {
	for _, engine := range mysqlFamilyEngines {
		t.Run(engine.String(), func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine)
			targetName, target := scratch.builtFrom(c, droppedCheckTable)
			referenceName, _ := scratch.builtFrom(c, droppedCheckTable)
			root := c.TempDir()
			generateCheckDrop(c, target, filepath.Join(root, "m"))
			options := clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()}

			hashed := clirun.Run(c, clirun.Compat, options, "migrate", "hash", "--dir", "file://m")
			c.Assert(hashed.ExitCode, qt.Equals, 0, historyComment(hashed))
			applied := clirun.Run(c, clirun.Compat, options, "migrate", "apply", "--url", target, "--dir", "file://m", "--allow-dirty")
			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(checkClauseBytes(c, scratch, targetName), qt.Equals, "")
			rolledBack := clirun.Run(c, clirun.Compat, options, "migrate", "down", "--url", target, "--dir", "file://m")

			c.Assert(rolledBack.ExitCode, qt.Equals, 0, historyComment(rolledBack))
			c.Assert(checkClauseBytes(c, scratch, targetName), qt.Equals, checkClauseBytes(c, scratch, referenceName))
			c.Assert(checkClauseBytes(c, scratch, referenceName), qt.Not(qt.Equals), "")
		})
	}
}

// generateCheckDrop has `ptah migrations generate` write, into dir, the
// migration that drops the CHECK of [droppedCheckTable] from the database at
// source, which holds it.
func generateCheckDrop(c *qt.C, source, dir string) {
	c.Helper()
	desired := quotedTextSchemaFile(c, "CREATE TABLE t (id int PRIMARY KEY, c varchar(20));")
	generated := runPtahIn(c, "migrations", "generate", "--db-url", source, "--schema-file", desired,
		"--migrations-dir", dir, "--name", "drop_check")
	c.Assert(generated.ExitCode, qt.Equals, 0, historyComment(generated))
	written, err := filepath.Glob(filepath.Join(dir, "*_drop_check.*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 2)
}

// checkClauseBytes answers, in hexadecimal, the CHECK_CLAUSE the server
// reports for the CHECK k of database, or "" where it holds none. The bytes
// are compared as the server holds them, with no reader in between.
func checkClauseBytes(c *qt.C, scratch mysqlScratch, database string) string {
	c.Helper()
	var clause string
	err := scratch.admin.QueryRowContext(c.Context(),
		`SELECT COALESCE(MAX(HEX(CHECK_CLAUSE)), '') FROM information_schema.CHECK_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = ? AND CONSTRAINT_NAME = 'k'`, database,
	).Scan(&clause)
	c.Assert(err, qt.IsNil)
	return clause
}
