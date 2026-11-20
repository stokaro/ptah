//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// The declared text of each literal holds a line that reads like a client
// delimiter directive, in both of its spellings. A script reader that takes
// the line for a directive removes it from the literal, so the database
// stores something other than what the model declares.
const (
	directiveTableComment  = "c\n-- atlas:delimiter $$\nd"
	directiveColumnComment = "x\nDELIMITER //\n/* y */ z"
	directiveDefault       = "a\n-- atlas:delimiter ~~\nDELIMITER $$\nb"
)

// directiveLiteralEngine is an engine the literals are applied to and the
// query that reads back what its catalog holds for them as a JSON array.
type directiveLiteralEngine struct {
	name string
	url  func(c *qt.C) string
	// attributes are the table and column comments the engine stores; SQLite
	// has no comment catalog, so its model declares none.
	tableAttributes, columnAttributes string
	read                              string
	// declared is the text the model declares, in the order read answers it.
	declared []string
}

var directiveLiteralEngines = []directiveLiteralEngine{
	{
		name: "postgresql",
		url: func(c *qt.C) string {
			target, _ := scratchReplayDatabase(c)
			return target
		},
		tableAttributes:  ` comment="` + escapeTagText(directiveTableComment) + `"`,
		columnAttributes: ` comment="` + escapeTagText(directiveColumnComment) + `"`,
		read: "SELECT json_build_array(n, obj_description('notes'::regclass, 'pg_class'), " +
			"col_description('notes'::regclass, 2))::text FROM notes",
		declared: []string{directiveDefault, directiveTableComment, directiveColumnComment},
	},
	{
		name:     "sqlite",
		url:      sqliteScratchURL,
		read:     "SELECT json_array(n) FROM notes",
		declared: []string{directiveDefault},
	},
}

// escapeTagText spells text's newlines the way a struct tag writes them.
func escapeTagText(text string) string {
	return strings.ReplaceAll(text, "\n", `\n`)
}

// writeDirectiveLiteralModel writes the Go model of test to a directory of its
// own and returns the directory.
func writeDirectiveLiteralModel(c *qt.C, test directiveLiteralEngine) string {
	c.Helper()
	dir := c.TempDir()
	source := "package entities\n\n" +
		"//ptah:schema:table name=\"notes\"" + test.tableAttributes + "\n" +
		"type Note struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n" +
		"\tID int\n" +
		"\t//ptah:schema:field name=\"n\" type=\"TEXT\" default=\"" + escapeTagText(directiveDefault) + "\"" + test.columnAttributes + "\n" +
		"\tN string\n" +
		"}\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "notes.go"), []byte(source), 0o600), qt.IsNil)
	return dir
}

// readDirectiveLiterals inserts a row that takes the default and returns what
// the engine's catalog holds for the literals, parsed out of read's JSON.
func readDirectiveLiterals(c *qt.C, database, read string) []string {
	c.Helper()
	var got []string
	c.Assert(json.Unmarshal([]byte(insertDefaultsAndRead(c, database, read)), &got), qt.IsNil)
	return got
}

// TestMigrationsGenerateAndUpKeepALiteralHoldingADirectiveLineE2E generates a
// migration for a model whose default and comments hold directive-looking
// lines, applies it with `migrations up`, and reads the literals back from the
// database: each holds the text the model declares.
func TestMigrationsGenerateAndUpKeepALiteralHoldingADirectiveLineE2E(t *testing.T) {
	for _, test := range directiveLiteralEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := test.url(c)
			migrations := filepath.Join(c.TempDir(), "migrations")

			generated, generateErr := runPtahNativeWithError("migrations", "generate", "--db-url", database,
				"--root-dir", writeDirectiveLiteralModel(c, test), "--migrations-dir", migrations, "--name", "notes")
			applied, applyErr := runPtahNativeWithError("migrations", "up", "--db-url", database, "--migrations-dir", migrations)

			c.Assert(generateErr, qt.IsNil, qt.Commentf("%s", generated))
			c.Assert(applyErr, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(readDirectiveLiterals(c, database, test.read), qt.DeepEquals, test.declared)
		})
	}
}

// TestSchemaApplyKeepsALiteralHoldingADirectiveLineE2E sends the same model
// through `schema apply`, which splits what it renders before it reaches the
// server, and reads the literals back.
func TestSchemaApplyKeepsALiteralHoldingADirectiveLineE2E(t *testing.T) {
	for _, test := range directiveLiteralEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := test.url(c)

			applied := runPtahIn(c, "schema", "apply", "--db-url", database,
				"--root-dir", writeDirectiveLiteralModel(c, test), "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(readDirectiveLiterals(c, database, test.read), qt.DeepEquals, test.declared)
		})
	}
}

// TestMigrationsUpRunsAMySQLRoutineBesideDollarNamesE2E applies a MySQL
// migration whose names hold dollar signs and whose procedure is written
// between DELIMITER lines. A dollar sign is a name character on MySQL, so
// `a$b$` opens no quote, the procedure body keeps its semicolons, and the
// statements after it run: the procedure exists and has inserted its rows.
func TestMigrationsUpRunsAMySQLRoutineBesideDollarNamesE2E(t *testing.T) {
	c := qt.New(t)
	database := mysqlFamilyScratchURL(c, dbtarget.MySQLAdmin, "routine")
	migrations := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
	script := "-- atlas:txmode none\n" +
		"CREATE TABLE a$b$ (id INT PRIMARY KEY, n VARCHAR(40));\n" +
		"DELIMITER $$\n" +
		"CREATE PROCEDURE p$q$() BEGIN INSERT INTO a$b$ VALUES (1, 'x'); INSERT INTO a$b$ VALUES (2, 'it''s'); END$$\n" +
		"DELIMITER ;\n" +
		"CALL p$q$();\n"
	c.Assert(os.WriteFile(filepath.Join(migrations, "20240101000000_routine.up.sql"), []byte(script), 0o600), qt.IsNil)

	applied, err := runPtahNativeWithError("migrations", "up", "--db-url", database, "--migrations-dir", migrations)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var got string
	query := "SELECT CONCAT_WS(',', (SELECT GROUP_CONCAT(n ORDER BY id) FROM `a$b$`), " +
		"(SELECT routine_name FROM information_schema.routines WHERE routine_schema = DATABASE()))"
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&got), qt.IsNil)
	c.Assert(got, qt.Equals, "x,it's,p$q$")
}
