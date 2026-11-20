//go:build integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// noBackslashEscapesEngines are the MySQL-family servers a session under
// NO_BACKSLASH_ESCAPES is opened on.
var noBackslashEscapesEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "mysql", engine: dbtarget.MySQLAdmin},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin},
}

// noBackslashEscapesDatabase creates an empty database on engine's server and
// returns an address whose sessions add NO_BACKSLASH_ESCAPES to the server's
// sql_mode, after checking that the mode reached a session.
func noBackslashEscapesDatabase(c *qt.C, engine dbtarget.Engine) string {
	c.Helper()
	database := withSQLMode(c, mysqlFamilyScratchURL(c, engine, "no_backslash_escapes"), noBackslashEscapesMode)
	c.Assert(sessionSQLMode(c, database), qt.Contains, "NO_BACKSLASH_ESCAPES")
	return database
}

// readNoBackslashEscapes answers query on a session of database.
func readNoBackslashEscapes(c *qt.C, database, query string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var got string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&got), qt.IsNil)
	return got
}

// writeNoBackslashEscapesSchema writes text to a schema file of its own and returns its path.
func writeNoBackslashEscapesSchema(c *qt.C, text string) string {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(text), 0o600), qt.IsNil)
	return file
}

// quoteOnlyModel declares a default, a comment and an enum value that hold a
// quote and no backslash, which read the same under every sql_mode.
var quoteOnlyModel = declaredModel{
	source: "package entities\n\n" +
		"//ptah:schema:table name=\"notes\"\n" +
		"type Note struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INT\" primary=\"true\"\n" +
		"\tID int\n" +
		"\t//ptah:schema:field name=\"a\" type=\"VARCHAR(30)\" default=\"it's;x\" comment=\"c'd\"\n" +
		"\tA string\n" +
		"\t//ptah:schema:field name=\"e\" type=\"ENUM\" enum=\"e'f,g\" default=\"g\"\n" +
		"\tE string\n" +
		"}\n",
	file: "notes.go",
	flag: "--root-dir",
}

// quoteOnlyRead reads, in hexadecimal, the default column a gives a row, its
// comment and the type of enum column e; quoteOnlyWant is what
// [quoteOnlyModel] stores.
const (
	quoteOnlyRead = "SELECT CONCAT_WS(',', HEX(a), (SELECT HEX(COLUMN_COMMENT) FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notes' AND COLUMN_NAME = 'a'), (SELECT HEX(COLUMN_TYPE) " +
		"FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notes' AND COLUMN_NAME = 'e')) FROM notes"
	quoteOnlyWant = "697427733B78,632764,656E756D282765272766272C27672729"
)

// TestApplyWithoutABackslashLiteralWithoutBackslashEscapesE2E applies
// [quoteOnlyModel] through a session under NO_BACKSLASH_ESCAPES. A doubled quote reads the same under both modes, so the
// server stores the characters the model declares and the second apply plans
// nothing.
func TestApplyWithoutABackslashLiteralWithoutBackslashEscapesE2E(t *testing.T) {
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := noBackslashEscapesDatabase(c, row.engine)

			got := applyEscapedLiteralModel(c, database, quoteOnlyModel, quoteOnlyRead)

			c.Assert(got, qt.Equals, quoteOnlyWant)
		})
	}
}

// TestApplySendsASchemaFileBodyAsWrittenWithoutBackslashEscapesE2E applies a
// schema file whose function and view bodies hold a backslash through a
// session under NO_BACKSLASH_ESCAPES. A body is the user's own SQL, so it
// reaches the server byte for byte and the session reads it: `'C:\path'` is
// C, a colon, a backslash and path, and `'a\\b'` holds two backslashes.
func TestApplySendsASchemaFileBodyAsWrittenWithoutBackslashEscapesE2E(t *testing.T) {
	schema := "CREATE TABLE notes (id int PRIMARY KEY);\n" +
		"CREATE FUNCTION path_of() RETURNS varchar(30) DETERMINISTIC RETURN 'C:\\path';\n" +
		"CREATE VIEW slashes AS SELECT 'a\\\\b' AS x;\n"
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := noBackslashEscapesDatabase(c, row.engine)

			applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", writeNoBackslashEscapesSchema(c, schema), "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(readNoBackslashEscapes(c, database, "SELECT CONCAT_WS(',', HEX(path_of()), (SELECT HEX(x) FROM slashes))"),
				qt.Equals, "433A5C70617468,615C5C62")
		})
	}
}

// TestApplySendsEditedSQLAsWrittenWithoutBackslashEscapesE2E applies a schema
// file with --edit through a session under NO_BACKSLASH_ESCAPES, with an
// editor that appends a statement setting a default of 'C:\p\' and one more
// statement after it. Edited SQL is the user's own, so it reaches the server
// byte for byte, and it is cut where that session ends the string, after the
// last backslash: column u stores C, a colon, a backslash, p and a backslash,
// and column v gets its default too.
func TestApplySendsEditedSQLAsWrittenWithoutBackslashEscapesE2E(t *testing.T) {
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := noBackslashEscapesDatabase(c, row.engine)
			dir := c.TempDir()
			appended := filepath.Join(dir, "appended.sql")
			c.Assert(os.WriteFile(appended, []byte("ALTER TABLE notes ALTER COLUMN u SET DEFAULT 'C:\\p\\'; ALTER TABLE notes ALTER COLUMN v SET DEFAULT 'y';\n"),
				0o600), qt.IsNil)
			editor := filepath.Join(dir, "append.sh")
			script := fmt.Sprintf("#!/bin/sh\ncat \"$1\" %q > \"$1.new\" && mv \"$1.new\" \"$1\"\n", appended)
			c.Assert(os.WriteFile(editor, []byte(script), 0o700), qt.IsNil) // #nosec G306 -- the editor must be executable
			env := append(os.Environ(), "VISUAL="+editor, "PTAH_ALLOW_NONINTERACTIVE_EDIT=1")
			schema := writeNoBackslashEscapesSchema(c, "CREATE TABLE notes (id int PRIMARY KEY, u varchar(30), v varchar(30));\n")

			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: env},
				"schema", "apply", "--db-url", database, "--schema-file", schema, "--edit", "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(insertDefaultsAndRead(c, database, "SELECT CONCAT_WS(',', HEX(u), HEX(v)) FROM notes"), qt.Equals, "433A5C705C,79")
		})
	}
}

// backslashRowsEntities declares a table whose rows a file carries.
const backslashRowsEntities = `package entities

//ptah:schema:data table="paths" key="name" file="paths.yaml"
//ptah:schema:table name="paths"
type Path struct {
	//ptah:schema:field name="name" type="VARCHAR(40)" primary="true"
	Name string
	//ptah:schema:field name="note" type="VARCHAR(40)" not_null="true"
	Note string
}
`

// backslashRowsRead reads every row of paths in hexadecimal, as name:note.
const backslashRowsRead = "SELECT GROUP_CONCAT(CONCAT(HEX(name), ':', HEX(note)) ORDER BY name SEPARATOR ',') FROM paths"

// sessionModeRows are the engines with a session under each sql_mode, applied
// alone and with a dev database whose session is under the same sql_mode, on
// which the plan is rehearsed first.
var sessionModeRows = []struct {
	name      string
	engine    dbtarget.Engine
	sqlMode   string
	rehearsed bool
}{
	{name: "mysql", engine: dbtarget.MySQLAdmin, sqlMode: serverSQLMode},
	{name: "mysql, rehearsed", engine: dbtarget.MySQLAdmin, sqlMode: serverSQLMode, rehearsed: true},
	{name: "mysql without backslash escapes", engine: dbtarget.MySQLAdmin, sqlMode: noBackslashEscapesMode},
	{name: "mysql without backslash escapes, rehearsed", engine: dbtarget.MySQLAdmin, sqlMode: noBackslashEscapesMode, rehearsed: true},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin, sqlMode: serverSQLMode},
	{name: "mariadb, rehearsed", engine: dbtarget.MariaDBAdmin, sqlMode: serverSQLMode, rehearsed: true},
	{name: "mariadb without backslash escapes", engine: dbtarget.MariaDBAdmin, sqlMode: noBackslashEscapesMode},
	{name: "mariadb without backslash escapes, rehearsed", engine: dbtarget.MariaDBAdmin, sqlMode: noBackslashEscapesMode, rehearsed: true},
}

// devArguments returns the --dev-url arguments of a rehearsed row: a dev
// database on engine's server whose session is under sqlMode.
func devArguments(c *qt.C, engine dbtarget.Engine, sqlMode string, rehearsed bool) []string {
	c.Helper()
	arguments := map[bool]func() []string{
		false: func() []string { return nil },
		true: func() []string {
			return []string{"--dev-url", withSQLMode(c, mysqlFamilyScratchURL(c, engine, "dev"), sqlMode)}
		},
	}
	return arguments[rehearsed]()
}

// TestApplyDeclaredRowsWithABackslashE2E applies declared rows whose key and
// values hold a backslash, one of them last and one before a quote and a
// semicolon, then changes the value of the row whose key holds one, on a
// session under each sql_mode. Each apply stores the characters the file
// declares: the insert, and the update whose WHERE clause names the key. The
// third apply plans nothing.
func TestApplyDeclaredRowsWithABackslashE2E(t *testing.T) {
	for _, row := range sessionModeRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "backslash_rows"), row.sqlMode)
			dev := devArguments(c, row.engine, row.sqlMode, row.rehearsed)
			root := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(backslashRowsEntities), 0o600), qt.IsNil)
			rowFile := filepath.Join(root, "paths.yaml")
			apply := func() clirun.Result {
				return runPtahIn(c, append([]string{"schema", "apply", "--db-url", database, "--root-dir", root, "--auto-approve"}, dev...)...)
			}
			const quoted = "- name: 'q'\n  note: 'it\\''s;x'\n"

			c.Assert(os.WriteFile(rowFile, []byte("- name: 'C:\\temp'\n  note: 'a\\b'\n- name: 'it''s'\n  note: 'x\\'\n"+quoted), 0o600), qt.IsNil)
			inserted := apply()
			insertedRows := readNoBackslashEscapes(c, database, backslashRowsRead)
			c.Assert(os.WriteFile(rowFile, []byte("- name: 'C:\\temp'\n  note: 'a\\c'\n- name: 'it''s'\n  note: 'x\\'\n"+quoted), 0o600), qt.IsNil)
			updated := apply()
			updatedRows := readNoBackslashEscapes(c, database, backslashRowsRead)
			again := apply()

			c.Assert(inserted.ExitCode, qt.Equals, 0, historyComment(inserted))
			c.Assert(insertedRows, qt.Equals, "433A5C74656D70:615C62,69742773:785C,71:69745C27733B78")
			c.Assert(updated.ExitCode, qt.Equals, 0, historyComment(updated))
			c.Assert(updatedRows, qt.Equals, "433A5C74656D70:615C63,69742773:785C,71:69745C27733B78")
			c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n", historyComment(again))
		})
	}
}

// backslashLiteralSchema is a schema file whose quoted default, column
// comment, enum value, index comment and table comment each hold a
// backslash, spelled for the default sql_mode.
const backslashLiteralSchema = "CREATE TABLE notes (id int PRIMARY KEY, a varchar(20) DEFAULT 'a\\\\b' COMMENT 'c\\\\d', " +
	"e enum('x\\\\y','z'), KEY k (a) COMMENT 'i\\\\j') COMMENT='t\\\\c';\n"

// TestSchemaDiffOnADevDatabaseOfEachModeIsSyncedE2E diffs a schema file
// against a database built from the same file. The file is the --from side,
// so the diff creates it on a dev database whose session is under each
// sql_mode and reads it back. The dev database stores the characters the file
// declares, so the diff reports no change.
func TestSchemaDiffOnADevDatabaseOfEachModeIsSyncedE2E(t *testing.T) {
	rows := []struct {
		name    string
		engine  dbtarget.Engine
		sqlMode string
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin, sqlMode: serverSQLMode},
		{name: "mysql without backslash escapes", engine: dbtarget.MySQLAdmin, sqlMode: noBackslashEscapesMode},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin, sqlMode: serverSQLMode},
		{name: "mariadb without backslash escapes", engine: dbtarget.MariaDBAdmin, sqlMode: noBackslashEscapesMode},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, row.engine)
			_, target := scratch.builtFrom(c, backslashLiteralSchema)
			dev := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "dev"), row.sqlMode)

			diff := runPtahIn(c, "schema", "diff", "--from", "file://"+writeNoBackslashEscapesSchema(c, backslashLiteralSchema), "--to", target,
				"--dev-url", dev)

			c.Assert(diff.ExitCode, qt.Equals, 0, historyComment(diff))
			c.Assert(diff.Stdout, qt.Equals, "Schemas are synced, no changes to be made.\n", historyComment(diff))
		})
	}
}

// modelDir writes model to a directory of its own and returns the flag that
// reads it and the path that flag takes.
func modelDir(c *qt.C, model declaredModel) (flag, path string) {
	c.Helper()
	dir := c.TempDir()
	file := filepath.Join(dir, model.file)
	c.Assert(os.WriteFile(file, []byte(model.source), 0o600), qt.IsNil)
	sources := map[string]string{"--root-dir": dir, "--schema-file": file}
	return model.flag, sources[model.flag]
}

// tablesIn counts the tables of the database a session of database is in.
const tablesIn = "SELECT CAST(COUNT(*) AS CHAR) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()"

// backslashModeRows are the engines with each pairing of sql_modes whose
// sessions read a backslash differently: first is the session the text is
// written for, second the one it would run on.
var backslashModeRows = []struct {
	name          string
	engine        dbtarget.Engine
	first, second string
}{
	{name: "mysql, a default mode target", engine: dbtarget.MySQLAdmin, first: serverSQLMode, second: noBackslashEscapesMode},
	{name: "mysql, a target without backslash escapes", engine: dbtarget.MySQLAdmin, first: noBackslashEscapesMode, second: serverSQLMode},
	{name: "mariadb, a default mode target", engine: dbtarget.MariaDBAdmin, first: serverSQLMode, second: noBackslashEscapesMode},
	{name: "mariadb, a target without backslash escapes", engine: dbtarget.MariaDBAdmin, first: noBackslashEscapesMode, second: serverSQLMode},
}

// TestApplyRefusesARehearsalAcrossBackslashModesE2E applies a model whose
// literals hold a backslash with a dev database whose session reads a
// backslash differently from the target's. The rehearsal would not run what
// the apply runs, so the apply is refused before it sends anything: neither
// database gains a table.
func TestApplyRefusesARehearsalAcrossBackslashModesE2E(t *testing.T) {
	for _, row := range backslashModeRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			target := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "rehearsed"), row.first)
			dev := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "dev"), row.second)
			flag, path := modelDir(c, escapedLiteralModel(` comment="c\\d"`, mysqlFamilyFields))

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, flag, path, "--dev-url", dev, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, "and a statement holds a backslash, which the two sessions read differently, "+
				"so the rehearsal would not run what the apply runs; give the dev database the target's sql_mode", historyComment(applied))
			c.Assert(readNoBackslashEscapes(c, target, tablesIn), qt.Equals, "0")
			c.Assert(readNoBackslashEscapes(c, dev, tablesIn), qt.Equals, "0")
		})
	}
}

// TestApplyRehearsesAcrossBackslashModesWithoutABackslashE2E applies
// [quoteOnlyModel], whose literals hold no backslash, with a dev database
// whose session reads a backslash differently from the target's. Both read
// the statements the same way, so the rehearsal runs and the apply stores the
// characters the model declares.
func TestApplyRehearsesAcrossBackslashModesWithoutABackslashE2E(t *testing.T) {
	for _, row := range backslashModeRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			target := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "rehearsed"), row.first)
			dev := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "dev"), row.second)
			flag, path := modelDir(c, quoteOnlyModel)

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, flag, path, "--dev-url", dev, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(insertDefaultsAndRead(c, target, quoteOnlyRead), qt.Equals, quoteOnlyWant)
		})
	}
}

// TestApplyRehearsesABackslashOnTheSameModeE2E applies a model whose literals
// hold a backslash with a dev database whose session is under the target's
// sql_mode, both with and without NO_BACKSLASH_ESCAPES. The rehearsal runs,
// and the target stores the characters the model declares.
func TestApplyRehearsesABackslashOnTheSameModeE2E(t *testing.T) {
	for _, row := range backslashModeRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			target := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "rehearsed"), row.first)
			dev := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "dev"), row.first)
			flag, path := modelDir(c, escapedLiteralModel(` comment="c\\d"`, mysqlFamilyFields))

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, flag, path, "--dev-url", dev, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(insertDefaultsAndRead(c, target, escapedLiteralMySQLRead), qt.Equals,
				"615C62,69745C27733B78,697427733B78,NULL,635C64,656E756D2827655C5C66272C27672729,615C5C62")
		})
	}
}

// savePlan writes the plan that brings database to model to a file and
// returns its path.
func savePlan(c *qt.C, database string, model declaredModel) string {
	c.Helper()
	flag, path := modelDir(c, model)
	plan := filepath.Join(c.TempDir(), "notes.plan.json")
	saved := runPtahIn(c, "schema", "plan", "--db-url", database, flag, path, "--output", plan)
	c.Assert(saved.ExitCode, qt.Equals, 0, historyComment(saved))
	return plan
}

// TestApplyRefusesAPlanWithABackslashOnABackslashEscapesSessionE2E saves a
// plan whose literals hold a backslash, from a session under each sql_mode,
// and applies it with --plan through a session under NO_BACKSLASH_ESCAPES. A
// saved plan is written for the default sql_mode and sent as written, so the
// session would store other characters than the plan declares: the apply is
// refused before it sends anything, and the database gains no table.
func TestApplyRefusesAPlanWithABackslashOnABackslashEscapesSessionE2E(t *testing.T) {
	rows := []struct {
		name    string
		engine  dbtarget.Engine
		planned string
	}{
		{name: "mysql, planned under the default mode", engine: dbtarget.MySQLAdmin, planned: serverSQLMode},
		{name: "mysql, planned without backslash escapes", engine: dbtarget.MySQLAdmin, planned: noBackslashEscapesMode},
		{name: "mariadb, planned under the default mode", engine: dbtarget.MariaDBAdmin, planned: serverSQLMode},
		{name: "mariadb, planned without backslash escapes", engine: dbtarget.MariaDBAdmin, planned: noBackslashEscapesMode},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := mysqlFamilyScratchURL(c, row.engine, "planned")
			plan := savePlan(c, withSQLMode(c, database, row.planned), escapedLiteralModel(` comment="c\\d"`, mysqlFamilyFields))
			session := withSQLMode(c, database, noBackslashEscapesMode)

			applied := runPtahIn(c, "schema", "apply", "--db-url", session, "--plan", plan, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, "the plan is written for a session whose sql_mode does not name NO_BACKSLASH_ESCAPES, "+
				"the session's sql_mode names NO_BACKSLASH_ESCAPES, and a statement holds a backslash", historyComment(applied))
			c.Assert(readNoBackslashEscapes(c, session, tablesIn), qt.Equals, "0")
		})
	}
}

// applyRefusal is the part of the `schema apply --json` document a caller acts
// on when the run is refused.
type applyRefusal struct {
	Outcome string `json:"outcome"`
	Refusal struct {
		Code string `json:"code"`
	} `json:"refusal"`
}

// decodeApplyRefusal reads the document a `schema apply --json` run printed.
func decodeApplyRefusal(c *qt.C, result clirun.Result) applyRefusal {
	c.Helper()
	var document applyRefusal
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil, historyComment(result))
	return document
}

// TestApplyReportsAPlanBackslashModeRefusalE2E runs the refused apply of
// [TestApplyRefusesAPlanWithABackslashOnABackslashEscapesSessionE2E] with
// --json, as a dry run and as a real one. The document ends the run as
// refused with the backslash-mode code, which a caller tells apart from a
// broken plan, and the database gains no table.
func TestApplyReportsAPlanBackslashModeRefusalE2E(t *testing.T) {
	rows := []struct {
		name   string
		engine dbtarget.Engine
		mode   []string
	}{
		{name: "mysql, a dry run", engine: dbtarget.MySQLAdmin, mode: []string{"--dry-run"}},
		{name: "mysql, an apply", engine: dbtarget.MySQLAdmin, mode: []string{"--auto-approve"}},
		{name: "mariadb, a dry run", engine: dbtarget.MariaDBAdmin, mode: []string{"--dry-run"}},
		{name: "mariadb, an apply", engine: dbtarget.MariaDBAdmin, mode: []string{"--auto-approve"}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := mysqlFamilyScratchURL(c, row.engine, "planned")
			plan := savePlan(c, database, escapedLiteralModel(` comment="c\\d"`, mysqlFamilyFields))
			session := withSQLMode(c, database, noBackslashEscapesMode)

			applied := runPtahIn(c, append([]string{"schema", "apply", "--db-url", session, "--plan", plan, "--json"}, row.mode...)...)

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, historyComment(applied))
			c.Assert(decodeApplyRefusal(c, applied), qt.DeepEquals, applyRefusal{
				Outcome: "refused",
				Refusal: struct {
					Code string `json:"code"`
				}{Code: "backslash-mode"},
			})
			c.Assert(readNoBackslashEscapes(c, session, tablesIn), qt.Equals, "0")
		})
	}
}

// TestApplyReportsARehearsalBackslashModeRefusalE2E runs the refused apply of
// [TestApplyRefusesARehearsalAcrossBackslashModesE2E] with --json: the
// rehearsal's refusal carries the same code as the saved plan's.
func TestApplyReportsARehearsalBackslashModeRefusalE2E(t *testing.T) {
	for _, row := range backslashModeRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			target := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "rehearsed"), row.first)
			dev := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "dev"), row.second)
			flag, path := modelDir(c, escapedLiteralModel(` comment="c\\d"`, mysqlFamilyFields))

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, flag, path, "--dev-url", dev, "--auto-approve", "--json")

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, historyComment(applied))
			c.Assert(decodeApplyRefusal(c, applied).Refusal.Code, qt.Equals, "backslash-mode", historyComment(applied))
			c.Assert(decodeApplyRefusal(c, applied).Outcome, qt.Equals, "refused")
			c.Assert(readNoBackslashEscapes(c, target, tablesIn), qt.Equals, "0")
		})
	}
}

// TestApplyAPlanWithoutABackslashOnABackslashEscapesSessionE2E saves a plan
// of [quoteOnlyModel], whose literals hold no backslash, and applies it with
// --plan through a session under NO_BACKSLASH_ESCAPES, which reads it as the
// default sql_mode does. The database stores the characters the model
// declares.
func TestApplyAPlanWithoutABackslashOnABackslashEscapesSessionE2E(t *testing.T) {
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := mysqlFamilyScratchURL(c, row.engine, "planned")
			plan := savePlan(c, database, quoteOnlyModel)
			session := withSQLMode(c, database, noBackslashEscapesMode)

			applied := runPtahIn(c, "schema", "apply", "--db-url", session, "--plan", plan, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(insertDefaultsAndRead(c, session, quoteOnlyRead), qt.Equals, quoteOnlyWant)
		})
	}
}

// TestApplyAPlanWithABackslashOnADefaultModeSessionE2E saves a plan whose
// literals hold a backslash, from a session under each sql_mode, and applies
// it with --plan through a session under the default sql_mode. The plan is
// written for that mode wherever it was planned, so the database stores the
// characters the model declares.
func TestApplyAPlanWithABackslashOnADefaultModeSessionE2E(t *testing.T) {
	rows := []struct {
		name    string
		engine  dbtarget.Engine
		planned string
	}{
		{name: "mysql, planned under the default mode", engine: dbtarget.MySQLAdmin, planned: serverSQLMode},
		{name: "mysql, planned without backslash escapes", engine: dbtarget.MySQLAdmin, planned: noBackslashEscapesMode},
		{name: "mariadb, planned under the default mode", engine: dbtarget.MariaDBAdmin, planned: serverSQLMode},
		{name: "mariadb, planned without backslash escapes", engine: dbtarget.MariaDBAdmin, planned: noBackslashEscapesMode},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := mysqlFamilyScratchURL(c, row.engine, "planned")
			plan := savePlan(c, withSQLMode(c, database, row.planned), escapedLiteralModel(` comment="c\\d"`, mysqlFamilyFields))

			applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--plan", plan, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(insertDefaultsAndRead(c, database, escapedLiteralMySQLRead), qt.Equals,
				"615C62,69745C27733B78,697427733B78,NULL,635C64,656E756D2827655C5C66272C27672729,615C5C62")
		})
	}
}

// quoteBackslashModel declares a default and a comment with a backslash
// before a quote and a semicolon, it\'s;x, and a default and a comment that
// end in a backslash.
var quoteBackslashModel = declaredModel{
	source: "package entities\n\n" +
		"//ptah:schema:table name=\"notes\"\n" +
		"type Note struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INT\" primary=\"true\"\n" +
		"\tID int\n" +
		"\t//ptah:schema:field name=\"q\" type=\"VARCHAR(30)\" default=\"it\\'s;x\" comment=\"it\\'s;y\"\n" +
		"\tQ string\n" +
		"\t//ptah:schema:field name=\"t\" type=\"VARCHAR(30)\" default=\"x\\\\\" comment=\"z\\\\\"\n" +
		"\tT string\n" +
		"}\n",
	file: "notes.go",
	flag: "--root-dir",
}

// quoteBackslashRead reads, in hexadecimal, the defaults columns q and t give
// a row and their comments.
const quoteBackslashRead = "SELECT CONCAT_WS(',', HEX(q), (SELECT HEX(COLUMN_COMMENT) FROM information_schema.COLUMNS " +
	"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notes' AND COLUMN_NAME = 'q'), HEX(t), (SELECT HEX(COLUMN_COMMENT) " +
	"FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notes' AND COLUMN_NAME = 't')) FROM notes"

// TestApplyKeepsABackslashBeforeAQuoteE2E applies [quoteBackslashModel] twice
// on a session under each sql_mode, alone and rehearsed on a dev database
// under the same sql_mode. Under NO_BACKSLASH_ESCAPES the literals keep a
// backslash before a quote, which ends the string there: the statement is
// sent whole rather than cut at the semicolon inside it. Each server stores
// the characters the model declares, and the second apply plans nothing.
func TestApplyKeepsABackslashBeforeAQuoteE2E(t *testing.T) {
	for _, row := range sessionModeRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "quote_backslash"), row.sqlMode)
			flag, path := modelDir(c, quoteBackslashModel)
			arguments := append([]string{"schema", "apply", "--db-url", database, flag, path, "--auto-approve"},
				devArguments(c, row.engine, row.sqlMode, row.rehearsed)...)

			created := runPtahIn(c, arguments...)
			again := runPtahIn(c, arguments...)

			c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
			c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n", historyComment(again))
			c.Assert(insertDefaultsAndRead(c, database, quoteBackslashRead), qt.Equals, "69745C27733B78,69745C27733B79,785C,7A5C")
		})
	}
}

// TestApplyRatesEachStatementOfAnEditedPlanWithoutBackslashEscapesE2E applies
// a schema file with --edit through a session under NO_BACKSLASH_ESCAPES, with
// an editor that appends `'a\'; DROP TABLE t; -- '`. That session reads the
// string as ending after the backslash, so the text holds a DROP TABLE, and
// the rating the prompt shows judges it as its own statement; the answer
// declines, and the database gains no table. Read with the default rule, the
// DROP would sit inside a string and pass the rating unseen.
func TestApplyRatesEachStatementOfAnEditedPlanWithoutBackslashEscapesE2E(t *testing.T) {
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := noBackslashEscapesDatabase(c, row.engine)
			dir := c.TempDir()
			appended := filepath.Join(dir, "appended.sql")
			c.Assert(os.WriteFile(appended, []byte("ALTER TABLE notes ALTER COLUMN u SET DEFAULT 'a\\'; DROP TABLE t; -- '\n"), 0o600), qt.IsNil)
			editor := filepath.Join(dir, "append.sh")
			script := fmt.Sprintf("#!/bin/sh\ncat \"$1\" %q > \"$1.new\" && mv \"$1.new\" \"$1\"\n", appended)
			c.Assert(os.WriteFile(editor, []byte(script), 0o700), qt.IsNil) // #nosec G306 -- the editor must be executable
			env := append(os.Environ(), "VISUAL="+editor, "PTAH_ALLOW_NONINTERACTIVE_EDIT=1")
			schema := writeNoBackslashEscapesSchema(c, "CREATE TABLE notes (id int PRIMARY KEY, u varchar(30));\nCREATE TABLE t (id int PRIMARY KEY);\n")

			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: env, Stdin: "no\n"},
				"schema", "apply", "--db-url", database, "--schema-file", schema, "--edit")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, "Plan rating: destructive\n", historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, "of 4, destructive", historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, "  DROP TABLE t\n", historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, "Schema apply canceled.", historyComment(applied))
			c.Assert(readNoBackslashEscapes(c, database, tablesIn), qt.Equals, "0")
		})
	}
}

// TestMigrationsGenerateWritesDefaultModeLiteralsE2E generates a migration
// file from a model whose literals hold a backslash, planned through a session
// under NO_BACKSLASH_ESCAPES, and runs it up through a session under the
// default sql_mode. A migration file is run later, by a session the generator
// cannot name, so it is written for the default sql_mode: the database stores
// the characters the model declares.
func TestMigrationsGenerateWritesDefaultModeLiteralsE2E(t *testing.T) {
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := mysqlFamilyScratchURL(c, row.engine, "generated")
			flag, path := modelDir(c, escapedLiteralModel(` comment="c\\d"`, mysqlFamilyFields))
			dir := filepath.Join(c.TempDir(), "migrations")

			generated := runPtahIn(c, "migrations", "generate", "--db-url", withSQLMode(c, database, noBackslashEscapesMode), flag, path,
				"--migrations-dir", dir, "--name", "notes")
			up := runPtahIn(c, "migrations", "up", "--db-url", database, "--migrations-dir", dir)

			c.Assert(generated.ExitCode, qt.Equals, 0, historyComment(generated))
			c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))
			c.Assert(insertDefaultsAndRead(c, database, escapedLiteralMySQLRead), qt.Equals,
				"615C62,69745C27733B78,697427733B78,NULL,635C64,656E756D2827655C5C66272C27672729,615C5C62")
		})
	}
}

// TestSchemaDiffPrintsDefaultModeLiteralsE2E diffs an empty database, read
// through a session under NO_BACKSLASH_ESCAPES, against a schema file whose
// literals hold a backslash. The statements a diff prints are run later, by a
// session the diff cannot name, so they are written for the default sql_mode,
// with each backslash doubled.
func TestSchemaDiffPrintsDefaultModeLiteralsE2E(t *testing.T) {
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			target := noBackslashEscapesDatabase(c, row.engine)
			dev := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "dev"), noBackslashEscapesMode)

			diff := runPtahIn(c, "schema", "diff", "--from", target, "--to", "file://"+writeNoBackslashEscapesSchema(c, backslashLiteralSchema),
				"--dev-url", dev)

			c.Assert(diff.ExitCode, qt.Equals, 0, historyComment(diff))
			c.Assert(diff.Stdout, qt.Contains, `DEFAULT 'a\\b'`, historyComment(diff))
			c.Assert(diff.Stdout, qt.Contains, `COMMENT 'c\\d'`, historyComment(diff))
			c.Assert(diff.Stdout, qt.Contains, `enum('x\\y','z')`, historyComment(diff))
		})
	}
}

// TestMigrationsDataWritesDefaultModeLiteralsE2E generates a data migration
// from declared rows whose values hold a backslash, one of them last and one
// before a quote and a semicolon, through a session under
// NO_BACKSLASH_ESCAPES, and runs it up through a session under the default
// sql_mode. The migration is run later, by a session the generator cannot
// name, so it is written for the default sql_mode: the database stores the
// characters the rows declare.
func TestMigrationsDataWritesDefaultModeLiteralsE2E(t *testing.T) {
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := mysqlFamilyScratchURL(c, row.engine, "data_rows")
			root := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(backslashRowsEntities), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(root, "paths.yaml"),
				[]byte("- name: 'C:\\temp\\x'\n  note: 'a\\b'\n- name: 'q'\n  note: 'it\\''s;x'\n- name: 'z'\n  note: 'x\\'\n"), 0o600), qt.IsNil)
			dir := filepath.Join(c.TempDir(), "migrations")

			created := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file",
				writeNoBackslashEscapesSchema(c, "CREATE TABLE paths (name varchar(40) PRIMARY KEY, note varchar(40) NOT NULL);\n"), "--auto-approve")
			generated := runPtahIn(c, "migrations", "data", "--db-url", withSQLMode(c, database, noBackslashEscapesMode), "--root-dir", root,
				"--migrations-dir", dir)
			up := runPtahIn(c, "migrations", "up", "--db-url", database, "--migrations-dir", dir)

			c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
			c.Assert(generated.ExitCode, qt.Equals, 0, historyComment(generated))
			c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))
			c.Assert(readNoBackslashEscapes(c, database, backslashRowsRead), qt.Equals, "433A5C74656D705C78:615C62,71:69745C27733B78,7A:785C")
		})
	}
}

// backslashRowsMigration creates paths and inserts rows whose values hold a
// backslash, spelled for a session under NO_BACKSLASH_ESCAPES. A migration
// cut for that session runs only without a transaction.
const backslashRowsMigration = "-- atlas:txmode none\n\n" +
	"CREATE TABLE paths (name varchar(40) PRIMARY KEY, note varchar(40) NOT NULL);\n" +
	"INSERT INTO paths VALUES ('C:\\temp', 'a\\b');\n"

// TestMigrationsCheckpointWritesDefaultModeDataRowsE2E checkpoints a
// directory whose rows hold a backslash, replayed into a shadow database
// through a session under NO_BACKSLASH_ESCAPES, and bootstraps a database
// from the checkpoint through a session under the default sql_mode. The
// checkpoint is run later, by a session the generator cannot name, so its rows
// are written for the default sql_mode: the database stores the characters
// the shadow read.
func TestMigrationsCheckpointWritesDefaultModeDataRowsE2E(t *testing.T) {
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_paths.up.sql"), []byte(backslashRowsMigration), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_paths.down.sql"), []byte("DROP TABLE paths;\n"), 0o600), qt.IsNil)
			shadow := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "shadow"), noBackslashEscapesMode)
			database := mysqlFamilyScratchURL(c, row.engine, "bootstrapped")

			checkpoint := runPtahIn(c, "migrations", "checkpoint", "--shadow-db", shadow, "--migrations-dir", dir, "--data-table", "paths")
			up := runPtahIn(c, "migrations", "up", "--db-url", database, "--migrations-dir", dir)

			c.Assert(checkpoint.ExitCode, qt.Equals, 0, historyComment(checkpoint))
			c.Assert(up.ExitCode, qt.Equals, 0, historyComment(up))
			c.Assert(readNoBackslashEscapes(c, database, backslashRowsRead), qt.Equals, "433A5C74656D70:615C62")
		})
	}
}

// backslashLiteralRead reads the default of notes.a from an inserted row, the
// comment of notes.a and the type of notes.e, in hexadecimal.
const backslashLiteralRead = "SELECT CONCAT_WS(',', HEX(a), " +
	"(SELECT HEX(COLUMN_COMMENT) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notes' AND COLUMN_NAME = 'a'), " +
	"(SELECT HEX(COLUMN_TYPE) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notes' AND COLUMN_NAME = 'e')) FROM notes"

// TestCompatMigrateDiffWritesDefaultModeLiteralsE2E writes a migration file
// with ptah-compat from a schema file whose literals hold a backslash,
// planned on a dev database whose session is under NO_BACKSLASH_ESCAPES, and
// applies it through a session under the default sql_mode. The file is run
// later, by a session the diff cannot name, so it is written for the default
// sql_mode: the database stores the characters the schema file declares.
func TestCompatMigrateDiffWritesDefaultModeLiteralsE2E(t *testing.T) {
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := "file://" + filepath.ToSlash(filepath.Join(c.TempDir(), "migrations"))
			dev := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "dev"), noBackslashEscapesMode)
			database := mysqlFamilyScratchURL(c, row.engine, "diffed")

			diff := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "migrate", "diff", "--dir", dir,
				"--to", "file://"+writeNoBackslashEscapesSchema(c, backslashLiteralSchema), "--dev-url", dev)
			applied := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir()}, "migrate", "apply", "--dir", dir, "-u", database)

			c.Assert(diff.ExitCode, qt.Equals, 0, historyComment(diff))
			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(insertDefaultsAndRead(c, database, backslashLiteralRead), qt.Equals, "615C62,635C64,656E756D2827785C5C79272C277A2729")
		})
	}
}

// printedSQLCommands are commands that print SQL a reader runs later, each with
// the binary that runs it and whether the database it reads holds the table
// already. {db} is that database.
var printedSQLCommands = []struct {
	name   string
	binary clirun.Target
	built  bool
	args   []string
}{
	{name: "migrations plan", binary: clirun.Ptah, args: []string{"migrations", "plan", "--db-url", "{db}", "--schema-file", "{schema}"}},
	{name: "schema compare", binary: clirun.Ptah, args: []string{"schema", "compare", "--db-url", "{db}", "--schema-file", "{schema}"}},
	{name: "db read", binary: clirun.Ptah, built: true, args: []string{"db", "read", "--db-url", "{db}"}},
	{name: "schema inspect", binary: clirun.Ptah, built: true, args: []string{"schema", "inspect", "--db-url", "{db}", "--format", "sql"}},
	{name: "compat schema inspect", binary: clirun.Compat, built: true, args: []string{"schema", "inspect", "-u", "{db}", "--format", "{{ sql . }}"}},
}

// TestPrintedSQLIsWrittenForTheDefaultModeE2E runs each of
// [printedSQLCommands] through a session under NO_BACKSLASH_ESCAPES, against a
// database that is empty or built from a schema file whose literals hold a
// backslash. The SQL is run later, by a session the command cannot name, so it
// is written for the default sql_mode, with each backslash doubled.
func TestPrintedSQLIsWrittenForTheDefaultModeE2E(t *testing.T) {
	for _, engine := range noBackslashEscapesEngines {
		for _, command := range printedSQLCommands {
			t.Run(engine.name+"/"+command.name, func(t *testing.T) {
				c := qt.New(t)
				databases := map[bool]func() string{
					false: func() string { return mysqlFamilyScratchURL(c, engine.engine, "printed") },
					true: func() string {
						_, database := newMySQLFamilyScratch(c, engine.engine).builtFrom(c, backslashLiteralSchema)
						return database
					},
				}
				replacer := strings.NewReplacer("{db}", withSQLMode(c, databases[command.built](), noBackslashEscapesMode),
					"{schema}", writeNoBackslashEscapesSchema(c, backslashLiteralSchema))
				args := make([]string, 0, len(command.args))
				for _, arg := range command.args {
					args = append(args, replacer.Replace(arg))
				}

				printed := clirun.Run(c, command.binary, clirun.Options{Dir: c.TempDir()}, args...)

				c.Assert(printed.ExitCode, qt.Equals, 0, historyComment(printed))
				c.Assert(printed.Stdout, qt.Contains, `DEFAULT 'a\\b'`, historyComment(printed))
				c.Assert(printed.Stdout, qt.Contains, `COMMENT 'c\\d'`, historyComment(printed))
				c.Assert(printed.Stdout, qt.Contains, `enum('x\\y','z')`, historyComment(printed))
			})
		}
	}
}
