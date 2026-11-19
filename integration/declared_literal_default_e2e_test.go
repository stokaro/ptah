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

// Go and YAML models whose defaults spell, without quotes, the same characters
// as the expressions in mySQLExpressionDefaultSchema and
// postgresExpressionDefaultSchema. default declares a literal, so each column
// stores those characters; default_expr declares the expression.
const (
	goLiteralDefaults = "package entities\n\n" +
		"//ptah:schema:table name=\"notes\"\n" +
		"type Note struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INT\" primary=\"true\"\n" +
		"\tID int\n" +
		"\t//ptah:schema:field name=\"a\" type=\"VARCHAR(30)\" default=\"1 + 2\"\n" +
		"\tA string\n" +
		"\t//ptah:schema:field name=\"b\" type=\"VARCHAR(30)\" default=\"(1 + 2)\"\n" +
		"\tB string\n" +
		"\t//ptah:schema:field name=\"n\" type=\"VARCHAR(30)\" default=\"now()\"\n" +
		"\tN string\n" +
		"}\n"
	goExpressionDefaults = "package entities\n\n" +
		"//ptah:schema:table name=\"notes\"\n" +
		"type Note struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INT\" primary=\"true\"\n" +
		"\tID int\n" +
		"\t//ptah:schema:field name=\"a\" type=\"VARCHAR(30)\" default_expr=\"1 + 2\"\n" +
		"\tA string\n" +
		"\t//ptah:schema:field name=\"b\" type=\"VARCHAR(30)\" default_expr=\"(1 + 2)\"\n" +
		"\tB string\n" +
		"\t//ptah:schema:field name=\"n\" type=\"VARCHAR(30)\" default_expr=\"now()\"\n" +
		"\tN string\n" +
		"}\n"
	yamlLiteralDefaults = "tables:\n  notes:\n    columns:\n" +
		"      id:\n        type: INT\n        primary: true\n" +
		"      a:\n        type: VARCHAR(30)\n        default: \"1 + 2\"\n" +
		"      b:\n        type: VARCHAR(30)\n        default: \"(1 + 2)\"\n" +
		"      n:\n        type: VARCHAR(30)\n        default: \"now()\"\n"
	yamlExpressionDefaults = "tables:\n  notes:\n    columns:\n" +
		"      id:\n        type: INT\n        primary: true\n" +
		"      a:\n        type: VARCHAR(30)\n        default_expr: \"1 + 2\"\n" +
		"      b:\n        type: VARCHAR(30)\n        default_expr: \"(1 + 2)\"\n" +
		"      n:\n        type: VARCHAR(30)\n        default_expr: \"now()\"\n"
)

// declaredModel is a model's source, the file it is written to, and the flag
// that hands it to `ptah schema apply`.
type declaredModel struct {
	source, file, flag string
}

var (
	literalDefaultModel        = declaredModel{source: goLiteralDefaults, file: "notes.go", flag: "--root-dir"}
	expressionDefaultModel     = declaredModel{source: goExpressionDefaults, file: "notes.go", flag: "--root-dir"}
	yamlLiteralDefaultModel    = declaredModel{source: yamlLiteralDefaults, file: "schema.yaml", flag: "--schema-file"}
	yamlExpressionDefaultModel = declaredModel{source: yamlExpressionDefaults, file: "schema.yaml", flag: "--schema-file"}
)

// TestApplyChangesADefaultToTheDeclaredKindE2E builds a table from a schema
// file and then applies a model whose defaults are of the other kind, with
// the same characters. The model's apply plans a change, the next apply plans
// nothing, and a row that takes the defaults reads back what the model
// declares: the characters where it declares a literal, 3 where it declares
// the expression.
func TestApplyChangesADefaultToTheDeclaredKindE2E(t *testing.T) {
	rows := []struct {
		name   string
		engine dbtarget.Engine
		schema string
		model  declaredModel
		want   string
	}{
		{name: "mysql, a literal over the expression", engine: dbtarget.MySQLAdmin, schema: mySQLExpressionDefaultSchema, model: literalDefaultModel, want: "1 + 2,(1 + 2),0"},
		{name: "mysql, the expression over a literal", engine: dbtarget.MySQLAdmin, schema: stringDefaultSchema, model: expressionDefaultModel, want: "3,3,1"},
		{name: "postgres, a literal over the expression", engine: dbtarget.PostgreSQL, schema: postgresExpressionDefaultSchema, model: literalDefaultModel, want: "1 + 2,(1 + 2),0"},
		{name: "postgres, the expression over a literal", engine: dbtarget.PostgreSQL, schema: stringDefaultSchema, model: expressionDefaultModel, want: "3,3,1"},
		{name: "mysql, a YAML literal over the expression", engine: dbtarget.MySQLAdmin, schema: mySQLExpressionDefaultSchema, model: yamlLiteralDefaultModel, want: "1 + 2,(1 + 2),0"},
		{name: "postgres, a YAML literal over the expression", engine: dbtarget.PostgreSQL, schema: postgresExpressionDefaultSchema, model: yamlLiteralDefaultModel, want: "1 + 2,(1 + 2),0"},
		{name: "postgres, the YAML expression over a literal", engine: dbtarget.PostgreSQL, schema: stringDefaultSchema, model: yamlExpressionDefaultModel, want: "3,3,1"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := declaredDefaultDatabase(c, row.engine)

			built, changed := applySchemaThenModel(c, database, row.schema, row.model)
			again := applyModel(c, database, row.model)

			c.Assert(built.ExitCode, qt.Equals, 0, historyComment(built))
			c.Assert(changed.ExitCode, qt.Equals, 0, historyComment(changed))
			c.Assert(changed.Stdout, qt.Not(qt.Equals), "Schema is synced, no changes to be made.\n")
			c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
			c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(readDeclaredDefaults(c, database), qt.Equals, row.want)
		})
	}
}

// TestApplyKeepsADefaultOfTheDeclaredKindE2E is the control: a model whose
// defaults are of the kind the table already holds plans nothing.
func TestApplyKeepsADefaultOfTheDeclaredKindE2E(t *testing.T) {
	rows := []struct {
		name   string
		engine dbtarget.Engine
		schema string
		model  declaredModel
	}{
		{name: "mysql, the expression", engine: dbtarget.MySQLAdmin, schema: mySQLExpressionDefaultSchema, model: expressionDefaultModel},
		{name: "mysql, a literal", engine: dbtarget.MySQLAdmin, schema: stringDefaultSchema, model: literalDefaultModel},
		{name: "postgres, the expression", engine: dbtarget.PostgreSQL, schema: postgresExpressionDefaultSchema, model: expressionDefaultModel},
		{name: "postgres, a literal", engine: dbtarget.PostgreSQL, schema: stringDefaultSchema, model: literalDefaultModel},
		{name: "mysql, a YAML literal", engine: dbtarget.MySQLAdmin, schema: stringDefaultSchema, model: yamlLiteralDefaultModel},
		{name: "postgres, the YAML expression", engine: dbtarget.PostgreSQL, schema: postgresExpressionDefaultSchema, model: yamlExpressionDefaultModel},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := declaredDefaultDatabase(c, row.engine)

			built, applied := applySchemaThenModel(c, database, row.schema, row.model)

			c.Assert(built.ExitCode, qt.Equals, 0, historyComment(built))
			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// declaredDefaultDatabase returns the address of an empty scratch database on
// engine.
func declaredDefaultDatabase(c *qt.C, engine dbtarget.Engine) string {
	c.Helper()
	urls := map[dbtarget.Engine]func() string{
		dbtarget.PostgreSQL: func() string { return postgresScratchDevURL(c, "declared_default") },
		dbtarget.MySQLAdmin: func() string {
			scratch := newMySQLFamilyScratch(c, engine)
			name, _ := scratch.database(c, "declared_default")
			return replaceMySQLDatabaseName(c, scratch.adminURL, name)
		},
	}
	return urls[engine]()
}

// applySchemaThenModel applies schema, a SQL schema file, to database and then
// model, and returns both runs.
func applySchemaThenModel(c *qt.C, database, schema string, model declaredModel) (built, applied clirun.Result) {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(schema), 0o600), qt.IsNil)
	built = runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", file, "--auto-approve")
	return built, applyModel(c, database, model)
}

// applyModel applies model to database: a Go package by its directory, a YAML
// schema by its file.
func applyModel(c *qt.C, database string, model declaredModel) clirun.Result {
	c.Helper()
	dir := c.TempDir()
	file := filepath.Join(dir, model.file)
	c.Assert(os.WriteFile(file, []byte(model.source), 0o600), qt.IsNil)
	sources := map[string]string{"--root-dir": dir, "--schema-file": file}
	return runPtahIn(c, "schema", "apply", "--db-url", database, model.flag, sources[model.flag], "--auto-approve")
}

// readDeclaredDefaults inserts a row that takes every default and reads a, b,
// and whether n holds something other than the characters now().
func readDeclaredDefaults(c *qt.C, database string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "INSERT INTO notes (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var got string
	query := "SELECT CONCAT_WS(',', a, b, CASE WHEN n <> 'now()' THEN 1 ELSE 0 END) FROM notes"
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&got), qt.IsNil)
	return got
}

// quotedLiteralModel declares a literal default with a quote inside it.
var quotedLiteralModel = declaredModel{
	source: "package entities\n\n" +
		"//ptah:schema:table name=\"notes\"\n" +
		"type Note struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INT\" primary=\"true\"\n" +
		"\tID int\n" +
		"\t//ptah:schema:field name=\"q\" type=\"VARCHAR(30)\" default=\"it's\"\n" +
		"\tQ string\n" +
		"}\n",
	file: "notes.go",
	flag: "--root-dir",
}

// TestApplyKeepsALiteralDefaultWithAQuoteE2E applies a Go model whose literal
// default holds a quote, twice. The catalog reports the string in quotes with
// the inner quote doubled, and the declaration is compared in the same form, so
// the second apply plans nothing; compared bare, the declaration differs from
// the report by the doubled quote, and every apply rewrites the default.
func TestApplyKeepsALiteralDefaultWithAQuoteE2E(t *testing.T) {
	rows := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "postgres", engine: dbtarget.PostgreSQL},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := declaredDefaultDatabase(c, row.engine)

			created := applyModel(c, database, quotedLiteralModel)
			again := applyModel(c, database, quotedLiteralModel)

			c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
			c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
			c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			c.Assert(readQuotedDefault(c, database), qt.Equals, "it's")
		})
	}
}

// readQuotedDefault inserts a row that takes the default and reads q.
func readQuotedDefault(c *qt.C, database string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "INSERT INTO notes (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var got string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT q FROM notes").Scan(&got), qt.IsNil)
	return got
}
