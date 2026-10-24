package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasreport"
)

// The pinned community binary reads the indent of `{{ sql . "  " }}` as the
// unit inside a statement, not as a prefix: the statements and their comments
// start at the first column, so the default and what an atlas.hcl writes
// leave the renderer's layout as it is.
func TestMigrateDiffIndentKeepsStatementsAtTheFirstColumn(t *testing.T) {
	c := qt.New(t)
	const plan = "-- create a table\nCREATE TABLE t (\n  id int,\n  note text\n);"
	var output bytes.Buffer
	report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{Dialect: platform.Postgres, Statements: []string{plan, "CREATE INDEX i ON t (id);"}})

	err := atlasreport.WriteMigrateDiff(&output, `{{ sql . "  " }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.String(), qt.Equals, plan+"\nCREATE INDEX i ON t (id);\n")
}

// Another indent replaces each level of the renderer's, as the binary writes
// `\t"id" bigint NOT NULL,` for `{{ sql . "\t" }}`. Indentation is
// presentation, never a change to a stored routine or literal, so a line that
// starts inside one keeps its spaces.
func TestMigrateDiffIndentPreservesQuotedSQL(t *testing.T) {
	tests := []struct{ name, dialect, sql, want string }{
		{"ordinary SQL", platform.Postgres, "CREATE TABLE t (\n  id int,\n    nested int\n);", "CREATE TABLE t (\n\tid int,\n\t\tnested int\n);\n"},
		{"a level and a space", platform.Postgres, "SELECT 1,\n   2;", "SELECT 1,\n\t 2;\n"},
		{"dollar body", platform.Postgres, "CREATE FUNCTION f() RETURNS text AS $body$\nBEGIN\n  RETURN 'first\n  second';\nEND;\n$body$ LANGUAGE plpgsql;", "CREATE FUNCTION f() RETURNS text AS $body$\nBEGIN\n  RETURN 'first\n  second';\nEND;\n$body$ LANGUAGE plpgsql;\n"},
		{"string", platform.Postgres, "SELECT 'first\n  second',\n  2;", "SELECT 'first\n  second',\n\t2;\n"},
		{"quoted identifier", platform.Postgres, "CREATE TABLE \"first\n  second\" (\n  id int\n);", "CREATE TABLE \"first\n  second\" (\n\tid int\n);\n"},
		{"escaped quote", platform.Postgres, "SELECT E'first\\'\n  second';", "SELECT E'first\\'\n  second';\n"},
		{"backslash escape", platform.MySQL, "SELECT 'first\\'\n  second';", "SELECT 'first\\'\n  second';\n"},
		{"bracket identifier", platform.SQLServer, "SELECT [first\n  second];", "SELECT [first\n  second];\n"},
		{"backtick identifier", platform.MySQL, "SELECT `first\n  second`;", "SELECT `first\n  second`;\n"},
		{"multiline string", platform.YDB, "SELECT @@first\n  second@@;", "SELECT @@first\n  second@@;\n"},
		{"doubled quote", platform.Postgres, "SELECT 'first''\n  second';", "SELECT 'first''\n  second';\n"},
		{"Unicode literal", platform.Postgres, "SELECT 'café\n  日本';", "SELECT 'café\n  日本';\n"},
		{"comment quotes", platform.Postgres, "-- 'not a string\n  SELECT 'first\n  second';", "-- 'not a string\n\tSELECT 'first\n  second';\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var output bytes.Buffer
			report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{Dialect: test.dialect, Statements: []string{test.sql}})
			err := atlasreport.WriteMigrateDiff(&output, `{{ sql . "\t" }}`, report)
			c.Assert(err, qt.IsNil)
			c.Assert(output.String(), qt.Equals, test.want)
		})
	}
}

// The same text is quoted differently by different engines, so a statement is
// indented as the dialect it was planned for reads it. Each pair below is one
// text read two ways, and a reading under any other dialect's rules, or under
// several at once, gets one of the two wrong.
func TestMigrateDiffIndentReadsTheStatementsDialect(t *testing.T) {
	tests := []struct{ name, dialect, sql, want string }{
		{"T-SQL global variables", platform.SQLServer, "SELECT @@ROWCOUNT,\n  @@ERROR;", "SELECT @@ROWCOUNT,\n\t@@ERROR;\n"},
		{"YQL multiline string", platform.YDB, "SELECT @@ROWCOUNT,\n  @@ERROR;", "SELECT @@ROWCOUNT,\n  @@ERROR;\n"},
		{"PostgreSQL array", platform.Postgres, "SELECT ARRAY[1,\n  2];", "SELECT ARRAY[1,\n\t2];\n"},
		{"T-SQL bracket identifier", platform.SQLServer, "SELECT ARRAY[1,\n  2];", "SELECT ARRAY[1,\n  2];\n"},
		{"standard string ending in a backslash", platform.Postgres, "SELECT 'a\\',\n  'b\n  c';", "SELECT 'a\\',\n\t'b\n  c';\n"},
		{"MySQL escaped quote", platform.MySQL, "SELECT 'a\\',\n  'b\n  c';", "SELECT 'a\\',\n  'b\n\tc';\n"},
		{"no dialect", "", "SELECT [first\n  second], `third\n  fourth`;", "SELECT [first\n\tsecond], `third\n  fourth`;\n"},
		{"unknown dialect", "nonesuch", "SELECT [first\n  second], `third\n  fourth`;", "SELECT [first\n\tsecond], `third\n  fourth`;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var output bytes.Buffer
			report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{Dialect: test.dialect, Statements: []string{test.sql}})
			err := atlasreport.WriteMigrateDiff(&output, `{{ sql . "\t" }}`, report)
			c.Assert(err, qt.IsNil)
			c.Assert(output.String(), qt.Equals, test.want)
		})
	}
}

func TestSchemaReportsIndentPreservesRoutineBody(t *testing.T) {
	c := qt.New(t)
	const body = "BEGIN\n  RETURN 'first\n  second';\nEND;"
	const statement = "CREATE FUNCTION f() RETURNS text AS $$" + body + "$$ LANGUAGE plpgsql;"
	reports := []struct {
		name    string
		marshal func(...string) (string, error)
	}{
		{"diff", atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{Dialect: platform.Postgres, Statements: []string{statement}}).MarshalSQL},
		{"apply", atlasreport.NewSchemaApply(atlasreport.SchemaApplyOptions{Driver: platform.Postgres, Statements: []string{statement}}).MarshalSQL},
		{"plan", atlasreport.NewSchemaPlan(atlasreport.SchemaPlanOptions{Dialect: platform.Postgres, Statements: []atlasreport.SchemaPlanChange{{Cmd: statement}}}).MarshalSQL},
		{"inspect", newInspectReport(c,
			&schemamodel.Database{Functions: []schemamodel.Function{{Name: "f", Returns: "text", Language: "plpgsql", Body: body}}},
			&catalog.Database{}, catalog.ServerInfo{Dialect: platform.Postgres, Capabilities: capability.Of(map[capability.Capability]bool{capability.Functions: true})}, nil,
			atlasreport.SchemaInspectReportOptions{},
		).MarshalSQL},
	}
	for _, report := range reports {
		t.Run(report.name, func(t *testing.T) {
			c := qt.New(t)
			output, err := report.marshal("\t")
			c.Assert(err, qt.IsNil)
			c.Assert(output, qt.Contains, body)
		})
	}
}

// Every report indents its statements as its own dialect reads them. A SQL
// Server bracket identifier is quoted only under SQL Server's rules, so a
// report that dropped its dialect would reindent the second line of the name.
func TestSchemaReportsIndentReadTheirDialect(t *testing.T) {
	c := qt.New(t)
	const name = "[first\n  second]"
	const statement = "CREATE TABLE " + name + " ([id] INT)"
	reports := []struct {
		name    string
		marshal func(...string) (string, error)
	}{
		{"diff", atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{Dialect: platform.SQLServer, Statements: []string{statement}}).MarshalSQL},
		{"apply", atlasreport.NewSchemaApply(atlasreport.SchemaApplyOptions{Driver: platform.SQLServer, Statements: []string{statement}}).MarshalSQL},
		{"plan", atlasreport.NewSchemaPlan(atlasreport.SchemaPlanOptions{Dialect: platform.SQLServer, Statements: []atlasreport.SchemaPlanChange{{Cmd: statement}}}).MarshalSQL},
		{"inspect", newInspectReport(c,
			&schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "first\n  second"}},
				Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INT", Primary: true}},
			},
			&catalog.Database{}, catalog.ServerInfo{Dialect: platform.SQLServer}, nil,
			atlasreport.SchemaInspectReportOptions{},
		).MarshalSQL},
	}
	for _, report := range reports {
		t.Run(report.name, func(t *testing.T) {
			c := qt.New(t)
			output, err := report.marshal("\t")
			c.Assert(err, qt.IsNil)
			c.Assert(output, qt.Contains, name)
		})
	}
}
