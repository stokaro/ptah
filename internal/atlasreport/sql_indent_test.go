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

// Indentation is presentation, never a change to a stored routine or literal.
func TestMigrateDiffIndentPreservesQuotedSQL(t *testing.T) {
	tests := []struct{ name, dialect, sql, want string }{
		{"dollar body", platform.Postgres, "CREATE FUNCTION f() RETURNS text AS $body$\nBEGIN\n RETURN 'first\nsecond';\nEND;\n$body$ LANGUAGE plpgsql;", "  CREATE FUNCTION f() RETURNS text AS $body$\nBEGIN\n RETURN 'first\nsecond';\nEND;\n$body$ LANGUAGE plpgsql;\n"},
		{"string", platform.Postgres, "SELECT 'first\nsecond';\nSELECT 2;", "  SELECT 'first\nsecond';\n  SELECT 2;\n"},
		{"quoted identifier", platform.Postgres, "CREATE TABLE \"first\nsecond\" (\n id int\n);", "  CREATE TABLE \"first\nsecond\" (\n   id int\n  );\n"},
		{"escaped quote", platform.Postgres, "SELECT E'first\\'\nsecond';", "  SELECT E'first\\'\nsecond';\n"},
		{"backslash escape", platform.MySQL, "SELECT 'first\\'\nsecond';", "  SELECT 'first\\'\nsecond';\n"},
		{"bracket identifier", platform.SQLServer, "SELECT [first\nsecond];", "  SELECT [first\nsecond];\n"},
		{"backtick identifier", platform.MySQL, "SELECT `first\nsecond`;", "  SELECT `first\nsecond`;\n"},
		{"multiline string", platform.YDB, "SELECT @@first\nsecond@@;", "  SELECT @@first\nsecond@@;\n"},
		{"doubled quote", platform.Postgres, "SELECT 'first''\nsecond';", "  SELECT 'first''\nsecond';\n"},
		{"Unicode literal", platform.Postgres, "SELECT 'café\n日本';", "  SELECT 'café\n日本';\n"},
		{"comment quotes", platform.Postgres, "-- 'not a string\nSELECT 'first\nsecond';", "  -- 'not a string\n  SELECT 'first\nsecond';\n"},
		{"ordinary SQL", platform.Postgres, "CREATE TABLE t (\n id int\n);", "  CREATE TABLE t (\n   id int\n  );\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var output bytes.Buffer
			report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{Dialect: test.dialect, Statements: []string{test.sql}})
			err := atlasreport.WriteMigrateDiff(&output, `{{ sql . "  " }}`, report)
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
		{"T-SQL global variables", platform.SQLServer, "SELECT @@ROWCOUNT,\n@@ERROR;", "  SELECT @@ROWCOUNT,\n  @@ERROR;\n"},
		{"YQL multiline string", platform.YDB, "SELECT @@ROWCOUNT,\n@@ERROR;", "  SELECT @@ROWCOUNT,\n@@ERROR;\n"},
		{"PostgreSQL array", platform.Postgres, "SELECT ARRAY[1,\n2];", "  SELECT ARRAY[1,\n  2];\n"},
		{"T-SQL bracket identifier", platform.SQLServer, "SELECT ARRAY[1,\n2];", "  SELECT ARRAY[1,\n2];\n"},
		{"standard string ending in a backslash", platform.Postgres, "SELECT 'a\\';\nSELECT 'b\nc';", "  SELECT 'a\\';\n  SELECT 'b\nc';\n"},
		{"MySQL escaped quote", platform.MySQL, "SELECT 'a\\';\nSELECT 'b\nc';", "  SELECT 'a\\';\nSELECT 'b\n  c';\n"},
		{"no dialect", "", "SELECT [first\nsecond], `third\nfourth`;", "  SELECT [first\n  second], `third\nfourth`;\n"},
		{"unknown dialect", "nonesuch", "SELECT [first\nsecond], `third\nfourth`;", "  SELECT [first\n  second], `third\nfourth`;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var output bytes.Buffer
			report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{Dialect: test.dialect, Statements: []string{test.sql}})
			err := atlasreport.WriteMigrateDiff(&output, `{{ sql . "  " }}`, report)
			c.Assert(err, qt.IsNil)
			c.Assert(output.String(), qt.Equals, test.want)
		})
	}
}

func TestSchemaReportsIndentPreservesRoutineBody(t *testing.T) {
	c := qt.New(t)
	const body = "BEGIN\n RETURN 'first\nsecond';\nEND;"
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
			output, err := report.marshal("  ")
			c.Assert(err, qt.IsNil)
			c.Assert(output, qt.Contains, body)
		})
	}
}

// Every report indents its statements as its own dialect reads them. A SQL
// Server bracket identifier is quoted only under SQL Server's rules, so a
// report that dropped its dialect would indent the second line of the name.
func TestSchemaReportsIndentReadTheirDialect(t *testing.T) {
	c := qt.New(t)
	const name = "[first\nsecond]"
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
				Tables: []schemamodel.Table{{StructName: "T", Name: "first\nsecond"}},
				Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INT", Primary: true}},
			},
			&catalog.Database{}, catalog.ServerInfo{Dialect: platform.SQLServer}, nil,
			atlasreport.SchemaInspectReportOptions{},
		).MarshalSQL},
	}
	for _, report := range reports {
		t.Run(report.name, func(t *testing.T) {
			c := qt.New(t)
			output, err := report.marshal("  ")
			c.Assert(err, qt.IsNil)
			c.Assert(output, qt.Contains, name)
		})
	}
}
