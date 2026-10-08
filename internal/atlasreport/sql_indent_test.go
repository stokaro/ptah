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
	tests := []struct{ name, sql, want string }{
		{"dollar body", "CREATE FUNCTION f() RETURNS text AS $body$\nBEGIN\n RETURN 'first\nsecond';\nEND;\n$body$ LANGUAGE plpgsql;", "  CREATE FUNCTION f() RETURNS text AS $body$\nBEGIN\n RETURN 'first\nsecond';\nEND;\n$body$ LANGUAGE plpgsql;\n"},
		{"string", "SELECT 'first\nsecond';\nSELECT 2;", "  SELECT 'first\nsecond';\n  SELECT 2;\n"},
		{"quoted identifier", "CREATE TABLE \"first\nsecond\" (\n id int\n);", "  CREATE TABLE \"first\nsecond\" (\n   id int\n  );\n"},
		{"escaped quote", "SELECT E'first\\'\nsecond';", "  SELECT E'first\\'\nsecond';\n"},
		{"bracket identifier", "SELECT [first\nsecond];", "  SELECT [first\nsecond];\n"},
		{"backtick identifier", "SELECT `first\nsecond`;", "  SELECT `first\nsecond`;\n"},
		{"doubled quote", "SELECT 'first''\nsecond';", "  SELECT 'first''\nsecond';\n"},
		{"Unicode literal", "SELECT 'café\n日本';", "  SELECT 'café\n日本';\n"},
		{"comment quotes", "-- 'not a string\nSELECT 'first\nsecond';", "  -- 'not a string\n  SELECT 'first\nsecond';\n"},
		{"ordinary SQL", "CREATE TABLE t (\n id int\n);", "  CREATE TABLE t (\n   id int\n  );\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var output bytes.Buffer
			err := atlasreport.WriteMigrateDiff(&output, `{{ sql . "  " }}`, atlasreport.NewSchemaDiff(nil, nil, []string{test.sql}))
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
		{"diff", atlasreport.NewSchemaDiff(nil, nil, []string{statement}).MarshalSQL},
		{"apply", atlasreport.NewSchemaApply(atlasreport.SchemaApplyOptions{Statements: []string{statement}}).MarshalSQL},
		{"plan", atlasreport.NewSchemaPlan(atlasreport.SchemaPlanOptions{Statements: []atlasreport.SchemaPlanChange{{Cmd: statement}}}).MarshalSQL},
		{"inspect", newInspectReport(c,
			&schemamodel.Database{Functions: []schemamodel.Function{{Name: "f", Returns: "text", Language: "plpgsql", Body: body}}},
			&catalog.Database{}, catalog.ServerInfo{Dialect: platform.Postgres, Capabilities: capability.Capabilities{capability.Functions: true}}, nil,
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
