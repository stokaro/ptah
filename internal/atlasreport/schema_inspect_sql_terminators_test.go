package atlasreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/atlasreport"
)

func TestRenderSchemaInspect_EmptySQLReturnsNoBytesAndKeepsOtherFormats(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "SQL", format: `{{ sql . }}`, want: ""},
		{name: "indented SQL", format: `{{ sql . "  " }}`, want: ""},
		{
			name:   "HCL control",
			format: `{{ hcl . }}`,
			want:   atlashclrender.GeneratedCodeMarker + "\n\nschema \"main\" {\n}\n\n",
		},
		{
			name:   "JSON control",
			format: `{{ json . }}`,
			want:   `{"schemas":[{"name":"main"}]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			output, err := atlasreport.RenderSchemaInspect(test.format, emptySQLiteInspectReport(false))

			c.Assert(err, qt.IsNil)
			c.Assert(output.Text, qt.Equals, test.want)
		})
	}
}

func TestRenderSchemaInspect_SQLKeepsRenderedTerminatorsAndIndentation(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{
			name:   "unindented",
			format: `{{ sql . }}`,
			want: "CREATE TABLE \"users\" (\n" +
				"  \"id\" INTEGER NOT NULL PRIMARY KEY,\n" +
				"  \"email\" TEXT\n" +
				");\n" +
				"CREATE INDEX IF NOT EXISTS \"idx_users_email\" ON \"users\" (\"email\");\n",
		},
		{
			name:   "every line stays indented",
			format: `{{ sql . "  " }}`,
			want: "  CREATE TABLE \"users\" (\n" +
				"    \"id\" INTEGER NOT NULL PRIMARY KEY,\n" +
				"    \"email\" TEXT\n" +
				"  );\n" +
				"  CREATE INDEX IF NOT EXISTS \"idx_users_email\" ON \"users\" (\"email\");\n" +
				"  ",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			output, err := atlasreport.RenderSchemaInspect(test.format, sqlTerminatorInspectReport())

			c.Assert(err, qt.IsNil)
			c.Assert(output.Text, qt.Equals, test.want)
		})
	}
}

func sqlTerminatorInspectReport() *atlasreport.SchemaInspectReport {
	return atlasreport.NewSchemaInspectReport(
		&goschema.Database{
			Tables: []goschema.Table{{StructName: "User", Name: "users"}},
			Fields: []goschema.Field{
				{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
				{StructName: "User", Name: "email", Type: "TEXT", Nullable: true},
			},
			Indexes: []goschema.Index{{
				Name: "idx_users_email", TableName: "users", Fields: []string{"email"},
			}},
		},
		&types.DBSchema{},
		types.DBInfo{Dialect: platform.SQLite, Schema: "main"},
		nil,
		atlasreport.SchemaInspectReportOptions{DescribeSchemas: true},
	)
}
