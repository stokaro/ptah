package atlasreport_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasreport"
)

func TestNormalizeSchemaInspectFormat_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "default", format: "", want: "{{ $.MarshalHCL }}"},
		{name: "hcl", format: "hcl", want: "{{ $.MarshalHCL }}"},
		{name: "sql", format: "sql", want: "{{ sql . }}"},
		{name: "json", format: "json", want: "{{ json . }}"},
		{name: "custom", format: "{{ json . }}", want: "{{ json . }}"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasreport.NormalizeSchemaInspectFormat(test.format)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestRenderSchemaInspectFormat_JSONTemplate(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	rendered, err := atlasreport.RenderSchemaInspectFormat(`{{ json . }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `"schemas":[{"name":"main"`)
	c.Assert(rendered, qt.Contains, `"tables":[{"name":"users"`)
	c.Assert(rendered, qt.Contains, `"columns":[{"name":"id","type":"integer"`)
}

func TestValidateSchemaInspectTemplate_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := atlasreport.ValidateSchemaInspectTemplate(`{{ unknown . }}`)

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*function "unknown" not defined.*`)
}

func TestRenderSchemaInspectFormat_SQLTemplateRemainsStringCompatible(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	rendered, err := atlasreport.RenderSchemaInspectFormat(`{{ len (sql .) }}:{{ printf "%s" (sql .) }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, "CREATE TABLE")
}

func TestRenderSchemaInspectFormat_SQLTemplateJSONUsesStringValue(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	rendered, err := atlasreport.RenderSchemaInspectFormat(`{{ json (sql .) }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `CREATE TABLE`)
	c.Assert(rendered, qt.Not(qt.Contains), `"Format"`)
}

func TestRenderSchemaInspectFormat_SQLSplitRendersTxtar(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	rendered, err := atlasreport.RenderSchemaInspectFormat(`{{ sql . | split }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, "-- main.sql --")
	c.Assert(rendered, qt.Contains, "-- tables/users.sql --")
	c.Assert(rendered, qt.Contains, "-- atlas:import ./tables/users.sql")
	c.Assert(rendered, qt.Contains, "CREATE TABLE")
}

func TestRenderSchemaInspectFormat_SQLSplitClassifiesPostgreSQLObjects(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()
	format := `{{ "CREATE MATERIALIZED VIEW user_stats AS SELECT 1; CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_users_email ON users (email);" | split }}`

	rendered, err := atlasreport.RenderSchemaInspectFormat(format, report)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, "-- materialized_views/user_stats.sql --")
	c.Assert(rendered, qt.Contains, "-- indexes/idx_users_email.sql --")
}

func TestRenderSchemaInspectFormat_SQLSplitRejectsDuplicatePaths(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()
	format := `{{ "CREATE TABLE users (id int); CREATE TABLE users (id int);" | split }}`

	rendered, err := atlasreport.RenderSchemaInspectFormat(format, report)

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*split generated duplicate output path "tables/users.sql"`)
	c.Assert(rendered, qt.Equals, "")
}

func TestRenderSchemaInspectFormat_HCLSplitRendersTxtar(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	rendered, err := atlasreport.RenderSchemaInspectFormat(`{{ hcl . | split "type" ".sqlite.hcl" }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, "-- tables/users.sqlite.hcl --")
	c.Assert(rendered, qt.Contains, `table "users"`)
	c.Assert(rendered, qt.Contains, `comment = "keeps { braces } in strings"`)
}

func TestRenderSchemaInspectFormat_HCLSplitKeepsSchemaQualifiedTablesDistinct(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()
	hcl := `schema "public" {}
schema "audit" {}
table "users" {
  schema = schema.public
}
table "users" {
  schema = schema.audit
}
`
	format := fmt.Sprintf(`{{ %q | split }}`, hcl)

	rendered, err := atlasreport.RenderSchemaInspectFormat(format, report)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, "-- tables/public_users.hcl --")
	c.Assert(rendered, qt.Contains, "-- tables/audit_users.hcl --")
}

func TestRenderSchemaInspectFormat_WriteSplitOutput(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()
	outDir := filepath.Join(t.TempDir(), "schema")

	rendered, err := atlasreport.RenderSchemaInspectFormat(`{{ sql . | split | write "`+outDir+`" }}`, report)

	c.Assert(rendered, qt.Equals, "")
	c.Assert(err, qt.IsNil)
	mainSQL, readErr := os.ReadFile(filepath.Join(outDir, "main.sql"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(mainSQL), qt.Contains, "-- atlas:import ./tables/users.sql")
	usersSQL, readErr := os.ReadFile(filepath.Join(outDir, "tables", "users.sql"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(usersSQL), qt.Contains, "CREATE TABLE")
}

func TestRenderSchemaInspectFormat_WriteRejectsTraversalPath(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	rendered, err := atlasreport.RenderSchemaInspectFormat(`{{ sql . | split | write "../outside" }}`, report)

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*resolve output directory: .*outside allowed root.*`)
	c.Assert(rendered, qt.Equals, "")
}

func TestRenderSchemaInspectFormat_SplitRejectsNonSchemaOutput(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	rendered, err := atlasreport.RenderSchemaInspectFormat(`{{ split . }}`, report)

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*split requires hcl or sql schema output`)
	c.Assert(rendered, qt.Equals, "")
}

func sampleSchemaInspectReport() *atlasreport.SchemaInspectReport {
	return atlasreport.NewSchemaInspectReport(
		&goschema.Database{
			Tables: []goschema.Table{
				{StructName: "User", Name: "users", Comment: "keeps { braces } in strings"},
			},
			Fields: []goschema.Field{
				{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
				{StructName: "User", Name: "email", Type: "TEXT"},
			},
		},
		&types.DBSchema{
			Tables: []types.DBTable{
				{
					Name:   "users",
					Schema: "main",
					Columns: []types.DBColumn{
						{Name: "id", DataType: "integer", IsNullable: "NO"},
					},
				},
			},
		},
		types.DBInfo{Dialect: "sqlite", Schema: "main"},
		nil,
	)
}
