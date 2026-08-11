package atlasreport_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasreport"
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

func TestRenderSchemaInspect_JSONTemplate(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	output, err := atlasreport.RenderSchemaInspect(`{{ json . }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Contains, `"schemas":[{"name":"main"`)
	c.Assert(output.Text, qt.Contains, `"tables":[{"name":"users"`)
	c.Assert(output.Text, qt.Contains, `"columns":[{"name":"id","type":"integer"`)
	c.Assert(output.Files, qt.HasLen, 0)
}

func TestValidateSchemaInspectTemplate_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := atlasreport.ValidateSchemaInspectTemplate(`{{ unknown . }}`)

	c.Assert(err, qt.ErrorMatches, `parse --format template: .*function "unknown" not defined.*`)
}

func TestRenderSchemaInspect_SQLTemplateRemainsStringCompatible(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	output, err := atlasreport.RenderSchemaInspect(`{{ len (sql .) }}:{{ printf "%s" (sql .) }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Contains, "CREATE TABLE")
}

func TestRenderSchemaInspect_SQLTemplateJSONUsesStringValue(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	output, err := atlasreport.RenderSchemaInspect(`{{ json (sql .) }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Contains, `CREATE TABLE`)
	c.Assert(output.Text, qt.Not(qt.Contains), `"Format"`)
}

func TestRenderSchemaInspect_SQLSplitRendersTxtar(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	output, err := atlasreport.RenderSchemaInspect(`{{ sql . | split }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Contains, "-- main.sql --")
	c.Assert(output.Text, qt.Contains, "-- tables/users.sql --")
	c.Assert(output.Text, qt.Contains, "-- atlas:import ./tables/users.sql")
	c.Assert(output.Text, qt.Contains, "CREATE TABLE")
}

func TestRenderSchemaInspect_SQLSplitObjectModeIsExplicit(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	output, err := atlasreport.RenderSchemaInspect(`{{ sql . | split "object" }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Contains, "-- main.sql --")
	c.Assert(output.Text, qt.Contains, "-- tables/users.sql --")
}

func TestRenderSchemaInspect_SQLSplitClassifiesPostgreSQLObjects(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()
	format := `{{ "CREATE MATERIALIZED VIEW user_stats AS SELECT 1; CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_users_email ON users (email);" | split }}`

	output, err := atlasreport.RenderSchemaInspect(format, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Contains, "-- materialized_views/user_stats.sql --")
	c.Assert(output.Text, qt.Contains, "-- indexes/idx_users_email.sql --")
}

func TestRenderSchemaInspect_SQLSplitTypeModeGroupsByObjectType(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()
	format := `{{ "CREATE TABLE users (id int); CREATE TABLE posts (id int); CREATE INDEX idx ON users (id);" | split "type" }}`

	output, err := atlasreport.RenderSchemaInspect(format, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Contains, "-- tables.sql --")
	c.Assert(output.Text, qt.Contains, "-- indexes.sql --")
	c.Assert(output.Text, qt.Not(qt.Contains), "-- main.sql --")
	c.Assert(output.Text, qt.Not(qt.Contains), "tables/users.sql")
	c.Assert(output.Text, qt.Contains, "CREATE TABLE users (id int);\n\nCREATE TABLE posts (id int);")
}

func TestRenderSchemaInspect_SQLSplitSchemaModeGroupsBySchema(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()
	format := `{{ "CREATE TABLE public.users (id int); CREATE TABLE audit.log (id int); CREATE TABLE sessions (id int);" | split "schema" }}`

	output, err := atlasreport.RenderSchemaInspect(format, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Contains, "-- public.sql --")
	c.Assert(output.Text, qt.Contains, "-- audit.sql --")
	// Unqualified objects belong to the report's default schema ("main" for
	// the sample SQLite report).
	c.Assert(output.Text, qt.Contains, "-- main.sql --")
	c.Assert(output.Text, qt.Contains, "CREATE TABLE sessions (id int);")
}

func TestRenderSchemaInspect_SQLSplitRejectsDuplicatePaths(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()
	format := `{{ "CREATE TABLE users (id int); CREATE TABLE users (id int);" | split }}`

	output, err := atlasreport.RenderSchemaInspect(format, report)

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*split generated duplicate output path "tables/users.sql"`)
	c.Assert(output.Text, qt.Equals, "")
}

func TestRenderSchemaInspect_HCLSplitRendersTxtar(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	output, err := atlasreport.RenderSchemaInspect(`{{ hcl . | split }}`, report)

	c.Assert(err, qt.IsNil)
	// The file name carries the schema because the table declares one. That
	// declaration is not cosmetic: without it the rendered HCL is invalid and
	// the pinned Atlas community binary v1.3.0 refuses the whole document
	// (stokaro/ptah#1234). Qualifying the name is also what keeps two tables of
	// the same name in different schemas from colliding on one path, which
	// validateUniqueSchemaInspectArchivePaths would otherwise reject.
	c.Assert(output.Text, qt.Contains, "-- schemas/main.hcl --")
	c.Assert(output.Text, qt.Contains, "-- tables/main_users.hcl --")
	c.Assert(output.Text, qt.Contains, `table "users"`)
	c.Assert(output.Text, qt.Contains, `comment = "keeps { braces } in strings"`)
}

func TestRenderSchemaInspect_HCLSplitTypeModeGroupsByObjectType(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	output, err := atlasreport.RenderSchemaInspect(`{{ hcl . | split "type" ".sqlite.hcl" }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Contains, "-- tables.sqlite.hcl --")
	c.Assert(output.Text, qt.Contains, `table "users"`)
	c.Assert(output.Text, qt.Not(qt.Contains), "-- tables/")
}

func TestRenderSchemaInspect_HCLSplitSchemaModeGroupsBySchema(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()
	hcl := `schema "public" {}
schema "audit" {}
table "users" {
  schema = schema.public
}
table "log" {
  schema = schema.audit
}
`
	format := fmt.Sprintf(`{{ %q | split "schema" ".my.hcl" }}`, hcl)

	output, err := atlasreport.RenderSchemaInspect(format, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Contains, "-- public.my.hcl --")
	c.Assert(output.Text, qt.Contains, "-- audit.my.hcl --")
	c.Assert(output.Text, qt.Contains, "schema \"public\" {}\n\ntable \"users\"")
}

func TestRenderSchemaInspect_HCLSplitKeepsSchemaQualifiedTablesDistinct(t *testing.T) {
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

	output, err := atlasreport.RenderSchemaInspect(format, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Contains, "-- tables/public_users.hcl --")
	c.Assert(output.Text, qt.Contains, "-- tables/audit_users.hcl --")
}

func TestRenderSchemaInspect_SplitRejectsUnsupportedMode(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	output, err := atlasreport.RenderSchemaInspect(`{{ sql . | split "table" }}`, report)

	c.Assert(err, qt.ErrorMatches, `execute --format template: .*unsupported split mode "table": supported modes are object, schema, and type`)
	c.Assert(output.Text, qt.Equals, "")
}

func TestRenderSchemaInspect_SplitRejectsUnsafeExtension(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	tests := []struct {
		name      string
		extension string
		wantErr   string
	}{
		{name: "no leading dot", extension: "hcl", wantErr: `.*split extension "hcl" must start with a dot.*`},
		{name: "path separator", extension: "./../x", wantErr: `.*split extension "\./\.\./x" must not contain path separators.*`},
		{name: "dot only", extension: ".", wantErr: `.*split extension "\." must start with a dot followed by a file suffix.*`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			format := fmt.Sprintf(`{{ sql . | split "type" %q }}`, test.extension)

			output, err := atlasreport.RenderSchemaInspect(format, report)

			c.Assert(err, qt.ErrorMatches, `execute --format template: `+test.wantErr)
			c.Assert(output.Text, qt.Equals, "")
		})
	}
}

// TestRenderSchemaInspect_WritePlansFilesWithoutTouchingFilesystem pins the
// purity contract: rendering a split|write template returns the planned files
// and writes nothing itself.
func TestRenderSchemaInspect_WritePlansFilesWithoutTouchingFilesystem(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()
	outDir := filepath.Join(t.TempDir(), "schema")

	output, err := atlasreport.RenderSchemaInspect(`{{ sql . | split | write "`+outDir+`" }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Equals, "")
	c.Assert(output.Files, qt.HasLen, 2)
	c.Assert(output.Files[0].Dir, qt.Equals, outDir)
	c.Assert(output.Files[0].Path, qt.Equals, "main.sql")
	c.Assert(output.Files[0].Data, qt.Contains, "-- atlas:import ./tables/users.sql")
	c.Assert(output.Files[1].Path, qt.Equals, "tables/users.sql")
	c.Assert(output.Files[1].Data, qt.Contains, "CREATE TABLE")
	_, statErr := os.Stat(outDir)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestRenderSchemaInspect_WriteDefaultsToCurrentDirectory(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	output, err := atlasreport.RenderSchemaInspect(`{{ sql . | split | write }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Files, qt.Not(qt.HasLen), 0)
	c.Assert(output.Files[0].Dir, qt.Equals, ".")
}

func TestRenderSchemaInspect_SplitRejectsNonSchemaOutput(t *testing.T) {
	c := qt.New(t)
	report := sampleSchemaInspectReport()

	tests := []struct {
		name   string
		format string
	}{
		{name: "report value", format: `{{ split . }}`},
		{name: "json text", format: `{{ json . | split }}`},
		{name: "mermaid text", format: `{{ mermaid . | split }}`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			output, err := atlasreport.RenderSchemaInspect(test.format, report)

			c.Assert(err, qt.ErrorMatches, `execute --format template: .*split requires hcl or sql schema output`)
			c.Assert(output.Text, qt.Equals, "")
		})
	}
}

// TestRenderSchemaInspect_JSONColumnTypeMatchesThePinnedBinary pins the type
// spelling the Atlas-compatible JSON inspect surface prints per column class.
//
// Every want below was measured on PostgreSQL 17.10 by running
// `schema inspect --format '{{ json . }}'` on the pinned community binary
// v1.3.0 and on ptah-compat against the same database. The domain rows are
// where the two disagreed: information_schema reports a domain column's BASE
// type in data_type, so Ptah printed "integer" for a column of `positive` and
// dropped the domain's CHECK with it, while the binary printed the domain.
//
// The array row is why this reads DomainName rather than FormattedType, which
// an array column fills too. There the binary prints the bare category "ARRAY"
// and Ptah already agreed; preferring FormattedType outright would print
// "character varying(100)[]" there and trade one disagreement for another.
// See stokaro/ptah#1242.
func TestRenderSchemaInspect_JSONColumnTypeMatchesThePinnedBinary(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		column types.DBColumn
		want   string
	}{
		{
			name: "domain column names the domain",
			column: types.DBColumn{
				Name: "qty", DataType: "integer", UDTName: "int4",
				FormattedType: "positive", DomainName: "positive", IsNullable: "NO",
			},
			want: `{"name":"qty","type":"positive"}`,
		},
		{
			name: "domain outside the search path keeps its qualifier",
			column: types.DBColumn{
				Name: "qty", DataType: "integer", UDTName: "int4",
				FormattedType: "doms.positive", DomainName: "positive", IsNullable: "NO",
			},
			want: `{"name":"qty","type":"doms.positive"}`,
		},
		{
			name: "array column stays the bare category",
			column: types.DBColumn{
				Name: "tags", DataType: "ARRAY", UDTName: "_varchar",
				FormattedType: "character varying(100)[]", IsNullable: "NO",
			},
			want: `{"name":"tags","type":"ARRAY"}`,
		},
		{
			name:   "plain column is untouched",
			column: types.DBColumn{Name: "id", DataType: "integer", IsNullable: "NO"},
			want:   `{"name":"id","type":"integer"}`,
		},
		{
			name: "a dialect that reports its own full spelling keeps it",
			column: types.DBColumn{
				Name: "mood", DataType: "enum", ColumnType: "enum('sad','ok')", IsNullable: "NO",
			},
			want: `{"name":"mood","type":"enum('sad','ok')"}`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			report := atlasreport.NewSchemaInspectReport(
				&goschema.Database{},
				&types.DBSchema{
					Tables: []types.DBTable{{
						Name:    "t",
						Schema:  "public",
						Columns: []types.DBColumn{test.column},
					}},
				},
				types.DBInfo{Dialect: "postgres", Schema: "public"},
				nil,
				// The run did not choose its own scope, so the SQL format would
				// leave the schema row out. This case renders JSON, which
				// describes it either way; the zero-value option is the
				// connected-schema one the fixture represents.
				atlasreport.SchemaInspectReportOptions{},
			)

			output, err := atlasreport.RenderSchemaInspect(`{{ json . }}`, report)

			c.Assert(err, qt.IsNil)
			c.Assert(output.Text, qt.Contains, test.want)
		})
	}
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
		atlasreport.SchemaInspectReportOptions{DescribeSchemas: true},
	)
}
