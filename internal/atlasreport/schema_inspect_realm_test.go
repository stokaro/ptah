package atlasreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasreport"
)

// TestRenderSchemaInspect_JSONRealmDocument pins the shape of the realm
// document against the pinned Atlas community binary v1.3.0.
//
// Every want string below was produced by that binary and is reproduced byte
// for byte, so a change of field order is a failure rather than a detail:
// `{{ json . }}` is the scripted surface, and a consumer diffs it.
//
// The rows are the three divergences of stokaro/ptah#1264 plus the control that
// stops the first one's fix from inventing a schema nobody has. Measured
// 2026-08-07 on PostgreSQL 17, MySQL 9.7 and SQLite.
func TestRenderSchemaInspect_JSONRealmDocument(t *testing.T) {
	tests := []struct {
		name   string
		schema *catalog.Database
		info   catalog.ServerInfo
		want   string
	}{
		{
			// An empty database. The realm's schemas used to be derived from
			// its tables, so no table meant no schema, and `omitempty` then
			// removed the key entirely.
			name: "empty postgres database still reports its schema",
			schema: &catalog.Database{
				Schemas: []catalog.Schema{
					{Name: "public", Comment: "standard public schema"},
				},
			},
			info: catalog.ServerInfo{Dialect: "postgres", Schema: "public"},
			want: `{"schemas":[{"name":"public","comment":"standard public schema"}]}`,
		},
		{
			// The comment comes AFTER the tables. Go emits object keys in field
			// order, embedded fields included, so this row is what pins the
			// attributes to the end of the struct.
			name: "schema comment follows the tables",
			schema: &catalog.Database{
				Schemas: []catalog.Schema{
					{Name: "public", Comment: "standard public schema"},
				},
				Tables: []catalog.Table{
					{
						Name:    "a",
						Columns: []catalog.Column{{Name: "id", DataType: "integer", IsNullable: "YES"}},
					},
				},
			},
			info: catalog.ServerInfo{Dialect: "postgres", Schema: "public"},
			want: `{"schemas":[{"name":"public","tables":[{"name":"a","columns":[{"name":"id","type":"integer","null":true}]}],"comment":"standard public schema"}]}`,
		},
		{
			// Realm scope: a schema that is not the connection's own keeps its
			// tables, and the two come back in byte order.
			name: "realm scope keeps every schema",
			schema: &catalog.Database{
				Schemas: []catalog.Schema{
					{Name: "public", Comment: "standard public schema"},
					{Name: "extra"},
				},
				Tables: []catalog.Table{
					{
						Name:    "a",
						Columns: []catalog.Column{{Name: "id", DataType: "integer", IsNullable: "YES"}},
					},
					{
						Name:    "b",
						Schema:  "extra",
						Columns: []catalog.Column{{Name: "id", DataType: "integer", IsNullable: "YES"}},
					},
				},
			},
			info: catalog.ServerInfo{Dialect: "postgres", Schema: "public"},
			want: `{"schemas":[{"name":"extra","tables":[{"name":"b","columns":[{"name":"id","type":"integer","null":true}]}]},{"name":"public","tables":[{"name":"a","columns":[{"name":"id","type":"integer","null":true}]}],"comment":"standard public schema"}]}`,
		},
		{
			// A table comment sits after the columns for the same reason a
			// schema comment sits after the tables.
			name: "table attributes follow the columns",
			schema: &catalog.Database{
				Schemas: []catalog.Schema{{Name: "app", Comment: "app schema comment"}},
				Tables: []catalog.Table{
					{
						Name:    "t",
						Schema:  "app",
						Comment: "table comment",
						Columns: []catalog.Column{{Name: "id", DataType: "integer", IsNullable: "YES"}},
					},
				},
			},
			info: catalog.ServerInfo{Dialect: "postgres", Schema: "app"},
			want: `{"schemas":[{"name":"app","tables":[{"name":"t","columns":[{"name":"id","type":"integer","null":true}],"comment":"table comment"}],"comment":"app schema comment"}]}`,
		},
		{
			// MySQL-family schemas carry a character set and a collation
			// instead of a comment, in the same position.
			name: "empty mysql database reports charset and collation",
			schema: &catalog.Database{
				Schemas: []catalog.Schema{
					{Name: "shop", Charset: "utf8mb4", Collate: "utf8mb4_0900_ai_ci"},
				},
			},
			info: catalog.ServerInfo{Dialect: "mysql", Schema: "shop"},
			want: `{"schemas":[{"name":"shop","charset":"utf8mb4","collate":"utf8mb4_0900_ai_ci"}]}`,
		},
		{
			// The control for every row above: a selection that matched nothing
			// is an empty document on both implementations, measured with
			// `--schema nope`. Reporting a schema here would describe one the
			// operator was told does not exist.
			name:   "a selection that matched nothing stays empty",
			schema: &catalog.Database{},
			info:   catalog.ServerInfo{Dialect: "postgres", Schema: "public"},
			want:   `{}`,
		},
		{
			// A reader that describes no schemas keeps rendering its tables, so
			// the schema list can never cost a table.
			name: "tables without a described schema keep their schema",
			schema: &catalog.Database{
				Tables: []catalog.Table{
					{
						Name:    "users",
						Columns: []catalog.Column{{Name: "id", DataType: "integer", IsNullable: "NO"}},
					},
				},
			},
			info: catalog.ServerInfo{Dialect: "sqlite", Schema: "main"},
			want: `{"schemas":[{"name":"main","tables":[{"name":"users","columns":[{"name":"id","type":"integer"}]}]}]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report := atlasreport.NewSchemaInspectReport(
				&schemamodel.Database{},
				test.schema,
				test.info,
				nil,
				atlasreport.SchemaInspectReportOptions{DescribeSchemas: true},
			)

			output, err := atlasreport.RenderSchemaInspect(`{{ json . }}`, report)

			c.Assert(err, qt.IsNil)
			c.Assert(output.Text, qt.Equals, test.want)
		})
	}
}
