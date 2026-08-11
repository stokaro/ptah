package atlasreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasreport"
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
	c := qt.New(t)

	tests := []struct {
		name   string
		schema *types.DBSchema
		info   types.DBInfo
		want   string
	}{
		{
			// An empty database. The realm's schemas used to be derived from
			// its tables, so no table meant no schema, and `omitempty` then
			// removed the key entirely.
			name: "empty postgres database still reports its schema",
			schema: &types.DBSchema{
				Schemas: []types.DBSchemaInfo{
					{Name: "public", Comment: "standard public schema"},
				},
			},
			info: types.DBInfo{Dialect: "postgres", Schema: "public"},
			want: `{"schemas":[{"name":"public","comment":"standard public schema"}]}`,
		},
		{
			// The comment comes AFTER the tables. Go emits object keys in field
			// order, embedded fields included, so this row is what pins the
			// attributes to the end of the struct.
			name: "schema comment follows the tables",
			schema: &types.DBSchema{
				Schemas: []types.DBSchemaInfo{
					{Name: "public", Comment: "standard public schema"},
				},
				Tables: []types.DBTable{
					{
						Name:    "a",
						Columns: []types.DBColumn{{Name: "id", DataType: "integer", IsNullable: "YES"}},
					},
				},
			},
			info: types.DBInfo{Dialect: "postgres", Schema: "public"},
			want: `{"schemas":[{"name":"public","tables":[{"name":"a","columns":[{"name":"id","type":"integer","null":true}]}],"comment":"standard public schema"}]}`,
		},
		{
			// Realm scope: a schema that is not the connection's own keeps its
			// tables, and the two come back in byte order.
			name: "realm scope keeps every schema",
			schema: &types.DBSchema{
				Schemas: []types.DBSchemaInfo{
					{Name: "public", Comment: "standard public schema"},
					{Name: "extra"},
				},
				Tables: []types.DBTable{
					{
						Name:    "a",
						Columns: []types.DBColumn{{Name: "id", DataType: "integer", IsNullable: "YES"}},
					},
					{
						Name:    "b",
						Schema:  "extra",
						Columns: []types.DBColumn{{Name: "id", DataType: "integer", IsNullable: "YES"}},
					},
				},
			},
			info: types.DBInfo{Dialect: "postgres", Schema: "public"},
			want: `{"schemas":[{"name":"extra","tables":[{"name":"b","columns":[{"name":"id","type":"integer","null":true}]}]},{"name":"public","tables":[{"name":"a","columns":[{"name":"id","type":"integer","null":true}]}],"comment":"standard public schema"}]}`,
		},
		{
			// A table comment sits after the columns for the same reason a
			// schema comment sits after the tables.
			name: "table attributes follow the columns",
			schema: &types.DBSchema{
				Schemas: []types.DBSchemaInfo{{Name: "app", Comment: "app schema comment"}},
				Tables: []types.DBTable{
					{
						Name:    "t",
						Schema:  "app",
						Comment: "table comment",
						Columns: []types.DBColumn{{Name: "id", DataType: "integer", IsNullable: "YES"}},
					},
				},
			},
			info: types.DBInfo{Dialect: "postgres", Schema: "app"},
			want: `{"schemas":[{"name":"app","tables":[{"name":"t","columns":[{"name":"id","type":"integer","null":true}],"comment":"table comment"}],"comment":"app schema comment"}]}`,
		},
		{
			// MySQL-family schemas carry a character set and a collation
			// instead of a comment, in the same position.
			name: "empty mysql database reports charset and collation",
			schema: &types.DBSchema{
				Schemas: []types.DBSchemaInfo{
					{Name: "shop", Charset: "utf8mb4", Collate: "utf8mb4_0900_ai_ci"},
				},
			},
			info: types.DBInfo{Dialect: "mysql", Schema: "shop"},
			want: `{"schemas":[{"name":"shop","charset":"utf8mb4","collate":"utf8mb4_0900_ai_ci"}]}`,
		},
		{
			// The control for every row above: a selection that matched nothing
			// is an empty document on both implementations, measured with
			// `--schema nope`. Reporting a schema here would describe one the
			// operator was told does not exist.
			name:   "a selection that matched nothing stays empty",
			schema: &types.DBSchema{},
			info:   types.DBInfo{Dialect: "postgres", Schema: "public"},
			want:   `{}`,
		},
		{
			// A reader that describes no schemas keeps rendering its tables, so
			// the schema list can never cost a table.
			name: "tables without a described schema keep their schema",
			schema: &types.DBSchema{
				Tables: []types.DBTable{
					{
						Name:    "users",
						Columns: []types.DBColumn{{Name: "id", DataType: "integer", IsNullable: "NO"}},
					},
				},
			},
			info: types.DBInfo{Dialect: "sqlite", Schema: "main"},
			want: `{"schemas":[{"name":"main","tables":[{"name":"users","columns":[{"name":"id","type":"integer"}]}]}]}`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			report := atlasreport.NewSchemaInspectReport(
				&goschema.Database{},
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
