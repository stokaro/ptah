package atlasreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasreport"
)

// foreignKeyServer is a whole MySQL server read with one foreign key from
// r5.x to referenced, which is in referencedSchema.
func foreignKeyServer(referencedSchema, referenced string) *catalog.Database {
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "r1"}, {Name: "r5"}},
		Tables: []catalog.Table{
			{Name: "t", Schema: "r1", Columns: []catalog.Column{{Name: "id", DataType: "int", IsNullable: "NO"}}},
			{Name: "u", Schema: "r5", Columns: []catalog.Column{{Name: "id", DataType: "int", IsNullable: "NO"}}},
			{Name: "x", Schema: "r5", Columns: []catalog.Column{{Name: "ref", DataType: "int", IsNullable: "YES"}}},
		},
		Constraints: []catalog.Constraint{{
			Name:           "fk",
			TableName:      "x",
			Schema:         "r5",
			Type:           "FOREIGN KEY",
			ColumnNames:    []string{"ref"},
			ForeignTable:   new(referenced),
			ForeignSchema:  referencedSchema,
			ForeignColumns: []string{"id"},
		}},
	}
}

// TestRenderSchemaInspect_JSONForeignKeyReference names the referenced table
// alone when it is in the key's own schema, as the pinned community binary
// v1.3.0 writes it: measured on MySQL 8.4.11 for `r5.x` referencing `r5.u`,
// and on PostgreSQL 18.6 for `app.x` referencing `app.u`, it writes
// `"table":"u"`. A table in another schema keeps its schema, which the
// community binary drops (`"table":"t"` for `r1.t`), so the document still
// says which table it is.
func TestRenderSchemaInspect_JSONForeignKeyReference(t *testing.T) {
	tests := []struct {
		name   string
		schema *catalog.Database
		want   string
	}{
		{
			name:   "a table of the key's own schema",
			schema: foreignKeyServer("r5", "u"),
			want:   `"references":{"table":"u","columns":["id"]}`,
		},
		{
			name:   "a table of another schema",
			schema: foreignKeyServer("r1", "t"),
			want:   `"references":{"table":"r1.t","columns":["id"]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report := atlasreport.NewSchemaInspectReport(
				&schemamodel.Database{}, test.schema, catalog.ServerInfo{Dialect: "mysql"}, nil,
				atlasreport.SchemaInspectReportOptions{DescribeSchemas: true},
			)

			output, err := atlasreport.RenderSchemaInspect(`{{ json . }}`, report)

			c.Assert(err, qt.IsNil)
			c.Assert(output.Text, qt.Contains, test.want)
		})
	}
}
