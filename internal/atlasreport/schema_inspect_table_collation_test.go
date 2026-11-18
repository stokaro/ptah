package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/atlasreport"
)

// collatedMySQLTable is a MySQL read of one table in shop whose charset and
// collation restate the schema's.
func collatedMySQLTable() *catalog.Database {
	return &catalog.Database{
		Schemas: []catalog.Schema{{Name: "shop", Charset: "utf8mb4", Collate: "utf8mb4_unicode_ci"}},
		Tables: []catalog.Table{{
			Name: "t", Schema: "shop", Comment: "orders", Charset: "utf8mb4", Collate: "utf8mb4_unicode_ci",
			Columns: []catalog.Column{{Name: "id", DataType: "int", ColumnType: "int"}},
		}},
	}
}

// The JSON document lists a MySQL table's charset and collation after its
// children, beside its comment, whether or not they restate the schema's, as
// the pinned community binary v1.3.0 does on MySQL 8.4.
func TestSchemaInspectJSON_ListsAMySQLTableCharsetAndCollation(t *testing.T) {
	c := qt.New(t)
	report := newInspectReport(c, &schemamodel.Database{}, collatedMySQLTable(),
		catalog.ServerInfo{Dialect: "mysql", Schema: "shop"}, nil, atlasreport.SchemaInspectReportOptions{})

	output, err := atlasreport.RenderSchemaInspect(`{{ json . }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Equals, `{"schemas":[{"name":"shop","tables":[{"name":"t","columns":[{"name":"id","type":"int"}],`+
		`"comment":"orders","charset":"utf8mb4","collate":"utf8mb4_unicode_ci"}],`+
		`"charset":"utf8mb4","collate":"utf8mb4_unicode_ci"}]}`)
}

// A table's options that state only its charset are what the document's
// charset attribute carries, so JSON reports nothing left out; an engine has
// no attribute there and is reported.
func TestSchemaInspectJSON_ReportsTheMySQLTableOptionsItLeavesOut(t *testing.T) {
	tests := []struct {
		name    string
		options schemaext.Value
		want    string
	}{
		{name: "an observed charset", options: &mysqlschema.ObservedTable{Charset: "utf8mb4"}},
		{name: "a declared charset", options: &mysqlschema.DesiredTable{Charset: "utf8mb4"}},
		{name: "a declared engine", options: &mysqlschema.DesiredTable{Engine: "MyISAM", Charset: "utf8mb4"},
			want: "warning: JSON schema inspection leaves out MySQL table options (1) from table \"shop.t\"\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var diagnostics bytes.Buffer
			db := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "t", Schema: "shop", Collate: "utf8mb4_unicode_ci",
				Facets: must.Must(schemaext.NewFacets(test.options))}}}
			report := newInspectReport(c, db, collatedMySQLTable(), catalog.ServerInfo{Dialect: "mysql", Schema: "shop"},
				&diagnostics, atlasreport.SchemaInspectReportOptions{})

			_, err := atlasreport.RenderSchemaInspect(`{{ json . }}`, report)

			c.Assert(err, qt.IsNil)
			c.Assert(diagnostics.String(), qt.Equals, test.want)
		})
	}
}
