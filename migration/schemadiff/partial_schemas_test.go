package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// A comparison of a whole MySQL server drops a database the document does not
// declare, and keeps one the read holds only part of: DROP DATABASE would drop
// what a selector left out with it (stokaro/ptah#6797). The tables the read
// holds of it are removed either way.
func TestCompareWithDatabaseInfo_PartialSchemas(t *testing.T) {
	tests := []struct {
		name        string
		partial     []string
		wantRemoved []string
	}{
		{name: "a database the read holds whole", wantRemoved: []string{"db"}},
		{name: "a database the read holds part of", partial: []string{"db"}},
		{name: "another database the read holds part of", partial: []string{"app"}, wantRemoved: []string{"db"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app"}}}
			held := &catalog.Database{
				Schemas: []catalog.Schema{{Name: "app"}, {Name: "db"}},
				Tables: []catalog.Table{{Schema: "db", Name: "u", Columns: []catalog.Column{
					{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", OrdinalPosition: 1},
				}}},
				PartialSchemas: test.partial,
			}
			server := catalog.ServerInfo{Dialect: "mysql", WholeServer: true, NameFolding: catalog.NameFoldingExact}

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, held, server, nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.SchemasRemoved, qt.DeepEquals, test.wantRemoved)
			c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, []string{"db.u"})
		})
	}
}
