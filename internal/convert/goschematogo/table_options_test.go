package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_TableOptionsAReadBindsToTheMySQLFamily pins the Go export of a
// table read from a MySQL or MariaDB server: the read binds the table's
// character set to both targets, so it is written once for each, and the
// table's collation is written as the directive's collate. The export reads
// back to the same collation and to the character set on each target.
func TestRender_TableOptionsAReadBindsToTheMySQLFamily(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs", Collate: "latin1_swedish_ci",
					Facets: must.Must(mysqlschema.WithObservedTable(schemaext.Facets{}, "latin1"))}},
				Fields: []schemamodel.Field{{StructName: "Doc", FieldName: "ID", Name: "id", Type: "INT", Primary: true}},
			}

			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(),
				Runtime: builtintest.Runtime(), Dialect: dialect, SingleFile: true})
			c.Assert(err, qt.IsNil)
			exported, err := goschema.ParseSource(builtintest.Annotations(), files[0].Name, files[0].Data)

			c.Assert(err, qt.IsNil)
			c.Assert(string(files[0].Data), qt.Contains,
				`//ptah:schema:table name="docs" collate="latin1_swedish_ci" platform.mariadb.charset="latin1" platform.mysql.charset="latin1"`+"\n")
			c.Assert(exported.Tables[0].Collate, qt.Equals, "latin1_swedish_ci")
			c.Assert(exported.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{
				"mariadb": {"charset": "latin1"}, "mysql": {"charset": "latin1"},
			})
		})
	}
}
