package atlasschema

// White-box testing required: renderInspectSchema selects the dialect passed
// to the catalog converter. Its exported callers require a live database, so
// this fixture isolates the conversion and rendering used by every source.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

func TestRenderInspectSchema_PreservesPrimaryKeyComment(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range []struct {
			name    string
			columns []string
			size    uint64
		}{
			{name: "comment alone", columns: []string{"id"}},
			{name: "comment and block size", columns: []string{"id"}, size: 8},
			{name: "composite key", columns: []string{"id", "tenant_id"}, size: 8},
		} {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				current := &catalog.Database{
					Tables: []catalog.Table{{
						Name: "accounts", RowFormat: "Compressed",
						Columns: []catalog.Column{
							{Name: "id", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
							{Name: "tenant_id", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO"},
						},
					}},
					Constraints: []catalog.Constraint{{
						Name: "PRIMARY", TableName: "accounts", Type: "PRIMARY KEY",
						ColumnNames: test.columns, Comment: "account identity", KeyBlockSize: test.size,
					}},
				}

				result, err := renderInspectSchema(current, catalog.ServerInfo{Dialect: dialect}, InspectOptions{Format: "sql"})

				c.Assert(err, qt.IsNil)
				c.Assert(result.Rendered, qt.Contains, "COMMENT 'account identity'")
				c.Assert(result.Schema.Tables, qt.HasLen, 1)
				c.Assert(result.Schema.Tables[0].PrimaryKey, qt.DeepEquals, test.columns)
				c.Assert(result.Schema.Tables[0].PrimaryKeyComment, qt.Equals, "account identity")
				c.Assert(result.Schema.Tables[0].PrimaryKeyBlockSize, qt.Equals, test.size)
				desired, _, err := sqlschema.Read([]byte(result.Rendered), dialect)
				c.Assert(err, qt.IsNil)
				c.Assert(schemadiff.CompareWithDialect(&desired, current, dialect).HasChanges(), qt.IsFalse)
			})
		}
	}
}
