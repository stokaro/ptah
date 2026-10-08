package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvert_DescribesAPrimaryKeyMethod keeps the access method of a
// single-column primary key the MySQL-family reader reports. The column has
// no place for it, so the key is written on the table, as a deferrable one is
// (stokaro/ptah#3853).
func TestConvert_DescribesAPrimaryKeyMethod(t *testing.T) {
	c := qt.New(t)
	hash := "HASH"
	schema := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "t", Type: "BASE TABLE",
			Columns: []catalog.Column{{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true}},
		}},
		Constraints: []catalog.Constraint{{
			TableName: "t", Name: "PRIMARY", Type: "PRIMARY KEY", ColumnNames: []string{"id"}, UsingMethod: &hash,
		}},
	}

	database := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), schema, "mariadb", must.Must(builtin.New())))

	c.Assert(database.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id"})
	c.Assert(database.Tables[0].PrimaryKeyMethod, qt.Equals, "HASH")
}
