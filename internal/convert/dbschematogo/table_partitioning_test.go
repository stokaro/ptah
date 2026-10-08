package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// A YDB table's settings, as the reader reports them, reach the declaration
// built from the database, cloned, so `ptah introspect` writes them back and a
// comparison of the two finds nothing to change.
func TestConvert_CarriesATablesPartitioning(t *testing.T) {
	c := qt.New(t)
	settings := &ast.YDBTablePartitioningSpec{ByLoad: new(true), MinPartitions: 4, KeyBloomFilter: new(true)}
	database := &catalog.Database{Tables: []catalog.Table{{Name: "items", Type: "TABLE", YDBPartitioning: settings,
		Columns: []catalog.Column{{Name: "id", DataType: "Uint64", ColumnType: "Uint64", IsNullable: "NO",
			IsPrimaryKey: true, OrdinalPosition: 1}}}}}

	converted := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), database, platform.YDB, must.Must(builtin.New())))

	c.Assert(converted.Tables, qt.HasLen, 1)
	c.Assert(converted.Tables[0].YDBPartitioning, qt.DeepEquals, settings)
	*converted.Tables[0].YDBPartitioning.ByLoad = false
	c.Assert(*settings.ByLoad, qt.IsTrue, qt.Commentf("the declaration shares no pointer with the description"))
}
