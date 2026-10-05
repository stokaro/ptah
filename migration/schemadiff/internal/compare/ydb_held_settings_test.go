package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestCurrentYDBSettings carries what each table and global index holds of
// its settings, for every table holding a setting off YDB's documented
// defaults, sorted by table: a table holding only defaults is left out, and so
// is one whose only tuned object is another table's.
func TestCurrentYDBSettings(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		Tables: []catalog.Table{
			{Schema: "app", Name: "z_items", YDBPartitioning: &ast.YDBTablePartitioningSpec{MinPartitions: 4}},
			{Schema: "app", Name: "plain"},
			{Schema: "app", Name: "a_orders"},
		},
		Indexes: []catalog.Index{
			{Schema: "app", TableName: "a_orders", Name: "by_customer", Partitioning: &ast.IndexPartitioningSpec{ByLoad: new(true)}},
			{Schema: "app", TableName: "a_orders", Name: "by_date"},
			{Schema: "app", TableName: "plain", Name: "by_name"},
		},
	}

	got := compare.CurrentYDBSettings(database)

	c.Assert(got, qt.DeepEquals, []difftypes.YDBHeldSettings{
		{TableName: "app.a_orders", Indexes: map[string]*ast.IndexPartitioningSpec{"by_customer": {ByLoad: new(true)}}},
		{TableName: "app.z_items", Partitioning: &ast.YDBTablePartitioningSpec{MinPartitions: 4}},
	})
	got[1].Partitioning.MinPartitions = 9
	c.Assert(database.Tables[0].YDBPartitioning.MinPartitions, qt.Equals, uint64(4),
		qt.Commentf("the carry must not share a pointer with the read"))
}

// TestCurrentYDBSettings_NothingTuned is the control: a read with no tuned
// table carries nothing, as every read of another engine does.
func TestCurrentYDBSettings_NothingTuned(t *testing.T) {
	c := qt.New(t)
	c.Assert(compare.CurrentYDBSettings(&catalog.Database{Tables: []catalog.Table{{Name: "t"}}}), qt.HasLen, 0)
	c.Assert(compare.CurrentYDBSettings(nil), qt.HasLen, 0)
}
