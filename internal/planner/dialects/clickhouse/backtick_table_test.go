package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// A table whose name holds a backtick is planned by its logical name, t`q, in
// the model and in the catalog alike, so the column change is the one change
// of that table, written with the backtick doubled. Read as one quoted
// identifier instead, the table's identity is not the captured table's, and
// the plan is refused as a feature table whose name disagrees with its
// captured identity.
func TestColumnChangeOfATableHoldingABacktick_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	storage := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()"}
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "t`q", StructName: "T", Facets: must.Must(schemaext.NewFacets(storage.Desired()))}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "T", Type: "UInt64"}, {Name: "c -- d", StructName: "T", Type: "Int64"}},
	}
	current := &catalog.Database{FeatureCoverage: observedStorageCoverage(), Tables: []catalog.Table{{
		Name: "t`q", Facets: must.Must(schemaext.NewFacets(storage)),
		Columns: []catalog.Column{
			{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO"},
			{Name: "c -- d", DataType: "Int32", ColumnType: "Int32", IsNullable: "NO"},
		},
	}}}
	diff, err := schemadiff.CompareWithDialect(t.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"ALTER TABLE `t``q` MODIFY COLUMN `c -- d` Int64"})
}

// The column a new sorting key adds belongs to the table holding the
// backtick: the storage owner reads the addition as a column of t`q, takes it
// into the ALTER TABLE that changes the key, and plans one statement. Read
// with the backtick still quoted, the column is no column of the captured
// table, and the plan is refused as a key over a column the table already
// has.
func TestSortingKeyColumnOfATableHoldingABacktick_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id"}
	after := before.Desired()
	after.OrderBy.Value = "id, k"
	source := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "t`q", StructName: "T", Facets: must.Must(schemaext.NewFacets(after))}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "T", Type: "UInt64"}, {Name: "k", StructName: "T", Type: "Int32"}},
	}
	current := &catalog.Database{FeatureCoverage: observedStorageCoverage(), Tables: []catalog.Table{{
		Name: "t`q", Facets: must.Must(schemaext.NewFacets(before)),
		Columns: []catalog.Column{{Name: "id", DataType: "UInt64", ColumnType: "UInt64", IsNullable: "NO", IsPrimaryKey: true}},
	}}}
	diff, err := schemadiff.CompareWithDialect(t.Context(), source, current, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, "clickhouse")
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"ALTER TABLE `t``q` ADD COLUMN `k` Int32, MODIFY ORDER BY (id, k)"})
}
