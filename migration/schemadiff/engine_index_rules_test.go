package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// filteredIndex is a SQL Server filtered index the declaration writes with
// declared and the catalog reports with stored.
func filteredIndex(declared, stored string) (*schemamodel.Database, *catalog.Database) {
	return &schemamodel.Database{Indexes: []schemamodel.Index{
			{Name: "idx_users_active", TableName: "users", Fields: []string{"email"}, Condition: declared},
		}},
		&catalog.Database{Indexes: []catalog.Index{
			{Name: "idx_users_active", TableName: "users", Columns: []string{"email"}, Condition: stored},
		}}
}

// TestCompareWithDialect_SQLServerFilteredIndexReadsTheCatalogSpelling drives
// the predicate rule SQL Server registers through a whole comparison. SQL
// Server stores `status = 1` as `([status]=(1))`, so without the rule an
// unchanged filtered index was dropped and created on every run; a predicate
// that really changed is still a replacement.
func TestCompareWithDialect_SQLServerFilteredIndexReadsTheCatalogSpelling(t *testing.T) {
	runtime := must.Must(builtin.New())

	t.Run("the catalog's spelling of the declared predicate", func(t *testing.T) {
		c := qt.New(t)
		desired, current := filteredIndex("status = 1", "([status]=(1))")

		diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "sqlserver", runtime)

		c.Assert(err, qt.IsNil)
		c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
		c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
	})

	t.Run("another predicate", func(t *testing.T) {
		c := qt.New(t)
		desired, current := filteredIndex("status = 1", "([status]=(2))")

		diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "sqlserver", runtime)

		c.Assert(err, qt.IsNil)
		c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{Name: "idx_users_active", TableName: "users"}})
		c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{Name: "idx_users_active", TableName: "users"}})
	})
}

// skippingIndex is a ClickHouse data-skipping index declared over fields and
// reported with the key system.data_skipping_indices holds for it.
func skippingIndex(fields []string, reportedKey string) (*schemamodel.Database, *catalog.Database) {
	return &schemamodel.Database{Indexes: []schemamodel.Index{
			{Name: "idx_events_ab", TableName: "events", Fields: fields},
		}},
		&catalog.Database{Indexes: []catalog.Index{
			{Name: "idx_events_ab", TableName: "events", Parts: []catalog.IndexPart{{Expr: reportedKey}}},
		}}
}

// TestCompareWithDialect_ClickHouseSkippingIndexKeyIsOneExpression drives the
// index rule ClickHouse registers through a whole comparison. ClickHouse
// reports a skipping index's key as one expression, `a, b` for an index over
// two fields, so the declared fields are that key; a key over other fields
// is a replacement. Without the rule a ClickHouse index is compared by name
// alone and the second case reports nothing.
func TestCompareWithDialect_ClickHouseSkippingIndexKeyIsOneExpression(t *testing.T) {
	runtime := must.Must(builtin.New())

	t.Run("the declared fields as the reported key", func(t *testing.T) {
		c := qt.New(t)
		desired, current := skippingIndex([]string{"a", "b"}, "a, b")

		diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "clickhouse", runtime)

		c.Assert(err, qt.IsNil)
		c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
		c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
	})

	t.Run("another key", func(t *testing.T) {
		c := qt.New(t)
		desired, current := skippingIndex([]string{"a", "c"}, "a, b")

		diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "clickhouse", runtime)

		c.Assert(err, qt.IsNil)
		c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{Name: "idx_events_ab", TableName: "events"}})
		c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{Name: "idx_events_ab", TableName: "events"}})
	})
}
