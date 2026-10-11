package ydbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/ydbsource"
)

// TestSelections_RequiresTheRoutedPools requires the pool each kept
// classifier and streaming query routes to, declared or read: a query that
// names no pool runs in YDB's default pool.
func TestSelections_RequiresTheRoutedPools(t *testing.T) {
	c := qt.New(t)
	kept := must.Must(schemaext.NewObjects(
		ydbworkload.DesiredClassifierObject("etl", "", ydbworkload.ClassifierSpec{ResourcePool: "batch.jobs", Rank: 10}),
		ydbworkload.ObservedClassifierObject("analysts", ydbworkload.ClassifierSpec{ResourcePool: "reports", Rank: 20}),
		ydbstreaming.ObservedObject("app", "copy", ydbstreaming.Spec{Text: "SELECT 1;", ResourcePool: "stream"}),
		ydbstreaming.ObservedObject("app", "plain", ydbstreaming.Spec{Text: "SELECT 2;"}),
		ydbworkload.ObservedPoolObject("idle", ydbworkload.PoolSpec{}),
	))

	result, err := ydbsource.Selections().Service.SelectObjects(c.Context(), selectext.Request{Kept: kept})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Cleared, qt.HasLen, 0)
	c.Assert(result.Requires, qt.ContentEquals, []objectidentity.ID{ydbworkload.PoolRef("batch.jobs"), ydbworkload.PoolRef("reports"),
		ydbworkload.PoolRef("stream"), ydbworkload.PoolRef("default")})
}

// TestSelections_NamesTheYDBSelectorTypes names each standalone YDB object by
// its own type in its directory, and a resource pool and its classifier by
// a database-wide name.
func TestSelections_NamesTheYDBSelectorTypes(t *testing.T) {
	c := qt.New(t)

	set := must.Must(selectext.NewSet(ydbsource.Selections()))

	types := make(map[string]selectext.Naming)
	for _, selection := range set.Selections() {
		types[selection.Type] = selection.Naming
	}
	c.Assert(types, qt.DeepEquals, map[string]selectext.Naming{
		"async_replication": selectext.InSchema, "coordination_node": selectext.InSchema, "external_data_source": selectext.InSchema,
		"external_table": selectext.InSchema, "resource_pool": selectext.DatabaseWide, "resource_pool_classifier": selectext.DatabaseWide,
		"secret": selectext.InSchema, "streaming_query": selectext.InSchema, "topic": selectext.InSchema, "transfer": selectext.InSchema,
	})
}
