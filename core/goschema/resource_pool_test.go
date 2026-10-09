package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbworkload"
)

// TestParseSource_ResourcePool_HappyPath reads a YDB resource pool and a
// classifier, each a declaration of its own: a classifier names its pool by
// an attribute, so it may name the pool `default`, which no declaration
// creates.
func TestParseSource_ResourcePool_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := `package entities

// Reporting limits the reporting queries.
//
//ptah:schema:resourcepool name="reporting" concurrent_query_limit="10" queue_size="20" database_load_cpu_threshold="80.5"
//ptah:schema:resourcepool:classifier name="reporters" resource_pool="reporting" member_name="analysts" rank="100"
type Reporting struct{}

//ptah:schema:resourcepool name="default" resource_weight="30"
//ptah:schema:resourcepool:classifier name="everyone" resource_pool="default" rank="1000"
type Defaults struct{}
`

	db, err := goschema.ParseSource("pools.go", source)

	c.Assert(err, qt.IsNil)
	objects, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.ContentEquals, []schemaext.Object{
		ydbworkload.DesiredPoolObject("reporting", "Reporting", ydbworkload.PoolSpec{
			ConcurrentQueryLimit: new(int32(10)), QueueSize: new(int32(20)), DatabaseLoadCPUThreshold: new(80.5),
		}),
		ydbworkload.DesiredPoolObject("default", "Defaults", ydbworkload.PoolSpec{ResourceWeight: new(30.0)}),
		ydbworkload.DesiredClassifierObject("reporters", "Reporting", ydbworkload.ClassifierSpec{
			ResourcePool: "reporting", MemberName: "analysts", Rank: 100,
		}),
		ydbworkload.DesiredClassifierObject("everyone", "Defaults", ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: 1000}),
	})
}

// TestParseSource_ResourcePool_FailurePath refuses a pool or a classifier YDB
// would refuse, or one whose meaning would depend on the database, where it
// was written.
func TestParseSource_ResourcePool_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		annotations   string
		wantErr       string
		wantIs        error
		wantAttribute string
	}{
		{name: "an unknown attribute", annotations: `//ptah:schema:resourcepool name="p" max_queries="3"`,
			wantErr:       `unknown annotation attribute "max_queries" on //ptah:schema:resourcepool at Pools`,
			wantIs:        ptaherr.ErrUnknownAttribute,
			wantAttribute: "max_queries"},
		{name: "no name", annotations: `//ptah:schema:resourcepool concurrent_query_limit="3"`,
			wantErr:       `missing required annotation attribute "name" on //ptah:schema:resourcepool at Pools`,
			wantIs:        ptaherr.ErrMissingRequiredAttribute,
			wantAttribute: "name"},
		{name: "a percentage out of range",
			annotations:   `//ptah:schema:resourcepool name="p" total_cpu_limit_percent_per_node="101"`,
			wantErr:       `invalid total_cpu_limit_percent_per_node "101": takes a percentage from 0 to 100; leave it out for no limit on //ptah:schema:resourcepool at Pools`,
			wantIs:        ptaherr.ErrInvalidAttributeValue,
			wantAttribute: "total_cpu_limit_percent_per_node"},
		{name: "a classifier without a rank",
			annotations:   `//ptah:schema:resourcepool:classifier name="c" resource_pool="default"`,
			wantErr:       `missing required annotation attribute "rank" on //ptah:schema:resourcepool:classifier at Pools`,
			wantIs:        ptaherr.ErrMissingRequiredAttribute,
			wantAttribute: "rank"},
		{name: "a classifier rank that is no number",
			annotations:   `//ptah:schema:resourcepool:classifier name="c" resource_pool="default" rank="first"`,
			wantErr:       `invalid rank "first": takes a whole number from 0 on //ptah:schema:resourcepool:classifier at Pools`,
			wantIs:        ptaherr.ErrInvalidAttributeValue,
			wantAttribute: "rank"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("pools.go", "package entities\n\n"+test.annotations+"\ntype Pools struct{}\n")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			var parseErr *ptaherr.ParseError
			c.Assert(err, qt.ErrorAs, &parseErr)
			c.Assert(parseErr.Attribute, qt.Equals, test.wantAttribute)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// Merging sources refuses duplicate workload identities, including identical
// settings with different Go holder names. Each object has one declaration.
func TestMerge_ResourcePools_Conflict(t *testing.T) {
	for _, test := range []struct {
		name          string
		first, second schemaext.Object
	}{
		{name: "same pool settings", first: ydbworkload.DesiredPoolObject("batch", "A", ydbworkload.PoolSpec{}), second: ydbworkload.DesiredPoolObject("batch", "B", ydbworkload.PoolSpec{})},
		{name: "different pool settings", first: ydbworkload.DesiredPoolObject("batch", "A", ydbworkload.PoolSpec{}), second: ydbworkload.DesiredPoolObject("batch", "C", ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(1))})},
		{name: "different classifier settings", first: ydbworkload.DesiredClassifierObject("c", "A", ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 1}), second: ydbworkload.DesiredClassifierObject("c", "B", ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 2})},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			first := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(test.first))}
			second := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(test.second))}
			merged, err := schemamodel.Merge(first, second)
			c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
			c.Assert(merged, qt.IsNil)
			c.Assert(must.Must(first.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{test.first})
			c.Assert(must.Must(second.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{test.second})
		})
	}
}

func TestMerge_ResourcePools_SeparateObjects(t *testing.T) {
	c := qt.New(t)
	pool := ydbworkload.DesiredPoolObject("batch", "Pool", ydbworkload.PoolSpec{})
	classifier := ydbworkload.DesiredClassifierObject("route", "Route", ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 1})
	first := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(pool))}
	second := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(classifier))}
	merged, err := schemamodel.Merge(first, second)
	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(merged.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{pool, classifier})
}
