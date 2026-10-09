package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

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

// Two files declaring one pool differently are refused when they are merged,
// as two declarations of one table are; the same pool twice is one pool.
func TestMerge_ResourcePools_Conflict(t *testing.T) {
	c := qt.New(t)
	first := &schemamodel.Database{ResourcePools: []schemamodel.ResourcePool{{StructName: "A", Name: "batch"}}}
	same := &schemamodel.Database{ResourcePools: []schemamodel.ResourcePool{{StructName: "B", Name: "batch"}}}
	other := &schemamodel.Database{ResourcePools: []schemamodel.ResourcePool{{StructName: "C", Name: "batch",
		Spec: ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(1))}}}}
	classifier := &schemamodel.Database{ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{{
		StructName: "A", Name: "c", Spec: ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 1}}}}
	otherClassifier := &schemamodel.Database{ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{{
		StructName: "B", Name: "c", Spec: ydbworkload.ClassifierSpec{ResourcePool: "batch", Rank: 2}}}}

	merged, mergeErr := schemamodel.Merge(first, same)
	conflict, conflictErr := schemamodel.Merge(first, other)
	classifierConflict, classifierErr := schemamodel.Merge(classifier, otherClassifier)

	c.Assert(mergeErr, qt.IsNil)
	c.Assert(merged.ResourcePools, qt.HasLen, 1)
	c.Assert(conflictErr, qt.ErrorMatches, `conflicting resource pool "batch" definitions`)
	c.Assert(conflict, qt.IsNil)
	c.Assert(classifierErr, qt.ErrorMatches, `conflicting resource pool classifier "c" definitions`)
	c.Assert(classifierConflict, qt.IsNil)
}
