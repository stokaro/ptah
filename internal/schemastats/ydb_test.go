package schemastats_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
)

func TestCollect_YDBFamilies(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables:            []schemamodel.Table{{Name: "orders"}, {Name: "users"}},
		AsyncReplications: make([]schemamodel.AsyncReplication, 7),
		Transfers:         make([]schemamodel.Transfer, 8),
	}
	var err error
	db.FeatureObjects, err = schemaext.NewObjects(
		ydbtopic.DesiredObject("", "events", "", ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "a"}, {Name: "b"}}}),
		ydbtopic.DesiredObject("app", "queue", "", ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "a"}, {Name: "b"}, {Name: "c"}}}),
		ydbcoordination.DesiredObject("", "a", "", ydbcoordination.Spec{}),
		ydbcoordination.DesiredObject("", "b", "", ydbcoordination.Spec{}),
		ydbcoordination.DesiredObject("", "c", "", ydbcoordination.Spec{}),
		ydbcoordination.DesiredObject("", "d", "", ydbcoordination.Spec{}),
		ydbschema.DesiredObject("", "orders", ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Consumers: []ydbtopic.ConsumerSpec{{Name: "a"}, {Name: "b"}}}),
		ydbschema.DesiredObject("", "orders", ydbschema.ChangefeedSpec{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"}),
		ydbschema.DesiredObject("", "users", ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Consumers: []ydbtopic.ConsumerSpec{{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"}}}),
	)
	c.Assert(err, qt.IsNil)
	for i := range 5 {
		db.FeatureObjects, err = db.FeatureObjects.With(ydbworkload.DesiredPoolObject(fmt.Sprintf("pool_%d", i), "", ydbworkload.PoolSpec{}))
		c.Assert(err, qt.IsNil)
	}
	for i := range 6 {
		db.FeatureObjects, err = db.FeatureObjects.With(ydbworkload.DesiredClassifierObject(fmt.Sprintf("classifier_%d", i), "", ydbworkload.ClassifierSpec{ResourcePool: "default", Rank: int64(i)}))
		c.Assert(err, qt.IsNil)
	}
	db.FeatureObjects = addStreamingMetricFixtures(c, db.FeatureObjects)
	db.FeatureObjects = addExternalMetricFixtures(c, db.FeatureObjects)
	db.FeatureObjects = addReplicationMetricFixtures(c, db.FeatureObjects)
	for i := range 9 {
		db.FeatureObjects, err = db.FeatureObjects.With(ydbsecret.DesiredObject("ext", fmt.Sprintf("secret_%d", i), "", "PTAH_SECRET_X"))
		c.Assert(err, qt.IsNil)
	}
	body := render(c, db, nil)
	for _, test := range []struct{ name, want string }{
		{"tables", "2"}, {"columns", "0"},
		{"topics", "2"}, {"topic_consumers", "5"},
		{"changefeeds", "3"}, {"changefeed_consumers", "6"},
		{"coordination_nodes", "4"}, {"resource_pools", "5"},
		{"resource_pool_classifiers", "6"}, {"async_replications", "9"},
		{"transfers", "11"}, {"secrets", "9"},
		{"external_data_sources", "10"}, {"external_tables", "3"},
		{"external_columns", "6"}, {"streaming_queries", "11"},
	} {
		c.Check(metricValue(c, body, "ptah_schema_"+test.name), qt.Equals, test.want, qt.Commentf("metric %s", test.name))
		c.Check(metricValue(c, render(c, nil, nil), "ptah_schema_"+test.name), qt.Equals, "0", qt.Commentf("empty metric %s", test.name))
	}
}

// addReplicationMetricFixtures adds two owned async replications and three
// owned transfers, which the owner counts beside the common ones.
func addReplicationMetricFixtures(c *qt.C, objects schemaext.Objects) schemaext.Objects {
	c.Helper()
	connection := ydbreplication.Connection{ConnectionString: "grpc://primary.example.com:2136/?database=/prod"}
	for i := range 2 {
		var err error
		objects, err = objects.With(ydbreplication.DesiredReplicationObject("", fmt.Sprintf("replication_%d", i), "",
			ydbreplication.ReplicationSpec{Connection: connection, Items: []ydbreplication.Item{{Source: "t", Target: fmt.Sprintf("replica_%d", i)}}}))
		c.Assert(err, qt.IsNil)
	}
	for i := range 3 {
		var err error
		objects, err = objects.With(ydbreplication.DesiredTransferObject("", fmt.Sprintf("transfer_%d", i), "",
			ydbreplication.TransferSpec{Source: "events", Target: "orders", Lambda: "($m) -> { return []; }"}))
		c.Assert(err, qt.IsNil)
	}
	return objects
}

func addStreamingMetricFixtures(c *qt.C, objects schemaext.Objects) schemaext.Objects {
	c.Helper()
	for i := range 11 {
		var err error
		objects, err = objects.With(ydbstreaming.DesiredObject("", fmt.Sprintf("query_%d", i), "", ydbstreaming.Spec{Text: "SELECT 1;"}, false))
		c.Assert(err, qt.IsNil)
	}
	return objects
}

// addExternalMetricFixtures adds ten data sources and three external tables
// holding six columns between them.
func addExternalMetricFixtures(c *qt.C, objects schemaext.Objects) schemaext.Objects {
	c.Helper()
	for i := range 10 {
		var err error
		objects, err = objects.With(ydbexternal.DesiredSourceObject("ext", fmt.Sprintf("source_%d", i), "",
			ydbexternal.DataSource{SourceType: "ObjectStorage", AuthMethod: "NONE"}))
		c.Assert(err, qt.IsNil)
	}
	for i, columns := range []int{2, 3, 1} {
		table := ydbexternal.Table{DataSource: "ext/source_0", Location: "f/"}
		for column := range columns {
			table.Columns = append(table.Columns, ydbexternal.Column{Name: fmt.Sprintf("c%d", column), Type: "Int64"})
		}
		var err error
		objects, err = objects.With(ydbexternal.DesiredTableObject("ext", fmt.Sprintf("table_%d", i), "", table))
		c.Assert(err, qt.IsNil)
	}
	return objects
}
