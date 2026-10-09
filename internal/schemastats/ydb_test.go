package schemastats_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbstreaming"
)

func TestCollect_YDBFamilies(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Topics: []schemamodel.Topic{
			{Spec: ast.TopicSpec{Consumers: make([]ast.TopicConsumerSpec, 2)}},
			{Spec: ast.TopicSpec{Consumers: make([]ast.TopicConsumerSpec, 3)}},
		},
		Tables: []schemamodel.Table{{Name: "orders"}, {Name: "users"}},
		ExternalTables: []schemamodel.ExternalTable{
			{Columns: make([]schemamodel.ExternalColumn, 2)},
			{Columns: make([]schemamodel.ExternalColumn, 3)},
			{},
		},
		ResourcePools:           make([]schemamodel.ResourcePool, 5),
		ResourcePoolClassifiers: make([]schemamodel.ResourcePoolClassifier, 6),
		AsyncReplications:       make([]schemamodel.AsyncReplication, 7),
		Transfers:               make([]schemamodel.Transfer, 8),
		Secrets:                 make([]schemamodel.Secret, 9),
		ExternalDataSources:     make([]schemamodel.ExternalDataSource, 10),
	}
	var err error
	db.FeatureObjects, err = schemaext.NewObjects(
		ydbcoordination.DesiredObject("", "a", "", ydbcoordination.Spec{}),
		ydbcoordination.DesiredObject("", "b", "", ydbcoordination.Spec{}),
		ydbcoordination.DesiredObject("", "c", "", ydbcoordination.Spec{}),
		ydbcoordination.DesiredObject("", "d", "", ydbcoordination.Spec{}),
		ydbschema.DesiredObject("", "orders", ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Consumers: []ast.TopicConsumerSpec{{Name: "a"}, {Name: "b"}}}),
		ydbschema.DesiredObject("", "orders", ydbschema.ChangefeedSpec{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"}),
		ydbschema.DesiredObject("", "users", ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Consumers: []ast.TopicConsumerSpec{{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"}}}),
	)
	c.Assert(err, qt.IsNil)
	db.FeatureObjects = addStreamingMetricFixtures(c, db.FeatureObjects)
	body := render(c, db, nil)
	for _, test := range []struct{ name, want string }{
		{"tables", "2"}, {"columns", "0"},
		{"topics", "2"}, {"topic_consumers", "5"},
		{"changefeeds", "3"}, {"changefeed_consumers", "6"},
		{"coordination_nodes", "4"}, {"resource_pools", "5"},
		{"resource_pool_classifiers", "6"}, {"async_replications", "7"},
		{"transfers", "8"}, {"secrets", "9"},
		{"external_data_sources", "10"}, {"external_tables", "3"},
		{"external_columns", "5"}, {"streaming_queries", "11"},
	} {
		c.Check(metricValue(c, body, "ptah_schema_"+test.name), qt.Equals, test.want, qt.Commentf("metric %s", test.name))
		c.Check(metricValue(c, render(c, nil, nil), "ptah_schema_"+test.name), qt.Equals, "0", qt.Commentf("empty metric %s", test.name))
	}
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
