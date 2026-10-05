package schemastats_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
)

func TestCollect_YDBFamilies(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Topics: []schemamodel.Topic{
			{Spec: ast.TopicSpec{Consumers: make([]ast.TopicConsumerSpec, 2)}},
			{Spec: ast.TopicSpec{Consumers: make([]ast.TopicConsumerSpec, 3)}},
		},
		Tables: []schemamodel.Table{
			{Changefeeds: []ast.ChangefeedSpec{{Consumers: make([]ast.TopicConsumerSpec, 2)}, {}}},
			{Changefeeds: []ast.ChangefeedSpec{{Consumers: make([]ast.TopicConsumerSpec, 4)}}},
		},
		ExternalTables: []schemamodel.ExternalTable{
			{Columns: make([]schemamodel.ExternalColumn, 2)},
			{Columns: make([]schemamodel.ExternalColumn, 3)},
			{},
		},
		CoordinationNodes:       make([]schemamodel.CoordinationNode, 4),
		ResourcePools:           make([]schemamodel.ResourcePool, 5),
		ResourcePoolClassifiers: make([]schemamodel.ResourcePoolClassifier, 6),
		AsyncReplications:       make([]schemamodel.AsyncReplication, 7),
		Transfers:               make([]schemamodel.Transfer, 8),
		Secrets:                 make([]schemamodel.Secret, 9),
		ExternalDataSources:     make([]schemamodel.ExternalDataSource, 10),
		StreamingQueries:        make([]schemamodel.StreamingQuery, 11),
	}
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
