package schemastats

import "ptah.run/core/schemamodel"

// Changefeed topics belong to their tables and are absent from db.Topics.
// Count their consumers separately so neither family includes the other.
func ydbMetrics(db *schemamodel.Database) []Metric {
	var topicConsumers, changefeeds, changefeedConsumers, externalColumns int
	for _, topic := range db.Topics {
		topicConsumers += len(topic.Spec.Consumers)
	}
	for _, table := range db.Tables {
		changefeeds += len(table.Changefeeds)
		for _, feed := range table.Changefeeds {
			changefeedConsumers += len(feed.Consumers)
		}
	}
	for _, table := range db.ExternalTables {
		externalColumns += len(table.Columns)
	}
	return []Metric{
		{Name: "topics", Help: "Standalone topics", Value: len(db.Topics)},
		{Name: "topic_consumers", Help: "Consumers of standalone topics", Value: topicConsumers},
		{Name: "changefeeds", Help: "Table changefeeds", Value: changefeeds},
		{Name: "changefeed_consumers", Help: "Consumers of table changefeeds", Value: changefeedConsumers},
		{Name: "coordination_nodes", Help: "Coordination nodes", Value: len(db.CoordinationNodes)},
		{Name: "resource_pools", Help: "Resource pools", Value: len(db.ResourcePools)},
		{Name: "resource_pool_classifiers", Help: "Resource pool classifiers", Value: len(db.ResourcePoolClassifiers)},
		{Name: "async_replications", Help: "Async replications", Value: len(db.AsyncReplications)},
		{Name: "transfers", Help: "Transfers", Value: len(db.Transfers)},
		{Name: "secrets", Help: "Secret objects, without their values", Value: len(db.Secrets)},
		{Name: "external_data_sources", Help: "External data sources", Value: len(db.ExternalDataSources)},
		{Name: "external_tables", Help: "External tables", Value: len(db.ExternalTables)},
		{Name: "external_columns", Help: "Columns across external tables", Value: externalColumns},
		{Name: "streaming_queries", Help: "Streaming queries", Value: len(db.StreamingQueries)},
	}
}
