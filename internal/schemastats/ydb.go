package schemastats

import "ptah.run/core/schemamodel"

// Standalone topic counts do not include table-owned streams. Feature reports
// supply those metrics independently from their owning providers.
func ydbMetrics(db *schemamodel.Database) []Metric {
	var topicConsumers, externalColumns int
	for _, topic := range db.Topics {
		topicConsumers += len(topic.Spec.Consumers)
	}
	for _, table := range db.ExternalTables {
		externalColumns += len(table.Columns)
	}
	return []Metric{
		{Name: "topics", Help: "Standalone topics", Value: len(db.Topics)},
		{Name: "topic_consumers", Help: "Consumers of standalone topics", Value: topicConsumers},
		{Name: "async_replications", Help: "Async replications", Value: len(db.AsyncReplications)},
		{Name: "transfers", Help: "Transfers", Value: len(db.Transfers)},
		{Name: "secrets", Help: "Secret objects, without their values", Value: len(db.Secrets)},
		{Name: "external_data_sources", Help: "External data sources", Value: len(db.ExternalDataSources)},
		{Name: "external_tables", Help: "External tables", Value: len(db.ExternalTables)},
		{Name: "external_columns", Help: "Columns across external tables", Value: externalColumns},
	}
}
