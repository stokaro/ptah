package schemastats

import "ptah.run/core/schemamodel"

// ydbMetrics counts the YDB families the common schema still carries. Topics,
// secrets and the other feature objects are counted by their owners' reports.
func ydbMetrics(db *schemamodel.Database) []Metric {
	var externalColumns int
	for _, table := range db.ExternalTables {
		externalColumns += len(table.Columns)
	}
	return []Metric{
		{Name: "async_replications", Help: "Async replications", Value: len(db.AsyncReplications)},
		{Name: "transfers", Help: "Transfers", Value: len(db.Transfers)},
		{Name: "external_data_sources", Help: "External data sources", Value: len(db.ExternalDataSources)},
		{Name: "external_tables", Help: "External tables", Value: len(db.ExternalTables)},
		{Name: "external_columns", Help: "Columns across external tables", Value: externalColumns},
	}
}
