package schemastats

import "ptah.run/core/schemamodel"

// ydbMetrics counts the YDB families the common schema still carries. Topics,
// secrets and the other feature objects are counted by their owners' reports.
func ydbMetrics(db *schemamodel.Database) []Metric {
	return []Metric{
		{Name: "async_replications", Help: "Async replications", Value: len(db.AsyncReplications)},
		{Name: "transfers", Help: "Transfers", Value: len(db.Transfers)},
	}
}
