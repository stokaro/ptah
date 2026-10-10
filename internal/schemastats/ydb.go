package schemastats

import (
	"slices"

	"ptah.run/core/schemamodel"
)

// addCommonReplications counts the async replications and transfers the
// common schema still carries into the replication owner's metrics of the same
// name, which count the owned ones. A runtime without that owner gets the
// metrics at at, where the common ones stood. Step 2 of the replication slice
// of #4140 moves every source onto the owner and deletes this.
func addCommonReplications(metrics []Metric, at int, db *schemamodel.Database) []Metric {
	for _, common := range []Metric{
		{Name: "async_replications", Help: "Async replications", Value: len(db.AsyncReplications)},
		{Name: "transfers", Help: "Transfers", Value: len(db.Transfers)},
	} {
		index := slices.IndexFunc(metrics, func(metric Metric) bool { return metric.Name == common.Name })
		if index < 0 {
			metrics = slices.Insert(metrics, at, common)
			at++
			continue
		}
		metrics[index].Value += common.Value
	}
	return metrics
}
