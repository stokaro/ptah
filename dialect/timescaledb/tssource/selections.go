package tssource

import (
	"ptah.run/core/selectext"
	"ptah.run/dialect/timescaledb/tsschema"
)

// Selections is how a command's scope names the TimescaleDB owner's
// objects: a continuous aggregate by its own name, in the schema that holds
// it, with the continuous_aggregate selector type. Hypertable settings need
// no selector: they are a facet of their table, which the scope keeps or
// drops with it.
func Selections() selectext.Extension {
	return selectext.Extension{Owner: tsschema.Owner, Selections: []selectext.Selection{
		{Kind: tsschema.ContinuousAggregateKind, Type: "continuous_aggregate", Naming: selectext.InSchema},
	}}
}
