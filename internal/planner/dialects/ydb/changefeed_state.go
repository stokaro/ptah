package ydb

import (
	"maps"
	"slices"

	"ptah.run/dialect/ydb/ydbschema"
)

func capturePlannedRebuilds(rebuilds map[string]*tableRebuild) error {
	for _, key := range slices.Sorted(maps.Keys(rebuilds)) {
		rebuild := rebuilds[key]
		current, desired, err := capturedRebuildChangefeeds(rebuild)
		if err != nil {
			return err
		}
		rebuild.currentStreams, rebuild.desiredStreams = current, desired
	}
	return nil
}

// capturedRebuildChangefeeds reads the operands already checked by the selected
// feature planner. It does not infer coverage or authorize a parent operation.
func capturedRebuildChangefeeds(rebuild *tableRebuild) (current, desired []ydbschema.ChangefeedSpec, err error) {
	observed, declared := rebuild.observation.Table, rebuild.declaration.Table
	current, err = ydbschema.ObservedChangefeeds(rebuild.observation.OwnedObjects, observed.Schema, observed.Name)
	if err != nil {
		return nil, nil, err
	}
	desired, err = ydbschema.DesiredChangefeeds(rebuild.declaration.OwnedObjects, declared.Schema, declared.Name)
	return current, desired, err
}
