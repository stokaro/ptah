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

// capturedRebuildChangefeeds reads raw operands for graph construction. The
// selected feature planner must authorize each parent before scheduling; reading
// these values does not infer coverage or authorize an operation.
func capturedRebuildChangefeeds(rebuild *tableRebuild) (current, desired []ydbschema.ChangefeedSpec, err error) {
	observed, declared := rebuild.observation.Table, rebuild.declaration.Table
	current, err = ydbschema.ObservedChangefeeds(rebuild.observation.OwnedObjects, observed.Schema, observed.Name)
	if err != nil {
		return nil, nil, err
	}
	desired, err = ydbschema.DesiredChangefeeds(rebuild.declaration.OwnedObjects, declared.Schema, declared.Name)
	return current, desired, err
}
