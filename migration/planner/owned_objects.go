package planner

import (
	"slices"

	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/schemaext"
)

// scopeCreatedObjects applies the owner's target policy to declarations carried
// by a CREATE TABLE. A hand-built diff bypasses schema comparison, so relying on
// comparison alone lets an unsupported child disappear in the target planner.
// Work on a copy: the caller can plan the same diff for another target.
func scopeCreatedObjects(runtime featureplan.Runtime, diff *difftypes.SchemaDiff, target string) (*difftypes.SchemaDiff, error) {
	scoper, ok := runtime.(schemaext.OffTargetScoper)
	if !ok || diff == nil || len(diff.TablesAdded) == 0 {
		return diff, nil
	}
	scoped := *diff
	scoped.TablesAdded = slices.Clone(diff.TablesAdded)
	scoped.SkippedFeatures = slices.Clone(diff.SkippedFeatures)
	for index, table := range scoped.TablesAdded {
		objects, skipped, err := scoper.ScopeOffTarget(target, table.OwnedObjects)
		if err != nil {
			return nil, err
		}
		scoped.TablesAdded[index].OwnedObjects = objects
		scoped.SkippedFeatures = append(scoped.SkippedFeatures, skipped...)
	}
	return &scoped, nil
}
