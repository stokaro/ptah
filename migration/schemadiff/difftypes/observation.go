package difftypes

import (
	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/migration/internal/tableidentity"
)

// TableObservationFor captures the feature namespace and common children of one
// observed table. The target dialect and identifier semantics must match the
// comparison that selected it; SQLite preserves whitespace in catalog names.
func TableObservationFor(current *catalog.Database, table catalog.Table, dialect string, semantics identifier.Semantics) schemacapture.TableObservation {
	result := schemacapture.TableObservation{Table: table.Clone()}
	if current == nil {
		return result
	}
	parent := tableidentity.Subject(table.Schema, table.Name, dialect, semantics)
	result.OwnedObjects = current.FeatureObjects.ForParent(parent)
	result.FeatureCoverage = current.FeatureCoverage.ForParent(parent)
	for _, index := range current.Indexes {
		if tableidentity.Subject(index.Schema, index.TableName, dialect, semantics).Key() == parent.Key() {
			result.Indexes = append(result.Indexes, index.Clone())
		}
	}
	for _, constraint := range current.Constraints {
		if tableidentity.Subject(constraint.Schema, constraint.TableName, dialect, semantics).Key() == parent.Key() {
			result.Constraints = append(result.Constraints, constraint.Clone())
		}
	}
	for _, trigger := range current.Triggers {
		if tableidentity.Subject(trigger.Schema, trigger.Table, dialect, semantics).Key() == parent.Key() {
			result.Triggers = append(result.Triggers, trigger)
		}
	}
	return result
}
