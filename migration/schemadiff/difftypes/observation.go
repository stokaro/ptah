package difftypes

import (
	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
)

// TableObservationFor captures the feature namespace and common children of one
// observed table using the same identifier semantics as its desired capture.
func TableObservationFor(current *catalog.Database, table catalog.Table, semantics identifier.Semantics) schemacapture.TableObservation {
	result := schemacapture.TableObservation{Table: table.Clone()}
	if current == nil {
		return result
	}
	builder := objectidentity.NewBuilder(semantics)
	parent := builder.TableParts(table.Schema, table.Name)
	result.OwnedObjects = current.FeatureObjects.ForParent(parent)
	result.FeatureCoverage = current.FeatureCoverage.ForParent(parent)
	for _, index := range current.Indexes {
		if builder.TableParts(index.Schema, index.TableName).Key() == parent.Key() {
			result.Indexes = append(result.Indexes, index.Clone())
		}
	}
	for _, constraint := range current.Constraints {
		if builder.TableParts(constraint.Schema, constraint.TableName).Key() == parent.Key() {
			result.Constraints = append(result.Constraints, constraint.Clone())
		}
	}
	for _, trigger := range current.Triggers {
		if builder.TableParts(trigger.Schema, trigger.Table).Key() == parent.Key() {
			result.Triggers = append(result.Triggers, trigger)
		}
	}
	return result
}
