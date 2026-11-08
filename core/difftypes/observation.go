package difftypes

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/internal/tableidentity"
)

// TableObservationFor captures the feature namespace, the common children, the
// grants and the removal blockers of one observed table. The target dialect and identifier
// semantics must match the comparison that selected it; SQLite preserves
// whitespace in catalog names.
func TableObservationFor(current *catalog.Database, table catalog.Table, dialect string, semantics identifier.Semantics) schemacapture.TableObservation {
	result := schemacapture.TableObservation{Table: table.Clone()}
	if current == nil {
		return result
	}
	parent := tableidentity.Subject(table.Schema, table.Name, dialect, semantics)
	result.Grants, result.GrantsDescribed = tableGrants(current, parent, dialect, semantics), current.NotDescribed.Describes(coverage.Grant)
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
	for _, blocker := range current.RemovalBlockers {
		if tableidentity.Subject(blocker.Schema, blocker.Table, dialect, semantics).Key() == parent.Key() {
			blocker.Columns = slices.Clone(blocker.Columns)
			result.RemovalBlockers = append(result.RemovalBlockers, blocker)
		}
	}
	return result
}

// tableGrants are the rows of current's grants whose object is the table
// parent names: kind TABLE, under the same identity. A partial revoke
// subtracts from a broader grant and is not a privilege the table holds, so
// it is left out.
func tableGrants(current *catalog.Database, parent objectidentity.ID, dialect string, semantics identifier.Semantics) []catalog.Grant {
	var grants []catalog.Grant
	for _, grant := range current.Grants {
		if !strings.EqualFold(grant.ObjectType, "TABLE") || grant.IsPartialRevoke ||
			tableidentity.Subject(grant.Schema, grant.ObjectName, dialect, semantics).Key() != parent.Key() {
			continue
		}
		grants = append(grants, grant)
	}
	return grants
}
