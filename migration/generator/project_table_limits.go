package generator

import (
	"slices"

	"ptah.run/core/platform/identifier"
	"ptah.run/migration/schemadiff/difftypes"
)

// pendingTableProjection identifies state that still needs a common or owning
// feature projector. Never return an unchanged capture for these transitions:
// that would claim a reverse input the forward plan did not leave behind.
func pendingTableProjection(diff *difftypes.SchemaDiff, table difftypes.TableDiff, semantics identifier.Semantics) bool {
	if len(table.ConstraintsAdded)+len(table.ConstraintsRemoved) > 0 ||
		table.YDBColumnTableChange != nil || unresolvedIndexHost(diff) ||
		pendingConstraintProjection(diff, table.TableName, semantics) {
		return true
	}
	key := semantics.QualifiedTableIdentityKey(table.TableName)
	for _, host := range changedTableChildren(diff) {
		if host == "" || semantics.QualifiedTableIdentityKey(host) == key {
			return true
		}
	}
	return false
}

// A missing owner could refer to any captured table. Refuse its projection
// instead of assigning an index by its name alone.
func unresolvedIndexHost(diff *difftypes.SchemaDiff) bool {
	var hosts []string
	for _, value := range diff.IndexesAdded {
		hosts = append(hosts, value.TableName)
	}
	for _, value := range diff.IndexesRemoved {
		hosts = append(hosts, value.TableName)
	}
	for _, value := range diff.IndexesRenamed {
		hosts = append(hosts, value.TableName)
	}
	for _, value := range diff.IndexVisibilityChanged {
		hosts = append(hosts, value.TableName)
	}
	for _, value := range diff.IndexCommentsChanged {
		hosts = append(hosts, value.TableName)
	}
	return slices.Contains(hosts, "")
}

func changedTableChildren(diff *difftypes.SchemaDiff) []string {
	var hosts []string
	for _, value := range diff.IndexPartitioningChanged {
		hosts = append(hosts, value.TableName)
	}
	for _, value := range diff.ConstraintCommentsChanged {
		if value.TableName == "" {
			hosts = append(hosts, "")
		}
	}
	for _, value := range diff.ConstraintsValidated {
		if value.TableName == "" {
			hosts = append(hosts, "")
		}
	}
	for _, value := range diff.TriggersAdded {
		hosts = append(hosts, value.TableName)
	}
	for _, value := range diff.TriggersRemoved {
		hosts = append(hosts, value.TableName)
	}
	for _, value := range diff.TriggersModified {
		hosts = append(hosts, value.TableName)
	}
	for _, value := range diff.RLSEnabledTablesAdded {
		hosts = append(hosts, value.Table)
	}
	for _, value := range diff.RLSEnabledTablesRemoved {
		hosts = append(hosts, value.Table)
	}
	for _, value := range diff.RLSForceChanged {
		hosts = append(hosts, value.Table)
	}
	return hosts
}

// An unresolved constraint owner or name cannot select a captured transition.
// Named lifecycle effects are delegated to the selected target's service.
func pendingConstraintProjection(diff *difftypes.SchemaDiff, table string, semantics identifier.Semantics) bool {
	key := semantics.QualifiedTableIdentityKey(table)
	for _, change := range diff.ConstraintsAdded {
		if change.TableName == "" || (semantics.QualifiedTableIdentityKey(change.TableName) == key && change.Name == "") {
			return true
		}
	}
	for _, change := range diff.ConstraintsRemoved {
		if change.TableName == "" || (semantics.QualifiedTableIdentityKey(change.TableName) == key && change.Name == "") {
			return true
		}
	}
	return false
}
