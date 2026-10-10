package generator

// Deep copies of a schema diff. Reversal rewrites what it is handed, so the
// forward diff a caller still holds has to be a different object.

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemapreparation"
	"ptah.run/migration/schemadiff/difftypes"
)

func cloneSchemaDiff(diff *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	clone := *diff
	clone.TablePreparation = cloneTablePreparation(diff.TablePreparation)
	clone.IdentifierSemantics = cloneIdentifierSemantics(diff.IdentifierSemantics)
	clone.TablesAdded = slices.Clone(diff.TablesAdded)
	clone.TablesRemoved = diff.TablesRemoved.Clone()
	clone.TablesModified = slices.Clone(diff.TablesModified)
	clone.EnumsAdded = slices.Clone(diff.EnumsAdded)
	clone.EnumsRemoved = slices.Clone(diff.EnumsRemoved)
	clone.EnumsModified = slices.Clone(diff.EnumsModified)
	clone.IndexesAdded = slices.Clone(diff.IndexesAdded)
	clone.IndexesRemoved = slices.Clone(diff.IndexesRemoved)
	clone.ConstraintBackedIndexRemovals = slices.Clone(diff.ConstraintBackedIndexRemovals)
	clone.IndexesRenamed = slices.Clone(diff.IndexesRenamed)
	clone.IndexPartitioningChanged = cloneIndexPartitioningChanges(diff.IndexPartitioningChanged)
	clone.IndexCommentsChanged = slices.Clone(diff.IndexCommentsChanged)
	clone.ExtensionsAdded = slices.Clone(diff.ExtensionsAdded)
	clone.ExtensionsRemoved = slices.Clone(diff.ExtensionsRemoved)
	clone.ExtensionsModified = slices.Clone(diff.ExtensionsModified)
	clone.FunctionsAdded = slices.Clone(diff.FunctionsAdded)
	clone.FunctionsRemoved = slices.Clone(diff.FunctionsRemoved)
	clone.FunctionsModified = slices.Clone(diff.FunctionsModified)
	clone.SequencesAdded = slices.Clone(diff.SequencesAdded)
	clone.SequencesRemoved = slices.Clone(diff.SequencesRemoved)
	clone.SequencesModified = slices.Clone(diff.SequencesModified)
	clone.DomainsAdded = slices.Clone(diff.DomainsAdded)
	clone.DomainsRemoved = slices.Clone(diff.DomainsRemoved)
	clone.DomainsModified = slices.Clone(diff.DomainsModified)
	clone.CompositeTypesAdded = slices.Clone(diff.CompositeTypesAdded)
	clone.CompositeTypesRemoved = slices.Clone(diff.CompositeTypesRemoved)
	clone.CompositeTypesModified = slices.Clone(diff.CompositeTypesModified)
	clone.RangesAdded = slices.Clone(diff.RangesAdded)
	clone.RangesRemoved = slices.Clone(diff.RangesRemoved)
	clone.RangesModified = slices.Clone(diff.RangesModified)
	clone.ViewsAdded = slices.Clone(diff.ViewsAdded)
	clone.ViewsRemoved = slices.Clone(diff.ViewsRemoved)
	clone.ViewsModified = slices.Clone(diff.ViewsModified)
	clone.SynonymsAdded = slices.Clone(diff.SynonymsAdded)
	clone.SynonymsRemoved = slices.Clone(diff.SynonymsRemoved)
	clone.SynonymsModified = slices.Clone(diff.SynonymsModified)
	clone.AsyncReplicationsAdded = cloneAsyncReplications(diff.AsyncReplicationsAdded)
	clone.AsyncReplicationsRemoved = cloneAsyncReplications(diff.AsyncReplicationsRemoved)
	clone.AsyncReplicationsModified = cloneAsyncReplicationDiffs(diff.AsyncReplicationsModified)
	clone.TransfersAdded = slices.Clone(diff.TransfersAdded)
	clone.TransfersRemoved = slices.Clone(diff.TransfersRemoved)
	clone.TransfersModified = cloneTransferDiffs(diff.TransfersModified)
	clone.ExternalDataSourcesAdded = slices.Clone(diff.ExternalDataSourcesAdded)
	clone.ExternalDataSourcesRemoved = slices.Clone(diff.ExternalDataSourcesRemoved)
	clone.ExternalDataSourcesChanged = slices.Clone(diff.ExternalDataSourcesChanged)
	clone.ExternalTablesAdded = slices.Clone(diff.ExternalTablesAdded)
	clone.ExternalTablesRemoved = slices.Clone(diff.ExternalTablesRemoved)
	clone.ExternalTablesChanged = slices.Clone(diff.ExternalTablesChanged)
	clone.DeclaredExternalTables = slices.Clone(diff.DeclaredExternalTables)
	clone.ExtendedPropertiesAdded = slices.Clone(diff.ExtendedPropertiesAdded)
	clone.ExtendedPropertiesRemoved = slices.Clone(diff.ExtendedPropertiesRemoved)
	clone.ExtendedPropertiesModified = slices.Clone(diff.ExtendedPropertiesModified)
	clone.MaterializedViewsAdded = slices.Clone(diff.MaterializedViewsAdded)
	clone.MaterializedViewsRemoved = slices.Clone(diff.MaterializedViewsRemoved)
	clone.MaterializedViewsModified = slices.Clone(diff.MaterializedViewsModified)
	clone.TriggersAdded = slices.Clone(diff.TriggersAdded)
	clone.TriggersRemoved = slices.Clone(diff.TriggersRemoved)
	clone.TriggersModified = slices.Clone(diff.TriggersModified)
	clone.RLSPoliciesAdded = slices.Clone(diff.RLSPoliciesAdded)
	clone.RLSPoliciesRemoved = slices.Clone(diff.RLSPoliciesRemoved)
	clone.RLSPoliciesModified = slices.Clone(diff.RLSPoliciesModified)
	clone.RLSEnabledTablesAdded = slices.Clone(diff.RLSEnabledTablesAdded)
	clone.RLSEnabledTablesRemoved = slices.Clone(diff.RLSEnabledTablesRemoved)
	clone.RLSForceChanged = slices.Clone(diff.RLSForceChanged)
	clone.RolesAdded = slices.Clone(diff.RolesAdded)
	clone.RolesRemoved = slices.Clone(diff.RolesRemoved)
	clone.RolesModified = slices.Clone(diff.RolesModified)
	clone.RoleMembershipsAdded = slices.Clone(diff.RoleMembershipsAdded)
	clone.RoleMembershipsRemoved = slices.Clone(diff.RoleMembershipsRemoved)
	clone.CurrentGrants = slices.Clone(diff.CurrentGrants)
	clone.CurrentYDBSettings = cloneYDBHeldSettings(diff.CurrentYDBSettings)
	clone.GrantsAdded = slices.Clone(diff.GrantsAdded)
	clone.GrantsRemoved = slices.Clone(diff.GrantsRemoved)
	clone.GrantOptionsAdded = slices.Clone(diff.GrantOptionsAdded)
	clone.GrantOptionsRevoked = slices.Clone(diff.GrantOptionsRevoked)
	clone.DefaultPrivilegesAdded = slices.Clone(diff.DefaultPrivilegesAdded)
	clone.DefaultPrivilegesRemoved = slices.Clone(diff.DefaultPrivilegesRemoved)
	clone.DefaultPrivilegeOptionsAdded = slices.Clone(diff.DefaultPrivilegeOptionsAdded)
	clone.DefaultPrivilegeOptionsRevoked = slices.Clone(diff.DefaultPrivilegeOptionsRevoked)
	clone.ConstraintsAdded = slices.Clone(diff.ConstraintsAdded)
	clone.ConstraintsAdded = slices.Clone(diff.ConstraintsAdded)
	clone.ConstraintsRemoved = slices.Clone(diff.ConstraintsRemoved)
	clone.ConstraintsRemoved = slices.Clone(diff.ConstraintsRemoved)
	clone.ForeignKeysRemovedWithTables = cloneForeignKeyRemovalInfos(diff.ForeignKeysRemovedWithTables)
	return &clone
}

func cloneForeignKeyRemovalInfos(values []difftypes.ForeignKeyRemovalInfo) []difftypes.ForeignKeyRemovalInfo {
	cloned := slices.Clone(values)
	for position := range cloned {
		cloned[position].Columns = slices.Clone(cloned[position].Columns)
		cloned[position].ForeignColumns = slices.Clone(cloned[position].ForeignColumns)
	}
	return cloned
}

func cloneIdentifierSemantics(
	semantics *identifier.Semantics,
) *identifier.Semantics {
	if semantics == nil {
		return nil
	}
	return new(semantics.Clone())
}

func cloneIdentifierSemanticsValue(
	semantics *identifier.Semantics,
) identifier.Semantics {
	if semantics == nil {
		return identifier.Semantics{}
	}
	return semantics.Clone()
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	return new(*value)
}

// cloneIndexPartitioningChanges copies the changes and the settings each one
// points at, so a reversal swapping them leaves the caller's diff alone.
func cloneIndexPartitioningChanges(changes []difftypes.IndexPartitioningChange) []difftypes.IndexPartitioningChange {
	if changes == nil {
		return nil
	}
	clone := make([]difftypes.IndexPartitioningChange, len(changes))
	for i, change := range changes {
		change.Partitioning = change.Partitioning.Clone()
		change.Previous = change.Previous.Clone()
		clone[i] = change
	}
	return clone
}

// cloneAsyncReplications copies the replications and the items each carries,
// so a reversal swapping them leaves the caller's diff alone.
func cloneAsyncReplications(replications difftypes.AsyncReplicationChanges) difftypes.AsyncReplicationChanges {
	if replications == nil {
		return nil
	}
	clone := make(difftypes.AsyncReplicationChanges, len(replications))
	for i, replication := range replications {
		replication.Spec = replication.Spec.Clone()
		clone[i] = replication
	}
	return clone
}

// cloneAsyncReplicationDiffs copies the changes, their lists and both specs.
func cloneAsyncReplicationDiffs(changes []difftypes.AsyncReplicationDiff) []difftypes.AsyncReplicationDiff {
	if changes == nil {
		return nil
	}
	clone := make([]difftypes.AsyncReplicationDiff, len(changes))
	for i, change := range changes {
		change.CreateOnlyChanged = slices.Clone(change.CreateOnlyChanged)
		change.Desired = change.Desired.Clone()
		change.Current = change.Current.Clone()
		clone[i] = change
	}
	return clone
}

// cloneTransferDiffs copies the changes and their lists.
func cloneTransferDiffs(changes []difftypes.TransferDiff) []difftypes.TransferDiff {
	if changes == nil {
		return nil
	}
	clone := make([]difftypes.TransferDiff, len(changes))
	for i, change := range changes {
		change.CreateOnlyChanged = slices.Clone(change.CreateOnlyChanged)
		clone[i] = change
	}
	return clone
}

// cloneYDBHeldSettings copies each table's held settings and its index map,
// so a planner that reads them cannot change the caller's diff.
func cloneYDBHeldSettings(settings []difftypes.YDBHeldSettings) []difftypes.YDBHeldSettings {
	if settings == nil {
		return nil
	}
	clone := make([]difftypes.YDBHeldSettings, len(settings))
	for i, table := range settings {
		table.Partitioning = table.Partitioning.Clone()
		if table.Indexes != nil {
			indexes := make(map[string]*ast.IndexPartitioningSpec, len(table.Indexes))
			for name, spec := range table.Indexes {
				indexes[name] = spec.Clone()
			}
			table.Indexes = indexes
		}
		clone[i] = table
	}
	return clone
}

// cloneTablePreparation preserves comparison provenance in both plan directions.
// It does not reinterpret forward captures as reverse declarations or observations.
func cloneTablePreparation(capture *schemapreparation.Capture) *schemapreparation.Capture {
	if capture == nil {
		return nil
	}
	return new(capture.Clone())
}
