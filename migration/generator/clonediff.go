package generator

// Deep copies of a schema diff. Reversal rewrites what it is handed, so the
// forward diff a caller still holds has to be a different object.

import (
	"slices"

	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func cloneSchemaDiff(diff *difftypes.SchemaDiff) *difftypes.SchemaDiff {
	clone := *diff
	clone.IdentifierSemantics = cloneIdentifierSemantics(diff.IdentifierSemantics)
	clone.TablesAdded = slices.Clone(diff.TablesAdded)
	clone.TablesRemoved = slices.Clone(diff.TablesRemoved)
	clone.TablesModified = slices.Clone(diff.TablesModified)
	clone.EnumsAdded = slices.Clone(diff.EnumsAdded)
	clone.EnumsRemoved = slices.Clone(diff.EnumsRemoved)
	clone.EnumsModified = slices.Clone(diff.EnumsModified)
	clone.IndexesAdded = slices.Clone(diff.IndexesAdded)
	clone.IndexesRemoved = slices.Clone(diff.IndexesRemoved)
	clone.ConstraintBackedIndexRemovals = slices.Clone(diff.ConstraintBackedIndexRemovals)
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
	clone.HypertablesAdded = slices.Clone(diff.HypertablesAdded)
	clone.HypertablesRemoved = slices.Clone(diff.HypertablesRemoved)
	clone.HypertablesModified = slices.Clone(diff.HypertablesModified)
	clone.ContinuousAggregatesAdded = slices.Clone(diff.ContinuousAggregatesAdded)
	clone.ContinuousAggregatesRemoved = slices.Clone(diff.ContinuousAggregatesRemoved)
	clone.ContinuousAggregatesModified = slices.Clone(diff.ContinuousAggregatesModified)
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
	clone.RolesAdded = slices.Clone(diff.RolesAdded)
	clone.RolesRemoved = slices.Clone(diff.RolesRemoved)
	clone.RolesModified = slices.Clone(diff.RolesModified)
	clone.GrantsAdded = slices.Clone(diff.GrantsAdded)
	clone.GrantsRemoved = slices.Clone(diff.GrantsRemoved)
	clone.GrantOptionsAdded = slices.Clone(diff.GrantOptionsAdded)
	clone.GrantOptionsRevoked = slices.Clone(diff.GrantOptionsRevoked)
	clone.ConstraintsAdded = slices.Clone(diff.ConstraintsAdded)
	clone.ConstraintsAddedWithTables = slices.Clone(diff.ConstraintsAddedWithTables)
	clone.ConstraintsRemoved = slices.Clone(diff.ConstraintsRemoved)
	clone.ConstraintsRemovedWithTables = slices.Clone(diff.ConstraintsRemovedWithTables)
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
