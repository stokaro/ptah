package atlasschema

import (
	"slices"
	"strings"

	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// DiffPolicy holds Atlas-compatible schema diff policy that maps to Ptah's
// local planning capabilities.
type DiffPolicy struct {
	SkipDropTable         bool
	ConcurrentIndexCreate bool
}

// ApplyDiffPolicy returns a shallow copy of diff with supported Atlas diff
// policy applied.
func ApplyDiffPolicy(diff *difftypes.SchemaDiff, policy DiffPolicy) *difftypes.SchemaDiff {
	return applyDiffPolicy(diff, policy)
}

func applyDiffPolicy(diff *difftypes.SchemaDiff, policy DiffPolicy) *difftypes.SchemaDiff {
	if diff == nil || !policy.SkipDropTable {
		return diff
	}
	filtered := *diff
	removedTables := tableSet(diff.TablesRemoved)
	filtered.TablesRemoved = nil
	filtered.IndexesRemovedWithTables, filtered.IndexesRemoved = filterIndexRemovalsByTable(
		filtered.IndexesRemovedWithTables,
		filtered.IndexesRemoved,
		diff.TablesRemoved,
	)
	filtered.ConstraintsRemovedWithTables, filtered.ConstraintsRemoved = filterConstraintRemovalsByTable(
		filtered.ConstraintsRemovedWithTables,
		filtered.ConstraintsRemoved,
		diff.TablesRemoved,
	)
	filtered.TriggersRemoved = slices.DeleteFunc(slices.Clone(filtered.TriggersRemoved), func(ref difftypes.TriggerRef) bool {
		return hasTable(removedTables, ref.TableName)
	})
	filtered.RLSPoliciesRemoved = slices.DeleteFunc(slices.Clone(filtered.RLSPoliciesRemoved), func(ref difftypes.RLSPolicyRef) bool {
		return hasTable(removedTables, ref.TableName)
	})
	filtered.RLSEnabledTablesRemoved = slices.DeleteFunc(slices.Clone(filtered.RLSEnabledTablesRemoved), func(name string) bool {
		return hasTable(removedTables, name)
	})
	filtered.GrantsRemoved = slices.DeleteFunc(slices.Clone(filtered.GrantsRemoved), func(ref difftypes.GrantRef) bool {
		return strings.EqualFold(ref.ObjectType, "TABLE") && hasTable(removedTables, ref.ObjectName)
	})
	return &filtered
}

func filterIndexRemovalsByTable(
	values []difftypes.IndexRemovalInfo,
	names []string,
	removedTables []string,
) ([]difftypes.IndexRemovalInfo, []string) {
	removedNames := map[string]struct{}{}
	filtered := slices.DeleteFunc(slices.Clone(values), func(value difftypes.IndexRemovalInfo) bool {
		matched := slices.Contains(removedTables, value.TableName)
		if matched {
			removedNames[value.Name] = struct{}{}
		}
		return matched
	})
	return filtered, filterRemovedNames(names, removedNames)
}

func filterConstraintRemovalsByTable(
	values []difftypes.ConstraintRemovalInfo,
	names []string,
	removedTables []string,
) ([]difftypes.ConstraintRemovalInfo, []string) {
	removedNames := map[string]struct{}{}
	filtered := slices.DeleteFunc(slices.Clone(values), func(value difftypes.ConstraintRemovalInfo) bool {
		matched := slices.Contains(removedTables, value.TableName)
		if matched {
			removedNames[value.Name] = struct{}{}
		}
		return matched
	})
	return filtered, filterRemovedNames(names, removedNames)
}

func filterRemovedNames(names []string, removedNames map[string]struct{}) []string {
	return slices.DeleteFunc(slices.Clone(names), func(name string) bool {
		_, matched := removedNames[name]
		return matched
	})
}

func tableSet(names []string) map[string]struct{} {
	values := make(map[string]struct{}, len(names))
	for _, name := range names {
		values[name] = struct{}{}
	}
	return values
}

func hasTable(values map[string]struct{}, name string) bool {
	_, ok := values[name]
	return ok
}
