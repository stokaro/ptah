package atlasschema

import (
	"slices"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/concurrentindex"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// DiffPolicy holds Atlas-compatible schema diff policy that maps to Ptah's
// local planning capabilities.
type DiffPolicy struct {
	SkipDropTable         bool
	ConcurrentIndexCreate bool
	ConcurrentIndexDrop   bool

	// ConcurrentIndexCreateDisabled is the operator saying no, which is a
	// different answer from not saying yes.
	//
	// ConcurrentIndexCreate builds EVERY addition concurrently. Its zero value
	// does not mean the opposite: it means nothing was requested, and a desired
	// description that asked for `CREATE INDEX CONCURRENTLY` is still honored
	// (stokaro/ptah#2019). Only this field turns that off, and it is set from an
	// explicit `false` in the project config rather than from an absent one --
	// which is why the config's presence bit is read and not just its value.
	ConcurrentIndexCreateDisabled bool
}

// declaredConcurrentIndexRefs is the index additions the desired description
// asked to build concurrently, subject to the operator's mode.
//
// The two early returns are the two cases where the declaration changes
// nothing: the operator turned concurrent builds off, and an instruction is not
// overruled by a description; or the operator turned them on for every
// addition, which already includes these.
//
// Capabilities are resolved to the dialect default when the caller pinned no
// server version, because that is what the planner does with the same nil and
// the two must not disagree. Reading the nil through [capability.Capabilities.Has]
// instead answers false for every key, which would leave the declaration
// silently unhonored on exactly the invocation that pins no version -- the
// common one.
func declaredConcurrentIndexRefs(
	policy DiffPolicy,
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	current *catalog.Database,
	dialect string,
	capabilities capability.Capabilities,
) []difftypes.IndexRef {
	if policy.ConcurrentIndexCreateDisabled || policy.ConcurrentIndexCreate {
		return nil
	}
	if capabilities == nil {
		capabilities = capability.ForDialect(dialect)
	}
	return concurrentindex.DeclaredRefs(diff, desired, current, catalog.ServerInfo{
		Dialect:      dialect,
		Capabilities: capabilities,
	})
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
	indexRemovals := slices.DeleteFunc(filtered.IndexRemovals(), func(ref difftypes.IndexRef) bool {
		return hasTable(removedTables, ref.TableName)
	})
	filtered.SetIndexRemovals(indexRemovals)
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

func filterConstraintRemovalsByTable(
	values []difftypes.ConstraintRemovalInfo,
	names,
	removedTables []string,
) ([]difftypes.ConstraintRemovalInfo, []string) {
	removedNames := make(map[string]struct{})
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
