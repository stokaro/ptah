// Package diffpolicy provides a declarative, dialect-agnostic policy that
// controls which destructive changes a planner emits.
//
// A project can list destructive change kinds to skip (for example
// drop_table) so the planner omits exactly those statements instead of
// tripping the coarse all-or-nothing destructive gate. Apply filters a schema
// diff so skipped change kinds — and, for a skipped table drop, the dependent
// object removals that a kept table must retain — are dropped from the plan,
// and reports what was omitted so callers can surface a clearly-marked comment
// in the generated migration.
//
// The vocabulary here is the single source of truth shared by the project
// config loader (config/projectconfig), the planner (migration/planner), and
// the generator (migration/generator).
package diffpolicy

import (
	"fmt"
	"slices"
	"strings"

	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// ChangeKind identifies a destructive schema change kind that a diff policy can
// skip. The string values are the stable identifiers accepted in project config
// (ptah.yaml diff.skip) and are safe to render in migration comments.
type ChangeKind string

const (
	// DropTable skips dropping tables that exist in the database but not in the
	// target schema. Dependent object removals (indexes, constraints, triggers,
	// RLS policies, grants) for a kept table are skipped with it.
	DropTable ChangeKind = "drop_table"
	// DropColumn skips dropping columns from tables that exist in both schemas.
	DropColumn ChangeKind = "drop_column"
	// DropIndex skips dropping standalone indexes. Indexes that are being
	// replaced (dropped and recreated under the same name) are not affected.
	DropIndex ChangeKind = "drop_index"
	// DropEnum skips dropping enum types that exist in the database but not in
	// the target schema.
	DropEnum ChangeKind = "drop_enum"
)

// AllChangeKinds returns every skippable change kind in declaration order.
func AllChangeKinds() []ChangeKind {
	return []ChangeKind{DropTable, DropColumn, DropIndex, DropEnum}
}

// ParseChangeKind validates raw and returns the matching ChangeKind. Matching
// is case-insensitive and tolerant of surrounding whitespace.
func ParseChangeKind(raw string) (ChangeKind, error) {
	kind := ChangeKind(strings.ToLower(strings.TrimSpace(raw)))
	if slices.Contains(AllChangeKinds(), kind) {
		return kind, nil
	}
	return "", fmt.Errorf("unknown diff skip change kind %q (supported: %s)", raw, joinChangeKinds(AllChangeKinds()))
}

func joinChangeKinds(kinds []ChangeKind) string {
	names := make([]string, len(kinds))
	for i, kind := range kinds {
		names[i] = string(kind)
	}
	return strings.Join(names, ", ")
}

// SkipSet is a set of change kinds a policy skips.
type SkipSet map[ChangeKind]struct{}

// NewSkipSet builds a SkipSet from kinds, collapsing duplicates. It returns nil
// (an empty, no-op set) when no kinds are given.
func NewSkipSet(kinds ...ChangeKind) SkipSet {
	if len(kinds) == 0 {
		return nil
	}
	set := make(SkipSet, len(kinds))
	for _, kind := range kinds {
		set[kind] = struct{}{}
	}
	return set
}

// Has reports whether kind is skipped.
func (s SkipSet) Has(kind ChangeKind) bool {
	_, ok := s[kind]
	return ok
}

// Empty reports whether the set skips nothing.
func (s SkipSet) Empty() bool {
	return len(s) == 0
}

// SkippedChange records one change omitted by the policy so a caller can emit a
// clearly-marked comment in its place.
type SkippedChange struct {
	// Kind is the change kind that caused the omission.
	Kind ChangeKind
	// Object is a human-readable identity of the omitted object, e.g. a table,
	// column, index, or enum name.
	Object string
}

// Comment returns the human-readable text describing this omission, without any
// SQL comment prefix. It is the single source of truth for the wording both the
// planner and the generator emit.
func (c SkippedChange) Comment() string {
	return fmt.Sprintf("SKIP: %s of %s omitted by diff policy (skip: %s)", c.Kind.ddl(), c.Object, c.Kind)
}

// ddl returns the DDL keyword phrase a change kind omits, for comment text.
func (k ChangeKind) ddl() string {
	switch k {
	case DropTable:
		return "DROP TABLE"
	case DropColumn:
		return "DROP COLUMN"
	case DropIndex:
		return "DROP INDEX"
	case DropEnum:
		return "DROP TYPE"
	default:
		return strings.ToUpper(string(k))
	}
}

// Apply returns a filtered copy of diff with every removal of a skipped change
// kind omitted, along with the omitted changes in emission order. For a skipped
// table drop it also removes the dependent object removals (indexes,
// constraints, triggers, RLS policies and enablement, and table-level grants)
// that a kept table must retain, so the plan stays internally consistent. diff
// is not mutated. A nil diff or empty skip set returns the input unchanged with
// no skipped changes.
func Apply(diff *types.SchemaDiff, skip SkipSet) (*types.SchemaDiff, []SkippedChange) {
	if diff == nil || skip.Empty() {
		return diff, nil
	}

	filtered := *diff
	var skipped []SkippedChange

	if skip.Has(DropTable) {
		skipped = append(skipped, changesForNames(DropTable, filtered.TablesRemoved)...)
		removedTables := filtered.TablesRemoved
		filtered.TablesRemoved = nil
		filtered = dropTableDependents(filtered, removedTables)
	}
	if skip.Has(DropColumn) {
		filtered.TablesModified, skipped = skipColumnRemovals(filtered.TablesModified, skipped)
	}
	if skip.Has(DropIndex) {
		added := sliceSet(filtered.IndexesAdded)
		var kept []string
		for _, name := range filtered.IndexesRemoved {
			// Preserve replacements (dropped then recreated under the same
			// name); skip only genuine standalone removals.
			if _, replacing := added[name]; replacing {
				kept = append(kept, name)
				continue
			}
			skipped = append(skipped, SkippedChange{Kind: DropIndex, Object: name})
		}
		filtered.IndexesRemoved = kept
	}
	if skip.Has(DropEnum) {
		skipped = append(skipped, changesForNames(DropEnum, filtered.EnumsRemoved)...)
		filtered.EnumsRemoved = nil
	}

	return &filtered, skipped
}

// dropTableDependents removes the dependent object removals that belong to
// kept tables, so skipping a table drop does not leave the plan revoking grants
// or dropping triggers/policies on a table that still exists. The
// table-qualified removal lists (IndexesRemovedWithTables,
// ConstraintsRemovedWithTables) carry the name->table correlation the bare name
// lists lack, so both are consulted before either is filtered.
//
// Only table-level grants are suppressed here; a revoke on an object owned by a
// kept table (for example its serial sequence) is left in place, which is
// harmless because the owned object is retained with the table.
func dropTableDependents(diff types.SchemaDiff, removedTables []string) types.SchemaDiff {
	tables := sliceSet(removedTables)

	removedIndexNames := namesForRemovedTables(diff.IndexesRemovedWithTables, tables, func(info types.IndexRemovalInfo) (string, string) {
		return info.Name, info.TableName
	})
	diff.IndexesRemovedWithTables = slices.DeleteFunc(slices.Clone(diff.IndexesRemovedWithTables), func(info types.IndexRemovalInfo) bool {
		return hasKey(tables, info.TableName)
	})
	diff.IndexesRemoved = deleteNames(diff.IndexesRemoved, removedIndexNames)

	removedConstraintNames := namesForRemovedTables(diff.ConstraintsRemovedWithTables, tables, func(info types.ConstraintRemovalInfo) (string, string) {
		return info.Name, info.TableName
	})
	diff.ConstraintsRemovedWithTables = slices.DeleteFunc(slices.Clone(diff.ConstraintsRemovedWithTables), func(info types.ConstraintRemovalInfo) bool {
		return hasKey(tables, info.TableName)
	})
	diff.ConstraintsRemoved = deleteNames(diff.ConstraintsRemoved, removedConstraintNames)

	diff.TriggersRemoved = slices.DeleteFunc(slices.Clone(diff.TriggersRemoved), func(ref types.TriggerRef) bool {
		return hasKey(tables, ref.TableName)
	})
	diff.RLSPoliciesRemoved = slices.DeleteFunc(slices.Clone(diff.RLSPoliciesRemoved), func(ref types.RLSPolicyRef) bool {
		return hasKey(tables, ref.TableName)
	})
	diff.RLSEnabledTablesRemoved = slices.DeleteFunc(slices.Clone(diff.RLSEnabledTablesRemoved), func(name string) bool {
		return hasKey(tables, name)
	})
	diff.GrantsRemoved = slices.DeleteFunc(slices.Clone(diff.GrantsRemoved), func(ref types.GrantRef) bool {
		return strings.EqualFold(ref.ObjectType, "TABLE") && hasKey(tables, ref.ObjectName)
	})
	return diff
}

// namesForRemovedTables collects the object names in withTables whose owning
// table is being removed, using nameOf to extract (name, table) from each entry.
func namesForRemovedTables[T any](withTables []T, tables map[string]struct{}, nameOf func(T) (name, table string)) map[string]struct{} {
	names := make(map[string]struct{})
	for _, entry := range withTables {
		name, table := nameOf(entry)
		if hasKey(tables, table) {
			names[name] = struct{}{}
		}
	}
	return names
}

func deleteNames(names []string, remove map[string]struct{}) []string {
	if len(remove) == 0 {
		return names
	}
	return slices.DeleteFunc(slices.Clone(names), func(name string) bool {
		return hasKey(remove, name)
	})
}

func skipColumnRemovals(tables []types.TableDiff, skipped []SkippedChange) ([]types.TableDiff, []SkippedChange) {
	out := slices.Clone(tables)
	for i := range out {
		if len(out[i].ColumnsRemoved) == 0 {
			continue
		}
		for _, column := range out[i].ColumnsRemoved {
			skipped = append(skipped, SkippedChange{Kind: DropColumn, Object: out[i].TableName + "." + column})
		}
		out[i].ColumnsRemoved = nil
	}
	return out, skipped
}

func changesForNames(kind ChangeKind, names []string) []SkippedChange {
	changes := make([]SkippedChange, len(names))
	for i, name := range names {
		changes[i] = SkippedChange{Kind: kind, Object: name}
	}
	return changes
}

func sliceSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func hasKey(set map[string]struct{}, key string) bool {
	_, ok := set[key]
	return ok
}
