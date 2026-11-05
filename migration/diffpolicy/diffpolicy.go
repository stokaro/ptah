// Package diffpolicy provides a declarative policy that controls which
// destructive changes a planner emits.
//
// A project can list destructive change kinds to skip (for example
// drop_table) so the planner omits exactly those statements instead of
// tripping the coarse all-or-nothing destructive gate. Apply filters a schema
// diff so skipped change kinds — and, for a skipped table drop, the dependent
// object removals that a kept table must retain — are dropped from the plan,
// and reports what was omitted so callers can surface a clearly-marked comment
// in the generated migration. Apply resolves index replacements without a
// dialect, which means conservative exact-match rules for a diff carrying no
// live identifier metadata; callers that know the target database should use
// ApplyForDialect so index replacements follow that dialect's table- or
// schema-scoped naming rules.
//
// The vocabulary here is the single source of truth shared by the project
// config loader (config/projectconfig), the planner (migration/planner), and
// the generator (migration/generator).
package diffpolicy

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/indexscope"
)

// AllChangeKinds returns every skippable change kind in declaration order.
func AllChangeKinds() []planext.ChangeKind {
	return []planext.ChangeKind{planext.DropTable, planext.DropColumn, planext.DropIndex, planext.DropEnum}
}

// ParseChangeKind validates raw and returns the matching planext.ChangeKind. Matching
// is case-insensitive and tolerant of surrounding whitespace.
func ParseChangeKind(raw string) (planext.ChangeKind, error) {
	kind := planext.ChangeKind(strings.ToLower(strings.TrimSpace(raw)))
	if slices.Contains(AllChangeKinds(), kind) {
		return kind, nil
	}
	return "", fmt.Errorf("unknown diff skip change kind %q (supported: %s)", raw, joinChangeKinds(AllChangeKinds()))
}

func joinChangeKinds(kinds []planext.ChangeKind) string {
	names := make([]string, len(kinds))
	for i, kind := range kinds {
		names[i] = string(kind)
	}
	return strings.Join(names, ", ")
}

// SkipSet is a set of change kinds a policy skips.
type SkipSet map[planext.ChangeKind]struct{}

// NewSkipSet builds a SkipSet from kinds, collapsing duplicates. It returns nil
// (an empty, no-op set) when no kinds are given.
func NewSkipSet(kinds ...planext.ChangeKind) SkipSet {
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
func (s SkipSet) Has(kind planext.ChangeKind) bool {
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
	Kind planext.ChangeKind
	// Object is a human-readable identity of the omitted object, e.g. a table,
	// column, index, or enum name.
	Object string
}

// Comment returns the human-readable text describing this omission, without any
// SQL comment prefix. It is the single source of truth for the wording both the
// planner and the generator emit.
func (c SkippedChange) Comment() string {
	return fmt.Sprintf("SKIP: %s of %s omitted by diff policy (skip: %s)", ddl(c.Kind), c.Object, c.Kind)
}

// ddl returns the DDL keyword phrase a change kind omits, for comment text.
func ddl(k planext.ChangeKind) string {
	switch k {
	case planext.DropTable:
		return "DROP TABLE"
	case planext.DropColumn:
		return "DROP COLUMN"
	case planext.DropIndex:
		return "DROP INDEX"
	case planext.DropEnum:
		return "DROP TYPE"
	default:
		return strings.ToUpper(string(k))
	}
}

// Apply returns a filtered copy of diff with every removal of a skipped change
// kind omitted, along with the omitted changes in emission order. For a skipped
// table drop it also removes the dependent object removals (indexes,
// constraints, triggers, RLS policies and enablement, and table-level grants)
// that a kept table must retain, so the plan stays internally consistent; those
// pruned dependents are not reported individually — the skipped table drop is
// the one record for the group. A table whose only changes were skipped column
// drops is left out of TablesModified, so the copy's
// [difftypes.SchemaDiff.HasChanges] answers for what is left. diff is not
// mutated. A nil diff or empty skip set returns the input unchanged with no
// skipped changes.
func Apply(diff *difftypes.SchemaDiff, skip SkipSet) (*difftypes.SchemaDiff, []SkippedChange) {
	return apply(diff, skip, "")
}

// ApplyForDialect is Apply with index-replacement detection matched to the
// target dialect's index-name namespace. This preserves required drop/create
// pairs without treating table-scoped same-name indexes as replacements.
//
// dialect accepts every spelling core/platform.NormalizeDialect resolves. It
// selects the index-name namespace a removal and an addition are compared in,
// through [difftypes.SchemaDiff.EffectiveIdentifierSemantics], which is what
// decides between the live semantics a diff may carry and offline rules for
// the dialect. Under a schema-scoped index namespace (PostgreSQL, for one) a
// removal is preserved when any addition recreates the name in the same
// schema, even on another table; under a table-scoped namespace (MySQL, for
// one) only a recreation on the same table counts. An empty or unrecognized
// dialect asks for no dialect rules at all, so ApplyForDialect(diff, skip, "")
// is exactly Apply(diff, skip).
func ApplyForDialect(diff *difftypes.SchemaDiff, skip SkipSet, dialect string) (*difftypes.SchemaDiff, []SkippedChange) {
	return apply(diff, skip, dialect)
}

func apply(
	diff *difftypes.SchemaDiff,
	skip SkipSet,
	dialect string,
) (*difftypes.SchemaDiff, []SkippedChange) {
	if diff == nil || skip.Empty() {
		return diff, nil
	}

	filtered := *diff
	if diff.TablePreparation != nil {
		filtered.TablePreparation = new(diff.TablePreparation.Clone())
	}
	var skipped []SkippedChange

	if skip.Has(planext.DropTable) {
		skipped = append(skipped, changesForNames(planext.DropTable, filtered.TablesRemoved.Names())...)
		removedTables := filtered.TablesRemoved.Names()
		filtered.TablesRemoved = nil
		filtered = dropTableDependents(filtered, removedTables)
	}
	if skip.Has(planext.DropColumn) {
		filtered.TablesModified, skipped = skipColumnRemovals(filtered.TablesModified, skipped, filtered.EffectiveIdentifierSemantics(dialect))
	}
	if skip.Has(planext.DropIndex) {
		additions := indexscope.NewConflictSetWithSemantics(
			filtered.EffectiveIdentifierSemantics(dialect),
			filtered.IndexAdditions(),
		)
		var kept []difftypes.IndexRef
		var retained []difftypes.IndexRef
		for _, ref := range filtered.IndexRemovals() {
			// Preserve replacements (dropped then recreated under the same
			// dialect-specific index namespace); skip only standalone removals.
			if additions.Contains(ref) {
				kept = append(kept, ref)
				continue
			}
			skipped = append(skipped, SkippedChange{Kind: planext.DropIndex, Object: indexRefObject(ref)})
			retained = append(retained, ref)
		}
		filtered.SetIndexRemovals(kept)
		filtered.IndexCommentsChanged = retainIndexComments(&filtered, retained, dialect)
	}
	if skip.Has(planext.DropEnum) {
		skipped = append(skipped, changesForNames(planext.DropEnum, filtered.EnumsRemoved.Names())...)
		filtered.EnumsRemoved = nil
	}

	return &filtered, skipped
}

// dropTableDependents removes the dependent object removals that belong to
// kept tables, so skipping a table drop does not leave the plan revoking grants
// or dropping triggers/policies on a table that still exists. The
// table-qualified removal lists (IndexesRemoved,
// ConstraintsRemoved) carry the object-to-table correlation needed
// to filter dependent changes.
//
// Only table-level grants are suppressed here; a revoke on an object owned by a
// kept table (for example its serial sequence) is left in place, which is
// harmless because the owned object is retained with the table.
func dropTableDependents(diff difftypes.SchemaDiff, removedTables []string) difftypes.SchemaDiff {
	tables := sliceSet(removedTables)

	indexRemovals := slices.DeleteFunc(diff.IndexRemovals(), func(ref difftypes.IndexRef) bool {
		return hasKey(tables, ref.TableName)
	})
	diff.SetIndexRemovals(indexRemovals)

	// One list, so one filter: the parallel name list this also had to prune
	// is gone, and the records already carry the host (stokaro/ptah#2315).
	diff.ConstraintsRemoved = slices.DeleteFunc(slices.Clone(diff.ConstraintsRemoved), func(info difftypes.ConstraintRemovalInfo) bool {
		return hasKey(tables, info.TableName)
	})

	diff.TriggersRemoved = slices.DeleteFunc(slices.Clone(diff.TriggersRemoved), func(ref difftypes.TriggerRef) bool {
		return hasKey(tables, ref.TableName)
	})
	diff.RLSPoliciesRemoved = slices.DeleteFunc(slices.Clone(diff.RLSPoliciesRemoved), func(ref difftypes.RLSPolicyRef) bool {
		return hasKey(tables, ref.TableName)
	})
	diff.RLSEnabledTablesRemoved = slices.DeleteFunc(slices.Clone(diff.RLSEnabledTablesRemoved), func(table schemamodel.RLSEnabledTable) bool {
		name := table.Table
		return hasKey(tables, name)
	})
	diff.GrantsRemoved = slices.DeleteFunc(slices.Clone(diff.GrantsRemoved), func(ref difftypes.GrantRef) bool {
		return strings.EqualFold(ref.ObjectType, "TABLE") && hasKey(tables, ref.ObjectName)
	})
	return diff
}

func indexRefObject(ref difftypes.IndexRef) string {
	if ref.TableName == "" {
		return ref.Name
	}
	return ref.TableName + "." + ref.Name
}

// skipColumnRemovals takes the column removals out of every table diff, and
// drops a table diff the skipped drops were the only change of: that table is
// not modified, and a caller asking [difftypes.SchemaDiff.HasChanges] of the
// filtered diff is told so. A table diff that removed no column is kept as it
// came.
//
// A table diff that keeps another change keeps the skipped columns in its
// declaration too. A planner that renders the table from the declaration
// rather than from the removal list would drop them otherwise: a SQLite
// rebuild creates the table again from the declared columns, and a provider
// that owns its tables plans the alteration from the declared and observed
// tables. Without this, a skipped drop of b beside a type change of a
// rebuilds t without b on SQLite.
func skipColumnRemovals(tables []difftypes.TableDiff, skipped []SkippedChange, semantics identifier.Semantics) ([]difftypes.TableDiff, []SkippedChange) {
	var out []difftypes.TableDiff
	for _, table := range tables {
		if len(table.ColumnsRemoved) == 0 {
			out = append(out, table)
			continue
		}
		for _, column := range table.ColumnsRemoved {
			skipped = append(skipped, SkippedChange{Kind: planext.DropColumn, Object: table.TableName + "." + column.Name})
		}
		retained := table.ColumnsRemoved
		table.ColumnsRemoved = nil
		if table.HasChanges() {
			table.Desired = declareRetainedColumns(table.Desired, table.Current, retained, semantics)
			out = append(out, table)
		}
	}
	return out, skipped
}

// declareRetainedColumns returns declared with each retained column added as
// a field of its table, at the place the observed table holds it: after the
// nearest column before it that the declaration also has, or first when there
// is none. A declaration that was not captured is returned as it came, and
// declared is not changed.
func declareRetainedColumns(
	declared schemacapture.TableDeclaration,
	observed schemacapture.TableObservation,
	retained difftypes.ColumnChanges,
	semantics identifier.Semantics,
) schemacapture.TableDeclaration {
	if !declared.HasTable() {
		return declared
	}
	position := make(map[string]int, len(observed.Table.Columns))
	for i, column := range observed.Table.Columns {
		position[semantics.ColumnIdentityKey(column.Name)] = i
	}
	fields := slices.Clone(declared.Fields)
	for _, column := range retained {
		column.StructName = declared.Table.StructName
		// The removal carries the column as the target observed it, and an
		// owner's settings on it are observed values. A declaration holds
		// desired ones, and a provider refuses an observed value there.
		column.Facets = schemaext.Facets{}
		fields = slices.Insert(fields, retainedFieldIndex(fields, declared.Table.StructName, column.Name, position, semantics), column)
	}
	declared.Fields = fields
	return declared
}

// retainedFieldIndex is where a retained column goes among fields: after the
// last field of the table that the observed table holds before it.
func retainedFieldIndex(fields []schemamodel.Field, structName, column string, position map[string]int, semantics identifier.Semantics) int {
	own, observed := position[semantics.ColumnIdentityKey(column)]
	at := 0
	for i, field := range fields {
		if field.StructName != structName {
			continue
		}
		before, held := position[semantics.ColumnIdentityKey(field.Name)]
		if !observed || (held && before < own) {
			at = i + 1
		}
	}
	return at
}

func changesForNames(kind planext.ChangeKind, names []string) []SkippedChange {
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
