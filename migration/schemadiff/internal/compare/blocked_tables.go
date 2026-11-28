package compare

import (
	"cmp"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// WithoutBlockedTables returns database without each table whose drop the
// selection blocks, and without every part that belongs to such a table, and
// records each one in diff.RemovalsBlocked.
//
// A blocked table is one the database holds and desired does not declare,
// that coverage would drop, and that [catalog.Database.TableRemovalBlockers]
// names: dropping it would take a child the selection leaves out. The table is
// kept as the database holds it, as an excluded table is. Its parts have to
// leave the comparison with it. Each comparator pairs its own kind, so a
// constraint, an index or a trigger of a table nobody declares reaches the
// diff as a removal of its own, and the SQL strips the table the drop leaves
// in place. A feature an owner keeps past its table, such as a ClickHouse row
// policy, would be dropped alone the same way.
//
// The facet check reads the feature coverage the source declares, since the
// owners have not made their effective state yet: they must not see the table
// either.
//
// database is not modified. The result is database itself when nothing is
// blocked.
func WithoutBlockedTables(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	dialect string,
	semantics identifier.Semantics,
	cov Coverage,
) *catalog.Database {
	if database == nil || len(database.TableRemovalBlockers) == 0 {
		return database
	}
	declared := make(map[tableIdentity]struct{})
	if desired != nil {
		for _, table := range desired.Tables {
			declared[tableMapIdentity(table.Schema, table.Name, dialect, semantics)] = struct{}{}
		}
	}
	builder := objectidentity.NewBuilder(semantics)
	blocked := make(map[tableIdentity]struct{})
	for _, table := range database.Tables {
		identity := tableMapIdentity(table.Schema, table.Name, dialect, semantics)
		if _, exists := declared[identity]; exists {
			continue
		}
		blocker, held := database.TableRemovalBlockers[catalog.QualifyTableName(table.Schema, table.Name)]
		if !held {
			continue
		}
		// A table a facet defines is not dropped by its absence alone; see
		// [TablesAndColumnsWithTableContext].
		if kind, defined := schemaext.DefiningKind(table.Facets); defined && (desired == nil ||
			!schemaext.PlansDefinedTableRemoval(desired.FeatureCoverage, kind, builder.TablePartsVerbatim(table.Schema, table.Name))) {
			continue
		}
		removal := difftypes.TableRemoval{Name: removedTableName(table.Schema, table.Name, dialect)}
		// A table coverage would not drop anyway is not a blocked drop: the
		// comparison leaves it alone without being told.
		if len(keepPlannedRemovals(cov, coverage.Schema, []difftypes.TableRemoval{removal}, tableRemovalSchemaOnly)) == 0 {
			continue
		}
		blocked[identity] = struct{}{}
		diff.RemovalsBlocked = append(diff.RemovalsBlocked, difftypes.BlockedRemoval{
			Kind: difftypes.BlockedTable, Name: removal.Name, BlockedBy: blocker,
		})
	}
	if len(blocked) == 0 {
		return database
	}
	SortRemovalsBlocked(diff)
	return withoutTables(database, blocked, dialect, semantics)
}

// withoutTables is a copy of database without tables and the parts a
// comparison pairs on its own: indexes, constraints, triggers, row level
// security policies, table grants, owned sequences and feature objects bound
// to the table. A part names its table by schema and name, or, for a policy
// and an owned sequence, through a qualified name. What an exclude selector
// keeps out of scope stays: the plan never removes it, and the comparison
// reads it only beside a part it pairs.
func withoutTables(database *catalog.Database, tables map[tableIdentity]struct{}, dialect string, semantics identifier.Semantics) *catalog.Database {
	in := func(schema, table string) bool {
		_, ok := tables[tableMapIdentity(schema, table, dialect, semantics)]
		return ok
	}
	inQualified := func(table, schema string) bool {
		ref, ok := tableref.Parse(table)
		if !ok {
			return in(schema, table)
		}
		return in(cmp.Or(ref.Schema, schema), ref.Name)
	}
	out := *database
	out.Tables = without(database.Tables, func(table catalog.Table) bool { return in(table.Schema, table.Name) })
	out.Indexes = without(database.Indexes, func(index catalog.Index) bool { return in(index.Schema, index.TableName) })
	out.Constraints = without(database.Constraints, func(constraint catalog.Constraint) bool {
		return in(constraint.Schema, constraint.TableName)
	})
	out.Triggers = without(database.Triggers, func(trigger catalog.Trigger) bool { return in(trigger.Schema, trigger.Table) })
	out.RLSPolicies = without(database.RLSPolicies, func(policy catalog.RLSPolicy) bool { return inQualified(policy.Table, "") })
	out.Grants = without(database.Grants, func(grant catalog.Grant) bool {
		return grant.ObjectType == "TABLE" && in(grant.Schema, grant.ObjectName)
	})
	out.Sequences = without(database.Sequences, func(sequence catalog.Sequence) bool {
		owner, ok := tableref.ParseColumn(sequence.OwnedBy)
		return ok && in(cmp.Or(owner.Schema, sequence.Schema), owner.Table)
	})
	out.FeatureObjects = database.FeatureObjects.Select(func(ref objectidentity.ID) bool {
		return ref.Parent.Empty() || !in(ref.Schema.Source, ref.Parent.Source)
	})
	return &out
}

// without is a copy of items without the ones drop reports.
func without[T any](items []T, drop func(T) bool) []T {
	if !slices.ContainsFunc(items, drop) {
		return items
	}
	return slices.DeleteFunc(slices.Clone(items), drop)
}
