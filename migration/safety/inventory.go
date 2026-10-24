package safety

import (
	"cmp"
	"slices"

	"ptah.run/core/difftypes"
)

// InventoryEntry counts one kind of object on both sides of a comparison.
//
// Kind is the stem of the kind's findings: "tables" counts what tables_added
// and tables_removed are counted from. Desired is how many objects of the kind
// the desired schema declares and Observed how many the database holds, both
// within the comparison's scope.
type InventoryEntry struct {
	Kind     string `json:"kind"`
	Desired  int    `json:"desired"`
	Observed int    `json:"observed"`
}

// Inventory returns how many objects of each kind the desired schema declares
// and the database holds, for every kind the comparison pairs one by one. A nil
// diff returns nil, and any other diff a non-nil slice.
//
// The counts come from the comparison and describe the objects the findings
// describe. Desired is the pairs the comparison matched
// ([difftypes.SchemaDiff.Matched]) and the additions, Observed the pairs and
// the removals, with the additions and the removals counted exactly as
// [ClassifySchemaDiff] counts them. So an object left out of the comparison,
// or withheld by it as undecided, is counted on neither side, a change that
// drops and creates an object again counts it once on each side, and a kind
// with no finding has equal counts. Columns and enum values are counted in the tables and the enum
// types both sides hold; see [difftypes.MatchedCounts].
//
// A kind neither side holds is left out, and a kind only one side holds is
// reported with a zero on the other. Entries are sorted by kind. An entry
// carries counts and never a name, so a report holding it says nothing about
// what the schema calls its objects.
//
// A feature owner's objects are not counted: the owner, not this comparison,
// decides which of them are the same object, and its reply reports only the
// ones that differ.
func Inventory(diff *difftypes.SchemaDiff) []InventoryEntry {
	if diff == nil {
		return nil
	}
	entries := make([]InventoryEntry, 0, len(objectKinds))
	for _, kind := range objectKinds {
		matched := kind.matched(diff)
		entry := InventoryEntry{
			Kind:     kind.stem,
			Desired:  matched + kind.added(diff),
			Observed: matched + kind.removed(diff),
		}
		if entry.Desired == 0 && entry.Observed == 0 {
			continue
		}
		entries = append(entries, entry)
	}
	slices.SortFunc(entries, func(a, b InventoryEntry) int { return cmp.Compare(a.Kind, b.Kind) })
	return entries
}

// objectKind is a kind of object the comparison pairs one by one. Its stem
// names its findings, stem_added and stem_removed, and its inventory entry.
// [ClassifySchemaDiff] counts the findings and [Inventory] the entry with the
// same added and removed, so the two cannot count one kind differently.
type objectKind struct {
	stem            string
	matched         func(*difftypes.SchemaDiff) int
	added           func(*difftypes.SchemaDiff) int
	removed         func(*difftypes.SchemaDiff) int
	addedSeverity   Severity
	removedSeverity Severity
}

// objectKinds are the kinds the comparison pairs one by one.
//
// Every category [ClassifySchemaDiff] reports either belongs to one of these
// kinds or names something that is not a kind of object:
//
//   - stem_modified counts the pairs of a kind that differ. They are already
//     counted on both sides.
//   - unique_protections_removed counts index removals a UNIQUE constraint
//     enforces. Each is one of the indexes_removed already.
//   - rls_force_added and rls_force_removed count tables whose FORCE ROW LEVEL
//     SECURITY flag turns on and off. The flag is a setting of a table, and a
//     table this diff enables carries its FORCE in the enablement, so counting
//     the flag here would miss it.
//   - vector_dimension_changed counts column modifications, which
//     columns_modified counts as well.
//   - table_constraints_added and table_constraints_removed count a table's own
//     constraint lists, which the comparison never fills: it reports every
//     constraint it compares under constraints_added and constraints_removed.
//     Only a diff assembled by hand reaches them.
//   - feature_changes and the feature_access categories count the changes a
//     feature owner reported, under the owner's change kind. The owner pairs
//     its objects itself and reports only the ones that differ, so nothing
//     here knows how many of them match.
var objectKinds = []objectKind{
	// A dropped database takes every object in it.
	{
		stem:          "schemas",
		matched:       func(d *difftypes.SchemaDiff) int { return d.Matched.Schemas },
		added:         func(d *difftypes.SchemaDiff) int { return len(d.SchemasAdded) },
		removed:       func(d *difftypes.SchemaDiff) int { return len(d.SchemasRemoved) },
		addedSeverity: Safe, removedSeverity: Destructive,
	},
	{
		stem:          "tables",
		matched:       func(d *difftypes.SchemaDiff) int { return d.Matched.Tables },
		added:         func(d *difftypes.SchemaDiff) int { return len(d.TablesAdded) },
		removed:       func(d *difftypes.SchemaDiff) int { return len(d.TablesRemoved) },
		addedSeverity: Safe, removedSeverity: Destructive,
	},
	{
		stem:    "columns",
		matched: func(d *difftypes.SchemaDiff) int { return d.Matched.Columns },
		added: func(d *difftypes.SchemaDiff) int {
			return sumTables(d, func(table difftypes.TableDiff) int { return len(table.ColumnsAdded) })
		},
		removed: func(d *difftypes.SchemaDiff) int {
			return sumTables(d, func(table difftypes.TableDiff) int { return len(table.ColumnsRemoved) })
		},
		addedSeverity: Warning, removedSeverity: Destructive,
	},
	{
		stem:          "enums",
		matched:       func(d *difftypes.SchemaDiff) int { return d.Matched.Enums },
		added:         func(d *difftypes.SchemaDiff) int { return len(d.EnumsAdded) },
		removed:       func(d *difftypes.SchemaDiff) int { return len(d.EnumsRemoved) },
		addedSeverity: Safe, removedSeverity: Destructive,
	},
	{
		stem:    "enum_values",
		matched: func(d *difftypes.SchemaDiff) int { return d.Matched.EnumValues },
		added: func(d *difftypes.SchemaDiff) int {
			return sumEnums(d, func(enum difftypes.EnumDiff) int { return len(enum.ValuesAdded) })
		},
		removed: func(d *difftypes.SchemaDiff) int {
			return sumEnums(d, func(enum difftypes.EnumDiff) int { return len(enum.ValuesRemoved) })
		},
		addedSeverity: Warning, removedSeverity: Destructive,
	},
	{
		stem:          "indexes",
		matched:       func(d *difftypes.SchemaDiff) int { return d.Matched.Indexes },
		added:         func(d *difftypes.SchemaDiff) int { return len(d.IndexesAdded) },
		removed:       func(d *difftypes.SchemaDiff) int { return len(d.IndexesRemoved) },
		addedSeverity: Warning, removedSeverity: Warning,
	},
	// A procedure is a routine the database holds like a function, and a
	// plan that drops one removes executable behavior as DROP FUNCTION does.
	// The diff keeps the removals apart only because the DROP verb has to
	// match the object; the additions and the modifications already count
	// both.
	{
		stem:          "functions",
		matched:       func(d *difftypes.SchemaDiff) int { return d.Matched.Functions },
		added:         func(d *difftypes.SchemaDiff) int { return len(d.FunctionsAdded) },
		removed:       func(d *difftypes.SchemaDiff) int { return len(d.FunctionsRemoved) + len(d.ProceduresRemoved) },
		addedSeverity: Safe, removedSeverity: Destructive,
	},
	{
		stem:          "rls_policies",
		matched:       func(d *difftypes.SchemaDiff) int { return d.Matched.RLSPolicies },
		added:         func(d *difftypes.SchemaDiff) int { return len(d.RLSPoliciesAdded) },
		removed:       func(d *difftypes.SchemaDiff) int { return len(d.RLSPoliciesRemoved) },
		addedSeverity: Safe, removedSeverity: Destructive,
	},
	{
		stem:          "rls_enabled_tables",
		matched:       func(d *difftypes.SchemaDiff) int { return d.Matched.RLSEnabledTables },
		added:         func(d *difftypes.SchemaDiff) int { return len(d.RLSEnabledTablesAdded) },
		removed:       func(d *difftypes.SchemaDiff) int { return len(d.RLSEnabledTablesRemoved) },
		addedSeverity: Safe, removedSeverity: Destructive,
	},
	{
		stem:          "roles",
		matched:       func(d *difftypes.SchemaDiff) int { return d.Matched.Roles },
		added:         func(d *difftypes.SchemaDiff) int { return len(d.RolesAdded) },
		removed:       func(d *difftypes.SchemaDiff) int { return len(d.RolesRemoved) },
		addedSeverity: Safe, removedSeverity: Destructive,
	},
	{
		stem:          "constraints",
		matched:       func(d *difftypes.SchemaDiff) int { return d.Matched.Constraints },
		added:         func(d *difftypes.SchemaDiff) int { return len(d.ConstraintsAdded) },
		removed:       func(d *difftypes.SchemaDiff) int { return len(d.ConstraintsRemoved) },
		addedSeverity: Warning, removedSeverity: Destructive,
	},
}

// sumTables adds count over the tables both sides hold.
func sumTables(diff *difftypes.SchemaDiff, count func(difftypes.TableDiff) int) int {
	total := 0
	for _, table := range diff.TablesModified {
		total += count(table)
	}
	return total
}

// sumEnums adds count over the enum types both sides hold.
func sumEnums(diff *difftypes.SchemaDiff, count func(difftypes.EnumDiff) int) int {
	total := 0
	for _, enum := range diff.EnumsModified {
		total += count(enum)
	}
	return total
}
