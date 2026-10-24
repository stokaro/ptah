package safety

import (
	"cmp"
	"maps"
	"slices"

	"ptah.run/core/difftypes"
)

// FindingCategory is one category [ClassifySchemaDiff] can report, as
// [FindingCategories] lists it.
type FindingCategory struct {
	// Category is the category's name. A qualified finding carries it before
	// the colon.
	Category string `json:"category"`
	// Severities are the severities a finding of the category can carry, from
	// the lowest rank to the highest. A category with more than one reports
	// the highest of the changes it counts in one diff.
	Severities []Severity `json:"severities"`
	// Bare reports whether a finding carries the category alone.
	Bare bool `json:"bare"`
	// Qualified reports whether a finding carries the category followed by a
	// colon and the kind of a feature owner's change, such as
	// feature_changes:ptah.run/pgextension/extension-change.
	Qualified bool `json:"qualified"`
}

// FindingCategories returns every category [ClassifySchemaDiff] can report,
// sorted by name: the categories it counts out of the diff's lists, and the
// ones a feature owner's changes are reported under. It reads the
// declarations the classifier counts from, so a category the classifier
// reports and this list leaves out cannot exist. Each call returns a new
// slice.
func FindingCategories() []FindingCategory {
	byName := make(map[string]*FindingCategory)
	for _, counted := range countedCategories {
		entry := byName[counted.name]
		if entry == nil {
			entry = &FindingCategory{Category: counted.name, Bare: true}
			byName[counted.name] = entry
		}
		entry.Severities = append(entry.Severities, counted.severity)
	}
	for _, feature := range featureCategories() {
		byName[feature.Category] = &feature
	}
	categories := make([]FindingCategory, 0, len(byName))
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		entry := *byName[name]
		entry.Severities = rankedSeverities(entry.Severities)
		categories = append(categories, entry)
	}
	return categories
}

// rankedSeverities is severities without repeats, from the lowest rank to the
// highest.
func rankedSeverities(severities []Severity) []Severity {
	ranked := slices.Clone(severities)
	slices.SortFunc(ranked, func(a, b Severity) int { return cmp.Compare(severityRank(a), severityRank(b)) })
	return slices.Compact(ranked)
}

// countedCategory is a category [ClassifySchemaDiff] counts out of the diff at
// one severity. A category counted at two severities has an entry for each,
// and a diff that holds both reports the higher.
type countedCategory struct {
	name     string
	severity Severity
	count    func(*difftypes.SchemaDiff) int
}

// countedCategories are the categories [ClassifySchemaDiff] counts out of the
// diff's lists: the additions and the removals of each kind of object, which
// are declared once with the kind for the inventory as well, and the rest.
// The changes a feature owner reports are classified apart from these; see
// [featureCategories].
var countedCategories = slices.Concat(kindCategories(), []countedCategory{
	// A changed character set or collation changes what a table created later
	// in the database defaults to.
	{name: "schemas_modified", severity: Warning, count: func(d *difftypes.SchemaDiff) int { return len(d.SchemasModified) }},
	// A removal whose object a UNIQUE constraint enforces takes the uniqueness
	// with it, whichever statement the engine spells it as, so it is counted
	// again under its own destructive category rather than folded into the
	// indexes_removed warning. Losing a uniqueness protection is not a
	// query-plan change and must not pass a destructive gate or a drift
	// threshold as one.
	{name: "unique_protections_removed", severity: Destructive, count: func(d *difftypes.SchemaDiff) int {
		return len(d.ConstraintBackedIndexRemovals)
	}},
	{name: "functions_modified", severity: Warning, count: func(d *difftypes.SchemaDiff) int { return len(d.FunctionsModified) }},
	{name: "rls_policies_modified", severity: Warning, count: func(d *difftypes.SchemaDiff) int { return len(d.RLSPoliciesModified) }},
	// NO FORCE counts as destructive for the reason a DISABLE does: afterwards
	// the table's owner, often the role the application connects as, reads and
	// writes past every policy the table keeps.
	{name: "rls_force_added", severity: Safe, count: func(d *difftypes.SchemaDiff) int {
		forced, _ := rlsForceDirections(d.RLSForceChanged)
		return forced
	}},
	{name: "rls_force_removed", severity: Destructive, count: func(d *difftypes.SchemaDiff) int {
		_, unforced := rlsForceDirections(d.RLSForceChanged)
		return unforced
	}},
	{name: "roles_modified", severity: Warning, count: func(d *difftypes.SchemaDiff) int { return len(d.RolesModified) }},
	// An index renamed in place keeps its rows and its definition, but a
	// reader that names it, as a YDB query does with VIEW, stops finding it,
	// which is how a renamed column or table reads.
	{name: "indexes_renamed", severity: Warning, count: func(d *difftypes.SchemaDiff) int { return len(d.IndexesRenamed) }},
	// Visibility decides which index the optimizer can use, and so query
	// plans, as the plan's ALTER INDEX is rated.
	{name: "index_visibility_changed", severity: Warning, count: func(d *difftypes.SchemaDiff) int {
		return len(d.IndexVisibilityChanged)
	}},
	// VALIDATE CONSTRAINT reads every row and fails on the first one the
	// constraint refuses, as ADD CONSTRAINT does.
	{name: "constraints_validated", severity: Warning, count: func(d *difftypes.SchemaDiff) int { return len(d.ConstraintsValidated) }},
	// ALTER SEQUENCE can move the next value onto a key a row already holds.
	{name: "sequences_modified", severity: Warning, count: func(d *difftypes.SchemaDiff) int { return len(d.SequencesModified) }},
	// A changed view or trigger changes what its readers get or what every
	// write does, as a changed function does.
	{name: "views_modified", severity: Warning, count: func(d *difftypes.SchemaDiff) int { return len(d.ViewsModified) }},
	{name: "triggers_modified", severity: Warning, count: func(d *difftypes.SchemaDiff) int { return len(d.TriggersModified) }},
	// A materialized view the plan replaces loses its rows, as one it drops
	// does, and one whose owner changes an attached setting in place keeps
	// them.
	{name: "materialized_views_modified", severity: Destructive, count: func(d *difftypes.SchemaDiff) int {
		return countViews(d, difftypes.MaterializedViewDiff.Replaces)
	}},
	{name: "materialized_views_modified", severity: Warning, count: func(d *difftypes.SchemaDiff) int {
		return countViews(d, func(view difftypes.MaterializedViewDiff) bool { return !view.Replaces() })
	}},
	// A grant option lets the grantee pass the privilege on, and it is a
	// setting of a grant both sides hold, judged as the grant is.
	{name: "grant_options_added", severity: Safe, count: func(d *difftypes.SchemaDiff) int { return len(d.GrantOptionsAdded) }},
	{name: "grant_options_removed", severity: Warning, count: func(d *difftypes.SchemaDiff) int { return len(d.GrantOptionsRevoked) }},
	// A comment is the one change an object takes in place without changing
	// what it holds or does. A column's comment is a modification of the
	// column, which columns_modified counts.
	{name: "comments_changed", severity: Safe, count: commentChanges},
	{name: "columns_modified", severity: Warning, count: func(d *difftypes.SchemaDiff) int {
		return sumTables(d, func(table difftypes.TableDiff) int { return len(table.ColumnsModified) })
	}},
	// Counted separately from columns_modified, and Destructive rather than
	// Warning, because it is the one column modification the server refuses
	// outright while any row holds a value (stokaro/ptah#2068). A reader who
	// sees "columns_modified: 1 (warning)" is told a cast is planned.
	{name: "vector_dimension_changed", severity: Destructive, count: func(d *difftypes.SchemaDiff) int {
		return sumTables(d, func(table difftypes.TableDiff) int { return vectorDimensionChanges(table.ColumnsModified) })
	}},
	{name: "table_constraints_added", severity: Warning, count: func(d *difftypes.SchemaDiff) int {
		return sumTables(d, func(table difftypes.TableDiff) int { return len(table.ConstraintsAdded) })
	}},
	{name: "table_constraints_removed", severity: Destructive, count: func(d *difftypes.SchemaDiff) int {
		return sumTables(d, func(table difftypes.TableDiff) int { return len(table.ConstraintsRemoved) })
	}},
})

// kindCategories are stem_added and stem_removed for every kind in
// [objectKinds], counted exactly as [Inventory] counts them.
func kindCategories() []countedCategory {
	categories := make([]countedCategory, 0, 2*len(objectKinds))
	for _, kind := range objectKinds {
		categories = append(categories,
			countedCategory{name: kind.stem + "_added", severity: kind.addedSeverity, count: kind.added},
			countedCategory{name: kind.stem + "_removed", severity: kind.removedSeverity, count: kind.removed},
		)
	}
	return categories
}

// countViews counts the modified materialized views matches holds for.
func countViews(diff *difftypes.SchemaDiff, matches func(difftypes.MaterializedViewDiff) bool) int {
	total := 0
	for _, view := range diff.MaterializedViewsModified {
		if matches(view) {
			total++
		}
	}
	return total
}

// commentChanges counts the comment transitions a plan writes in place on
// tables, indexes, constraints and the objects that take a comment through a
// statement of their own.
func commentChanges(diff *difftypes.SchemaDiff) int {
	total := len(diff.ObjectCommentsChanged) + len(diff.IndexCommentsChanged) + len(diff.ConstraintCommentsChanged)
	for _, table := range diff.TablesModified {
		if table.CommentChange != nil {
			total++
		}
	}
	return total
}
