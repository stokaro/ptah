package lint

import (
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
)

// The rebuild shape is lintkit's (see [lintkit.Rebuilds]). This file keeps
// what only the host knows: the surface it reports on, and where the old
// table's columns come from.

// fileRebuilds finds the table rebuilds of an up migration. SQLite's Atlas
// profile recognizes a replacement only when the old columns are known: a
// copy that leaves a column out, or fills it from anything but the old column
// of its name, is reported as a column loss by columnDroppedRule.
//
// Where the run's split judged the rebuild facts, the owner's effects are the
// answer. Whether a copy keeps every column depends on what the old table
// holds, and an owner may know that where the host does not: YDB's reads it
// from the directory's own history, since YDB has no dev database. Elsewhere
// the host matches the shape itself, against the columns the dev database
// reported.
func fileRebuilds(file *File) lintkit.RebuildSteps {
	steps := lintkit.RebuildSteps{Drops: make(map[int]bool), Renames: make(map[int]bool)}
	if !file.IsUp {
		return steps
	}
	if file.compatibility == CompatibilityProfileAtlas {
		if !lintOf(file.dialect).AtlasRebuildColumnDiff {
			return steps
		}
		for index, change := range lintkit.RebuildChanges(file.contract, baselineColumns(file)) {
			steps.Drops[index] = true
			for _, rename := range change.Renames {
				steps.Renames[rename] = true
			}
		}
		return steps
	}
	if !rebuildsJudged(file) {
		return lintkit.Rebuilds(file.contract, baselineColumns(file))
	}
	for i := range file.Statements {
		if file.Statements[i].hasEffect(lintext.FactRebuildDrop) {
			steps.Drops[i] = true
		}
		if file.Statements[i].hasEffect(lintext.FactRebuildRename) {
			steps.Renames[i] = true
		}
	}
	return steps
}

// rebuildsJudged reports whether the run's split judged the rebuild facts. A
// split judges both or neither ([lintext.ValidateSplit]).
func rebuildsJudged(file *File) bool {
	return slices.Contains(file.judged, lintext.FactRebuildDrop)
}

// baselineColumns says what a replaced table holds before the file runs, from
// the schema state the run read from the dev database.
func baselineColumns(file *File) lintkit.Columns {
	return func(_ int, table string) ([]string, bool) {
		columns := file.baseline.TableColumns(table)
		if len(columns) == 0 {
			return nil, false
		}
		names := make([]string, 0, len(columns))
		for _, column := range columns {
			names = append(names, column.Name)
		}
		return names, true
	}
}

// rebuildBaselineSubjects are the rebuild drops whose copy the dev database
// would check against the old table's columns. A rebuild the run's split
// judged needs no dev database.
//
// Every drop that ends a rebuild's steps is one, whatever its copy writes: a
// copy that fills a column with a constant loses nothing when the old table
// has no such column, and only the starting state shows that. Asking only
// about the drops [lintkit.Rebuilds] keeps without the state would leave that
// copy reported as a drop on a run that had a dev database.
func rebuildBaselineSubjects(file *File) []int {
	if !file.IsUp || rebuildsJudged(file) {
		return nil
	}
	if file.compatibility == CompatibilityProfileAtlas && !lintOf(file.dialect).AtlasRebuildColumnDiff {
		return nil
	}
	return lintkit.RebuildCandidates(file.contract)
}
