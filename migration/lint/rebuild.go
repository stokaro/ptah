package lint

import (
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
)

// The rebuild shape is lintkit's (see [lintkit.Rebuilds]). This file keeps
// what only the host knows: the surface it reports on, and where the old
// table's columns come from.

// fileRebuilds finds the table rebuilds of an up migration on the native
// surface. Only the native surface recognizes them: the compatibility surface
// reports what the analyzer it is compatible with reports, which was not
// measured for this shape.
//
// Where the run's split judged the rebuild facts, the owner's effects are the
// answer. Whether a copy keeps every column depends on what the old table
// holds, and an owner may know that where the host does not: YDB's reads it
// from the directory's own history, since YDB has no dev database. Elsewhere
// the host matches the shape itself, against the columns the dev database
// reported.
func fileRebuilds(file *File) lintkit.RebuildSteps {
	steps := lintkit.RebuildSteps{Drops: make(map[int]bool), Renames: make(map[int]bool)}
	if !file.IsUp || file.compatibility == CompatibilityProfileAtlas {
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
func rebuildBaselineSubjects(file *File) []int {
	if rebuildsJudged(file) {
		return nil
	}
	steps := fileRebuilds(file)
	subjects := make([]int, 0, len(steps.Drops))
	for index := range steps.Drops {
		subjects = append(subjects, index)
	}
	slices.Sort(subjects)
	return subjects
}
