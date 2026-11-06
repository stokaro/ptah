package atlasfilter

import (
	"strings"

	"ptah.run/core/selectext"
	"ptah.run/internal/readscope"
)

// Each object kind a reader needs a privilege to see, with the selector type
// that leaves it out and the part of a name a selector has to reach.
var (
	// A table leaves with its constraints and triggers, as [exclusionState.filterConstraints]
	// and [exclusionState.filterTriggers] drop them with it.
	tableKinds = []readscope.Kind{readscope.Tables, readscope.Constraints, readscope.Triggers}

	selectorKinds = []struct {
		selectorType string
		kinds        []readscope.Kind
		child        bool
	}{
		{selectorType: "table", kinds: tableKinds},
		{selectorType: "view", kinds: []readscope.Kind{readscope.Views}},
		{selectorType: "function", kinds: []readscope.Kind{readscope.Routines}},
		{selectorType: "trigger", kinds: []readscope.Kind{readscope.Triggers}, child: true},
		{selectorType: "constraint", kinds: []readscope.Kind{readscope.Constraints}, child: true},
	}
)

// KindsLeftOut names the object kinds a run's selectors leave out whole, so a
// schema read does not have to answer for them.
//
// Only a selector that names a whole kind counts: `*[type=trigger]` does, and
// `audit_*[type=trigger]` does not, because a read that cannot see a trigger
// cannot say what it is called. A foreign key selector does not leave constraints
// out either: the catalog row that hides a foreign key hides a primary key with
// it. A malformed selector counts for nothing here; the filter refuses it.
//
// `diff.skip` is not a selector and has no say. It leaves destructive changes out
// of a plan, and the comparison still reads the kind to find what to add.
func KindsLeftOut(selections selectext.Set, exclude, include []string) []readscope.Kind {
	if !selections.Selected() {
		return nil
	}
	words := vocabulary{selections: selections}
	var left []readscope.Kind
	for _, value := range exclude {
		for part := range strings.SplitSeq(value, ",") {
			pattern, err := parsePattern(part, filterKindExclude, words)
			if err == nil {
				left = append(left, pattern.wholeKinds()...)
			}
		}
	}
	selectors, err := parseIncludeSelectors(include, words)
	if err == nil && len(selectors) > 0 {
		left = append(left, kindsNotIncluded(selectors)...)
	}
	return left
}

// wholeKinds names the kinds this exclusion removes every object of.
func (p resourcePattern) wholeKinds() []readscope.Kind {
	if p.glob == "" || len(p.fields) > 0 || strings.Trim(p.glob, "*.") != "" {
		return nil
	}
	var kinds []readscope.Kind
	for _, candidate := range selectorKinds {
		// The two spellings a filter offers an object of this kind: bare and
		// qualified by its schema.
		names := []string{"object", "schema.object"}
		if candidate.child {
			names = []string{"table.object", "schema.table.object"}
		}
		if p.matches(candidate.selectorType, names...) {
			kinds = append(kinds, candidate.kinds...)
		}
	}
	return kinds
}

// kindsNotIncluded names the kinds no include selector can select. A selector
// with no type can name an object of any kind. Children ride along with their
// table, so only top-level types appear here.
func kindsNotIncluded(selectors []resourcePattern) []readscope.Kind {
	var left []readscope.Kind
	for _, candidate := range selectorKinds {
		if candidate.child {
			continue
		}
		selected := false
		for _, selector := range selectors {
			_, typed := selector.types[candidate.selectorType]
			selected = selected || typed || len(selector.types) == 0
		}
		if !selected {
			left = append(left, candidate.kinds...)
		}
	}
	return left
}
