package atlasfilter

import (
	"context"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/selectext"
	"ptah.run/internal/readscope"
)

// ReadScope returns ctx marking what a schema read under the run's selectors
// does not have to describe: the kinds they leave out whole, as
// [KindsLeftOut] names them, and each object they leave out, for which a
// reader then sends no statement of its own; see [ObjectSelector]. It is the
// context a read of a compared state runs under.
func ReadScope(ctx context.Context, selections selectext.Set, exclude, include []string) context.Context {
	ctx = readscope.Without(ctx, KindsLeftOut(selections, exclude, include)...)
	return readscope.Selecting(ctx, ObjectSelector(selections, exclude, include))
}

// ObjectSelector returns the [readscope.Selector] that keeps an object of a
// read where the run's selectors keep it, and nil where they name nothing.
//
// It asks the projection the run applies after the read, [ScopeDatabase]
// with the exclude and include selectors, over a state holding only the
// objects of the read, so the two cannot disagree about what a selector
// names. The read does not know how the run counts a pattern's parts, from
// the realm or from the read's current schema, so an object is kept where
// either count keeps it, and every object is kept where no count can be
// projected: leaving out an object the run keeps would describe it wrong,
// and keeping one it leaves out costs a statement.
func ObjectSelector(selections selectext.Set, exclude, include []string) readscope.Selector {
	if !selections.Selected() || !slices.ContainsFunc(exclude, NamesSelector) && !slices.ContainsFunc(include, NamesSelector) {
		return nil
	}
	exclude, include = slices.Clone(exclude), slices.Clone(include)
	return func(read readscope.Read) []bool {
		state := readState(read)
		kept := make([]bool, len(read.Objects))
		projected := false
		for _, realm := range patternCounts(read.Current) {
			scope := Scope{
				Selections:            selections,
				Include:               include,
				Exclude:               exclude,
				Dialect:               read.Dialect,
				DefaultSchema:         read.Current,
				RealmRelativePatterns: realm,
			}
			filtered, _, err := ScopeDatabaseSelectionReport(state, scope)
			if err != nil {
				continue
			}
			projected = true
			held := objectsOf(filtered)
			for index, object := range read.Objects {
				kept[index] = kept[index] || held[object]
			}
		}
		if !projected {
			for index := range kept {
				kept[index] = true
			}
		}
		return kept
	}
}

// patternCounts are the ways a run reading a connection whose current schema
// is current can count a pattern's parts: from the realm, and from current
// where there is one.
func patternCounts(current string) []bool {
	if current == "" {
		return []bool{true}
	}
	return []bool{false, true}
}

// readState is the state holding the objects of read and nothing else.
func readState(read readscope.Read) *catalog.Database {
	state := &catalog.Database{}
	for _, object := range read.Objects {
		switch object.Kind {
		case readscope.Tables:
			state.Tables = append(state.Tables, catalog.Table{Schema: object.Schema, Name: object.Name})
		case readscope.Views:
			state.Views = append(state.Views, catalog.View{Schema: object.Schema, Name: object.Name})
		case readscope.Triggers:
			state.Triggers = append(state.Triggers, catalog.Trigger{Schema: object.Schema, Table: object.Table, Name: object.Name})
		case readscope.Routines:
			state.Functions = append(state.Functions, catalog.Function{Schema: object.Schema, Name: object.Name})
		}
	}
	return state
}

// objectsOf is the set of objects state holds, named as [readState] names
// them.
func objectsOf(state *catalog.Database) map[readscope.Object]bool {
	held := make(map[readscope.Object]bool)
	if state == nil {
		return held
	}
	for _, table := range state.Tables {
		held[readscope.Object{Kind: readscope.Tables, Schema: table.Schema, Name: table.Name}] = true
	}
	for _, view := range state.Views {
		held[readscope.Object{Kind: readscope.Views, Schema: view.Schema, Name: view.Name}] = true
	}
	for _, trigger := range state.Triggers {
		held[readscope.Object{Kind: readscope.Triggers, Schema: trigger.Schema, Table: trigger.Table, Name: trigger.Name}] = true
	}
	for _, function := range state.Functions {
		held[readscope.Object{Kind: readscope.Routines, Schema: function.Schema, Name: function.Name}] = true
	}
	return held
}
