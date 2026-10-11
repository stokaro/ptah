package ydblint

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/yqlddl"
)

// history is, by file name, the YDB schema the directory's own up migrations
// leave before each migration file runs: for an up file, the up migrations of
// lower versions; for a down file, those and the up migration of its own
// version, which the down half undoes. Files are read in version order, as the
// migrator applies them. A file of neither half, or without a version, starts
// from nothing, and so does every file of a nil history.
type history map[string]*ydbSchema

// newHistory reads the history of a run's files.
func newHistory(files []lintext.File) history {
	ups := make([]int, 0, len(files))
	for i := range files {
		if files[i].Up && files[i].Version > 0 && !files[i].Repeatable {
			ups = append(ups, i)
		}
	}
	slices.SortStableFunc(ups, func(a, b int) int { return cmp.Compare(files[a].Version, files[b].Version) })
	h := make(history, len(files))
	state := (*ydbSchema)(nil).clone()
	after := make(map[int64]*ydbSchema, len(ups))
	for _, index := range ups {
		file := &files[index]
		h[file.Name] = state.clone()
		for i := range file.Statements {
			state.apply(yqlddl.Read(file.Statements[i].SQL))
			state.applyComment(file.Statements[i].SQL)
		}
		after[file.Version] = state.clone()
	}
	for i := range files {
		if !files[i].Down || files[i].Version <= 0 {
			continue
		}
		h[files[i].Name] = stateThrough(ups, files, after, files[i].Version)
	}
	return h
}

// stateThrough is the schema after every up migration whose version is at
// most version.
func stateThrough(ups []int, files []lintext.File, after map[int64]*ydbSchema, version int64) *ydbSchema {
	var latest *ydbSchema
	for _, index := range ups {
		if files[index].Version > version {
			break
		}
		latest = after[files[index].Version]
	}
	return latest
}

// before is a copy of the schema the file starts from.
func (h history) before(file *lintkit.File) *ydbSchema {
	return h[file.Name].clone()
}

// columns says what a replaced table holds just before each statement of the
// file named name runs: the file's starting schema with the statements before
// it applied.
func (h history) columns(name string, statements []lintext.Statement) lintkit.Columns {
	state := h[name].clone()
	applied := 0
	return func(index int, wanted string) ([]string, bool) {
		for ; applied < index; applied++ {
			state.apply(yqlddl.Read(statements[applied].SQL))
		}
		for path, table := range state.tables {
			if strings.EqualFold(path, wanted) && table.columnsKnown {
				return table.columns, true
			}
		}
		return nil, false
	}
}

// atlasSurface is the [lintext.CheckRequest] Compatibility of the
// Atlas-compatible command surface.
const atlasSurface = "atlas"

// rebuilds finds the table rebuilds of an up file on Ptah's own surface, the
// one that recognizes them; see [lintkit.Rebuilds].
func (h history) rebuilds(file *lintkit.File) lintkit.RebuildSteps {
	if !file.IsUp || file.Compatibility == atlasSurface {
		return lintkit.RebuildSteps{Drops: make(map[int]bool), Renames: make(map[int]bool)}
	}
	return lintkit.Rebuilds(file.Statements, h.columns(file.Name, file.Statements))
}
