package migrationfile

import "slices"

// AtlasKeyedByPosition reports whether the Atlas files of one directory cannot
// each run under the number their name spells in an order that follows
// execution order: the directory holds a file whose version is not a decimal
// number above zero, such as v1_x.sql, 00_x.sql or -5_x.sql, or a numbered
// repeatable such as 1R__rep.sql beside 1_a.sql, which spells the same number.
// [Discover] then gives every file of the directory its position in execution
// order as its [File.OrderKey].
//
// A key above every number, given to such a file alone, would not follow
// execution order: 00_snapshot.sql runs first and would compare above
// 1_a.sql, so a checkpoint in it would skip both files after it.
func AtlasKeyedByPosition(files []File) bool {
	numbered := make(map[int64]bool)
	for _, file := range files {
		if file.Format == DirFormatAtlas && !file.Repeatable && file.Version > 0 {
			numbered[file.Version] = true
		}
	}
	for _, file := range files {
		if file.Format != DirFormatAtlas {
			continue
		}
		if file.Version == 0 && !file.Repeatable {
			return true
		}
		if file.Repeatable && numbered[file.Version] {
			return true
		}
	}
	return false
}

// assignOrderKeys sets the [File.OrderKey] of every file of one selection.
//
// A Ptah file runs under its version. An Atlas file runs under the number its
// name spells, and the repeatable R__x.sql above every such number, since it
// runs after every numbered file. Where [AtlasKeyedByPosition] holds, every
// Atlas file runs under its position in file-name order instead, 1 for the
// first, and files that carry one version, such as 1_a.up.sql and
// 1_a.down.sql, share it.
func assignOrderKeys(files []File) {
	if !AtlasKeyedByPosition(files) {
		var highest int64
		for _, file := range files {
			if file.Format == DirFormatAtlas && !file.Repeatable {
				highest = max(highest, file.Version)
			}
		}
		for i := range files {
			files[i].OrderKey = files[i].Version
			if files[i].Format == DirFormatAtlas && files[i].Repeatable && files[i].Version == 0 {
				files[i].OrderKey = highest + 1
			}
		}
		return
	}
	order := make([]int, len(files))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		switch {
		case files[a].Path < files[b].Path:
			return -1
		case files[a].Path > files[b].Path:
			return 1
		}
		return 0
	})
	positions := make(map[string]int64, len(files))
	for _, i := range order {
		identity := files[i].RevisionVersion()
		position, seen := positions[identity]
		if !seen {
			position = int64(len(positions) + 1)
			positions[identity] = position
		}
		files[i].OrderKey = position
	}
}
