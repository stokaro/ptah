package ydbindex

import (
	"slices"
	"strings"
)

// UniqueIndexName is the name of the global unique index that holds a UNIQUE
// constraint on YDB, which has unique indexes and no UNIQUE constraint: the
// constraint's own name, or for one that has none -- a column's UNIQUE, or a
// table-level UNIQUE left unnamed -- `<table>_<column>[_<column>...]_key`, the
// name PostgreSQL gives the same key. table is the table's own name, without
// its directory.
//
// The renderer writes the index under this name and the comparison reads a
// declared constraint as an index of this name, so a schema applied once
// plans nothing the second time. The two cannot name it differently, which is
// why the name is decided here and nowhere else.
func UniqueIndexName(table, declared string, columns []string) string {
	if strings.TrimSpace(declared) != "" {
		return declared
	}
	return table + "_" + strings.Join(columns, "_") + "_key"
}

// UniqueIsTheKey reports whether a UNIQUE over columns names the table's key
// columns, in any order, which every row already holds unique. Such a UNIQUE
// needs no index of its own, and YDB refuses one over the key (`index keys
// shouldn't be table keys`), so it folds into the key, as it does on
// PostgreSQL, where `id int PRIMARY KEY UNIQUE` builds one key.
func UniqueIsTheKey(columns, key []string) bool {
	if len(key) == 0 || len(columns) != len(key) {
		return false
	}
	return slices.Equal(slices.Sorted(slices.Values(columns)), slices.Sorted(slices.Values(key)))
}
