// Package ydbindex reads the access method an index declaration names as the
// kind of global index YDB builds for it, and writes the kind back as the
// clause YQL takes. It also holds the other answers about a global index that
// the renderer, the reader, the comparison and the planner must give alike:
// how its partitioning resolves ([Resolve]), and which unique index a UNIQUE
// constraint becomes ([UniqueIndexName]).
//
// It is one package rather than a switch in the renderer because the schema
// comparison asks the same question: an index whose kind changed from
// synchronous to asynchronous is a different index and has to be rebuilt, and
// the comparison can only see that by reading both sides the way the renderer
// wrote one of them.
//
// YDB's row tables carry global indexes only: `INDEX i LOCAL ON (a)` answers
// `local index must specify subtype with USING`, and the LOCAL subtypes are
// column-table indexes. A global index is an ordered table of its own, which
// is why a declared BTREE builds one and nothing is lost. The vector and
// full-text kinds are later families' work, and a declaration naming one is
// refused here rather than built as a plain index.
package ydbindex

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/ydbtype"
)

// Kind is how YDB maintains a global index.
type Kind int

// The kinds Ptah builds. The zero value names no kind.
const (
	// Sync is `GLOBAL SYNC`, YDB's default: a write commits together with
	// its index entries.
	Sync Kind = iota + 1
	// Async is `GLOBAL ASYNC`: a write commits without waiting for the
	// index, and a read through the index may trail the table.
	Async
)

// KindOf reads a declared access method. The empty method, BTREE, GLOBAL and
// SYNC name the synchronous index; ASYNC names the asynchronous one. Any
// other method is an error naming it, because building a plain index for it
// would give the author an index that searches differently from the one they
// declared.
func KindOf(method string) (Kind, error) {
	normalized := strings.Join(strings.Fields(strings.ToUpper(strings.ReplaceAll(method, "_", " "))), " ")
	switch normalized {
	case "", "BTREE", "GLOBAL", "SYNC", "GLOBAL SYNC":
		return Sync, nil
	case "ASYNC", "GLOBAL ASYNC":
		return Async, nil
	default:
		return 0, fmt.Errorf("index method %q has no YDB counterpart: a row table has global indexes, "+
			"synchronous or asynchronous; declare type \"async\" for an asynchronous one", method)
	}
}

// Clause writes the kind as YQL spells it in an INDEX clause, with UNIQUE in
// the place YDB reads it.
func (k Kind) Clause(unique bool) string {
	switch {
	case k == Async:
		return "GLOBAL ASYNC"
	case unique:
		return "GLOBAL UNIQUE SYNC"
	default:
		return "GLOBAL SYNC"
	}
}

// String names the kind the way a declaration may: sync or async.
func (k Kind) String() string {
	switch k {
	case Sync:
		return "sync"
	case Async:
		return "async"
	default:
		return "unknown"
	}
}

// ShapeRefusal says why YDB refuses an index over columns that covers cover,
// on a table keyed on key, and answers "" for an index YDB takes. columnType
// answers the YDB type of a column the table declares, and false for a column
// it does not.
//
// Measured on local-ydb 26.2.1.14 and 25.1.4.7: an index whose columns are the
// key answers `index keys shouldn't be table keys`, a covered key column
// answers `the same column can't be used as key and data column for one
// index`, and a Float, Double, Json, JsonDocument or Yson index column answers
// `wrong key type ... for being key`. The renderer asks this of a new table's
// indexes and the planner of an index added to a table that exists, so the two
// cannot accept different indexes.
func ShapeRefusal(columns, cover, key []string, columnType func(string) (string, bool)) string {
	if slices.Equal(columns, key) {
		return "its columns are the table's key, which YDB refuses (`index keys shouldn't be table keys`)"
	}
	for _, column := range cover {
		if slices.Contains(key, column) {
			return fmt.Sprintf("it covers key column %q, which every YDB index carries already", column)
		}
	}
	for _, column := range columns {
		ydbType, declared := columnType(column)
		if !declared {
			return fmt.Sprintf("it names column %q, which the table does not declare", column)
		}
		if !ydbtype.KeyComparable(ydbType) {
			return fmt.Sprintf("column %q is %s, which YDB refuses as an index key", column, ydbType)
		}
	}
	return ""
}
