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
// is why a declared BTREE builds one and nothing is lost. A vector index,
// `GLOBAL USING vector_kmeans_tree`, is a global index too, built over a tree
// of tables and declared with its settings ([ResolveVector]). Full-text
// indexes declare their analyzers through [ResolveFullText].
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
	// Vector is `GLOBAL USING vector_kmeans_tree`: an approximate
	// nearest-neighbor index whose last column holds the vectors and whose
	// other columns are a prefix a search names.
	Vector
	// FullTextPlain stores tokens for full-text matching.
	FullTextPlain
	// FullTextRelevance also stores data used to rank full-text matches.
	FullTextRelevance
)

// VectorMethod is the access method a vector index declares, as YQL writes it
// after USING.
const VectorMethod = "vector_kmeans_tree"

// KindOf reads a declared access method. The empty method, BTREE, GLOBAL and
// SYNC name the synchronous index; ASYNC names the asynchronous one;
// vector_kmeans_tree names the vector index, in the spelling a declaration
// uses and in the clause a reader reports. Any other method is an error naming
// it, because building a plain index for it would give the author an index
// that searches differently from the one they declared. pgvector's hnsw and
// ivfflat are refused by name with the method YDB has instead: measured,
// `GLOBAL USING hnsw` answers `HNSW index subtype is not supported` on
// 25.1.4.7 and 26.2.1.14.
func KindOf(method string) (Kind, error) {
	normalized := strings.Join(strings.Fields(strings.ToUpper(strings.ReplaceAll(method, "_", " "))), " ")
	switch normalized {
	case "", "BTREE", "GLOBAL", "SYNC", "GLOBAL SYNC":
		return Sync, nil
	case "ASYNC", "GLOBAL ASYNC":
		return Async, nil
	case "VECTOR KMEANS TREE", "GLOBAL USING VECTOR KMEANS TREE":
		return Vector, nil
	case "FULLTEXT PLAIN", "GLOBAL USING FULLTEXT PLAIN":
		return FullTextPlain, nil
	case "FULLTEXT RELEVANCE", "GLOBAL USING FULLTEXT RELEVANCE":
		return FullTextRelevance, nil
	case "HNSW", "IVFFLAT":
		return 0, fmt.Errorf("index method %q is pgvector's and has no YDB counterpart: YDB's vector index "+
			"is %s, declared with type %q and its distance or similarity, vector_type, vector_dimension, "+
			"levels and clusters", method, VectorMethod, VectorMethod)
	default:
		return 0, fmt.Errorf("index method %q has no YDB counterpart: a row table has global indexes, "+
			"synchronous or asynchronous, vector and full-text indexes; declare type \"async\" for an asynchronous one "+
			"and type %q for a vector one", method, VectorMethod)
	}
}

// Clause writes the kind as YQL spells it in an INDEX clause, with UNIQUE in
// the place YDB reads it. A vector index is never unique (`VECTOR_KMEANS_TREE
// index can only be GLOBAL [SYNC]`), and its settings follow the column list
// rather than this clause.
func (k Kind) Clause(unique bool) string {
	switch {
	case k == Async:
		return "GLOBAL ASYNC"
	case k == Vector || k.IsFullText():
		return "GLOBAL USING " + k.String()
	case unique:
		return "GLOBAL UNIQUE SYNC"
	default:
		return "GLOBAL SYNC"
	}
}

// String names the kind the way a declaration may: sync, async or
// vector_kmeans_tree.
func (k Kind) String() string {
	switch k {
	case Sync:
		return "sync"
	case Async:
		return "async"
	case Vector:
		return VectorMethod
	case FullTextPlain:
		return FullTextPlainMethod
	case FullTextRelevance:
		return FullTextRelevanceMethod
	default:
		return "unknown"
	}
}

// Shape is an index as [ShapeRefusal] reads it.
type Shape struct {
	// Kind is the index's kind.
	Kind Kind
	// Columns are the columns the index is keyed on, in order. A vector
	// index's last column holds the vectors.
	Columns []string
	// Cover are the columns it covers.
	Cover []string
	// Dimension is a vector index's vector_dimension, and zero for any
	// other kind.
	Dimension uint64
}

// Column is what [ShapeRefusal] knows of a column the table declares.
type Column struct {
	// Type is the YDB type the column is written as.
	Type string
	// Dimension is the dimension a VECTOR(n) declaration names, and zero for
	// any other column; see [ydbtype.Mapping.Dimension].
	Dimension uint64
}

// ShapeRefusal says why YDB refuses index on a table keyed on key, and
// answers "" for an index YDB takes. column answers a column the table
// declares, and false for one it does not.
//
// Measured on local-ydb 26.2.1.14 and 25.1.4.7: a global index whose columns
// are the key answers `index keys shouldn't be table keys`, a covered key
// column answers `the same column can't be used as key and data column for one
// index`, and a Float, Double, Json, JsonDocument or Yson index column answers
// `wrong key type ... for being key`. A vector index is built over its key
// columns without complaint, and its last column has to be a String: a Utf8
// or a Uint64 there answers `Embedding column 'emb' expected type 'String' but
// got Utf8` on 26.2 and `Index column 'emb' expected type 'String'` on 25.1.
// YDB does not compare that column's declared dimension with the index's,
// and a row of the wrong length is left out of the index without an error, so
// the two are held to agree here. The renderer asks this of a new table's
// indexes and the planner of an index added to a table that exists, so the two
// cannot accept different indexes.
func ShapeRefusal(index Shape, key []string, column func(string) (Column, bool)) string {
	if index.Kind.IsFullText() {
		if reason := fullTextShapeRefusal(index, key, column); reason != "" {
			return reason
		}
	}

	if index.Kind != Vector && slices.Equal(index.Columns, key) {
		return "its columns are the table's key, which YDB refuses (`index keys shouldn't be table keys`)"
	}
	for _, covered := range index.Cover {
		if slices.Contains(key, covered) {
			return fmt.Sprintf("it covers key column %q, which every YDB index carries already", covered)
		}
	}
	for position, name := range index.Columns {
		declared, ok := column(name)
		switch {
		case !ok:
			return fmt.Sprintf("it names column %q, which the table does not declare", name)
		case index.Kind.IsFullText() && position == len(index.Columns)-1:
			if declared.Type != ydbtype.String && declared.Type != ydbtype.Utf8 {
				return fmt.Sprintf("its text column %q is %s; a full-text index reads String or Utf8", name, declared.Type)
			}
		case index.Kind == Vector && position == len(index.Columns)-1:
			return vectorColumnRefusal(name, declared, index.Dimension)
		case !ydbtype.KeyComparable(declared.Type):
			return fmt.Sprintf("column %q is %s, which YDB refuses as an index key", name, declared.Type)
		}
	}
	return ""
}

// vectorColumnRefusal says why a vector index of dimension cannot read its
// vectors from column, and answers "" where it can.
func vectorColumnRefusal(name string, column Column, dimension uint64) string {
	switch {
	case column.Type != ydbtype.String:
		return fmt.Sprintf("its vector column %q is %s, and a YDB vector index reads a String column "+
			"(`Embedding column '%s' expected type 'String' but got %s`)", name, column.Type, name, column.Type)
	case column.Dimension != 0 && column.Dimension != dimension:
		return fmt.Sprintf("its vector column %q is declared with dimension %d and the index with "+
			"vector_dimension %d; YDB checks neither, and leaves a vector of the wrong length out of the index",
			name, column.Dimension, dimension)
	}
	return ""
}

// fullTextShapeRefusal holds the declaration to the measured 26.2 grammar:
// one Uint64 primary key and one analyzed column, without a prefix.
func fullTextShapeRefusal(index Shape, key []string, column func(string) (Column, bool)) string {
	if len(index.Columns) != 1 {
		return "a YDB full-text index requires exactly one text column; this release does not support prefix columns"
	}
	if len(key) != 1 {
		return "a YDB full-text index requires exactly one Uint64 primary key column"
	}
	primary, known := column(key[0])
	if !known || primary.Type != ydbtype.Uint64 {
		return "a YDB full-text index requires a Uint64 primary key column"
	}
	return ""
}
