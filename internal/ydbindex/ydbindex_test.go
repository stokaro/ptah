package ydbindex_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbindex"
)

// TestKindOf_HappyPath pins the spellings a declaration may use for each kind
// and the clause each kind renders. The clauses were accepted inline in CREATE
// TABLE on every line from 25.1.4.7 to 26.2.1.14 and read back as GlobalSync,
// GlobalAsync and GlobalUnique.
func TestKindOf_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		want       ydbindex.Kind
		wantClause string
		wantUnique string
	}{
		{name: "no method", method: "", want: ydbindex.Sync, wantClause: "GLOBAL SYNC", wantUnique: "GLOBAL UNIQUE SYNC"},
		{name: "btree", method: "btree", want: ydbindex.Sync, wantClause: "GLOBAL SYNC", wantUnique: "GLOBAL UNIQUE SYNC"},
		{name: "global sync with an underscore", method: "global_sync", want: ydbindex.Sync, wantClause: "GLOBAL SYNC", wantUnique: "GLOBAL UNIQUE SYNC"},
		{name: "async", method: "async", want: ydbindex.Async, wantClause: "GLOBAL ASYNC", wantUnique: "GLOBAL ASYNC"},
		{name: "global async in capitals", method: "GLOBAL  ASYNC", want: ydbindex.Async, wantClause: "GLOBAL ASYNC", wantUnique: "GLOBAL ASYNC"},
		{name: "vector, as a declaration names it", method: "vector_kmeans_tree", want: ydbindex.Vector,
			wantClause: "GLOBAL USING vector_kmeans_tree", wantUnique: "GLOBAL USING vector_kmeans_tree"},
		{name: "vector, as the reader reports it", method: "GLOBAL USING vector_kmeans_tree", want: ydbindex.Vector,
			wantClause: "GLOBAL USING vector_kmeans_tree", wantUnique: "GLOBAL USING vector_kmeans_tree"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.KindOf(test.method)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(got.Clause(false), qt.Equals, test.wantClause)
			c.Assert(got.Clause(true), qt.Equals, test.wantUnique)
		})
	}
}

// TestKindOf_FailurePath refuses the access methods YDB's row tables have no
// index for, rather than building a plain index under their name.
func TestKindOf_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		wantErr string
	}{
		{name: "hash", method: "hash", wantErr: `index method "hash" has no YDB counterpart: .*`},
		{name: "gin", method: "GIN", wantErr: `index method "GIN" has no YDB counterpart: .*`},
		{name: "local", method: "LOCAL", wantErr: `index method "LOCAL" has no YDB counterpart: .*`},
		{name: "fulltext", method: "fulltext_plain", wantErr: `index method "fulltext_plain" has no YDB counterpart: .*`},
		{name: "pgvector hnsw", method: "hnsw",
			wantErr: `index method "hnsw" is pgvector's and has no YDB counterpart: YDB's vector index is vector_kmeans_tree, .*`},
		{name: "pgvector ivfflat", method: "IVFFLAT",
			wantErr: `index method "IVFFLAT" is pgvector's and has no YDB counterpart: YDB's vector index is vector_kmeans_tree, .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbindex.KindOf(test.method)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, ydbindex.Kind(0))
			c.Assert(got.String(), qt.Equals, "unknown")
		})
	}
}
