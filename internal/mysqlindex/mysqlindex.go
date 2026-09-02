// Package mysqlindex answers which access method a MySQL-family index asks
// for, from either half of a comparison: a desired index's declared type, or
// the `INDEX_TYPE` the server reports for one that exists.
//
// It exists because the renderer and the comparator have to agree. The renderer
// decides whether the DDL carries a `SPATIAL` or `FULLTEXT` prefix; the
// comparator decides whether what the server has satisfies what was asked for.
// Two answers to that question is how a comparison came to report `InSync` for
// a table whose index was a different access method from the one its schema
// declared (stokaro/ptah#2721).
package mysqlindex

import "strings"

// Kind is the access method a MySQL-family index carries.
//
// Three values rather than the engine's whole `INDEX_TYPE` vocabulary, because
// three is what the DDL can express: a MySQL-family `CREATE INDEX` takes a
// `SPATIAL` or `FULLTEXT` prefix or no prefix at all, and everything else the
// server reports -- `BTREE`, `HASH`, `RTREE` -- is the engine's own choice for
// an index nobody asked a method of.
type Kind string

const (
	// Plain is an index whose DDL carries no access-method prefix.
	Plain Kind = ""
	// FullText is a FULLTEXT index.
	FullText Kind = "FULLTEXT"
	// Spatial is a SPATIAL index.
	Spatial Kind = "SPATIAL"
)

// KindOf classifies an index type as one of the three kinds.
//
// The same function reads both sides of a comparison, and that is the point:
// a desired index declaring `spatial` and a server reporting `SPATIAL` are the
// same request, and a server reporting `BTREE` for an index whose desired state
// declares nothing is not a different one.
//
// Anything unrecognized is [Plain]. The desired state's index type is an
// author's free-text attribute and the server's is one of a fixed set, so a
// value neither side can act on is an index with no method asked of it rather
// than an error: refusing here would fail a comparison over a schema the
// renderer is perfectly able to emit.
func KindOf(indexType string) Kind {
	switch strings.ToUpper(strings.TrimSpace(indexType)) {
	case string(FullText):
		return FullText
	case string(Spatial):
		return Spatial
	default:
		return Plain
	}
}

// Prefix is the DDL keyword this kind renders as, empty for [Plain].
func (k Kind) Prefix() string {
	return string(k)
}

// SatisfiedBy reports whether an index the server has answers a desired index
// of this kind.
//
// A desired kind of [Plain] is satisfied by anything, and that asymmetry is
// deliberate rather than an omission. An index declaring no method asks for the
// engine's default, and the engines do not agree on what that is: measured on
// MySQL 8.4.11, `CREATE INDEX` over a `POINT` column produces `INDEX_TYPE
// SPATIAL`, while MariaDB 11.8.9 produces `BTREE` for the same statement.
// Comparing a declared-nothing index against whatever the server chose would
// plan a rebuild on MySQL that MySQL then undoes, forever.
//
// The other direction is the one #2721 is about and is compared: a schema
// asking for `SPATIAL` is not satisfied by a `BTREE` index of the same name
// over the same column, and reporting it as synced left the drift invisible
// until somebody read MariaDB's own metadata.
func (k Kind) SatisfiedBy(reported Kind) bool {
	return k == Plain || k == reported
}
