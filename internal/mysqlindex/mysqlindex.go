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

import (
	"strings"

	"go.5x5.cz/ptah/core/platform"
)

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

// Method is the `USING` clause a MySQL-family index asks for, empty where it
// asks for none.
//
// It reads the same field [KindOf] does, because an index carries a prefix or a
// method and never both: `SPATIAL` and `FULLTEXT` take no `USING` clause, and
// `USING {BTREE|HASH}` is the whole of what the grammar allows beside them.
//
// BTREE folds to empty on purpose, and that is not a shortcut. Ptah reads an
// existing index's method out of `information_schema.STATISTICS.INDEX_TYPE`,
// which reports `BTREE` for a declared `USING BTREE` and for an index that
// asked for nothing alike -- measured 2026-09-03 on MySQL 8.4.11 and MariaDB
// 11.8.9. The two are therefore the same index to every reader Ptah has, and a
// renderer that emitted `USING BTREE` for one of them would put a clause into
// the DDL of every index it ever read back from a server.
//
// HASH is the one spelling that survives to be worth carrying, and only where
// the storage engine implements it. Measured on the same pair, `KEY k USING
// HASH (a)`:
//
//	MySQL 8.4.11    InnoDB   INDEX_TYPE BTREE, and SHOW CREATE TABLE drops the clause
//	MySQL 8.4.11    MEMORY   INDEX_TYPE HASH
//	MariaDB 11.8.9  InnoDB   INDEX_TYPE HASH, and SHOW CREATE TABLE prints it back
//	MariaDB 11.8.9  MEMORY   INDEX_TYPE HASH
//
// So the clause is honored per storage engine rather than per dialect, which is
// why this answers what the DDL asked for and leaves what the server did with
// it to the reader. See stokaro/ptah#2825.
func Method(indexType string) string {
	if strings.EqualFold(strings.TrimSpace(indexType), "HASH") {
		return "HASH"
	}
	return ""
}

// MethodSatisfiedBy reports whether the method a server reports answers the
// method a desired index asked for.
//
// The asymmetry is [Kind.SatisfiedBy]'s, for the same reason: a desired index
// asking for no method takes whatever the engine chose, and comparing that
// would plan a rebuild the server undoes.
//
// The dialect decides the other direction, and it is a measurement rather than
// a convenience. `KEY k USING HASH (a)`, 2026-09-03:
//
//	                MySQL 8.4.11   MariaDB 11.8.9
//	InnoDB          BTREE          HASH
//	MEMORY          HASH           HASH
//	MyISAM          BTREE          HASH
//	Aria            --             HASH
//	ARCHIVE         error 1030     error 1286
//
// MariaDB records the declaration on every engine that takes an index, so what
// the catalog reports there is what was asked for and a difference is real.
// MySQL records it only on MEMORY, so a desired HASH read back as BTREE is the
// ordinary outcome on the default engine and reporting it would plan a rebuild
// MySQL immediately undoes.
//
// Deciding the MySQL case properly needs the table's storage engine, which
// this comparison does not have; until it does, MySQL accepts whatever the
// server reports. That leaves a desired HASH against a BTREE on MySQL + MEMORY
// unreported, which is the narrow half of stokaro/ptah#2834 and is recorded
// there rather than guessed at here.
func MethodSatisfiedBy(dialect, desiredType, reportedType string) bool {
	desired := Method(desiredType)
	if desired == "" {
		return true
	}
	if platform.NormalizeDialect(dialect) != platform.MariaDB {
		return true
	}
	return desired == Method(reportedType)
}
