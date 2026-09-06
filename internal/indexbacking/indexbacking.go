// Package indexbacking answers one question about a live catalog: is the index
// the server reports the physical backing of a constraint, created and dropped
// through that constraint rather than on its own.
//
// The question is asked by two trees for two different purposes, and until this
// package they answered it twice. `migration/schemadiff` asks it to decide
// which reported index a comparison must ignore, because a constraint change
// already speaks for it. `internal/convert/dbschematogo` asks it to decide
// which of the two representations describes the object, because emitting both
// produces one name and two objects.
//
// Those uses stay where they are. What was duplicated is the evidence, and the
// two encodings of it had drifted: the comparator decided per dialect and from
// the index's own structure, the converter decided from name equality with no
// dialect at all. They compensated on every shape a reader produces -- that is
// what `migration/schemadiff`'s ownership round trip measures -- and the
// compensation was a coincidence two trees had to keep, rather than a rule one
// of them stated (stokaro/ptah#2606, ADR 0015 D2).
//
// A name match is evidence about a name. It is not evidence about an object,
// which is why nothing here decides from a name alone.
package indexbacking

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
)

// Kind is the constraint kind an index can be the backing of.
//
// It is a closed set rather than the catalog's `Type` string, so a caller that
// adds a constraint kind has to say here whether a server backs it with an
// index instead of falling through a switch as "no".
type Kind uint8

const (
	// None is a constraint kind no server enforces with a separate index --
	// CHECK, most notably -- and the answer for an unrecognized type.
	None Kind = iota
	// PrimaryKey is a PRIMARY KEY constraint.
	PrimaryKey
	// Unique is a UNIQUE constraint.
	Unique
	// Exclusion is an EXCLUDE constraint.
	Exclusion
	// ForeignKey is a FOREIGN KEY constraint.
	ForeignKey
)

// KindOf maps a catalog constraint's type to the kind this package reasons
// about. The comparison is case-insensitive because the readers do not agree on
// case: PostgreSQL reports `PRIMARY KEY`, and a fixture may carry `primary key`.
//
// An unrecognized type is None, which no server backs. That is the safe answer:
// a kind nothing recognizes is compared and described on its own terms rather
// than being silently attached to an index.
func KindOf(constraintType string) Kind {
	switch strings.ToUpper(strings.TrimSpace(constraintType)) {
	case "PRIMARY KEY":
		return PrimaryKey
	case "UNIQUE":
		return Unique
	case "EXCLUDE":
		return Exclusion
	case "FOREIGN KEY":
		return ForeignKey
	default:
		return None
	}
}

// ServerBacks reports whether a constraint of this kind is enforced, on this
// dialect, by an index the reader reports alongside ordinary indexes.
//
// It answers whether such an index EXISTS to be attributed, not which one. The
// second question needs the constraint's columns and belongs to the caller,
// because the match is structural: `migration/schemadiff` compares a foreign
// key's columns against each candidate index's leading columns rather than
// trusting the shared name.
//
// An unknown or empty dialect answers only what every server does. That is the
// honest answer for a caller that has no dialect -- `catalog.Database` carries
// none, and `atlascompat.DBSchemaToGoSchema` takes none -- and it is the answer
// that path has always produced. It is stated here rather than implied by a
// switch nobody wrote an arm in.
func ServerBacks(dialect string, kind Kind) bool {
	normalized := platform.NormalizeDialect(dialect)
	switch kind {
	case PrimaryKey:
		// Every server enforces a primary key with an index. The reader marks
		// that index rather than leaving it to be recognized by its name, so
		// the answer a caller wants is Unaddressable, which reads the mark.
		return true
	case Unique:
		// SQL Server keeps a UNIQUE constraint and a unique index as separate
		// objects, so neither backs the other and its index query already
		// filters `is_unique_constraint = 0`.
		return normalized != platform.SQLServer
	case Exclusion:
		// An EXCLUDE constraint is a PostgreSQL-family construct and is always
		// implemented by an index.
		return true
	case ForeignKey:
		// MySQL and MariaDB create an index for every foreign key unasked, and
		// Spanner reports a backing index for one. The PostgreSQL family
		// creates none: a foreign key there is enforced by the referenced
		// key's index, not by one of its own.
		return normalized == platform.MySQL ||
			normalized == platform.MariaDB ||
			normalized == platform.Spanner
	case None:
		return false
	default:
		return false
	}
}

// NamedAfterConstraint reports whether name equality on the same table is
// sufficient evidence that an index is this kind's backing, on this dialect.
//
// It is deliberately narrower than [ServerBacks], and the gap is the point.
// MySQL, MariaDB and Spanner do name a foreign key's backing index after the
// constraint, so ServerBacks answers true for it -- but an ordinary index may
// carry that name too, so the name alone does not identify the object. The
// comparator matches a foreign key's backing on the constraint's COLUMNS as
// well, and a caller with no such structural match must not attribute one from
// the name.
//
// A primary key is false for the other reason: its index is recognized by the
// mark the reader puts on it, which [Unaddressable] reads. Attributing it from
// a constraint's name would make the answer depend on a spelling instead.
//
// A name match is evidence about a name. This function says the two cases where
// the servers make it evidence about an object as well.
func NamedAfterConstraint(dialect string, kind Kind) bool {
	switch kind {
	case Unique, Exclusion:
		return ServerBacks(dialect, kind)
	case PrimaryKey, ForeignKey, None:
		return false
	default:
		return false
	}
}

// Unaddressable reports whether a reported index has no standalone existence:
// nothing can declare it, so nothing can be compared against a declaration of
// it, and no description should offer it as an index a reader could remove.
//
// Two shapes qualify, and both are recognized from the catalog rather than from
// a name that looks generated. A primary key's index is created and dropped
// with the key, and the reader marks it. SQLite names its own backing structure
// `sqlite_autoindex_*` and no statement can refer to that name -- which is a
// fact about SQLite's spelling of an internal object rather than a guess about
// a user's naming, and it is the one place a prefix is load-bearing.
func Unaddressable(index catalog.Index, dialect string) bool {
	return index.IsPrimary || isSQLiteInternalAutoindex(index.Name, dialect)
}

func isSQLiteInternalAutoindex(indexName, dialect string) bool {
	return platform.NormalizeDialect(dialect) == platform.SQLite &&
		strings.HasPrefix(indexName, "sqlite_autoindex_")
}
