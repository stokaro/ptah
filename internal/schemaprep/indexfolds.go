package schemaprep

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgname"
)

// PrimaryKeyOutsideList is the [IndexConstraintFold.Into] of a constraint that
// folds into a primary key the table or its columns declare, rather than a
// PRIMARY KEY entry of the constraint list.
const PrimaryKeyOutsideList = -1

// ColumnKeyOutsideList is the [IndexConstraintFold.Folded] of a column's own
// UNIQUE, which the model keeps on the column rather than in the constraint
// list. [IndexConstraintFold.Column] names the column.
const ColumnKeyOutsideList = -2

// IndexConstraintFold is a UNIQUE or EXCLUDE constraint, or a column's own
// UNIQUE, that PostgreSQL does not build when one CREATE TABLE declares it,
// because an index constraint the same statement declares builds the same
// index.
type IndexConstraintFold struct {
	// Folded is the position of the constraint the server leaves out, in the
	// list given to [FoldedIndexConstraints], or [ColumnKeyOutsideList].
	Folded int
	// Into is the position of the constraint whose index the server builds
	// instead, or [PrimaryKeyOutsideList].
	Into int
	// Column is the column whose own UNIQUE folds, when Folded is
	// [ColumnKeyOutsideList], and empty otherwise.
	Column string
}

// FoldedIndexConstraints answers which UNIQUE and EXCLUDE constraints of table,
// and which of its columns' own UNIQUEs, PostgreSQL leaves out of one CREATE
// TABLE that declares the table's primary key, its columns and, in list order,
// every constraint of constraints that belongs to table. fields may hold the
// columns of every table; the primary key and the columns' own keys are read
// from the ones table owns. The answer is in list order, then in the order of
// fields, and nil on every dialect but PostgreSQL.
//
// The server builds one index for index constraints with the same key, keeps
// the first, and drops the rest without a word. The primary key is first
// whatever its place in the statement. A name a dropped constraint carries
// goes to the kept one when that one has none, so `id int PRIMARY KEY,
// CONSTRAINT u UNIQUE (id)` leaves one constraint: the primary key, named u.
// A CREATE TABLE and an ALTER TABLE that adds the second build both, and so do
// two ADD clauses of one ALTER TABLE. Measured on PostgreSQL 18.6, which
// also shows the key: the columns, the INCLUDE columns and NULLS NOT DISTINCT
// of a PRIMARY KEY or UNIQUE, and the access method, the elements and the WHERE
// predicate of an EXCLUDE. A UNIQUE and an EXCLUDE never fold together.
//
// Elements and predicates compare as [pgname.Tokens] reads them. Spellings
// that differ in tokens and parse alike, such as a redundant pair of
// parentheses, are not recognized, and the server folds a pair this keeps.
// Deferral is part of the key too: `UNIQUE (a) DEFERRABLE, UNIQUE (a)` in one
// CREATE TABLE builds two constraints on PostgreSQL 18.6, and so does a pair
// that differs only in INITIALLY DEFERRED.
//
// A column's own UNIQUE is a key over that column alone, and it folds with an
// equal primary key or UNIQUE the same way. Measured on PostgreSQL 18.6:
//
//	body of c                                        keys of c
//	a int UNIQUE, CONSTRAINT uq_a UNIQUE (a)         uq_a
//	CONSTRAINT uq_a UNIQUE (a), a int UNIQUE         uq_a
//	a int UNIQUE, UNIQUE (a)                         c_a_key
//	id int PRIMARY KEY UNIQUE                        c_pkey
//	a int UNIQUE, PRIMARY KEY (a)                    c_pkey
//
// A column's own UNIQUE has no name, so the server reports the same key
// whichever of two equal ones it keeps: the name the others give, or its own.
// The rule places the columns after the list, so a column's key is the one that
// folds, and a key of the list keeps the name it carries.
func FoldedIndexConstraints(
	table schemamodel.Table,
	fields []schemamodel.Field,
	constraints []schemamodel.Constraint,
	dialect string,
) []IndexConstraintFold {
	if platform.NormalizeDialect(dialect) != platform.Postgres {
		return nil
	}
	type keptIndex struct {
		key      indexKey
		position int
	}
	var kept []keptIndex
	if key, position, ok := primaryKeyIndex(table, fields, constraints); ok {
		kept = append(kept, keptIndex{key: key, position: position})
	}
	var folds []IndexConstraintFold
	for position, constraint := range constraints {
		if !ConstraintBelongsToTable(constraint, table) {
			continue
		}
		key, ok := indexKeyOf(constraint)
		if !ok || isPrimaryKeyConstraint(constraint) {
			continue
		}
		into := slices.IndexFunc(kept, func(earlier keptIndex) bool { return earlier.key == key })
		if into >= 0 {
			folds = append(folds, IndexConstraintFold{Folded: position, Into: kept[into].position})
			continue
		}
		kept = append(kept, keptIndex{key: key, position: position})
	}
	for _, field := range fields {
		if field.StructName != table.StructName || !field.Unique {
			continue
		}
		key := indexKey{columns: field.Name}
		into := slices.IndexFunc(kept, func(earlier keptIndex) bool { return earlier.key == key })
		if into >= 0 {
			folds = append(folds, IndexConstraintFold{
				Folded: ColumnKeyOutsideList, Into: kept[into].position, Column: field.Name,
			})
		}
	}
	return folds
}

// indexKey is what decides whether two index constraints build the same
// index: the server builds one index for equal keys. A primary key and a UNIQUE
// have the same kind of key, so they fold together. A list of names, or of
// [pgname.Tokens] entries, is joined by [indexKeySeparator].
type indexKey struct {
	columns       string
	include       string
	notDistinct   bool
	method        string
	elements      string
	where         string
	deferrable    bool
	deferredFirst bool
}

// indexKeySeparator joins the entries of a key's list. PostgreSQL allows it in
// no name and no token.
const indexKeySeparator = "\x00"

func indexKeyOf(constraint schemamodel.Constraint) (indexKey, bool) {
	switch strings.ToUpper(constraint.Type) {
	case "PRIMARY KEY":
		return indexKey{
			columns:       strings.Join(constraint.Columns, indexKeySeparator),
			include:       strings.Join(constraint.IncludeColumns, indexKeySeparator),
			deferrable:    constraint.Deferrable,
			deferredFirst: initiallyDeferred(constraint.Initially),
		}, true
	case "UNIQUE":
		return indexKey{
			columns:       strings.Join(constraint.Columns, indexKeySeparator),
			include:       strings.Join(constraint.IncludeColumns, indexKeySeparator),
			notDistinct:   constraint.NullsDistinct != nil && !*constraint.NullsDistinct,
			deferrable:    constraint.Deferrable,
			deferredFirst: initiallyDeferred(constraint.Initially),
		}, true
	case "EXCLUDE":
		return indexKey{
			method:        tokenText(constraint.UsingMethod),
			elements:      tokenText(constraint.ExcludeElements),
			where:         tokenText(constraint.WhereCondition),
			deferrable:    constraint.Deferrable,
			deferredFirst: initiallyDeferred(constraint.Initially),
		}, true
	default:
		return indexKey{}, false
	}
}

func tokenText(text string) string {
	return strings.Join(pgname.Tokens(text), indexKeySeparator)
}

// primaryKeyIndex answers the key of table's primary key and its position in
// constraints, or [PrimaryKeyOutsideList] when the table or its columns declare
// it. A PRIMARY KEY entry of the list comes first, then the table's key
// columns, then the columns that declare themselves primary, in order.
func primaryKeyIndex(
	table schemamodel.Table,
	fields []schemamodel.Field,
	constraints []schemamodel.Constraint,
) (indexKey, int, bool) {
	for position, constraint := range constraints {
		if !ConstraintBelongsToTable(constraint, table) || !isPrimaryKeyConstraint(constraint) {
			continue
		}
		key, _ := indexKeyOf(constraint)
		return key, position, true
	}
	columns := table.PrimaryKey
	if len(columns) == 0 {
		for _, field := range fields {
			if field.StructName == table.StructName && field.Primary {
				columns = append(columns, field.Name)
			}
		}
	}
	if len(columns) == 0 {
		return indexKey{}, 0, false
	}
	return indexKey{
		columns:       strings.Join(columns, indexKeySeparator),
		include:       strings.Join(table.PrimaryKeyInclude, indexKeySeparator),
		deferrable:    table.PrimaryKeyDeferrable,
		deferredFirst: initiallyDeferred(table.PrimaryKeyInitially),
	}, PrimaryKeyOutsideList, true
}

func isPrimaryKeyConstraint(constraint schemamodel.Constraint) bool {
	return strings.EqualFold(constraint.Type, "PRIMARY KEY")
}

// initiallyDeferred reports whether a timing checks at the end of the
// transaction by default. Empty and "immediate" both check at once.
func initiallyDeferred(initially string) bool {
	return strings.EqualFold(initially, "deferred")
}
