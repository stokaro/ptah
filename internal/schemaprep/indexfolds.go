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

// IndexConstraintFold is a UNIQUE or EXCLUDE constraint PostgreSQL does not
// build when one CREATE TABLE declares it, because an index constraint the
// same statement declares builds the same index.
type IndexConstraintFold struct {
	// Folded is the position of the constraint the server leaves out, in the
	// list given to [FoldedIndexConstraints].
	Folded int
	// Into is the position of the constraint whose index the server builds
	// instead, or [PrimaryKeyOutsideList].
	Into int
}

// FoldedIndexConstraints answers which UNIQUE and EXCLUDE constraints of table
// PostgreSQL leaves out of one CREATE TABLE that declares the table's primary
// key and, in list order, every constraint of constraints that belongs to
// table. fields may hold the columns of every table; the primary key is read
// from the ones table owns. The answer is in list order, and nil on every
// dialect but PostgreSQL.
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
// DEFERRABLE is part of the server's key and not of this one: Ptah neither
// writes nor reads it on these constraints, so the statement the server sees
// never carries it.
//
// A column's own UNIQUE is not in the list and is not compared. The server
// folds a table's UNIQUE over the same column into it too; the model keeps the
// column's key on the column, and the comparison matches it by its column.
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
	return folds
}

// indexKey is what decides whether two index constraints build the same
// index: the server builds one index for equal keys. A primary key and a UNIQUE
// have the same kind of key, so they fold together. A list of names, or of
// [pgname.Tokens] entries, is joined by [indexKeySeparator].
type indexKey struct {
	columns     string
	include     string
	notDistinct bool
	method      string
	elements    string
	where       string
}

// indexKeySeparator joins the entries of a key's list. PostgreSQL allows it in
// no name and no token.
const indexKeySeparator = "\x00"

func indexKeyOf(constraint schemamodel.Constraint) (indexKey, bool) {
	switch strings.ToUpper(constraint.Type) {
	case "PRIMARY KEY":
		return indexKey{
			columns: strings.Join(constraint.Columns, indexKeySeparator),
			include: strings.Join(constraint.IncludeColumns, indexKeySeparator),
		}, true
	case "UNIQUE":
		return indexKey{
			columns:     strings.Join(constraint.Columns, indexKeySeparator),
			include:     strings.Join(constraint.IncludeColumns, indexKeySeparator),
			notDistinct: constraint.NullsDistinct != nil && !*constraint.NullsDistinct,
		}, true
	case "EXCLUDE":
		return indexKey{
			method:   tokenText(constraint.UsingMethod),
			elements: tokenText(constraint.ExcludeElements),
			where:    tokenText(constraint.WhereCondition),
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
		columns: strings.Join(columns, indexKeySeparator),
		include: strings.Join(table.PrimaryKeyInclude, indexKeySeparator),
	}, PrimaryKeyOutsideList, true
}

func isPrimaryKeyConstraint(constraint schemamodel.Constraint) bool {
	return strings.EqualFold(constraint.Type, "PRIMARY KEY")
}
