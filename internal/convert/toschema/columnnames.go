package toschema

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"go.5x5.cz/ptah/core/schemamodel"
)

// ErrNonASCIIColumnName is the class of a MySQL-family key column Ptah has no
// one comparison rule for.
//
// The engines do not share a column-name equivalence, and neither does one
// engine share it with itself across two questions. Measured 2026-09-03 on
// MySQL 8.4.11 and MariaDB 11.8.9, over a utf8mb4 connection.
//
// Two columns on one table whose names differ only by the pair -- does the
// engine consider them one name:
//
//	I and dotless ı            distinct      ERROR 1060, one name
//	sigma and final sigma      distinct      ERROR 1060, one name
//	dotted İ and i             ERROR 1060    distinct
//	Kelvin sign and K          ERROR 1060    ERROR 1060, one name
//
// And the same pairs used to resolve a foreign key's local column against a
// column declared with the other spelling:
//
//	I -> dotless ı             ERROR 1072    accepted, reuses the covering key
//	sigma -> final sigma       ERROR 1072    accepted, reuses the covering key
//	dotted İ -> i              accepted      ERROR 1072
//	K -> Kelvin sign           accepted      ERROR 1072
//
// The first three rows agree: whichever engine folds a pair is the one that
// resolves the reference. The fourth does not. MariaDB calls `K` and the
// Kelvin sign one name when it refuses a duplicate column, and a different
// column when it resolves a foreign key -- so the rule is not one per dialect
// either.
//
// Ptah compared these with strings.EqualFold when deciding whether an existing
// key covers a foreign key, which is a third rule again, matching neither
// engine on any row. So a key column that cannot be compared is refused rather
// than guessed at. Refusing is not the answer this deserves --
// stokaro/ptah#2771 carries the engine-specific modeling -- but it is the one
// that neither pairs a key with a column the server keeps distinct nor misses
// coverage the server has.
var ErrNonASCIIColumnName = errors.New(
	"a non-ASCII key column has no single comparison rule across the MySQL family")

// refuseNonASCIIKeyColumns fails closed on a table whose keys name a column
// Ptah cannot compare.
//
// Only columns a key or a constraint names are asked about, plus a column
// carrying its own UNIQUE, whose name becomes an index name. A column that
// nothing indexes and no constraint references takes part in no comparison
// here, and refusing it would reject a table both engines create and Ptah
// renders correctly.
func refuseNonASCIIKeyColumns(
	database *schemamodel.Database, table schemamodel.Table,
	fieldsStart int, order []namedElement,
) error {
	for _, field := range database.Fields[fieldsStart:] {
		// A column-level UNIQUE is an index named after its column, so the
		// column's name is compared as an index name too.
		if !field.Unique {
			continue
		}
		if err := refuseNonASCIIColumns([]string{field.Name}, table); err != nil {
			return err
		}
	}
	for _, element := range order {
		// The branch comes first, not the value: the half of a namedElement
		// that is not set holds noPosition, so reading the constraint slice
		// before asking which half this is indexes it at -1.
		if element.isIndex() {
			if err := refuseNonASCIIColumns(database.Indexes[element.index].Fields, table); err != nil {
				return err
			}
			continue
		}
		if err := refuseNonASCIIColumns(
			database.Constraints[element.constraint].Columns, table); err != nil {
			return err
		}
	}
	return nil
}

// refuseNonASCIIColumns reports the first column name that cannot be compared.
func refuseNonASCIIColumns(columns []string, table schemamodel.Table) error {
	for _, column := range columns {
		if isASCII(column) {
			continue
		}
		return fmt.Errorf("%w: %s on %s", ErrNonASCIIColumnName, column, table.Name)
	}
	return nil
}

// isASCII reports whether every rune in name is one Ptah and both engines
// compare the same way.
//
// It is the one predicate behind both [ErrNonASCIIColumnName] and
// [ErrNonASCIIIndexName], because the two refusals ask the same question about
// different identifiers: ASCII case folding is shared and deterministic across
// the family, and nothing above it is.
func isASCII(name string) bool {
	for _, r := range name {
		if r >= utf8.RuneSelf {
			return false
		}
	}
	return true
}
