package parser

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
)

// refuseTableElementTypo reports a table element that reads as a column and
// describes an index.
//
// `parseTableElement` recognises a fixed set of first words -- CONSTRAINT,
// PRIMARY, UNIQUE, FOREIGN, CHECK, EXCLUDE, SPATIAL, FULLTEXT, INDEX, KEY --
// and everything else falls through to `parseColumnDefinition`, which reads the
// first word as a column name and whatever follows as a type. One keystroke off
// any of those keywords therefore produces a column, silently:
//
//	FULLTEXTT ft_b (bio)   ->   column `FULLTEXTT` of type `ft_b(bio)`
//
// Nothing refused it and nothing reported it. `ptah schema render` exited 0 and
// emitted a CREATE TABLE for a table nobody described, whose only tell was a
// `0 indexes` on a summary line most readers skim; applying it failed at the
// server with a type error naming neither the typo nor the line
// (stokaro/ptah#2753).
//
// # What separates the two
//
// Not the shape: `name TYPE(args)` and `KEYWORD name (columns)` are the same
// three tokens, which is why the fallthrough could not tell them apart. And not
// a vocabulary of types either -- this parser accepts a type it has never heard
// of on purpose, because extension and user-defined types are ordinary.
// `geometry(Point, 4326)`, `Nullable(String)` and a domain somebody declared in
// the same file all reach the renderer verbatim, and a closed list would refuse
// them.
//
// What separates them is a fact about SQL rather than about spelling: **a type
// is not an expression, so it cannot name a column.** An index's parenthesised
// list names columns of the table being defined; a type's arguments are widths,
// precisions, literals, or the names of other types. So a column whose type
// carries a parenthesised list whose every argument is a column of this same
// table is an index declaration that lost its keyword.
//
// EVERY argument, not any: one that is not a column is enough to make the
// parentheses type parameters. That is what leaves `geometry(Point, 4326)`
// alone even in a table that has a `point` column -- `4326` is not one -- and
// what leaves `Nullable(String)` alone in a table with no `String` column.
//
// A filter requiring each argument to be one unquoted word was here and came
// out again. It could not be measured: a quoted column name is recorded WITH
// its quotes, so no stored name ever equals a non-identifier argument, and
// every input that filter refused was already refused by the column-membership
// half. What it did do was cost coverage -- `KEYY k ("my col")` names a column
// nobody can spell bare, and the filter let that typo through.
//
// # Where it does not run
//
// ClickHouse, because there the conjunction stops being unusual: `Nullable(T)`,
// `Array(T)`, `LowCardinality(T)` and `Map(K, V)` all take a bare type name, so
// a table with a column named `String` would be refused for writing an ordinary
// type. It also has nothing to gain -- its inline `INDEX` is read by
// `parseInlineSkippingIndex` before this fallthrough is reached, and it has no
// MySQL-style `FULLTEXT name (col)` in a table body at all.
//
// Measured against every SQL file this repository tracks, parsed once per
// dialect: 1922 successful parses, zero findings. The residue is a table
// carrying a column named after a type argument -- PostGIS `geography(Point)`
// beside a column called `point` -- which is refused rather than accepted, and
// the message says which of the two readings it took.
func refuseTableElementTypo(table *ast.CreateTableNode, dialect string, position int) error {
	if dialect == platform.ClickHouse {
		return nil
	}
	declared := make(map[string]bool, len(table.Columns))
	for _, column := range table.Columns {
		declared[strings.ToLower(column.Name)] = true
	}
	for _, column := range table.Columns {
		arguments := typeArguments(column.Type)
		if len(arguments) == 0 || !allDeclared(arguments, declared) {
			continue
		}
		return fmt.Errorf(
			"table %q declares %q with type %q at position %d, and %s a column of %q: "+
				"a type cannot name a column, so this reads as a table-level index or "+
				"constraint whose keyword is misspelled -- write INDEX, KEY, UNIQUE, "+
				"FULLTEXT, SPATIAL, PRIMARY KEY, FOREIGN KEY, CHECK, EXCLUDE or "+
				"CONSTRAINT before it, or rename the column it collides with",
			table.Name, column.Name, column.Type, position,
			argumentClause(arguments), table.Name)
	}
	return nil
}

// typeArguments is the type's parenthesised argument list, comma-separated and
// trimmed, or nothing when the type carries no parentheses.
//
// The array suffix `text[]` leaves here empty-handed, having none.
func typeArguments(columnType string) []string {
	open := strings.Index(columnType, "(")
	closing := strings.LastIndex(columnType, ")")
	if open < 0 || closing < open {
		return nil
	}
	inner := strings.TrimSpace(columnType[open+1 : closing])
	if inner == "" {
		return nil
	}
	arguments := make([]string, 0, strings.Count(inner, ",")+1)
	for part := range strings.SplitSeq(inner, ",") {
		arguments = append(arguments, strings.TrimSpace(part))
	}
	return arguments
}

// allDeclared reports whether every argument names a column of the table.
//
// Every one, not any: a single argument that is not a column is enough to make
// the parentheses type parameters, which is what keeps `geometry(Point, 4326)`
// out of this even in a table that has a `point` column. A number, a string
// literal and a qualified name are all names no column has, so each of them
// answers false here without a separate rule about its shape.
func allDeclared(arguments []string, declared map[string]bool) bool {
	for _, argument := range arguments {
		if !declared[strings.ToLower(argument)] {
			return false
		}
	}
	return true
}

// argumentClause renders the collision the refusal names, in the singular or
// the plural.
func argumentClause(arguments []string) string {
	quoted := make([]string, len(arguments))
	for index, argument := range arguments {
		quoted[index] = fmt.Sprintf("%q", argument)
	}
	if len(quoted) == 1 {
		return quoted[0] + " is"
	}
	return strings.Join(quoted, ", ") + " are each"
}
