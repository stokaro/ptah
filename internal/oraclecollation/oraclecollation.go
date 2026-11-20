// Package oraclecollation holds what the Oracle renderer, reader and planner
// agree on about a column collation, so that each reads the same rule instead
// of its own copy.
//
// Oracle takes a column COLLATE only where the database runs with
// MAX_STRING_SIZE=EXTENDED, and only on VARCHAR2, NVARCHAR2, CHAR and NCHAR.
// A character column that names none reads USING_NLS_COMP from the catalog.
// Changing a column's collation is refused while the column belongs to an
// index, a key, a CHECK or a generated column; [RefuseKeyedColumnChange] says
// so before any statement runs.
package oraclecollation

import (
	"regexp"
	"slices"
	"strings"
)

// Inherited is the collation Oracle reports for a character column that
// declares none.
const Inherited = "USING_NLS_COMP"

// Valid reports whether name is spelled as an Oracle collation name: an
// identifier of letters, digits, underscores, dollar signs and number signs
// that starts with a letter. The renderer writes the name as it was declared,
// so a name outside this shape would reach the statement as written.
func Valid(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		c := name[i]
		letter := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
		switch {
		case letter:
		case i > 0 && (c >= '0' && c <= '9' || c == '_' || c == '$' || c == '#'):
		default:
			return false
		}
	}
	return true
}

// Collatable reports whether a column of renderedType, written as Oracle
// spells it (`VARCHAR2(20 CHAR)`, `CLOB`), takes a collation. Measured on
// Oracle Free 23.26, a collation on CLOB or NCLOB answers ORA-43912 and one on
// NUMBER, RAW, DATE, JSON or BOOLEAN answers ORA-43911.
func Collatable(renderedType string) bool {
	name, _, _ := strings.Cut(strings.TrimSpace(renderedType), "(")
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR":
		return true
	default:
		return false
	}
}

// Read turns the collation the catalog reports for a column, and the default
// collation of its table, into the two values the model holds.
//
// effective is the collation the column compares with, empty for a column that
// takes USING_NLS_COMP and for a column with no character type, which reports
// none. declared is effective unless the column takes its table's default,
// where the catalog cannot say whether the column named that collation itself.
// Names are upper case, as the catalog stores them.
func Read(column, tableDefault string) (declared, effective string) {
	column = strings.TrimSpace(column)
	if column == "" || strings.EqualFold(column, Inherited) {
		return "", ""
	}
	effective = strings.ToUpper(column)
	if strings.EqualFold(column, strings.TrimSpace(tableDefault)) {
		return "", effective
	}
	return effective, effective
}

// sortKey matches the expression Oracle keeps for an index key over a column
// with a collation: NLSSORT of the column under the collation's sort.
var sortKey = regexp.MustCompile(`^NLSSORT\("([^"]+)",\s*'nls_sort=''([A-Za-z][A-Za-z0-9_$#]*)'''\)$`)

// SortKeyColumn names the column an index key stands for, and the sort the key
// applies, when the catalog lists the key under a hidden column.
//
// An index over a column with a collation other than USING_NLS_COMP is a
// function-based index. Measured on Oracle Free 23.26, `CREATE INDEX i ON t
// (c)` over `c VARCHAR2(10) COLLATE BINARY_CI` gives INDEX_TYPE
// FUNCTION-BASED NORMAL, ALL_IND_COLUMNS.COLUMN_NAME SYS_NC00004$ and
// ALL_IND_EXPRESSIONS.COLUMN_EXPRESSION NLSSORT("C",'nls_sort=”BINARY_CI”'),
// where the declaration says `c`. Read as listed, the index never matches its
// declaration and every plan would replace it. hidden is the column the
// catalog lists and expression the key's expression; ok is false for any key
// that is not exactly the sort key of one column, such as UPPER("B"), which is
// a function of the column and no key over the column itself.
//
// The expression alone does not make the key the column's own: a user can write
// `CREATE INDEX i ON t (NLSSORT(c, 'nls_sort=binary_ci'))` over a column that
// declares no collation, and the catalog lists the same expression. The caller
// maps the key to the column only when the column holds the sort the key
// applies, which is why the sort is returned.
func SortKeyColumn(hidden, expression string) (column, sort string, ok bool) {
	if !strings.HasPrefix(hidden, "SYS_NC") || !strings.HasSuffix(hidden, "$") {
		return "", "", false
	}
	match := sortKey.FindStringSubmatch(strings.TrimSpace(expression))
	if match == nil {
		return "", "", false
	}
	return match[1], match[2], true
}

// Known reports whether Oracle takes name as a column collation, given the
// sorts the server lists in V$NLS_VALID_VALUES for PARAMETER='SORT'.
//
// Measured on Oracle Free 23.26 (every listed sort tried bare, with _CI, with
// _AI and with _CS): a collation is a listed sort, a listed sort followed by
// _CI or _AI, or USING_NLS_COMP. BINARY is a listed sort, so BINARY_CI and
// BINARY_AI follow; they are accepted here even when the list is empty. Anything
// else answers ORA-12746, and so does USING_NLS_COMP_CI. Names compare
// without regard to case, because Oracle upper-cases an unquoted one.
func Known(name string, sorts []string) bool {
	name = strings.ToUpper(strings.TrimSpace(name))
	switch name {
	case Inherited, "BINARY", "BINARY_CI", "BINARY_AI":
		return true
	}
	base := name
	if cut, found := strings.CutSuffix(name, "_CI"); found {
		base = cut
	} else if cut, found := strings.CutSuffix(name, "_AI"); found {
		base = cut
	}
	return slices.ContainsFunc(sorts, func(sort string) bool {
		sort = strings.TrimSpace(sort)
		return strings.EqualFold(sort, name) || strings.EqualFold(sort, base)
	})
}
