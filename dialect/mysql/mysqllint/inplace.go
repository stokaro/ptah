package mysqllint

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintkit/mytype"
)

// The MySQL cost rules fire where a copy is established, and stay quiet where
// the server applies the change in place. Quiet is correct, but from the
// report it reads the same as "the rule did not look", and DS103 and MY101
// still describe the statement as one that may truncate values or rebuild the
// table. MY130P in this file is the other half of the measurement for the
// MySQL family: an info-severity finding that says the clause was judged and
// applied in place, and how, for a statement whose every clause the
// comparison could judge (stokaro/ptah#2956). The PostgreSQL owner reports
// the same for PostgreSQL as PG301P.
//
// The type half is the measurement in mysqlcost.go. The attribute half was
// measured on MySQL 8.4.11 and MariaDB 11.8.9 by asking for ALGORITHM=INSTANT
// and then INPLACE with LOCK=NONE, on a latin1 VARCHAR(10) column of a table
// with a row:
//
//	MODIFY / CHANGE clause                    MySQL              MariaDB
//	VARCHAR(10) -> VARCHAR(20)                INPLACE, LOCK=NONE  INSTANT
//	utf8mb3 -> utf8mb4, collation change,
//	  no key on the column                    INPLACE, LOCK=NONE  INSTANT
//	NOT NULL added, NOT NULL dropped          INPLACE, LOCK=NONE  INPLACE, LOCK=NONE
//	DEFAULT, COMMENT, AFTER, INVISIBLE,
//	  CHANGE that renames                     INSTANT             INSTANT
//	AUTO_INCREMENT added                      copy                copy
//
// An INPLACE change of nullability rebuilds the table in place, online, and
// the finding says so rather than folding it into "in place"; a NOT NULL
// added on a nullable column still fails on a NULL row, which DD103 reports
// beside this. A clause that carries anything outside the measured set --
// AUTO_INCREMENT, a generated expression, ON UPDATE, a key -- or a statement
// with a clause of another kind is not judged and gets no finding, so the
// finding never says "in place" of a statement the measurement did not
// cover. The rule reads the schema state the version starts from and stays
// silent without it, named as unmet.

// mysqlInPlaceStatements lists the statements MY130P judges: every MODIFY
// or CHANGE with a type, and every CONVERT TO CHARACTER SET.
func mysqlInPlaceStatements(file *lintkit.File) []int {
	indexes := append(columnChangeStatements(file), charsetConversionStatements(file)...)
	slices.Sort(indexes)
	return slices.Compact(indexes)
}

func mysqlInPlaceRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "MY130P",
			Title:    "column change applied in place",
			Severity: lintext.Info,
			Targets:  family(),
			Subsumes: []string{"DS103", "MY101"},
			Input:    lintext.InputBaselineSchema,
		},
		BaselineSubjects: mysqlInPlaceStatements,
		File: func(file *lintkit.File) []lintext.Violation {
			if !file.IsUp {
				return nil
			}
			var findings []lintext.Violation
			for _, index := range mysqlInPlaceStatements(file) {
				message, ok := mysqlInPlaceMessage(file, index)
				if !ok {
					continue
				}
				findings = append(findings, lintext.Violation{Statement: index, Message: message})
			}
			return findings
		},
	}
}

// mysqlInPlaceMessage judges one ALTER TABLE statement as a whole. Every
// clause must be a MODIFY or CHANGE whose type change is in place or absent
// and whose attributes are all in the measured set, or the statement must
// be one CONVERT TO CHARACTER SET whose every column converts in place.
func mysqlInPlaceMessage(file *lintkit.File, index int) (string, bool) {
	stmt := &file.Statements[index]
	w := stmt.Words
	clauses := lintkit.ClauseStarts(w)
	if conversion, ok := mysqlInPlaceConversion(file, index); ok && len(clauses) == 1 {
		return conversion, true
	}
	var sentences []string
	for _, change := range resolveColumnChanges(file) {
		if change.site.Statement != index {
			continue
		}
		sentence, ok := mysqlInPlaceClause(file, change)
		if !ok {
			return "", false
		}
		sentences = append(sentences, sentence)
	}
	if len(sentences) == 0 || len(sentences) != len(clauses) {
		return "", false
	}
	return strings.Join(sentences, ". ") + ". Nothing in the statement copies the table or blocks writes", true
}

// mysqlInPlaceConversion judges a CONVERT TO CHARACTER SET: every character
// column must convert in place, or the table must hold none.
func mysqlInPlaceConversion(file *lintkit.File, index int) (string, bool) {
	for _, conversion := range resolveCharsetConversions(file) {
		if conversion.site.statement != index {
			continue
		}
		var reasons []string
		for _, column := range conversion.columns {
			if column.transition.outcome != typeInPlace {
				return "", false
			}
			reasons = append(reasons, column.name+" ("+column.transition.why+")")
		}
		head := fmt.Sprintf("CONVERT TO CHARACTER SET %s on %s ", conversion.site.target, conversion.site.table.Name)
		if len(reasons) == 0 {
			return head + "re-encodes no column, so both servers apply it as a metadata change (ALGORITHM=INSTANT)", true
		}
		return head + fmt.Sprintf("converts %d column%s in place, with writes allowed (INPLACE with LOCK=NONE on MySQL, INSTANT on MariaDB): %s",
			len(reasons), lintkit.Plural(len(reasons)), strings.Join(reasons, "; ")), true
	}
	return "", false
}

// mysqlAttributes is what a MODIFY or CHANGE clause says after the type,
// limited to the attributes the measurement covers.
type mysqlAttributes struct {
	notNull bool
	// instant names the attributes InnoDB changes as metadata: DEFAULT,
	// COMMENT, the position, the visibility.
	instant []string
	unknown bool
}

// parseMySQLAttributes reads the words after the type up to the clause end.
// A DEFAULT value is one word or one parenthesized expression.
func parseMySQLAttributes(w []string, from, end int) mysqlAttributes {
	var attrs mysqlAttributes
	for k := from; k < end; k++ {
		switch w[k] {
		case "NOT":
			if k+1 < end && w[k+1] == "NULL" {
				attrs.notNull = true
				k++
				continue
			}
			attrs.unknown = true
		case "NULL":
		case "DEFAULT":
			attrs.instant = append(attrs.instant, "DEFAULT")
			k = skipDefaultValue(w, k+1, end) - 1
		case "COMMENT":
			attrs.instant = append(attrs.instant, "COMMENT")
			k++
		case "AFTER":
			attrs.instant = append(attrs.instant, "position")
			k++
		case "FIRST":
			attrs.instant = append(attrs.instant, "position")
		case "INVISIBLE", "VISIBLE":
			attrs.instant = append(attrs.instant, "visibility")
		default:
			attrs.unknown = true
		}
	}
	return attrs
}

// skipDefaultValue returns the index after a DEFAULT value: a parenthesized
// expression, or one word.
func skipDefaultValue(w []string, k, end int) int {
	if k >= end {
		return end
	}
	if w[k] != "(" {
		return k + 1
	}
	depth := 0
	for ; k < end; k++ {
		switch w[k] {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return k + 1
			}
		}
	}
	return end
}

// mysqlInPlaceClause judges one MODIFY or CHANGE clause: the type change,
// the nullability, and the instant attributes, each with the measured
// algorithm, or nothing when any part is outside the measurement.
func mysqlInPlaceClause(file *lintkit.File, change resolvedColumnChange) (string, bool) {
	if change.transition.outcome != typeInPlace && change.transition.outcome != typeUnchanged {
		return "", false
	}
	stmt := &file.Statements[change.site.Statement]
	w := stmt.Words
	_, after, ok := mytype.ParseAt(w, change.site.TypeStart, "")
	if !ok {
		return "", false
	}
	attrs := parseMySQLAttributes(w, after, lintkit.ClauseEndFrom(w, change.site.TypeStart))
	if attrs.unknown {
		return "", false
	}
	column, ok := file.Baseline.Column(change.site.Table.Normalized, lintkit.NormalizeIdent(change.site.OldName))
	if !ok {
		return "", false
	}
	var parts []string
	if change.transition.outcome == typeInPlace {
		parts = append(parts, change.transition.why+", in place with writes allowed (INPLACE with LOCK=NONE on MySQL, INSTANT on MariaDB)")
	}
	switch {
	case attrs.notNull && !column.NotNull:
		parts = append(parts, "sets NOT NULL, which rebuilds the table in place with writes allowed (INPLACE, LOCK=NONE) and fails on a NULL row")
	case !attrs.notNull && column.NotNull:
		parts = append(parts, "drops NOT NULL, which rebuilds the table in place with writes allowed (INPLACE, LOCK=NONE)")
	}
	if change.site.OldName != change.site.NewName {
		attrs.instant = append([]string{"name"}, attrs.instant...)
	}
	if len(attrs.instant) > 0 {
		parts = append(parts, "changes the "+strings.Join(slices.Compact(attrs.instant), ", ")+" as metadata (ALGORITHM=INSTANT)")
	}
	if len(parts) == 0 {
		return "", false
	}
	return fmt.Sprintf("%s %s.%s %s", change.clause(), change.site.Table.Name, change.site.NewName, strings.Join(parts, "; ")), true
}
