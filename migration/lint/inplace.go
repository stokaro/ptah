package lint

import (
	"fmt"
	"slices"
	"strings"
)

// The cost rules in this package fire where a copy or a rewrite is
// established, and stay quiet where the server applies the change in place.
// Quiet is correct, but from the report it reads the same as "the rule did
// not look", and DS103 and MY101 still describe the statement as one that may
// truncate values or rebuild the table. The two rules in this file are the
// other half of the measurement: an info-severity finding that says the
// clause was judged and applied in place, and how, for a statement whose
// every clause the comparison could judge (stokaro/ptah#2956).
//
// The type half is the measurement in mysqlcost.go and pgcost.go. The
// attribute half was measured on MySQL 8.4.11 and MariaDB 11.8.9 by asking
// for ALGORITHM=INSTANT and then INPLACE with LOCK=NONE, on a latin1
// VARCHAR(10) column of a table with a row:
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
// and on PostgreSQL 18.6 by relfilenode and the heap-scan counter, beside a
// varchar(10) -> varchar(20) that is itself a catalog edit:
//
//	ALTER COLUMN clause                       table      indexes    scans
//	SET DEFAULT, DROP DEFAULT, DROP NOT NULL  kept       kept       0
//	SET NOT NULL                              kept       kept       1
//
// An INPLACE change of nullability rebuilds the table in place, online, and
// the finding says so rather than folding it into "in place"; a NOT NULL
// added on a nullable column still fails on a NULL row, which DD103 reports
// beside this. A clause that carries anything outside the measured set --
// AUTO_INCREMENT, a generated expression, ON UPDATE, a key -- or a statement
// with a clause of another kind is not judged and gets no finding, so the
// finding never says "in place" of a statement the measurement did not
// cover. Both rules read the schema state the version starts from and stay
// silent without it, named as unmet.

// inPlaceRules is the family: one info-severity finding per engine family.
func inPlaceRules() []Rule {
	return []Rule{mysqlInPlaceRule(), postgresInPlaceRule()}
}

// mysqlInPlaceStatements lists the statements MY130P judges: every MODIFY
// or CHANGE with a type, and every CONVERT TO CHARACTER SET.
func mysqlInPlaceStatements(file *File) []int {
	indexes := append(columnChangeStatements(file), charsetConversionStatements(file)...)
	slices.Sort(indexes)
	return slices.Compact(indexes)
}

func mysqlInPlaceRule() Rule {
	return Rule{
		Code:             "MY130P",
		Title:            "column change applied in place",
		Severity:         SeverityInfo,
		Dialects:         mysqlFamily,
		Subsumes:         []string{"DS103", "MY101"},
		Input:            InputBaselineSchema,
		BaselineSubjects: mysqlInPlaceStatements,
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			var findings []Finding
			for _, index := range mysqlInPlaceStatements(file) {
				message, ok := mysqlInPlaceMessage(file, index)
				if !ok {
					continue
				}
				findings = append(findings, inPlaceFinding(file, index, "MY130P", "column change applied in place", message))
			}
			return findings
		},
	}
}

func postgresInPlaceRule() Rule {
	return Rule{
		Code:             "PG301P",
		Title:            "column type change applied as a catalog edit",
		Severity:         SeverityInfo,
		Dialects:         []string{"postgres"},
		Subsumes:         []string{"DS103"},
		Input:            InputBaselineSchema,
		BaselineSubjects: pgTypeChangeStatements,
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			var findings []Finding
			for _, index := range pgTypeChangeStatements(file) {
				message, ok := pgInPlaceMessage(file, index)
				if !ok {
					continue
				}
				findings = append(findings, inPlaceFinding(file, index, "PG301P", "column type change applied as a catalog edit", message))
			}
			return findings
		},
	}
}

func inPlaceFinding(file *File, index int, code, title, message string) Finding {
	stmt := &file.Statements[index]
	return Finding{
		Rule:     code,
		Title:    title,
		Severity: SeverityInfo,
		File:     file.Path,
		Line:     stmt.Line,
		Message:  message,
		Context:  statementFindingContext(index),
	}
}

// mysqlInPlaceMessage judges one ALTER TABLE statement as a whole. Every
// clause must be a MODIFY or CHANGE whose type change is in place or absent
// and whose attributes are all in the measured set, or the statement must
// be one CONVERT TO CHARACTER SET whose every column converts in place.
func mysqlInPlaceMessage(file *File, index int) (string, bool) {
	stmt := &file.Statements[index]
	w := stmt.Words
	clauses := clauseStarts(w)
	if conversion, ok := mysqlInPlaceConversion(file, index); ok && len(clauses) == 1 {
		return conversion, true
	}
	var sentences []string
	for _, change := range resolveColumnChanges(file) {
		if change.site.statement != index {
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
func mysqlInPlaceConversion(file *File, index int) (string, bool) {
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
		head := fmt.Sprintf("CONVERT TO CHARACTER SET %s on %s ", conversion.site.target, conversion.site.table.name)
		if len(reasons) == 0 {
			return head + "re-encodes no column, so both servers apply it as a metadata change (ALGORITHM=INSTANT)", true
		}
		return head + fmt.Sprintf("converts %d column%s in place, with writes allowed (INPLACE with LOCK=NONE on MySQL, INSTANT on MariaDB): %s",
			len(reasons), plural(len(reasons)), strings.Join(reasons, "; ")), true
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
func mysqlInPlaceClause(file *File, change resolvedColumnChange) (string, bool) {
	if change.transition.outcome != typeInPlace && change.transition.outcome != typeUnchanged {
		return "", false
	}
	stmt := &file.Statements[change.site.statement]
	w := stmt.Words
	_, after, ok := parseMySQLTypeAt(w, change.site.typeStart, "")
	if !ok {
		return "", false
	}
	attrs := parseMySQLAttributes(w, after, clauseEndFrom(w, change.site.typeStart))
	if attrs.unknown {
		return "", false
	}
	column, ok := file.baseline.column(change.site.table.normalized, normalizeIdent(change.site.oldName))
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
	if change.site.oldName != change.site.newName {
		attrs.instant = append([]string{"name"}, attrs.instant...)
	}
	if len(attrs.instant) > 0 {
		parts = append(parts, "changes the "+strings.Join(slices.Compact(attrs.instant), ", ")+" as metadata (ALGORITHM=INSTANT)")
	}
	if len(parts) == 0 {
		return "", false
	}
	return fmt.Sprintf("%s %s.%s %s", change.clause(), change.site.table.name, change.site.newName, strings.Join(parts, "; ")), true
}

// pgInPlaceMessage judges one ALTER TABLE statement as a whole: every ALTER
// COLUMN ... TYPE must be a catalog edit, and every other clause must be a
// SET DEFAULT, DROP DEFAULT or DROP NOT NULL, which are catalog edits too.
func pgInPlaceMessage(file *File, index int) (string, bool) {
	stmt := &file.Statements[index]
	w := stmt.Words
	changes := 0
	var sentences []string
	for _, change := range resolvePGTypeChanges(file) {
		if change.site.statement != index {
			continue
		}
		if change.transition.outcome != pgInPlace && change.transition.outcome != pgUnchanged {
			return "", false
		}
		changes++
		if change.transition.outcome == pgInPlace {
			sentences = append(sentences, fmt.Sprintf("ALTER COLUMN %s.%s TYPE %s %s", change.site.table.name, change.site.column, change.updated.spell(), change.transition.why))
		}
	}
	others := 0
	for _, i := range clauseStarts(w) {
		switch {
		case alterColumnTypeClause(w, i):
		case pgCatalogEditClause(w, i):
			others++
		default:
			return "", false
		}
	}
	if changes == 0 || changes+others != len(clauseStarts(w)) {
		return "", false
	}
	message := "PostgreSQL applies the statement as a catalog edit: no table rewrite, no index rebuild, and no scan of the rows"
	if len(sentences) > 0 {
		message = strings.Join(sentences, "; ") + "; " + message
	} else {
		message = fmt.Sprintf("ALTER TABLE %s restates its column types; ", alterTableReference(w, stmt.sourceWords).name) + message
	}
	if others > 0 {
		message += fmt.Sprintf(", the %d SET DEFAULT, DROP DEFAULT or DROP NOT NULL clause%s in the same statement included", others, plural(others))
	}
	return message, true
}

// pgCatalogEditClause recognizes ALTER [COLUMN] name SET DEFAULT ..., DROP
// DEFAULT and DROP NOT NULL.
func pgCatalogEditClause(w []string, i int) bool {
	if i >= len(w) || w[i] != "ALTER" {
		return false
	}
	j := i + 1
	if j < len(w) && w[j] == "COLUMN" {
		j++
	}
	j = skipIfExists(w, j)
	if j >= len(w) || !identLike(w[j]) {
		return false
	}
	rest := w[j+1 : clauseEnd(w, i)]
	return hasWordPrefix(rest, "SET", "DEFAULT") || hasWordPrefix(rest, "DROP", "DEFAULT") || hasWordPrefix(rest, "DROP", "NOT", "NULL")
}
