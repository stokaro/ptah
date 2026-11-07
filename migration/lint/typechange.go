package lint

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintkit/mytype"
	"ptah.run/internal/lintkit/pgtype"
)

// DS103 reads the statement text: every MODIFY, CHANGE, ALTER COLUMN ... TYPE and
// SQL Server's ALTER COLUMN name type is a column type change to it, and the finding
// says the change can truncate or reject existing values. On the MySQL family a MODIFY
// restates the whole column, so `MODIFY total INT NOT NULL` on an INT column changes
// the nullability and not the type, and the finding is wrong about what the statement
// did (stokaro/ptah#2959). SQL Server's clause restates the type the same way.
//
// The schema state the version starts from refines that. A statement whose
// every type clause names the type the column already has, as the same
// comparison MY130 and PG301 make judges it, changed no type, and DS103 says
// nothing about it; DD103 reports the nullability on its own terms. A
// collation is not a type: a clause that changes only the collation leaves
// the type unchanged here, and PG301 or MY130 report the rebuild or the copy
// where one is measured. Anything the state cannot settle -- a column it
// does not know, a spelling the parser does not, a clause the site readers
// do not recognize, a dialect with no type comparison -- keeps the finding,
// so the refinement can only take away a finding it has established to be
// wrong. Without the state the rule reports exactly as it did before the
// state existed, and the run names it as unmet.

// typeChangeStatements lists the statements DS103 reads, for the baseline
// request and for the check.
func typeChangeStatements(file *File) []int {
	var indexes []int
	for index := range file.Statements {
		stmt := &file.Statements[index]
		if !lintkit.IsAlterTable(stmt.Words) {
			continue
		}
		if lintkit.ScanModifyChange(stmt.Words) || scanAlterColumnType(stmt.Words) || scanRestatedColumn(stmt.Words) {
			indexes = append(indexes, index)
		}
	}
	return indexes
}

// typeUnchangedByState reports that the state the version starts from
// establishes every type clause of the statement as a restatement of the
// column's current type.
func typeUnchangedByState(file *File, index int) bool {
	if file.baseline.Empty() {
		return false
	}
	stmt := &file.Statements[index]
	clauses := typeChangeClauses(stmt.Words)
	described, _ := platform.Lookup(file.dialect)
	switch described.Lint.TypeRestatements {
	case "pgtype":
		return pgTypesUnchanged(file, index, clauses)
	case "mytype":
		return mysqlTypesUnchanged(file, index, clauses)
	case "restated":
		return restatedTypesUnchanged(file, index, clauses)
	default:
		return false
	}
}

// typeChangeClauses counts the clauses DS103's scanners match in one
// statement, so a clause the site readers did not turn into a site is
// noticed rather than silently taken as unchanged.
func typeChangeClauses(w []string) int {
	count := 0
	for _, i := range lintkit.ClauseStarts(w) {
		_, _, restated := lintkit.RestatedColumnClause(w, w, i)
		if modifyChangeClause(w, i) || lintkit.AlterColumnTypeClause(w, i) || restated {
			count++
		}
	}
	return count
}

// scanRestatedColumn reports SQL Server's ALTER COLUMN name type.
func scanRestatedColumn(w []string) bool {
	return slices.ContainsFunc(lintkit.ClauseStarts(w), func(i int) bool {
		_, _, restated := lintkit.RestatedColumnClause(w, w, i)
		return restated
	})
}

func modifyChangeClause(w []string, i int) bool {
	if i >= len(w) || (w[i] != "MODIFY" && w[i] != "CHANGE") {
		return false
	}
	j := i + 1
	if j < len(w) && w[j] == "COLUMN" {
		j++
	}
	j = lintkit.SkipIfExists(w, j)
	return j < len(w) && lintkit.IdentLike(w[j])
}

// pgTypesUnchanged judges every ALTER COLUMN ... TYPE clause of one
// statement against the column's current type, the way PG301 does.
func pgTypesUnchanged(file *File, index, clauses int) bool {
	resolved := 0
	for _, site := range pgtype.ChangeSites(file.contract) {
		if site.Statement != index {
			continue
		}
		column, ok := file.baseline.Column(site.Table.Normalized, lintkit.NormalizeIdent(site.Column))
		if !ok {
			return false
		}
		old, ok := pgtype.ParseSpelling(column.ColumnType)
		if !ok {
			return false
		}
		updated, ok := pgtype.ParseAt(file.Statements[index].Words, site.TypeStart, site.TypeEnd)
		if !ok || pgtype.Compare(old, updated).Outcome != pgtype.Unchanged {
			return false
		}
		resolved++
	}
	return resolved > 0 && resolved == clauses
}

// mysqlTypesUnchanged judges every MODIFY and CHANGE clause of one statement
// against the column's current storage type, the way MY130 does: the same
// base, parameters, sign, character set and member list. The collation is
// not compared, because it is not a type.
func mysqlTypesUnchanged(file *File, index, clauses int) bool {
	resolved := 0
	for _, site := range mytype.ChangeSites(file.contract) {
		if site.Statement != index {
			continue
		}
		column, ok := file.baseline.Column(site.Table.Normalized, lintkit.NormalizeIdent(site.OldName))
		if !ok {
			return false
		}
		old, ok := mytype.ParseSpelling(column.ColumnType, column.Charset)
		if !ok {
			return false
		}
		tableCharset := mytype.TableCharsetBefore(file.contract, index, site.Table, column.TableCharset)
		updated, _, ok := mytype.ParseAt(file.Statements[index].Words, site.TypeStart, tableCharset)
		if !ok || old.Spell() != updated.Spell() || old.Charset != updated.Charset {
			return false
		}
		resolved++
	}
	return resolved > 0 && resolved == clauses
}

// restatedTypesUnchanged judges SQL Server's ALTER COLUMN clause against the column's
// current type. The clause has to name a type to change a nullability, so `ALTER COLUMN
// total int NOT NULL` over an int changes no type. The two are compared as written, with
// case and space folded, since a spelling they do not share leaves the finding standing.
func restatedTypesUnchanged(file *File, index, clauses int) bool {
	stmt := &file.Statements[index]
	w := stmt.Words
	table := lintkit.AlterTableReference(w, stmt.sourceWords)
	resolved := 0
	for _, i := range lintkit.ClauseStarts(w) {
		name, typeStart, ok := lintkit.RestatedColumnClause(w, stmt.sourceWords, i)
		if !ok {
			continue
		}
		column, known := file.baseline.Column(table.Normalized, name.Normalized)
		if !known || !strings.EqualFold(restatedType(w[typeStart:lintkit.ClauseEnd(w, i)]), foldedSpaces(column.ColumnType)) {
			return false
		}
		resolved++
	}
	return resolved > 0 && resolved == clauses
}

// restatedType is the type a clause names, up to the words that say something else about
// the column.
func restatedType(words []string) string {
	end := slices.IndexFunc(words, func(word string) bool {
		return word == "NULL" || word == "NOT" || word == "COLLATE" || word == "SPARSE"
	})
	if end < 0 {
		end = len(words)
	}
	return strings.Join(words[:end], "")
}

func foldedSpaces(spelling string) string {
	return strings.Join(strings.Fields(spelling), "")
}
