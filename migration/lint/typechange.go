package lint

import (
	"slices"

	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintkit/mytype"
	"ptah.run/internal/lintkit/pgtype"
)

// DS103 reads the statement text: every MODIFY, CHANGE and ALTER COLUMN ...
// TYPE is a column type change to it, and the finding says the change can
// truncate or reject existing values. On the MySQL family a MODIFY restates
// the whole column, so `MODIFY total INT NOT NULL` on an INT column changes
// the nullability and not the type, and the finding is wrong about what the
// statement did (stokaro/ptah#2959).
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
		if lintkit.ScanModifyChange(stmt.Words) || scanAlterColumnType(stmt.Words) {
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
	switch {
	case file.dialect == "postgres":
		return pgTypesUnchanged(file, index, clauses)
	case slices.Contains(lintkit.MySQLFamily, file.dialect):
		return mysqlTypesUnchanged(file, index, clauses)
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
		if modifyChangeClause(w, i) || lintkit.AlterColumnTypeClause(w, i) {
			count++
		}
	}
	return count
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
