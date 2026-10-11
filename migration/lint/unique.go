package lint

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/internal/lintkit"
)

// A unique index is built over the rows a table already holds, and the
// build fails on the first duplicate it meets. Nothing in the migration says
// whether the column is unique today, so the two rules in this file report
// the structural risk for the two shapes Atlas names -- a unique index added
// to an existing table (MF101) and an index that is dropped and rebuilt as
// unique (MF102) -- and hand the operator the query that settles it before
// the version runs. The missing-down and empty-file rules that carried these
// two identifiers before the convention are MF101P and MF102P now, and a
// selector that names a code selects that code alone, so a config naming
// MF101 reaches the check and not the file-form rule ([SelectorSelects]).
//
// Measured on PostgreSQL 18.6 and on MySQL 8.4.11 and MariaDB 11.8.9
// (stokaro/ptah#2942):
//
//	CREATE UNIQUE INDEX over a duplicate      PostgreSQL: could not create unique index "t_a"
//	                                          MySQL, MariaDB: Duplicate entry '1' for key 'u1'
//	ADD CONSTRAINT ... UNIQUE over a duplicate PostgreSQL: the same error
//	CREATE UNIQUE INDEX CONCURRENTLY, failed  PostgreSQL: the same error, and pg_index keeps
//	                                          the index with indisvalid = false until it is dropped
//	two NULLs under a unique index            pass on every engine; PostgreSQL fails them
//	                                          only under NULLS NOT DISTINCT
//	ADD UNIQUE USING INDEX over a plain index PostgreSQL refuses: "t_idx" is not a unique index
//
// Two facts the file knows without a database: a table this migration
// creates holds no rows when the index is built, and a column this migration
// adds holds NULL in every existing row unless the clause gives it a DEFAULT
// -- in which case every row holds the same value and the build fails on the
// second row. Both are read from the earlier statements of the same file.
//
// What the file does not know is the data. The dev database a run replays
// the directory on holds the schema and nothing else, so a proven violation
// is not claimed; the message carries the GROUP BY that proves or clears it.
// The schema does settle two cases the text cannot (stokaro/ptah#2957): a
// unique index or primary key already covering the key's columns, or a
// subset of them, proves the rows unique, so the build cannot fail and
// neither rule fires; and an index the file drops by name is known by its
// columns, so a unique one rebuilt over the same columns under a new name
// is the replacement MF102 describes. Both read the state through
// [InputBaselineRefinement]: the rules report from the text alone and the
// state refines the report, and a run without it is named as unmet.

// uniqueIndexStatements lists the statements that build a unique index, for
// the baseline request.
func uniqueIndexStatements(file *File) []int {
	var indexes []int
	seen := -1
	for _, site := range lintkit.UniqueIndexSites(file.contract, file.dialect) {
		if site.Statement == seen {
			continue
		}
		seen = site.Statement
		indexes = append(indexes, site.Statement)
	}
	return indexes
}

// droppedIndexNames lists the indexes and constraints the statements before
// index drop, plus those the statement at index drops in an earlier clause,
// normalized for comparison.
func droppedIndexNames(file *File, index int) map[string]bool {
	dropped := make(map[string]bool)
	for i := 0; i <= index && i < len(file.Statements); i++ {
		w := file.Statements[i].Words
		switch {
		case lintkit.HasWordPrefix(w, "DROP", "INDEX"):
			j := 2
			if j < len(w) && w[j] == "CONCURRENTLY" {
				j++
			}
			j = lintkit.SkipIfExists(w, j)
			for ; j < len(w); j++ {
				if lintkit.IdentLike(w[j]) {
					dropped[lintkit.NormalizeIdent(lintkit.LastComponent(w[j]))] = true
				}
			}
		case lintkit.IsAlterTable(w):
			for _, j := range lintkit.ClauseStarts(w) {
				if lintkit.HasWordPrefix(w[j:], "DROP", "INDEX") || lintkit.HasWordPrefix(w[j:], "DROP", "KEY") || lintkit.HasWordPrefix(w[j:], "DROP", "CONSTRAINT") {
					k := lintkit.SkipIfExists(w, j+2)
					if k < len(w) && lintkit.IdentLike(w[k]) {
						dropped[lintkit.NormalizeIdent(w[k])] = true
					}
				}
			}
		}
	}
	return dropped
}

// addedColumn is a column an earlier statement of the file added, with
// whether the clause gave it a DEFAULT.
type addedColumn struct {
	hasDefault bool
}

// columnsAddedBefore lists the columns added to table by the statements
// before index, keyed by normalized name.
func columnsAddedBefore(file *File, index int, table lintkit.TableReference) map[string]addedColumn {
	added := make(map[string]addedColumn)
	for i := 0; i < index && i < len(file.Statements); i++ {
		stmt := &file.Statements[i]
		w := stmt.Words
		if !lintkit.IsAlterTable(w) || lintkit.AlterTableReference(w, stmt.sourceWords).Normalized != table.Normalized {
			continue
		}
		for _, j := range lintkit.ClauseStarts(w) {
			start, end, ok := lintkit.AddColumnClause(w, j)
			if !ok {
				continue
			}
			clause := w[start:end]
			added[lintkit.NormalizeIdent(w[start-1])] = addedColumn{
				hasDefault: slices.Contains(clause, "DEFAULT") || slices.Contains(clause, "GENERATED"),
			}
		}
	}
	return added
}

// tablesCreatedBefore lists the tables the statements before index create.
func tablesCreatedBefore(file *File, index int) map[string]bool {
	created := make(map[string]bool)
	for i := 0; i < index && i < len(file.Statements); i++ {
		if ref := lintkit.CreatedTableRef(file.Statements[i].Words); ref != "" {
			created[ref] = true
		}
	}
	return created
}

// uniqueRisk is what the file knows about the rows a unique index meets.
type uniqueRisk int

const (
	// uniqueNoRows: the table is created in this file, so the index meets
	// no rows.
	uniqueNoRows uniqueRisk = iota
	// uniqueAllNull: every key column is added in this file without a
	// DEFAULT, so every existing row holds NULL, which does not collide.
	uniqueAllNull
	// uniqueSameDefault: a key column is added in this file with a DEFAULT,
	// so every existing row holds the same value.
	uniqueSameDefault
	// uniqueUnknown: the rows are whatever the table holds.
	uniqueUnknown
	// uniqueProven: a unique index or primary key in the state the version
	// starts from already covers the key's columns or a subset of them, so
	// no two rows can collide on the new key.
	uniqueProven
)

func classifyUniqueRisk(file *File, site lintkit.UniqueIndexSite) uniqueRisk {
	if lintkit.RefersToCreated(tablesCreatedBefore(file, site.Statement), site.Table.Normalized) {
		return uniqueNoRows
	}
	if provenUnique(file, site) {
		return uniqueProven
	}
	added := columnsAddedBefore(file, site.Statement, site.Table)
	allNull := !site.NullsNotDistinct
	for _, column := range site.Columns {
		entry, ok := added[lintkit.NormalizeIdent(column)]
		switch {
		case !ok:
			allNull = false
		case entry.hasDefault:
			return uniqueSameDefault
		}
	}
	if allNull && len(site.Columns) > 0 {
		return uniqueAllNull
	}
	return uniqueUnknown
}

// provenUnique reports whether the baseline holds a unique index or primary
// key over whole columns that are all among the new key's, which proves the
// new key cannot meet a duplicate. Under NULLS NOT DISTINCT two NULLs
// collide where the existing index let them pass, so the proof then needs
// every covering column NOT NULL as well.
func provenUnique(file *File, site lintkit.UniqueIndexSite) bool {
	columns := make([]string, 0, len(site.Columns))
	for _, column := range site.Columns {
		columns = append(columns, lintkit.NormalizeIdent(column))
	}
	covering, ok := file.baseline.UniqueCovering(site.Table.Normalized, columns)
	if !ok {
		return false
	}
	if !site.NullsNotDistinct {
		return true
	}
	for _, name := range lintkit.WholeKeyColumns(covering) {
		column, known := file.baseline.Column(site.Table.Normalized, name)
		if !known || !column.NotNull {
			return false
		}
	}
	return true
}

// replacedIndex names the index a unique build replaces: one the file
// dropped earlier under the same name, or one it dropped whose columns, as
// the baseline records them, are exactly the new key's. The second result
// reports the new-name form, for the message.
func replacedIndex(file *File, site lintkit.UniqueIndexSite) (name string, renamed, ok bool) {
	dropped := droppedIndexNames(file, site.Statement)
	if site.Name != "" && dropped[lintkit.NormalizeIdent(lintkit.LastComponent(site.Name))] {
		return site.Name, false, true
	}
	columns := make([]string, 0, len(site.Columns))
	for _, column := range site.Columns {
		columns = append(columns, lintkit.NormalizeIdent(column))
	}
	for _, droppedName := range slices.Sorted(maps.Keys(dropped)) {
		idx, found := file.baseline.IndexNamed(site.Table.Normalized, droppedName)
		if !found || !lintkit.SameColumnSet(lintkit.WholeKeyColumns(idx), columns) {
			continue
		}
		return idx.Name, true, true
	}
	return "", false, false
}

// duplicateQuery is the statement that proves or clears the risk.
func duplicateQuery(site lintkit.UniqueIndexSite) string {
	columns := strings.Join(site.Columns, ", ")
	return fmt.Sprintf("SELECT %s, COUNT(*) FROM %s GROUP BY %s HAVING COUNT(*) > 1", columns, site.Table.Name, columns)
}

// uniqueFailure is the failure both families report, and what it leaves.
//
// YDB words it its own way: measured on 26.2.1.14 with EnableAddUniqueIndex
// on, ADD INDEX ... GLOBAL UNIQUE SYNC over two rows holding the same value
// answers `Duplicate key found: (v=a)`.
func uniqueFailure(site lintkit.UniqueIndexSite) string {
	if site.YQL {
		return "fails on the first duplicate it meets (YDB: Duplicate key found), leaving the migration half applied"
	}
	message := "fails on the first duplicate it meets (PostgreSQL: could not create unique index; MySQL and MariaDB: Duplicate entry for key), " +
		"leaving the migration half applied"
	if site.Concurrently {
		message += ", and a CONCURRENTLY build that fails leaves an invalid index of that name behind, which must be dropped before the retry"
	}
	return message
}

// uniqueAdvice says what to do about it, given what the file knows.
func uniqueAdvice(site lintkit.UniqueIndexSite, risk uniqueRisk) string {
	switch risk {
	case uniqueSameDefault:
		return fmt.Sprintf("Every existing row holds the DEFAULT this migration gives the column, so the build fails as soon as two rows exist. "+
			"Add the column without a DEFAULT, backfill distinct values, then build the index; %s shows what is left", duplicateQuery(site))
	default:
		advice := fmt.Sprintf("Nothing in the migration says whether %s is unique today. Check first with %s and deduplicate before this version runs",
			strings.Join(site.Columns, ", "), duplicateQuery(site))
		if !site.NullsNotDistinct {
			advice += "; rows where every key column is NULL do not collide"
		}
		return advice
	}
}

// uniqueRules is the family: the two shapes Atlas names, on every dialect.
func uniqueRules() []Rule {
	return []Rule{uniqueIndexAddedRule(), indexMadeUniqueRule()}
}

// riskIsSettled reports the classifications under which no unique build
// can fail, so neither rule reports it.
func riskIsSettled(risk uniqueRisk) bool {
	return risk == uniqueNoRows || risk == uniqueAllNull || risk == uniqueProven
}

func uniqueIndexAddedRule() Rule {
	return Rule{
		Code:             "MF101",
		Title:            "unique index over existing rows may fail",
		Severity:         SeverityWarning,
		Input:            InputBaselineRefinement,
		BaselineSubjects: uniqueIndexStatements,
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			var findings []Finding
			for _, site := range lintkit.UniqueIndexSites(file.contract, file.dialect) {
				risk := classifyUniqueRisk(file, site)
				if riskIsSettled(risk) {
					continue
				}
				message := fmt.Sprintf("%s builds over the rows %s already holds and %s. %s",
					site.Label(), site.Table.Name, uniqueFailure(site), uniqueAdvice(site, risk))
				findings = append(findings, uniqueFinding(file, site, "MF101", "unique index over existing rows may fail", message))
			}
			return findings
		},
	}
}

func indexMadeUniqueRule() Rule {
	return Rule{
		Code:             "MF102",
		Title:            "index rebuilt as unique may fail",
		Severity:         SeverityWarning,
		Subsumes:         []string{"MF101"},
		Input:            InputBaselineRefinement,
		BaselineSubjects: uniqueIndexStatements,
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			var findings []Finding
			for _, site := range lintkit.UniqueIndexSites(file.contract, file.dialect) {
				replaced, renamed, ok := replacedIndex(file, site)
				if !ok {
					continue
				}
				risk := classifyUniqueRisk(file, site)
				if riskIsSettled(risk) {
					continue
				}
				replacement := " with a unique one"
				if renamed {
					replacement = ", which covered the same columns, with a unique one under a new name"
				}
				message := fmt.Sprintf("%s replaces the index %s dropped earlier%s over the rows %s already holds: the drop succeeds and the unique build then %s, "+
					"which also leaves the table without the index it had. %s. On PostgreSQL build the unique index CONCURRENTLY under a new name first "+
					"and drop the old one only once it exists, so a failure leaves the old index in place",
					site.Label(), replaced, replacement, site.Table.Name, uniqueFailure(site), uniqueAdvice(site, risk))
				findings = append(findings, uniqueFinding(file, site, "MF102", "index rebuilt as unique may fail", message))
			}
			return findings
		},
	}
}

func uniqueFinding(file *File, site lintkit.UniqueIndexSite, code, title, message string) Finding {
	stmt := &file.Statements[site.Statement]
	subjects := make([]Subject, 0, len(site.Columns))
	for _, column := range site.Columns {
		subjects = append(subjects, Subject{Kind: SubjectColumn, Name: column, Parent: site.Table.Name})
	}
	return Finding{
		Rule:     code,
		Title:    title,
		Severity: SeverityWarning,
		File:     file.Path,
		Line:     stmt.Line,
		Message:  message,
		Context:  statementFindingContext(site.Statement, subjects...),
	}
}
