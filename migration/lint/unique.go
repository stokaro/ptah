package lint

import (
	"fmt"
	"slices"
	"strings"
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
// Two things the file knows without a database: a table this migration
// creates holds no rows when the index is built, and a column this migration
// adds holds NULL in every existing row unless the clause gives it a DEFAULT
// -- in which case every row holds the same value and the build fails on the
// second row. Both are read from the earlier statements of the same file.
//
// What the file does not know is the data. The dev database a run replays
// the directory on holds the schema and nothing else, so a proven violation
// is not claimed; the message carries the GROUP BY that proves or clears it.

// uniqueIndexSite is one unique index or constraint a statement builds.
type uniqueIndexSite struct {
	statement int
	// name is the index or constraint name as written, empty when the
	// server picks one.
	name string
	// table is the indexed table and columns its key columns, as written.
	table   tableReference
	columns []string
	// concurrently and nullsNotDistinct are the PostgreSQL modifiers that
	// change what a failure leaves behind and what counts as a duplicate.
	concurrently     bool
	nullsNotDistinct bool
	// spelled is the clause head for the message: CREATE UNIQUE INDEX or
	// ADD UNIQUE.
	spelled string
}

// uniqueIndexSites finds every unique index a file builds, in file order.
func uniqueIndexSites(file *File) []uniqueIndexSite {
	var sites []uniqueIndexSite
	for index := range file.Statements {
		stmt := &file.Statements[index]
		w := stmt.Words
		switch {
		case isCreateIndex(w) && len(w) > 1 && w[1] == "UNIQUE":
			if site, ok := createUniqueIndexSite(w, stmt.sourceWords); ok {
				site.statement = index
				sites = append(sites, site)
			}
		case isAlterTable(w):
			table := alterTableReference(w, stmt.sourceWords)
			for _, i := range clauseStarts(w) {
				site, ok := addUniqueSite(w, stmt.sourceWords, i)
				if !ok {
					continue
				}
				site.statement = index
				site.table = table
				sites = append(sites, site)
			}
		}
	}
	return sites
}

// createUniqueIndexSite reads CREATE UNIQUE INDEX [CONCURRENTLY] [IF NOT
// EXISTS] [name] ON [ONLY] table [USING method] (columns) [NULLS NOT DISTINCT].
func createUniqueIndexSite(w, sourceWords []string) (uniqueIndexSite, bool) {
	site := uniqueIndexSite{spelled: "CREATE UNIQUE INDEX"}
	j := 3
	if j < len(w) && w[j] == "CONCURRENTLY" {
		site.concurrently = true
		j++
	}
	j = skipIfNotExists(w, j)
	if j < len(w) && w[j] != "ON" && identLike(w[j]) {
		site.name = sourceWordAt(w, sourceWords, j)
		j++
	}
	if j >= len(w) || w[j] != "ON" {
		return site, false
	}
	j++
	if j < len(w) && w[j] == "ONLY" {
		j++
	}
	table, next := tableRefAt(w, sourceWords, j)
	if table.normalized == "" {
		return site, false
	}
	site.table = table
	j = next
	if j+1 < len(w) && w[j] == "USING" {
		j += 2
	}
	columns, next, ok := keyColumnList(w, sourceWords, j)
	if !ok {
		return site, false
	}
	site.columns = columns
	site.nullsNotDistinct = hasWordSeq(w[next:], "NULLS", "NOT", "DISTINCT")
	return site, true
}

// addUniqueSite reads ADD [CONSTRAINT name] UNIQUE [INDEX|KEY] [name]
// [USING method] (columns) at a clause start.
func addUniqueSite(w, sourceWords []string, i int) (uniqueIndexSite, bool) {
	end := clauseEnd(w, i)
	site := uniqueIndexSite{spelled: "ADD UNIQUE"}
	j := i
	if j >= end || w[j] != "ADD" {
		return site, false
	}
	j++
	if j+1 < end && w[j] == "CONSTRAINT" && identLike(w[j+1]) {
		site.name = sourceWordAt(w, sourceWords, j+1)
		j += 2
	}
	if j >= end || w[j] != "UNIQUE" {
		return site, false
	}
	j++
	if j < end && (w[j] == "INDEX" || w[j] == "KEY") {
		j++
	}
	name, j := uniqueNameAndMethod(w[:end], sourceWords, j)
	if name != "" {
		site.name = name
	}
	columns, next, ok := keyColumnList(w[:end], sourceWords, j)
	if !ok {
		return site, false
	}
	site.columns = columns
	site.nullsNotDistinct = hasWordSeq(w[next:end], "NULLS", "NOT", "DISTINCT")
	return site, true
}

// uniqueNameAndMethod reads the optional index name and USING method between
// UNIQUE [INDEX|KEY] and the column list. The PostgreSQL USING INDEX form
// attaches an index that already exists: it names no column list, so the
// caller's column-list requirement is what keeps it from being a site.
func uniqueNameAndMethod(w, sourceWords []string, j int) (name string, next int) {
	if j < len(w) && w[j] != "(" && w[j] != "USING" && identLike(w[j]) {
		name = sourceWordAt(w, sourceWords, j)
		j++
	}
	if j+1 < len(w) && w[j] == "USING" {
		j += 2
	}
	return name, j
}

// keyColumnList reads ( a, b (10), c DESC, (expr) ) and returns the column
// names it names, in order, and the index after the closing parenthesis.
// A key part that is an expression contributes no name.
func keyColumnList(w, sourceWords []string, j int) ([]string, int, bool) {
	if j >= len(w) || w[j] != "(" {
		return nil, j, false
	}
	var columns []string
	depth := 0
	partStart := true
	for k := j; k < len(w); k++ {
		switch w[k] {
		case "(":
			depth++
			partStart = depth == 1
			continue
		case ")":
			depth--
			if depth == 0 {
				return columns, k + 1, len(columns) > 0
			}
		case ",":
			if depth == 1 {
				partStart = true
			}
		default:
			if depth == 1 && partStart && identLike(w[k]) {
				columns = append(columns, sourceWordAt(w, sourceWords, k))
			}
			partStart = false
		}
	}
	return nil, j, false
}

// droppedIndexNames lists the indexes and constraints the statements before
// index drop, plus those the statement at index drops in an earlier clause,
// normalized for comparison.
func droppedIndexNames(file *File, index int) map[string]bool {
	dropped := make(map[string]bool)
	for i := 0; i <= index && i < len(file.Statements); i++ {
		w := file.Statements[i].Words
		switch {
		case hasWordPrefix(w, "DROP", "INDEX"):
			j := 2
			if j < len(w) && w[j] == "CONCURRENTLY" {
				j++
			}
			j = skipIfExists(w, j)
			for ; j < len(w); j++ {
				if identLike(w[j]) {
					dropped[normalizeIdent(lastComponent(w[j]))] = true
				}
			}
		case isAlterTable(w):
			for _, j := range clauseStarts(w) {
				if hasWordPrefix(w[j:], "DROP", "INDEX") || hasWordPrefix(w[j:], "DROP", "KEY") || hasWordPrefix(w[j:], "DROP", "CONSTRAINT") {
					k := skipIfExists(w, j+2)
					if k < len(w) && identLike(w[k]) {
						dropped[normalizeIdent(w[k])] = true
					}
				}
			}
		}
	}
	return dropped
}

func lastComponent(word string) string {
	return word[strings.LastIndex(word, ".")+1:]
}

// addedColumn is a column an earlier statement of the file added, with
// whether the clause gave it a DEFAULT.
type addedColumn struct {
	hasDefault bool
}

// columnsAddedBefore lists the columns added to table by the statements
// before index, keyed by normalized name.
func columnsAddedBefore(file *File, index int, table tableReference) map[string]addedColumn {
	added := make(map[string]addedColumn)
	for i := 0; i < index && i < len(file.Statements); i++ {
		stmt := &file.Statements[i]
		w := stmt.Words
		if !isAlterTable(w) || alterTableReference(w, stmt.sourceWords).normalized != table.normalized {
			continue
		}
		for _, j := range clauseStarts(w) {
			start, end, ok := addColumnClause(w, j)
			if !ok {
				continue
			}
			clause := w[start:end]
			added[normalizeIdent(w[start-1])] = addedColumn{
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
		if ref := createdTableRef(file.Statements[i].Words); ref != "" {
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
)

func classifyUniqueRisk(file *File, site uniqueIndexSite) uniqueRisk {
	if refersToCreated(tablesCreatedBefore(file, site.statement), site.table.normalized) {
		return uniqueNoRows
	}
	added := columnsAddedBefore(file, site.statement, site.table)
	allNull := !site.nullsNotDistinct
	for _, column := range site.columns {
		entry, ok := added[normalizeIdent(column)]
		switch {
		case !ok:
			allNull = false
		case entry.hasDefault:
			return uniqueSameDefault
		}
	}
	if allNull && len(site.columns) > 0 {
		return uniqueAllNull
	}
	return uniqueUnknown
}

// duplicateQuery is the statement that proves or clears the risk.
func duplicateQuery(site uniqueIndexSite) string {
	columns := strings.Join(site.columns, ", ")
	return fmt.Sprintf("SELECT %s, COUNT(*) FROM %s GROUP BY %s HAVING COUNT(*) > 1", columns, site.table.name, columns)
}

func (s uniqueIndexSite) label() string {
	if s.name != "" {
		return s.spelled + " " + s.name
	}
	return s.spelled
}

// uniqueFailure is the failure both families report, and what it leaves.
func uniqueFailure(site uniqueIndexSite) string {
	message := "fails on the first duplicate it meets (PostgreSQL: could not create unique index; MySQL and MariaDB: Duplicate entry for key), " +
		"leaving the migration half applied"
	if site.concurrently {
		message += ", and a CONCURRENTLY build that fails leaves an invalid index of that name behind, which must be dropped before the retry"
	}
	return message
}

// uniqueAdvice says what to do about it, given what the file knows.
func uniqueAdvice(site uniqueIndexSite, risk uniqueRisk) string {
	switch risk {
	case uniqueSameDefault:
		return fmt.Sprintf("Every existing row holds the DEFAULT this migration gives the column, so the build fails as soon as two rows exist. "+
			"Add the column without a DEFAULT, backfill distinct values, then build the index; %s shows what is left", duplicateQuery(site))
	default:
		advice := fmt.Sprintf("Nothing in the migration says whether %s is unique today. Check first with %s and deduplicate before this version runs",
			strings.Join(site.columns, ", "), duplicateQuery(site))
		if !site.nullsNotDistinct {
			advice += "; rows where every key column is NULL do not collide"
		}
		return advice
	}
}

// uniqueRules is the family: the two shapes Atlas names, on every dialect.
func uniqueRules() []Rule {
	return []Rule{uniqueIndexAddedRule(), indexMadeUniqueRule()}
}

func uniqueIndexAddedRule() Rule {
	return Rule{
		Code:     "MF101",
		Title:    "unique index over existing rows may fail",
		Severity: SeverityWarning,
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			var findings []Finding
			for _, site := range uniqueIndexSites(file) {
				risk := classifyUniqueRisk(file, site)
				if risk == uniqueNoRows || risk == uniqueAllNull {
					continue
				}
				message := fmt.Sprintf("%s builds over the rows %s already holds and %s. %s",
					site.label(), site.table.name, uniqueFailure(site), uniqueAdvice(site, risk))
				findings = append(findings, uniqueFinding(file, site, "MF101", "unique index over existing rows may fail", message))
			}
			return findings
		},
	}
}

func indexMadeUniqueRule() Rule {
	return Rule{
		Code:     "MF102",
		Title:    "index rebuilt as unique may fail",
		Severity: SeverityWarning,
		Subsumes: []string{"MF101"},
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			var findings []Finding
			for _, site := range uniqueIndexSites(file) {
				if site.name == "" || !droppedIndexNames(file, site.statement)[normalizeIdent(lastComponent(site.name))] {
					continue
				}
				risk := classifyUniqueRisk(file, site)
				if risk == uniqueNoRows || risk == uniqueAllNull {
					continue
				}
				message := fmt.Sprintf("%s replaces the index %s dropped earlier with a unique one over the rows %s already holds: the drop succeeds and the unique build then %s, "+
					"which also leaves the table without the index it had. %s. On PostgreSQL build the unique index CONCURRENTLY under a new name first "+
					"and drop the old one only once it exists, so a failure leaves the old index in place",
					site.label(), site.name, site.table.name, uniqueFailure(site), uniqueAdvice(site, risk))
				findings = append(findings, uniqueFinding(file, site, "MF102", "index rebuilt as unique may fail", message))
			}
			return findings
		},
	}
}

func uniqueFinding(file *File, site uniqueIndexSite, code, title, message string) Finding {
	stmt := &file.Statements[site.statement]
	subjects := make([]Subject, 0, len(site.columns))
	for _, column := range site.columns {
		subjects = append(subjects, Subject{Kind: SubjectColumn, Name: column, Parent: site.table.name})
	}
	return Finding{
		Rule:     code,
		Title:    title,
		Severity: SeverityWarning,
		File:     file.Path,
		Line:     stmt.Line,
		Message:  message,
		Context:  statementFindingContext(site.statement, subjects...),
	}
}
