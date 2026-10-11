package pglint

import (
	"fmt"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintkit/pgtype"
)

// PostgreSQL applies an ALTER COLUMN ... TYPE in one of three ways: as a
// catalog edit, when it can prove the stored bytes are already valid for the
// new type; as a scan that rebuilds every index on the column while the heap
// stays; or as a rewrite of the whole table and every index on it, under
// the ACCESS EXCLUSIVE lock the statement holds throughout. Adding a primary
// key has its own scan question: the constraint sets its columns NOT NULL,
// and a column that is not already NOT NULL is checked row by row on top of
// the index build. The two rules in this file name those costs for the two
// Atlas codes that report them, PG301 and PG304, and fire only where the
// cost is established from the column's current type or nullability.
//
// Every claim below was measured on PostgreSQL 18.6 against a table of a
// thousand rows carrying an index on the column, by comparing the relation's
// and the index's relfilenode before and after the statement and reading
// the heap-scan counter (stokaro/ptah#2942):
//
//	ALTER COLUMN ... TYPE                          table      index      scans
//	integer -> bigint, smallint, numeric, text     rewritten  rebuilt    3
//	uuid -> text, json -> jsonb, USING a * 2       rewritten  rebuilt    3
//	varchar(10) -> varchar(20), varchar, text      kept       kept       0
//	varchar(20) -> varchar(10)                     rewritten  rebuilt    3
//	varchar(20) -> varchar(2), a value too long    aborted: value too long for type
//	text -> varchar                                kept       kept       0
//	text -> varchar(10), char(10) -> char(20)      rewritten  rebuilt    3
//	numeric(10,2) -> numeric(12,2), numeric        kept       kept       0
//	numeric(10,2) -> numeric(10,3), numeric(10)    rewritten  rebuilt    3
//	numeric(12,2) -> numeric(10,2)                 rewritten  rebuilt    3
//	numeric -> numeric(10,2)                       rewritten  rebuilt    3
//	timestamp(3) -> timestamp(6), timestamp        kept       kept       0
//	timestamp(6) -> timestamp(3)                   rewritten  rebuilt    3
//	timestamp -> timestamptz, TimeZone=UTC         kept       rebuilt    1
//	timestamp -> timestamptz, TimeZone=Prague      rewritten  rebuilt    3
//	interval -> interval(3)                        rewritten  rebuilt    3
//	bit varying(8) -> bit varying(16)              kept       kept       0
//	bit(8) -> bit(16)                              aborted: bit string length 8 does not match
//	text -> text COLLATE "C"                       kept       rebuilt    1
//	  (with no index on the column)                kept       -          0
//	integer[] -> bigint[], varchar(10)[] -> (20)[] rewritten  rebuilt    3
//	integer -> smallint, a value out of range      aborted: smallint out of range
//	same type, or SET DATA TYPE spelling           kept       kept       0
//
//	ADD PRIMARY KEY                                            scans
//	(a), a nullable                                            2
//	(a), a NOT NULL                                            1
//	(a, id), one of them nullable                              2
//	USING INDEX, a nullable                                    1
//	USING INDEX, a NOT NULL                                    0
//	USING INDEX, a nullable but a validated CHECK proves it    0
//	(a) with a NULL in a                                       aborted: contains null values
//
// The collation half was measured again with the column's indexes and its
// current collation in hand (stokaro/ptah#2957), by relfilenode on the
// same server, an index on the column throughout:
//
//	ALTER COLUMN ... TYPE, collation only          table      index on the column
//	"C" -> "C", the collation restated             kept       kept
//	default -> COLLATE "default"                   kept       kept
//	"C" -> "en-US-x-icu", default -> "C"           kept       rebuilt
//	"C" -> no COLLATE clause (resets to default)   kept       rebuilt
//	varchar(20) -> varchar(40) COLLATE "POSIX"     kept       rebuilt (the widening is in place)
//	default -> "C" with no index on the column     kept       -
//	text -> varchar(20) COLLATE "C", "C" before    rewritten  rebuilt (the narrowing decides)
//
// So a widening that keeps every stored byte valid -- a longer varchar, an
// unlimited varchar or text, a higher numeric precision at the same scale,
// a longer fractional-seconds precision, a longer bit varying -- is a
// catalog edit, and everything else is the rewrite. A change of collation
// keeps the heap and rebuilds every index on the column, reading the table
// once per index; on a column no index covers it is a catalog edit, and a
// collation restated, or "default" on a column with none, changes nothing.
// A clause that names no COLLATE resets the column to the type's default
// collation, so on a column declared with one it is a change too. A USING
// expression on a change that would otherwise be in place is still left to
// DS103, since only the expression decides.
//
// The rules read the column's current type, collation and nullability, and
// the indexes on it, from the schema state the version starts from
// ([lintext.BaselineColumn.ColumnType] as the report composes it from the
// catalog, [lintext.BaselineColumn.Collation], [lintext.BaselineColumn.NotNull]
// and [lintext.BaselineIndex]); without that state PG301 leaves DS103 to
// report the statement, PG304 leaves PG104, and migration lint names them as
// inputs the run did not supply.

// pgClauseCollation reads the COLLATE clause of one ALTER COLUMN ... TYPE
// clause, spelled as the source spells it: a quoted name keeps its case, an
// unquoted one folds to lower case the way the server folds it, and a schema
// qualifier is dropped, since pg_catalog."C" and "C" name one collation.
// The second result reports whether the clause names one at all.
func pgClauseCollation(w, sourceWords []string, start, end int) (string, bool) {
	for k := start; k+1 < end; k++ {
		if w[k] != "COLLATE" {
			continue
		}
		name := k + 1
		for name+2 < end && w[name+1] == "." {
			name += 2
		}
		return pgCollationName(lintkit.SourceWordAt(w, sourceWords, name)), true
	}
	return "", false
}

func pgCollationName(word string) string {
	if strings.HasPrefix(word, `"`) {
		return strings.Trim(word, `"`)
	}
	return strings.ToLower(word)
}

// pgCollationChange compares the collation a column has with the one the
// clause leaves it with. A clause that names none resets the column to the
// type's default collation, and "default" names that same default, so
// either spelling on a column declared with the default changes nothing.
// The two spellings returned are for the message.
func pgCollationChange(current, clause string) (from, to string, changed bool) {
	if clause == "default" {
		clause = ""
	}
	if current == clause {
		return "", "", false
	}
	return pgCollationLabel(current), pgCollationLabel(clause), true
}

func pgCollationLabel(name string) string {
	if name == "" {
		return "the database default"
	}
	return `"` + name + `"`
}

// pgCollationTransition judges a collation change on a column whose type
// change, if any, is in place: with an index on the column the indexes are
// rebuilt, without one the change is a catalog edit.
func pgCollationTransition(from, to string, indexes []lintext.BaselineIndex) pgtype.Transition {
	change := fmt.Sprintf("changes the collation from %s to %s", from, to)
	if len(indexes) == 0 {
		return pgtype.Transition{Outcome: pgtype.InPlace, Why: change + " on a column no index reads, which is a catalog edit"}
	}
	names := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		names = append(names, idx.Name)
	}
	return pgtype.Transition{Outcome: pgtype.IndexRebuild, Why: change, Indexes: names}
}

func pgTypeChangeStatements(file *lintkit.File) []int {
	var indexes []int
	seen := -1
	for _, site := range pgtype.ChangeSites(file.Statements) {
		if site.Statement == seen {
			continue
		}
		seen = site.Statement
		indexes = append(indexes, site.Statement)
	}
	return indexes
}

type resolvedPGTypeChange struct {
	site       pgtype.ChangeSite
	old        pgtype.Type
	updated    pgtype.Type
	transition pgtype.Transition
}

func resolvePGTypeChanges(file *lintkit.File) []resolvedPGTypeChange {
	if !file.IsUp {
		return nil
	}
	var resolved []resolvedPGTypeChange
	for _, site := range pgtype.ChangeSites(file.Statements) {
		column, ok := file.Baseline.Column(site.Table.Normalized, lintkit.NormalizeIdent(site.Column))
		if !ok {
			continue
		}
		old, ok := pgtype.ParseSpelling(column.ColumnType)
		if !ok {
			continue
		}
		stmt := &file.Statements[site.Statement]
		updated, ok := pgtype.ParseAt(stmt.Words, site.TypeStart, site.TypeEnd)
		if !ok {
			continue
		}
		change := resolvedPGTypeChange{
			site:       site,
			old:        old,
			updated:    updated,
			transition: pgtype.Compare(old, updated),
		}
		if change.transition.Outcome == pgtype.Unchanged || change.transition.Outcome == pgtype.InPlace {
			clause, _ := pgClauseCollation(stmt.Words, stmt.SourceWords, site.TypeStart, site.TypeEnd)
			if from, to, changed := pgCollationChange(column.Collation, clause); changed {
				change.transition = pgCollationTransition(from, to,
					file.Baseline.ColumnIndexes(site.Table.Normalized, lintkit.NormalizeIdent(site.Column)))
			}
		}
		resolved = append(resolved, change)
	}
	return resolved
}

// postgresCostRules is the family: the two costs Atlas names for
// PostgreSQL, each fired only where it is established.
func postgresCostRules() []lintkit.Rule {
	return []lintkit.Rule{postgresTypeRewriteRule(), postgresPrimaryKeyScanRule()}
}

const pgRewriteConsequence = "PostgreSQL rewrites the whole table and rebuilds every index on it under the " +
	"ACCESS EXCLUSIVE lock the statement holds, blocking reads and writes until the rewrite finishes"

func postgresTypeRewriteRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "PG301",
			Title:    "column type change rewrites the table",
			Severity: lintext.Warning,
			Targets:  []string{"postgres"},
			Subsumes: []string{"DS103"},
			Input:    lintext.InputBaselineSchema,
		},
		BaselineSubjects: pgTypeChangeStatements,
		File: func(file *lintkit.File) []lintext.Violation {
			var findings []lintext.Violation
			for _, change := range resolvePGTypeChanges(file) {
				message, ok := pgRewriteMessage(change)
				if !ok {
					continue
				}
				findings = append(findings, lintext.Violation{Statement: change.site.Statement, Message: message,
					Subjects: []lintext.Subject{{Kind: lintext.SubjectColumn, Name: change.site.Column, Parent: change.site.Table.Name,
						DataType: change.updated.Spell()}}})
			}
			return findings
		},
	}
}

// pgRewriteMessage composes the finding for a change that rewrites.
func pgRewriteMessage(change resolvedPGTypeChange) (string, bool) {
	head := fmt.Sprintf("ALTER COLUMN %s.%s TYPE %s %s; ", change.site.Table.Name, change.site.Column, change.updated.Spell(), change.transition.Why)
	switch change.transition.Outcome {
	case pgtype.Rewrite:
		message := head + pgRewriteConsequence
		if change.transition.Abort != "" {
			message += fmt.Sprintf(", and a stored value the new type cannot hold aborts the statement (%s)", change.transition.Abort)
		}
		return message + ". A widening PostgreSQL can prove safe is a catalog edit instead: a longer or unlimited varchar, " +
			"a higher numeric precision at the same scale, a longer fractional-seconds precision. Where the change cannot be " +
			"expressed that way, add a new column, backfill it in batches, and swap the two", true
	case pgtype.RewriteUnlessUTC:
		return head + "every stored value is reinterpreted in the session TimeZone, so PostgreSQL rewrites the whole table " +
			"unless TimeZone is UTC when the statement runs, and rebuilds every index on the column either way, under the " +
			"ACCESS EXCLUSIVE lock the statement holds. Run it with TimeZone set to UTC if the values are UTC, or add a new " +
			"column, backfill it in batches, and swap the two", true
	case pgtype.IndexRebuild:
		return head + fmt.Sprintf("PostgreSQL keeps the table but rebuilds every index on the column (%s), reading the table "+
			"once per index, under the ACCESS EXCLUSIVE lock the statement holds; the same change on a column no index reads "+
			"is a catalog edit. Where the table cannot be locked for that long, add a new column with the collation, backfill "+
			"it in batches, index it CONCURRENTLY, and swap the two", strings.Join(change.transition.Indexes, ", ")), true
	default:
		return "", false
	}
}

// pgPrimaryKeySite is one ADD [CONSTRAINT name] PRIMARY KEY (columns), or
// ADD [CONSTRAINT name] PRIMARY KEY USING INDEX name, whose columns are the
// index's and are resolved against the file and the baseline.
type pgPrimaryKeySite struct {
	statement  int
	table      lintkit.TableReference
	columns    []string
	usingIndex string
}

func pgPrimaryKeySites(file *lintkit.File) []pgPrimaryKeySite {
	var sites []pgPrimaryKeySite
	for index := range file.Statements {
		stmt := &file.Statements[index]
		w := stmt.Words
		if !lintkit.IsAlterTable(w) {
			continue
		}
		table := lintkit.AlterTableReference(w, stmt.SourceWords)
		for _, i := range lintkit.ClauseStarts(w) {
			columns, usingIndex, ok := primaryKeyClause(w, stmt.SourceWords, i)
			if !ok {
				continue
			}
			sites = append(sites, pgPrimaryKeySite{statement: index, table: table, columns: columns, usingIndex: usingIndex})
		}
	}
	return sites
}

// primaryKeyClause reads an ADD PRIMARY KEY clause: the column list it
// names, or the index a USING INDEX form attaches.
func primaryKeyClause(w, sourceWords []string, i int) (columns []string, usingIndex string, ok bool) {
	end := lintkit.ClauseEnd(w, i)
	j := i
	if j >= end || w[j] != "ADD" {
		return nil, "", false
	}
	j++
	if j+1 < end && w[j] == "CONSTRAINT" && lintkit.IdentLike(w[j+1]) {
		j += 2
	}
	if j+2 >= end || w[j] != "PRIMARY" || w[j+1] != "KEY" {
		return nil, "", false
	}
	j += 2
	if j+2 < end && w[j] == "USING" && w[j+1] == "INDEX" && lintkit.IdentLike(w[j+2]) {
		return nil, lintkit.SourceWordAt(w, sourceWords, j+2), true
	}
	if w[j] != "(" {
		return nil, "", false
	}
	for k := j + 1; k < end && w[k] != ")"; k++ {
		if w[k] == "," {
			continue
		}
		if !lintkit.IdentLike(w[k]) {
			return nil, "", false
		}
		columns = append(columns, lintkit.SourceWordAt(w, sourceWords, k))
	}
	return columns, "", len(columns) > 0
}

// primaryKeyColumns resolves the columns a site sets NOT NULL: the ones it
// lists, or for a USING INDEX form the key columns of the index it names,
// found among the unique indexes the file builds earlier or in the state
// the version starts from. An index the file cannot find resolves nothing.
func primaryKeyColumns(file *lintkit.File, site pgPrimaryKeySite) []string {
	if site.usingIndex == "" {
		return site.columns
	}
	name := lintkit.NormalizeIdent(lintkit.LastComponent(site.usingIndex))
	for _, built := range lintkit.UniqueIndexSites(file.Statements) {
		if built.Statement < site.statement && built.Table.Normalized == site.table.Normalized &&
			lintkit.NormalizeIdent(lintkit.LastComponent(built.Name)) == name {
			return built.Columns
		}
	}
	idx, ok := file.Baseline.IndexNamed(site.table.Normalized, name)
	if !ok {
		return nil
	}
	columns := make([]string, 0, len(idx.Parts))
	for _, part := range idx.Parts {
		if part.Column == "" {
			return nil
		}
		columns = append(columns, part.Column)
	}
	return columns
}

func pgPrimaryKeyStatements(file *lintkit.File) []int {
	var indexes []int
	seen := -1
	for _, site := range pgPrimaryKeySites(file) {
		if site.statement == seen {
			continue
		}
		seen = site.statement
		indexes = append(indexes, site.statement)
	}
	return indexes
}

func postgresPrimaryKeyScanRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "PG304",
			Title:    "primary key over nullable columns scans the table",
			Severity: lintext.Warning,
			Targets:  []string{"postgres"},
			Input:    lintext.InputBaselineSchema,
		},
		BaselineSubjects: pgPrimaryKeyStatements,
		File: func(file *lintkit.File) []lintext.Violation {
			if !file.IsUp {
				return nil
			}
			var findings []lintext.Violation
			for _, site := range pgPrimaryKeySites(file) {
				columns := primaryKeyColumns(file, site)
				nullable := nullablePrimaryKeyColumns(file, site, columns)
				if len(nullable) == 0 {
					continue
				}
				message := primaryKeyScanMessage(site, columns, nullable)
				subjects := make([]lintext.Subject, 0, len(nullable))
				for _, column := range nullable {
					subjects = append(subjects, lintext.Subject{Kind: lintext.SubjectColumn, Name: column, Parent: site.table.Name})
				}
				findings = append(findings, lintext.Violation{Statement: site.statement, Message: message, Subjects: subjects})
			}
			return findings
		},
	}
}

// primaryKeyScanMessage says what the scan costs for the form the clause
// took. A key built over a column list builds the index and checks the NOT
// NULL; one attached USING INDEX has the index already, so the check is the
// only scan left, and a validated CHECK removes it.
func primaryKeyScanMessage(site pgPrimaryKeySite, columns, nullable []string) string {
	list := strings.Join(nullable, ", ")
	if site.usingIndex != "" {
		return fmt.Sprintf("ADD PRIMARY KEY USING INDEX %s on %s sets %s NOT NULL: the index is already built, so the scan "+
			"PostgreSQL still runs is the one that checks %s, under the ACCESS EXCLUSIVE lock the constraint takes, and a row "+
			"holding NULL aborts the statement (column contains null values). Prove the NOT NULL first with a CHECK "+
			"(%s IS NOT NULL) added NOT VALID and then validated: measured, the attach then scans nothing while the lock is held",
			site.usingIndex, site.table.Name, list, pluralColumns(nullable), nullable[0])
	}
	return fmt.Sprintf("ADD PRIMARY KEY (%s) on %s sets %s NOT NULL, so besides building the unique index PostgreSQL "+
		"scans every row to check %s, both under the ACCESS EXCLUSIVE lock the constraint takes, and a row holding NULL "+
		"aborts the statement (column contains null values). Prove the NOT NULL first with a CHECK (%s IS NOT NULL) "+
		"added NOT VALID and then validated, build the unique index CONCURRENTLY, and attach it with ADD PRIMARY KEY "+
		"USING INDEX: measured, that path scans nothing while the lock is held",
		strings.Join(columns, ", "), site.table.Name, list, pluralColumns(nullable), nullable[0])
}

// nullablePrimaryKeyColumns are the key's columns the baseline knows and
// reports as nullable, in key order.
func nullablePrimaryKeyColumns(file *lintkit.File, site pgPrimaryKeySite, columns []string) []string {
	var nullable []string
	for _, name := range columns {
		column, ok := file.Baseline.Column(site.table.Normalized, lintkit.NormalizeIdent(name))
		if !ok || column.NotNull {
			continue
		}
		nullable = append(nullable, name)
	}
	return nullable
}

func pluralColumns(columns []string) string {
	if len(columns) == 1 {
		return "it"
	}
	return "them"
}
