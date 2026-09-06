package lint

import (
	"fmt"
	"strconv"
	"strings"
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
// So a widening that keeps every stored byte valid -- a longer varchar, an
// unlimited varchar or text, a higher numeric precision at the same scale,
// a longer fractional-seconds precision, a longer bit varying -- is a
// catalog edit, and everything else is the rewrite. A change of collation
// keeps the heap and rebuilds the indexes on the column; the baseline does
// not carry indexes or the column's current collation, so that case is left
// to DS103 rather than claimed either way. So is a USING expression on a
// change that would otherwise be in place, since only the expression decides.
//
// The rules read the column's current type and nullability from the schema
// state the version starts from ([BaselineColumn.ColumnType] as the report
// composes it from the catalog, and [BaselineColumn.NotNull]); without that
// state PG301 leaves DS103 to report the statement, PG304 leaves PG104, and
// [Analysis.UnmetInputs] names them.

// pgType is one column type as PostgreSQL's catalog holds it: the canonical
// name format_type prints, the type modifier, and whether it is an array.
type pgType struct {
	// base is format_type's spelling: integer, character varying, timestamp
	// without time zone, ... For a type this file does not know, base is the
	// name as written and known is false.
	base  string
	known bool
	// mods is the type modifier: a length, (precision, scale), or a
	// fractional-seconds precision. nil is "no modifier".
	mods  []int
	array bool
}

// pgBaseSynonyms folds the spellings of one catalog type.
var pgBaseSynonyms = map[string]string{
	"INT": "integer", "INTEGER": "integer", "INT4": "integer", "SERIAL": "integer", "SERIAL4": "integer",
	"BIGINT": "bigint", "INT8": "bigint", "BIGSERIAL": "bigint", "SERIAL8": "bigint",
	"SMALLINT": "smallint", "INT2": "smallint", "SMALLSERIAL": "smallint", "SERIAL2": "smallint",
	"REAL": "real", "FLOAT4": "real",
	"FLOAT8": "double precision", "DOUBLE": "double precision",
	"NUMERIC": "numeric", "DECIMAL": "numeric", "DEC": "numeric",
	"BOOL": "boolean", "BOOLEAN": "boolean",
	"VARCHAR": "character varying", "CHARACTER": "character", "CHAR": "character", "BPCHAR": "character",
	"TEXT": "text", "BYTEA": "bytea", "UUID": "uuid", "JSON": "json", "JSONB": "jsonb", "XML": "xml",
	"DATE": "date", "MONEY": "money", "INET": "inet", "CIDR": "cidr", "MACADDR": "macaddr", "MACADDR8": "macaddr8",
	"TIMESTAMP": "timestamp without time zone", "TIMESTAMPTZ": "timestamp with time zone",
	"TIME": "time without time zone", "TIMETZ": "time with time zone",
	"INTERVAL": "interval",
	"BIT":      "bit", "VARBIT": "bit varying",
	"OID": "oid", "NAME": "name",
}

// pgPrecisionBases are the families whose modifier is a fractional-seconds
// precision that defaults to 6.
var pgPrecisionBases = map[string]bool{
	"timestamp without time zone": true, "timestamp with time zone": true,
	"time without time zone": true, "time with time zone": true,
	"interval": true,
}

// parsePGTypeAt reads a type from statement words starting at words[i] and
// the COLLATE and USING clauses that may follow it, up to end.
func parsePGTypeAt(words []string, i, end int) (pgType, bool) {
	var t pgType
	j := i
	if j >= end {
		return t, false
	}
	j = t.parsePGBase(words, j, end)
	if t.base == "" {
		return t, false
	}
	mods, next, ok := parseTypeParams(words[:end], j)
	if !ok {
		return t, false
	}
	t.mods = mods
	j = t.parsePGZone(words, next, end)
	t.parsePGArray(words, j, end)
	// What follows -- COLLATE, USING -- is read by nobody: a collation change
	// costs an index rebuild only where an index exists, and a USING
	// expression decides for itself, so neither is claimed either way.
	return t.finish(), true
}

// parsePGBase reads the type name, folding the two-word spellings, and
// returns the index after it.
func (t *pgType) parsePGBase(words []string, j, end int) int {
	word := words[j]
	switch {
	case word == "DOUBLE" && j+1 < end && words[j+1] == "PRECISION":
		t.base, t.known = "double precision", true
		return j + 2
	case word == "CHARACTER" && j+1 < end && words[j+1] == "VARYING":
		t.base, t.known = "character varying", true
		return j + 2
	case word == "BIT" && j+1 < end && words[j+1] == "VARYING":
		t.base, t.known = "bit varying", true
		return j + 2
	case word == "FLOAT":
		t.base, t.known = "float", true
		return j + 1
	}
	if base, ok := pgBaseSynonyms[word]; ok {
		t.base, t.known = base, true
		return j + 1
	}
	if !identLike(word) {
		return j
	}
	// A qualified or quoted name: kept as written, compared by equality only.
	parts := []string{strings.Trim(word, `"`)}
	k := j + 1
	for k+1 < end && words[k] == "." && identLike(words[k+1]) {
		parts = append(parts, strings.Trim(words[k+1], `"`))
		k += 2
	}
	t.base = strings.ToLower(strings.Join(parts, "."))
	return k
}

// parsePGZone reads WITH TIME ZONE or WITHOUT TIME ZONE after a timestamp
// or time.
func (t *pgType) parsePGZone(words []string, j, end int) int {
	if j+2 >= end || (words[j] != "WITH" && words[j] != "WITHOUT") || words[j+1] != "TIME" || words[j+2] != "ZONE" {
		return j
	}
	family, _, _ := strings.Cut(t.base, " ")
	if family != "timestamp" && family != "time" {
		return j
	}
	t.base = family + " " + strings.ToLower(words[j]) + " time zone"
	return j + 3
}

// parsePGArray reads [] or ARRAY after the type.
func (t *pgType) parsePGArray(words []string, j, end int) {
	if j < end && (words[j] == "[" || words[j] == "ARRAY") {
		t.array = true
	}
}

// finish applies the defaults the catalog applies.
func (t pgType) finish() pgType {
	switch {
	case t.base == "float":
		t.base = "double precision"
		if len(t.mods) == 1 && t.mods[0] <= 24 {
			t.base = "real"
		}
		t.mods = nil
	case t.base == "character" || t.base == "bit":
		if len(t.mods) == 0 {
			t.mods = []int{1}
		}
	case pgPrecisionBases[t.base]:
		if len(t.mods) == 0 {
			t.mods = []int{6}
		}
	case t.base == "numeric" && len(t.mods) == 1:
		t.mods = []int{t.mods[0], 0}
	}
	return t
}

// parsePGTypeSpelling reads the type as the report spells it from the
// catalog -- `character varying(20)`, `timestamp(3) without time zone`,
// `integer[]` -- through the same tokenizer and parser as the statement.
func parsePGTypeSpelling(spelling string) (pgType, bool) {
	spelling = strings.TrimSpace(spelling)
	if spelling == "" {
		return pgType{}, false
	}
	words := tokenizeWords(spelling, modeForDialect("postgres"))
	return parsePGTypeAt(words, 0, len(words))
}

// spell renders the canonical form for a message, the way format_type would.
func (t pgType) spell() string {
	var b strings.Builder
	family, zone := t.base, ""
	if i := strings.Index(t.base, " with"); i > 0 {
		family, zone = t.base[:i], t.base[i+1:]
	}
	b.WriteString(family)
	if len(t.mods) > 0 && (!pgPrecisionBases[t.base] || t.mods[0] != 6) {
		b.WriteString("(")
		for i, m := range t.mods {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(strconv.Itoa(m))
		}
		b.WriteString(")")
	}
	if zone != "" {
		b.WriteString(" " + zone)
	}
	if t.array {
		b.WriteString("[]")
	}
	return b.String()
}

func (t pgType) sameAs(o pgType) bool {
	if t.base != o.base || t.array != o.array || len(t.mods) != len(o.mods) {
		return false
	}
	for i := range t.mods {
		if t.mods[i] != o.mods[i] {
			return false
		}
	}
	return true
}

// pgOutcome is what PostgreSQL does with one type change. Only the two
// rewrites produce a finding; the other three are told apart for the
// message a future rule might want, and are silent alike today.
type pgOutcome int

const (
	pgUnchanged pgOutcome = iota
	pgInPlace
	pgUnknown
	pgRewrite
	// pgRewriteUnlessUTC is the timestamp / timestamptz conversion, which
	// rewrites unless the session TimeZone is UTC and rebuilds the column's
	// indexes either way.
	pgRewriteUnlessUTC
)

type pgTransition struct {
	outcome pgOutcome
	why     string
	// abort names the error a stored value the new type cannot hold raises,
	// where one was measured.
	abort string
}

// comparePGTypes decides the outcome for one column.
func comparePGTypes(old, updated pgType) pgTransition {
	if !old.known || !updated.known {
		if old.base == updated.base && old.array == updated.array {
			return pgTransition{outcome: pgUnchanged}
		}
		return pgTransition{outcome: pgUnknown}
	}
	return comparePGKnownTypes(old, updated)
}

func comparePGKnownTypes(old, updated pgType) pgTransition {
	change := fmt.Sprintf("changes the type from %s to %s", old.spell(), updated.spell())
	switch {
	case old.sameAs(updated):
		return pgTransition{outcome: pgUnchanged}
	case old.array || updated.array:
		return pgTransition{pgRewrite, change + ", and PostgreSQL rewrites an array column for any change of its element type", ""}
	case old.base != updated.base:
		return comparePGBases(old, updated, change)
	}
	switch old.base {
	case "character varying":
		return compareVarchar(old, updated)
	case "numeric":
		return compareNumeric(old, updated, change)
	case "bit varying":
		if len(old.mods) == 1 && (len(updated.mods) == 0 || updated.mods[0] >= old.mods[0]) {
			return pgTransition{pgInPlace, "widens a bit varying", ""}
		}
		return pgTransition{pgRewrite, change, "bit string too long for type"}
	case "bit":
		return pgTransition{pgRewrite, change, "bit string length does not match type"}
	}
	if pgPrecisionBases[old.base] {
		if updated.mods[0] >= old.mods[0] {
			return pgTransition{pgInPlace, "lengthens a fractional-seconds precision", ""}
		}
		return pgTransition{pgRewrite, change + ", which rounds every stored value to the shorter precision", ""}
	}
	return pgTransition{pgRewrite, change, ""}
}

// comparePGBases judges a change between two catalog types.
func comparePGBases(old, updated pgType, change string) pgTransition {
	switch {
	case old.base == "character varying" && updated.base == "text":
		return pgTransition{pgInPlace, "drops a varchar's limit", ""}
	case old.base == "text" && updated.base == "character varying" && len(updated.mods) == 0:
		return pgTransition{pgInPlace, "renames text to an unlimited varchar", ""}
	case old.base == "text" && updated.base == "character varying":
		return pgTransition{pgRewrite, change + ", which checks every value against the new limit", "value too long for type"}
	case isTimestampFamily(old.base) && isTimestampFamily(updated.base):
		return pgTransition{pgRewriteUnlessUTC, change, ""}
	}
	abort := ""
	if strings.HasSuffix(updated.base, "int") {
		abort = updated.base + " out of range"
	}
	return pgTransition{pgRewrite, change, abort}
}

func isTimestampFamily(base string) bool {
	return strings.HasPrefix(base, "timestamp ")
}

// compareVarchar judges a change of limit on a character varying column.
func compareVarchar(old, updated pgType) pgTransition {
	change := fmt.Sprintf("changes the type from %s to %s", old.spell(), updated.spell())
	switch {
	case len(updated.mods) == 0:
		return pgTransition{pgInPlace, "drops a varchar's limit", ""}
	case len(old.mods) == 0:
		return pgTransition{pgRewrite, change + ", which checks every value against the new limit", "value too long for type"}
	case updated.mods[0] >= old.mods[0]:
		return pgTransition{pgInPlace, "widens a varchar", ""}
	default:
		return pgTransition{pgRewrite, change + ", which checks every value against the shorter limit", "value too long for type"}
	}
}

// compareNumeric judges a change of precision or scale.
func compareNumeric(old, updated pgType, change string) pgTransition {
	switch {
	case len(updated.mods) == 0:
		return pgTransition{pgInPlace, "drops a numeric's precision", ""}
	case len(old.mods) == 0:
		return pgTransition{pgRewrite, change + ", which checks and rounds every value to the new precision and scale", "numeric field overflow"}
	case updated.mods[1] == old.mods[1] && updated.mods[0] >= old.mods[0]:
		return pgTransition{pgInPlace, "raises a numeric's precision at the same scale", ""}
	case updated.mods[1] != old.mods[1]:
		return pgTransition{pgRewrite, change + ", and a change of scale rounds every stored value", "numeric field overflow"}
	default:
		return pgTransition{pgRewrite, change + ", which checks every value against the lower precision", "numeric field overflow"}
	}
}

// pgTypeChangeSite is one ALTER [COLUMN] name [SET DATA] TYPE clause.
type pgTypeChangeSite struct {
	statement int
	table     tableReference
	column    string
	typeStart int
	typeEnd   int
}

func pgTypeChangeSites(file *File) []pgTypeChangeSite {
	var sites []pgTypeChangeSite
	for index := range file.Statements {
		stmt := &file.Statements[index]
		w := stmt.Words
		if !isAlterTable(w) {
			continue
		}
		table := alterTableReference(w, stmt.sourceWords)
		for _, i := range clauseStarts(w) {
			if i >= len(w) || w[i] != "ALTER" {
				continue
			}
			j := i + 1
			if j < len(w) && w[j] == "COLUMN" {
				j++
			}
			j = skipIfExists(w, j)
			if j >= len(w) || !identLike(w[j]) {
				continue
			}
			column := sourceWordAt(w, stmt.sourceWords, j)
			k := j + 1
			if k+1 < len(w) && w[k] == "SET" && w[k+1] == "DATA" {
				k += 2
			}
			if k >= len(w) || w[k] != "TYPE" {
				continue
			}
			sites = append(sites, pgTypeChangeSite{
				statement: index,
				table:     table,
				column:    column,
				typeStart: k + 1,
				typeEnd:   clauseEnd(w, i),
			})
		}
	}
	return sites
}

func pgTypeChangeStatements(file *File) []int {
	var indexes []int
	seen := -1
	for _, site := range pgTypeChangeSites(file) {
		if site.statement == seen {
			continue
		}
		seen = site.statement
		indexes = append(indexes, site.statement)
	}
	return indexes
}

type resolvedPGTypeChange struct {
	site       pgTypeChangeSite
	old        pgType
	updated    pgType
	transition pgTransition
}

func resolvePGTypeChanges(file *File) []resolvedPGTypeChange {
	if !file.IsUp {
		return nil
	}
	var resolved []resolvedPGTypeChange
	for _, site := range pgTypeChangeSites(file) {
		column, ok := file.baseline.column(site.table.normalized, normalizeIdent(site.column))
		if !ok {
			continue
		}
		old, ok := parsePGTypeSpelling(column.ColumnType)
		if !ok {
			continue
		}
		stmt := &file.Statements[site.statement]
		updated, ok := parsePGTypeAt(stmt.Words, site.typeStart, site.typeEnd)
		if !ok {
			continue
		}
		resolved = append(resolved, resolvedPGTypeChange{
			site:       site,
			old:        old,
			updated:    updated,
			transition: comparePGTypes(old, updated),
		})
	}
	return resolved
}

// postgresCostRules is the family: the two costs Atlas names for
// PostgreSQL, each fired only where it is established.
func postgresCostRules() []Rule {
	return []Rule{postgresTypeRewriteRule(), postgresPrimaryKeyScanRule()}
}

const pgRewriteConsequence = "PostgreSQL rewrites the whole table and rebuilds every index on it under the " +
	"ACCESS EXCLUSIVE lock the statement holds, blocking reads and writes until the rewrite finishes"

func postgresTypeRewriteRule() Rule {
	return Rule{
		Code:             "PG301",
		Title:            "column type change rewrites the table",
		Severity:         SeverityWarning,
		Dialects:         []string{"postgres"},
		Subsumes:         []string{"DS103"},
		Input:            InputBaselineSchema,
		BaselineSubjects: pgTypeChangeStatements,
		CheckFile: func(file *File) []Finding {
			var findings []Finding
			for _, change := range resolvePGTypeChanges(file) {
				message, ok := pgRewriteMessage(change)
				if !ok {
					continue
				}
				findings = append(findings, costFinding(file, change.site.statement, "PG301", "column type change rewrites the table", message,
					Subject{Kind: SubjectColumn, Name: change.site.column, Parent: change.site.table.name, DataType: change.updated.spell()}))
			}
			return findings
		},
	}
}

// pgRewriteMessage composes the finding for a change that rewrites.
func pgRewriteMessage(change resolvedPGTypeChange) (string, bool) {
	head := fmt.Sprintf("ALTER COLUMN %s.%s TYPE %s %s; ", change.site.table.name, change.site.column, change.updated.spell(), change.transition.why)
	switch change.transition.outcome {
	case pgRewrite:
		message := head + pgRewriteConsequence
		if change.transition.abort != "" {
			message += fmt.Sprintf(", and a stored value the new type cannot hold aborts the statement (%s)", change.transition.abort)
		}
		return message + ". A widening PostgreSQL can prove safe is a catalog edit instead: a longer or unlimited varchar, " +
			"a higher numeric precision at the same scale, a longer fractional-seconds precision. Where the change cannot be " +
			"expressed that way, add a new column, backfill it in batches, and swap the two", true
	case pgRewriteUnlessUTC:
		return head + "every stored value is reinterpreted in the session TimeZone, so PostgreSQL rewrites the whole table " +
			"unless TimeZone is UTC when the statement runs, and rebuilds every index on the column either way, under the " +
			"ACCESS EXCLUSIVE lock the statement holds. Run it with TimeZone set to UTC if the values are UTC, or add a new " +
			"column, backfill it in batches, and swap the two", true
	default:
		return "", false
	}
}

// pgPrimaryKeySite is one ADD [CONSTRAINT name] PRIMARY KEY (columns).
type pgPrimaryKeySite struct {
	statement int
	table     tableReference
	columns   []string
}

func pgPrimaryKeySites(file *File) []pgPrimaryKeySite {
	var sites []pgPrimaryKeySite
	for index := range file.Statements {
		stmt := &file.Statements[index]
		w := stmt.Words
		if !isAlterTable(w) {
			continue
		}
		table := alterTableReference(w, stmt.sourceWords)
		for _, i := range clauseStarts(w) {
			columns, ok := primaryKeyColumns(w, stmt.sourceWords, i)
			if !ok {
				continue
			}
			sites = append(sites, pgPrimaryKeySite{statement: index, table: table, columns: columns})
		}
	}
	return sites
}

// primaryKeyColumns reads the column list of an ADD PRIMARY KEY clause. A
// USING INDEX form names no columns, so it is not a site.
func primaryKeyColumns(w, sourceWords []string, i int) ([]string, bool) {
	end := clauseEnd(w, i)
	j := i
	if j >= end || w[j] != "ADD" {
		return nil, false
	}
	j++
	if j+1 < end && w[j] == "CONSTRAINT" && identLike(w[j+1]) {
		j += 2
	}
	if j+2 >= end || w[j] != "PRIMARY" || w[j+1] != "KEY" || w[j+2] != "(" {
		return nil, false
	}
	var columns []string
	for k := j + 3; k < end && w[k] != ")"; k++ {
		if w[k] == "," {
			continue
		}
		if !identLike(w[k]) {
			return nil, false
		}
		columns = append(columns, sourceWordAt(w, sourceWords, k))
	}
	return columns, len(columns) > 0
}

func pgPrimaryKeyStatements(file *File) []int {
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

func postgresPrimaryKeyScanRule() Rule {
	return Rule{
		Code:             "PG304",
		Title:            "primary key over nullable columns scans the table",
		Severity:         SeverityWarning,
		Dialects:         []string{"postgres"},
		Input:            InputBaselineSchema,
		BaselineSubjects: pgPrimaryKeyStatements,
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			var findings []Finding
			for _, site := range pgPrimaryKeySites(file) {
				nullable := nullablePrimaryKeyColumns(file, site)
				if len(nullable) == 0 {
					continue
				}
				list := strings.Join(nullable, ", ")
				message := fmt.Sprintf("ADD PRIMARY KEY (%s) on %s sets %s NOT NULL, so besides building the unique index PostgreSQL "+
					"scans every row to check %s, both under the ACCESS EXCLUSIVE lock the constraint takes, and a row holding NULL "+
					"aborts the statement (column contains null values). Prove the NOT NULL first with a CHECK (%s IS NOT NULL) "+
					"added NOT VALID and then validated, build the unique index CONCURRENTLY, and attach it with ADD PRIMARY KEY "+
					"USING INDEX: measured, that path scans nothing while the lock is held",
					strings.Join(site.columns, ", "), site.table.name, list, pluralColumns(nullable), nullable[0])
				subjects := make([]Subject, 0, len(nullable))
				for _, column := range nullable {
					subjects = append(subjects, Subject{Kind: SubjectColumn, Name: column, Parent: site.table.name})
				}
				stmt := &file.Statements[site.statement]
				findings = append(findings, Finding{
					Rule:     "PG304",
					Title:    "primary key over nullable columns scans the table",
					Severity: SeverityWarning,
					File:     file.Path,
					Line:     stmt.Line,
					Message:  message,
					Context:  statementFindingContext(site.statement, subjects...),
				})
			}
			return findings
		},
	}
}

// nullablePrimaryKeyColumns are the key's columns the baseline knows and
// reports as nullable, in key order.
func nullablePrimaryKeyColumns(file *File, site pgPrimaryKeySite) []string {
	var nullable []string
	for _, name := range site.columns {
		column, ok := file.baseline.column(site.table.normalized, normalizeIdent(name))
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
