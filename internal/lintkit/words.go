package lintkit

import (
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"ptah.run/core/lintext"
)

// RoutineHeaderWords returns the words of a routine definition's header, and
// reports whether the statement is one.
//
// The header ends where the body begins, which is AS, BEGIN or RETURN. Reading
// past it is what a scan of the whole statement does, and a body is words: one
// carrying `SET` would satisfy the rule for a header that pins nothing.
func RoutineHeaderWords(w []string) ([]string, bool) {
	if _, ok := RoutineDefinitionKind(w); !ok {
		return nil, false
	}
	for i, word := range w {
		switch word {
		case "AS", "BEGIN", "RETURN":
			return w[:i], true
		}
	}
	return w, true
}

// ContainsWord reports whether a word list holds one.
func ContainsWord(w []string, want string) bool {
	return slices.Contains(w, want)
}

// RoutineDefinitionKind names the routine a statement defines, and reports
// whether it defines one.
//
// Anchored on the statement, for the reason DS106 is: a routine body is words,
// and a scan that walked them would answer for the body rather than about it.
func RoutineDefinitionKind(w []string) (string, bool) {
	var rest []string
	switch {
	case HasWordPrefix(w, "CREATE", "OR", "REPLACE"):
		rest = w[3:]
	case HasWordPrefix(w, "CREATE"):
		rest = w[1:]
	default:
		return "", false
	}
	if len(rest) == 0 {
		return "", false
	}
	switch rest[0] {
	case "FUNCTION":
		return "function", true
	case "PROCEDURE":
		return "procedure", true
	default:
		return "", false
	}
}

// IsAlterTable reports whether the statement's words begin with ALTER TABLE.
func IsAlterTable(w []string) bool {
	return HasWordPrefix(w, "ALTER", "TABLE")
}

// IsCreateIndex reports whether the statement's words begin with
// CREATE [UNIQUE] INDEX.
func IsCreateIndex(w []string) bool {
	if len(w) == 0 || w[0] != "CREATE" {
		return false
	}
	j := 1
	if j < len(w) && w[j] == "UNIQUE" {
		j++
	}
	return j < len(w) && w[j] == "INDEX"
}

// HasWordPrefix reports whether the word sequence starts with the given words.
func HasWordPrefix(w []string, prefix ...string) bool {
	if len(w) < len(prefix) {
		return false
	}
	for i, p := range prefix {
		if w[i] != p {
			return false
		}
	}
	return true
}

// HasWordSeq reports whether the given words appear consecutively anywhere in
// the sequence.
func HasWordSeq(w []string, seq ...string) bool {
	for i := 0; i+len(seq) <= len(w); i++ {
		if HasWordPrefix(w[i:], seq...) {
			return true
		}
	}
	return false
}

// IdentLike reports whether a word can name a column or table: a quoted
// identifier, or a bare word starting with a letter, underscore, or digit.
// Single-quoted string literals are values, never identifiers.
func IdentLike(word string) bool {
	if word == "" {
		return false
	}
	switch word[0] {
	case '`', '"':
		return true
	case '\'':
		return false
	}
	r, _ := utf8.DecodeRuneInString(word)
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// SkipIfExists advances past an optional IF EXISTS at w[j].
func SkipIfExists(w []string, j int) int {
	if j+1 < len(w) && w[j] == "IF" && w[j+1] == "EXISTS" {
		return j + 2
	}
	return j
}

// ClauseStarts returns the word indices where the clauses of an ALTER TABLE
// statement begin: the first word after the table reference and each word
// after a top-level comma. Commas inside parentheses (type parameters,
// expressions) do not start a clause. Anchoring scans to clause heads keeps
// clause keywords (DROP, MODIFY, RENAME, ...) from being confused with
// column names appearing mid-clause, e.g. ADD COLUMN modify TEXT.
func ClauseStarts(w []string) []int {
	j := 2
	// PostgreSQL grammar: ALTER TABLE [ IF EXISTS ] [ ONLY ] name [ * ].
	// Skip the modifiers in any order to stay robust.
	for j < len(w) {
		if w[j] == "ONLY" {
			j++
			continue
		}
		if next := SkipIfExists(w, j); next != j {
			j = next
			continue
		}
		break
	}
	if ref, next := TableRefAt(w, w, j); ref.Normalized != "" {
		j = next
		if j < len(w) && w[j] == "*" {
			j++ // postgres: name * (include descendant tables)
		}
	}
	starts := []int{j}
	depth := 0
	for k := j; k < len(w); k++ {
		switch w[k] {
		case "(":
			depth++
		case ")":
			if depth > 0 {
				depth--
			}
		case ",":
			if depth == 0 {
				starts = append(starts, k+1)
			}
		}
	}
	return starts
}

// ScanDropClause reports whether any clause of an ALTER TABLE statement begins
// with the given word sequence, anchoring on the clause heads so that only a
// top-level DROP form matches. It backs the typed constraint-drop predicates.
func ScanDropClause(w []string, prefix ...string) bool {
	for _, i := range ClauseStarts(w) {
		if HasWordPrefix(w[i:], prefix...) {
			return true
		}
	}
	return false
}

// ScanDropPrimaryKey reports whether an ALTER TABLE statement drops a primary
// key via the DROP PRIMARY KEY form.
func ScanDropPrimaryKey(w []string) bool { return ScanDropClause(w, "DROP", "PRIMARY", "KEY") }

// ScanAddStoredGeneratedColumn reports whether an ALTER TABLE adds a column
// GENERATED ALWAYS AS ... STORED.
func ScanAddStoredGeneratedColumn(w []string) bool {
	for _, i := range ClauseStarts(w) {
		start, end, ok := AddColumnClause(w, i)
		if ok && HasWordSeq(w[start:end], "GENERATED", "ALWAYS", "AS") && slices.Contains(w[start:end], "STORED") {
			return true
		}
	}
	return false
}

// AddColumnClause reads the ADD [COLUMN] [IF NOT EXISTS] name clause that
// starts at i, and returns the word range of the column definition after the
// name. A clause that adds a constraint or an index is not one.
func AddColumnClause(w []string, i int) (start, end int, ok bool) {
	if i >= len(w) || w[i] != "ADD" {
		return 0, 0, false
	}
	j := i + 1
	if j < len(w) && w[j] == "COLUMN" {
		j++
	}
	if j < len(w) && AddConstraintTargets[w[j]] {
		return 0, 0, false
	}
	j = SkipIfNotExists(w, j)
	if j >= len(w) || !IdentLike(w[j]) {
		return 0, 0, false
	}
	return j + 1, ClauseEnd(w, i), true
}

// AddConstraintTargets are the words after ADD that start a constraint or an
// index rather than a column.
var AddConstraintTargets = map[string]bool{
	"CHECK":      true,
	"CONSTRAINT": true,
	"FOREIGN":    true,
	"FULLTEXT":   true,
	"INDEX":      true,
	"KEY":        true,
	"PRIMARY":    true,
	"SPATIAL":    true,
	"UNIQUE":     true,
}

// SkipIfNotExists returns the position after an IF NOT EXISTS at j, or j.
func SkipIfNotExists(w []string, j int) int {
	if j+2 < len(w) && w[j] == "IF" && w[j+1] == "NOT" && w[j+2] == "EXISTS" {
		return j + 3
	}
	return j
}

// ClauseEnd returns the position of the comma that ends the clause starting
// at start, outside parentheses, or the end of the words.
func ClauseEnd(w []string, start int) int {
	depth := 0
	for i := start; i < len(w); i++ {
		switch w[i] {
		case "(":
			depth++
		case ")":
			if depth > 0 {
				depth--
			}
		case ",":
			if depth == 0 {
				return i
			}
		}
	}
	return len(w)
}

// ScanAddPrimaryKey reports whether an ALTER TABLE adds a primary key, bare
// or as a named constraint.
func ScanAddPrimaryKey(w []string) bool {
	for _, i := range ClauseStarts(w) {
		if HasWordPrefix(w[i:], "ADD", "PRIMARY", "KEY") ||
			HasWordPrefix(w[i:], "ADD", "CONSTRAINT") && slices.Contains(w[i:ClauseEnd(w, i)], "PRIMARY") {
			return true
		}
	}
	return false
}

// AnyAlterClause reports whether any clause of an ALTER TABLE satisfies match.
func AnyAlterClause(w []string, match func(clause []string) bool) bool {
	for _, i := range ClauseStarts(w) {
		if match(w[i:ClauseEnd(w, i)]) {
			return true
		}
	}
	return false
}

// AddsCheckConstraint reports whether one ALTER TABLE clause adds a CHECK
// constraint.
func AddsCheckConstraint(clause []string) bool {
	return HasWordPrefix(clause, "ADD", "CHECK") ||
		HasWordPrefix(clause, "ADD", "CONSTRAINT") && slices.Contains(clause, "CHECK")
}

// AddsForeignKey reports whether one ALTER TABLE clause adds a foreign key.
func AddsForeignKey(clause []string) bool {
	return HasWordPrefix(clause, "ADD", "FOREIGN", "KEY") ||
		HasWordPrefix(clause, "ADD", "CONSTRAINT") && HasWordSeq(clause, "FOREIGN", "KEY")
}

// ScanSetNotNull reports whether an ALTER TABLE sets a column NOT NULL.
func ScanSetNotNull(w []string) bool {
	for _, i := range ClauseStarts(w) {
		if HasWordPrefix(w[i:], "ALTER", "COLUMN") && HasWordSeq(w[i:], "SET", "NOT", "NULL") {
			return true
		}
		if HasWordPrefix(w[i:], "ALTER") && len(w[i:]) > 1 && IdentLike(w[i+1]) && HasWordSeq(w[i:], "SET", "NOT", "NULL") {
			return true
		}
	}
	return false
}

// CreatedTableRef returns the normalized reference of the table a CREATE
// TABLE statement creates, or "" for any other statement. Temporary-table
// modifiers and IF NOT EXISTS are skipped.
func CreatedTableRef(w []string) string {
	if len(w) == 0 || w[0] != "CREATE" {
		return ""
	}
	j := 1
	for j < len(w) {
		switch w[j] {
		case "GLOBAL", "LOCAL", "TEMPORARY", "TEMP", "UNLOGGED":
			j++
			continue
		}
		break
	}
	if j >= len(w) || w[j] != "TABLE" {
		return ""
	}
	j++
	if j+2 < len(w) && w[j] == "IF" && w[j+1] == "NOT" && w[j+2] == "EXISTS" {
		j += 3
	}
	ref, _ := TableRefAt(w, w, j)
	return ref.Normalized
}

// TableRefAt reads a possibly schema-qualified table reference at w[j] and
// returns separate display and comparison forms plus the index past its end.
func TableRefAt(w, sourceWords []string, j int) (TableReference, int) {
	identifier, next, ok := IdentifierAt(w, sourceWords, j)
	if !ok {
		return TableReference{}, j
	}
	displayParts := []string{identifier.Name}
	normalizedParts := []string{identifier.Normalized}
	j = next
	for j < len(w) && w[j] == "." {
		identifier, next, ok = IdentifierAt(w, sourceWords, j+1)
		if !ok {
			break
		}
		displayParts = append(displayParts, identifier.Name)
		normalizedParts = append(normalizedParts, identifier.Normalized)
		j = next
	}
	return TableReference{
		Name:       strings.Join(displayParts, "."),
		Normalized: strings.Join(normalizedParts, "."),
	}, j
}

// IdentifierAt reads one identifier at index: a bare or quoted word, or a
// bracketed name. It returns the reference, the position after it, and
// whether one was there.
func IdentifierAt(w, sourceWords []string, index int) (TableReference, int, bool) {
	if index >= len(w) {
		return TableReference{}, index, false
	}
	if IdentLike(w[index]) {
		return TableReference{
			Name:       SourceWordAt(w, sourceWords, index),
			Normalized: NormalizeIdent(w[index]),
		}, index + 1, true
	}
	if w[index] != "[" {
		return TableReference{}, index, false
	}
	end := index + 1
	for end < len(w) && w[end] != "]" {
		end++
	}
	if end == index+1 || end >= len(w) {
		return TableReference{}, index, false
	}
	return TableReference{
		Name:       "[" + strings.Join(sourceWords[index+1:end], " ") + "]",
		Normalized: strings.ToUpper(strings.Join(w[index+1:end], " ")),
	}, end + 1, true
}

// TableReference is a table a statement names: as the author spelled it, and
// normalized for comparison.
type TableReference struct {
	Name       string
	Normalized string
}

// AlterTableReference is the table an ALTER TABLE statement alters, past ONLY
// and IF EXISTS; the zero value for any other statement.
func AlterTableReference(w, sourceWords []string) TableReference {
	if !HasWordPrefix(w, "ALTER", "TABLE") {
		return TableReference{}
	}
	j := 2
	for j < len(w) {
		if w[j] == "ONLY" {
			j++
			continue
		}
		if next := SkipIfExists(w, j); next != j {
			j = next
			continue
		}
		break
	}
	ref, _ := TableRefAt(w, sourceWords, j)
	return ref
}

// SourceWordAt is the word at index as the author spelled it, or the
// uppercased word where the source spelling is missing.
func SourceWordAt(words, sourceWords []string, index int) string {
	if index < len(sourceWords) {
		return sourceWords[index]
	}
	return words[index]
}

// NormalizeIdent strips identifier quoting and uppercases for comparison.
func NormalizeIdent(word string) string {
	return strings.ToUpper(strings.Trim(word, "`\""))
}

// RefersToCreated reports whether ref names one of the created tables,
// comparing full references when both sides are schema-qualified and last
// components otherwise.
func RefersToCreated(created map[string]bool, ref string) bool {
	if ref == "" {
		return false
	}
	if created[ref] {
		return true
	}
	last := ref[strings.LastIndex(ref, ".")+1:]
	for c := range created {
		if c[strings.LastIndex(c, ".")+1:] != last {
			continue
		}
		if !strings.Contains(ref, ".") || !strings.Contains(c, ".") {
			return true
		}
	}
	return false
}

// AlterColumnTypeClause reports whether the clause at i is an ALTER [COLUMN]
// name [SET DATA] TYPE clause.
func AlterColumnTypeClause(w []string, i int) bool {
	if i >= len(w) || w[i] != "ALTER" {
		return false
	}
	j := i + 1
	if j < len(w) && w[j] == "COLUMN" {
		j++
	}
	j = SkipIfExists(w, j)
	if j >= len(w) || !IdentLike(w[j]) {
		return false
	}
	k := j + 1
	if k+1 < len(w) && w[k] == "SET" && w[k+1] == "DATA" {
		k += 2
	}
	return k < len(w) && w[k] == "TYPE"
}

// RedefinesPrimaryKey reports one statement that drops a primary key
// and adds another.
//
// Both halves in one statement is the point: dropping alone is MY133 and
// CD103's subject, adding alone is MY132's and PG104's, and the redefinition
// costs what neither of those reports on its own.
func RedefinesPrimaryKey(w []string) bool {
	if !IsAlterTable(w) {
		return false
	}
	dropped := ScanDropPrimaryKey(w) || ScanDropClause(w, "DROP", "CONSTRAINT")
	return dropped && ScanAddPrimaryKey(w)
}

// Plural is the suffix that makes a count of n read as English: "s" unless n
// is one.
func Plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// ParseTypeParams reads an optional ( n [, m] ) after a type keyword.
func ParseTypeParams(words []string, j int) ([]int, int, bool) {
	if j >= len(words) || words[j] != "(" {
		return nil, j, true
	}
	var params []int
	k := j + 1
	for k < len(words) {
		switch words[k] {
		case ")":
			return params, k + 1, true
		case ",":
			k++
			continue
		}
		n, err := strconv.Atoi(words[k])
		if err != nil {
			return nil, j, false
		}
		params = append(params, n)
		k++
	}
	return nil, j, false
}

// MySQLFamily is the pair Ptah's MySQL rules are defined for.
var MySQLFamily = []string{"mysql", "mariadb"}

// ClauseEndFrom is the end of the clause a word index sits in.
func ClauseEndFrom(w []string, index int) int {
	end := len(w)
	for _, start := range ClauseStarts(w) {
		if start > index {
			end = min(end, start-1)
		}
	}
	return end
}

// ScanModifyChange reports whether an ALTER TABLE statement rewrites a column
// via the MySQL-family MODIFY/CHANGE clauses (COLUMN keyword optional).
func ScanModifyChange(w []string) bool {
	for _, i := range ClauseStarts(w) {
		if i >= len(w) || (w[i] != "MODIFY" && w[i] != "CHANGE") {
			continue
		}
		j := i + 1
		if j < len(w) && w[j] == "COLUMN" {
			j++
		}
		j = SkipIfExists(w, j)
		if j < len(w) && IdentLike(w[j]) {
			return true
		}
	}
	return false
}

// NonNullableAddedColumnSubjects are the columns an ALTER TABLE adds NOT NULL
// without a DEFAULT, each with the table and the type as written.
func NonNullableAddedColumnSubjects(w, sourceWords []string) []lintext.Subject {
	table := AlterTableReference(w, sourceWords)
	var subjects []lintext.Subject
	for _, i := range ClauseStarts(w) {
		start, end, ok := AddColumnClause(w, i)
		if !ok {
			continue
		}
		clause := w[start:end]
		if HasWordSeq(clause, "NOT", "NULL") && !slices.Contains(clause, "DEFAULT") {
			subjects = append(subjects, lintext.Subject{
				Kind:     lintext.SubjectColumn,
				Name:     SourceWordAt(w, sourceWords, start-1),
				Parent:   table.Name,
				DataType: ColumnDataType(clause, sourceWords[start:end]),
			})
		}
	}
	return subjects
}

// ColumnDataType is the type part of a column definition as written: the
// words before the first constraint word outside parentheses.
func ColumnDataType(definition, sourceDefinition []string) string {
	end := len(definition)
	depth := 0
	for i, word := range definition {
		switch word {
		case "(", "[":
			depth++
		case ")", "]":
			depth = max(0, depth-1)
		default:
			if depth == 0 && ColumnConstraintWord(word) {
				end = i
				return strings.Join(sourceDefinition[:end], " ")
			}
		}
	}
	return strings.Join(sourceDefinition[:end], " ")
}

// ColumnConstraintWord reports a word that starts a column constraint or
// attribute after the type.
func ColumnConstraintWord(word string) bool {
	switch word {
	case "CHECK", "COLLATE", "COMMENT", "CONSTRAINT", "DEFAULT", "GENERATED",
		"IDENTITY", "NOT", "NULL", "PRIMARY", "REFERENCES", "UNIQUE":
		return true
	default:
		return false
	}
}

// DroppedTablesNotCreated returns tables that were not created earlier in the
// same file and whether every target was parsed. An incomplete parse must remain
// destructive because treating an unknown target as safe would be fail-open.
func DroppedTablesNotCreated(
	w,
	sourceWords []string,
	created map[string]bool,
) ([]TableReference, bool) {
	var unsafe []TableReference
	j := SkipIfExists(w, 2)
	for {
		if j < len(w) && w[j] == "ONLY" {
			j++
		}
		ref, next := TableRefAt(w, sourceWords, j)
		if ref.Normalized == "" {
			return unsafe, false
		}
		if !RefersToCreated(created, ref.Normalized) {
			unsafe = append(unsafe, ref)
		}
		if next < len(w) && w[next] == "*" {
			next++
		}
		if next < len(w) && w[next] == "," {
			j = next + 1
			continue
		}
		return unsafe, true
	}
}
