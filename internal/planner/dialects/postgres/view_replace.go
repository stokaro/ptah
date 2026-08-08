package postgres

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// viewReplaceVerdict is what the mechanical test can say about replacing one
// view body with another.
//
// Three answers are needed rather than two, because "PostgreSQL will refuse
// this" and "this parser cannot tell" select different plans. A refusal is a
// fact about the change and holds in both directions. Not being able to tell is
// a fact about the parser, and the caller resolves it from the direction it is
// planning: see viewReplaceKeepsDependents.
type viewReplaceVerdict int

const (
	// viewReplaceUndecidable means at least one of the two bodies has a shape
	// this parser does not model, so nothing is known about the change: a WITH
	// prefix, a top-level set operation, a parenthesized query, a star
	// projection whose columns are not spelled out, a select item whose output
	// column name cannot be derived, or an empty previous body.
	viewReplaceUndecidable viewReplaceVerdict = iota

	// viewReplaceAppendsColumns means both bodies parsed, they read the same
	// relations, and the new select list is the old one with columns appended
	// to the end. That is the one shape PostgreSQL accepts for an in-place
	// replace.
	viewReplaceAppendsColumns

	// viewReplaceMovesColumns means both bodies parsed and the new select list
	// is NOT the old one with columns appended -- a column was dropped, renamed
	// or given a different type-determining expression -- or the two read
	// different relations, which fixes the types of the shared items and cannot
	// be checked from the select list alone. PostgreSQL refuses the replace for
	// the first group; for the second it may or may not, and answering "do not
	// replace" is the only appliable choice.
	viewReplaceMovesColumns
)

// viewReplaceLegality classifies a change from previousBody to nextBody.
//
// PostgreSQL accepts CREATE OR REPLACE VIEW only when the new query produces the
// old column list with columns appended to the end -- the same names, the same
// types, in the same order. Measured on PostgreSQL 17.10 against a view over
// (id bigint, email text, age integer):
//
//	append a trailing column   accepted
//	drop the appended column   ERROR: cannot drop columns from view
//	rename a column            ERROR: cannot change name of view column "id" to "uid"
//	change a column type       ERROR: cannot change data type of view column "id"
//	change only the predicate  accepted
//
// Column names are read off the select list, which is the only place a view's
// projection is written down. Two items are the same column when their output
// names match and either both are plain references to the same column, or both
// are the same expression text. That comparison sees through the two spellings
// the same view legitimately has -- "SELECT id FROM t" as authored and
// "SELECT t.id FROM t" as pg_get_viewdef reads it back -- while a cast, a rename
// or a different expression changes it.
//
// A select item's TYPE, though, is fixed by the relation it reads, and the
// select list does not say what that relation is. Comparing the items alone
// answered "appends only" for a swapped relation, which PostgreSQL then refused:
//
//	CREATE VIEW v AS SELECT id FROM b;            -- b.id is text
//	CREATE OR REPLACE VIEW v AS SELECT id FROM a; -- a.id is bigint
//	ERROR:  cannot change data type of view column "id" from text to bigint
//
// So the FROM/JOIN text is part of the comparison: any change to the relations,
// including one that merely spells them differently, answers
// viewReplaceMovesColumns. The clauses that do not decide a type -- WHERE,
// GROUP BY, HAVING, ORDER BY and the rest -- are excluded, so a predicate-only
// edit still answers viewReplaceAppendsColumns and keeps the cheap replace.
//
// "Spelled the same way" is decided by PostgreSQL's identifier rules rather
// than by letter case alone -- see normalizeExpression, which folds case
// everywhere except inside quoted identifiers, because "Foo" and "foo" are two
// relations.
//
// The residual assumption is narrow: the shared items read the same relations,
// spelled the same way, and those relations' columns were not retyped by the
// same migration. PostgreSQL refuses to alter a column type a view depends on,
// so reaching that last state requires dropping the view first, which is a
// different plan than this one.
func viewReplaceLegality(previousBody, nextBody string) viewReplaceVerdict {
	previous, previousFrom, ok := viewProjection(previousBody)
	if !ok {
		return viewReplaceUndecidable
	}
	next, nextFrom, ok := viewProjection(nextBody)
	if !ok {
		return viewReplaceUndecidable
	}
	if previousFrom != nextFrom {
		return viewReplaceMovesColumns
	}
	if len(next) < len(previous) {
		return viewReplaceMovesColumns
	}
	for i, item := range previous {
		if item != next[i] {
			return viewReplaceMovesColumns
		}
	}
	return viewReplaceAppendsColumns
}

// viewSelectItem is one entry of a view's top-level select list, reduced to the
// two properties PostgreSQL compares when replacing a view: the output column
// name, and whatever determines that column's type.
type viewSelectItem struct {
	// column is the output column name PostgreSQL records for the item.
	column string
	// source is the item's type-determining text: "col:" plus the referenced
	// column name for a plain reference, or "expr:" plus the normalized
	// expression otherwise. The prefixes keep a bare reference to "x" distinct
	// from an expression that happens to read "x".
	source string
}

// viewProjection parses a view body into its top-level select list and the
// normalized text of the relations it reads. The last result is false whenever
// the body has a shape this parser does not model, in which case the caller
// knows nothing about the change and must not treat the view as replaceable.
func viewProjection(body string) ([]viewSelectItem, string, bool) {
	projection, from, ok := viewProjectionText(body)
	if !ok {
		return nil, "", false
	}

	parts := splitTopLevel(projection, ',')
	items := make([]viewSelectItem, 0, len(parts))
	for _, part := range parts {
		item, ok := parseViewSelectItem(part)
		if !ok {
			return nil, "", false
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, "", false
	}
	return items, from, true
}

// viewProjectionText returns the text between the leading SELECT and the
// top-level FROM (or the end of the query when there is none), plus the
// normalized FROM/JOIN text that follows it.
//
// The FROM clause ends at the first top-level clause keyword that cannot
// introduce a relation, so a change to a predicate, a grouping or an ordering
// is not mistaken for a change to what the view reads.
func viewProjectionText(body string) (projection, from string, ok bool) {
	query := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(stripSQLComments(body)), ";"))
	if query == "" {
		return "", "", false
	}

	rest, ok := cutLeadingKeyword(query, "select")
	if !ok {
		// A WITH prefix or a parenthesized query. Both can project columns this
		// parser would have to resolve through another query level.
		return "", "", false
	}
	if after, ok := cutLeadingKeyword(rest, "all"); ok {
		rest = after
	} else if after, ok := cutLeadingKeyword(rest, "distinct"); ok {
		// DISTINCT ON (...) puts an expression list before the projection.
		if _, isOn := cutLeadingKeyword(after, "on"); isOn {
			return "", "", false
		}
		rest = after
	}

	// A top-level set operation resolves the result types across both branches,
	// so the first branch's select list does not decide them on its own.
	for _, keyword := range []string{"union", "intersect", "except"} {
		if indexTopLevelKeyword(rest, keyword) >= 0 {
			return "", "", false
		}
	}

	projection = rest
	if start := indexTopLevelKeyword(rest, "from"); start >= 0 {
		projection = rest[:start]
		from = normalizeExpression(rest[start+len("from") : viewFromClauseEnd(rest, start+len("from"))])
	}
	projection = strings.TrimSpace(projection)
	if projection == "" {
		return "", "", false
	}
	return projection, from, true
}

// viewFromClauseEnd returns the offset at which the FROM clause that starts at
// start gives way to a clause that cannot introduce a relation.
//
// Everything from FROM up to that point decides the types of the columns the
// select list projects; everything after it filters, groups or orders rows that
// already have their types. Keeping the split here is what lets a predicate-only
// edit stay replaceable while a swapped relation does not.
func viewFromClauseEnd(text string, start int) int {
	end := len(text)
	for _, keyword := range []string{"where", "group", "having", "window", "order", "limit", "offset", "fetch", "for"} {
		forEachTopLevelKeyword(text, keyword, func(offset int) {
			if offset >= start && offset < end {
				end = offset
			}
		})
	}
	return end
}

// parseViewSelectItem reduces one select-list entry to its output column and
// the text that fixes its type.
func parseViewSelectItem(text string) (viewSelectItem, bool) {
	expression := strings.TrimSpace(text)
	if expression == "" {
		return viewSelectItem{}, false
	}

	alias := ""
	if as := lastTopLevelKeyword(expression, "as"); as >= 0 {
		aliasText := strings.TrimSpace(expression[as+len("as"):])
		name, ok := plainIdentifier(aliasText)
		if !ok {
			return viewSelectItem{}, false
		}
		alias = name
		expression = strings.TrimSpace(expression[:as])
	}

	if expression == "" {
		return viewSelectItem{}, false
	}
	// A star projection does not spell its columns out, so neither their names
	// nor their count can be read here.
	if strings.HasSuffix(expression, "*") {
		return viewSelectItem{}, false
	}

	if column, ok := plainColumnReference(expression); ok {
		name := alias
		if name == "" {
			name = column
		}
		return viewSelectItem{column: name, source: "col:" + column}, true
	}

	// An expression with no alias takes an output name PostgreSQL derives from
	// the expression itself, which this parser does not reproduce.
	if alias == "" {
		return viewSelectItem{}, false
	}
	return viewSelectItem{column: alias, source: "expr:" + normalizeExpression(expression)}, true
}

// plainColumnReference returns the referenced column name when the expression
// is nothing but a (possibly qualified) column reference.
func plainColumnReference(expression string) (string, bool) {
	parts := splitTopLevel(expression, '.')
	if len(parts) == 0 || len(parts) > 3 {
		return "", false
	}
	last := ""
	for _, part := range parts {
		name, ok := plainIdentifier(part)
		if !ok {
			return "", false
		}
		last = name
	}
	return last, true
}

// plainIdentifier normalizes a single SQL identifier the way PostgreSQL does:
// an unquoted identifier folds to lower case, a quoted one keeps its spelling.
func plainIdentifier(text string) (string, bool) {
	name := strings.TrimSpace(text)
	if name == "" {
		return "", false
	}
	if strings.HasPrefix(name, `"`) {
		if len(name) < 2 || !strings.HasSuffix(name, `"`) {
			return "", false
		}
		inner := name[1 : len(name)-1]
		if inner == "" || strings.Contains(inner, `"`) {
			return "", false
		}
		return inner, true
	}
	for i, r := range name {
		if r == '_' || unicode.IsLetter(r) {
			continue
		}
		if i > 0 && (r == '$' || unicode.IsDigit(r)) {
			continue
		}
		return "", false
	}
	return strings.ToLower(name), true
}

// normalizeExpression collapses the differences that never change an
// expression's type: surrounding and repeated whitespace, and letter case
// outside quoted identifiers. Case folding is safe there because no SQL
// construct changes its result type with the case of its spelling -- a string
// literal's case does not alter that it is a string, and a function or type
// name is case-insensitive.
//
// A quoted identifier is the exception, and it is the whole reason this is not
// one call to strings.ToLower. PostgreSQL folds an unquoted identifier to lower
// case but keeps a quoted one exactly, so "Foo" and "foo" are two different
// relations. Folding them together answered "the relations did not change" for
// a swap between them, and the CREATE OR REPLACE VIEW that produced was refused
// on PostgreSQL 17.10 with `cannot change data type of view column "id" from
// bigint to text`. The contents of a quoted identifier are therefore copied
// through byte for byte, whitespace included, because "my view" and "my  view"
// are also two different relations.
//
// The quotation marks are kept, so a quoted identifier never compares equal to
// the same word unquoted. That is the conservative side: `"foo"` and `foo` are
// the same relation to PostgreSQL and this answers "changed", which costs a
// drop and recreate rather than an un-appliable replace.
//
// A string literal is skipped over rather than interpreted, so a quotation mark
// inside one does not open an identifier -- but its own case is still folded,
// because a literal decides no column's type.
func normalizeExpression(expression string) string {
	var out strings.Builder
	pendingSpace := false
	written := false

	writeSpace := func() {
		if pendingSpace && written {
			out.WriteByte(' ')
		}
		pendingSpace = false
	}

	for i := 0; i < len(expression); {
		switch {
		case expression[i] == '"':
			end := quotedIdentifierEnd(expression, i)
			writeSpace()
			out.WriteString(expression[i:end])
			written = true
			i = end
		case expression[i] == '\'':
			end := stringLiteralEnd(expression, i)
			writeSpace()
			out.WriteString(strings.ToLower(expression[i:end]))
			written = true
			i = end
		case isASCIISpace(expression[i]):
			pendingSpace = true
			i++
		default:
			writeSpace()
			r, size := utf8.DecodeRuneInString(expression[i:])
			out.WriteRune(unicode.ToLower(r))
			written = true
			i += size
		}
	}
	return out.String()
}

// stringLiteralEnd returns the offset just past the '...' literal that starts at
// start, treating a doubled quotation mark as one inside it.
func stringLiteralEnd(text string, start int) int {
	for i := start + 1; i < len(text); {
		switch {
		case text[i] == '\'' && i+1 < len(text) && text[i+1] == '\'':
			i += 2
		case text[i] == '\'':
			return i + 1
		default:
			i++
		}
	}
	return len(text)
}

func isASCIISpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\v' || b == '\f'
}

// quotedIdentifierEnd returns the offset just past the quoted identifier that
// starts at start, treating a doubled quotation mark as one inside it.
func quotedIdentifierEnd(text string, start int) int {
	for i := start + 1; i < len(text); {
		switch {
		case text[i] == '"' && i+1 < len(text) && text[i+1] == '"':
			i += 2
		case text[i] == '"':
			return i + 1
		default:
			i++
		}
	}
	return len(text)
}

// splitTopLevel splits on a separator that appears outside every parenthesis,
// single-quoted string and quoted identifier.
func splitTopLevel(text string, separator rune) []string {
	var parts []string
	var current strings.Builder
	scanSQL(text, func(r rune, topLevel bool) {
		if topLevel && r == separator {
			parts = append(parts, current.String())
			current.Reset()
			return
		}
		current.WriteRune(r)
	})
	return append(parts, current.String())
}

// indexTopLevelKeyword returns the byte offset of the first occurrence of a
// whole-word keyword outside every parenthesis and quote, or -1.
func indexTopLevelKeyword(text, keyword string) int {
	found := -1
	forEachTopLevelKeyword(text, keyword, func(offset int) {
		if found < 0 {
			found = offset
		}
	})
	return found
}

// lastTopLevelKeyword returns the byte offset of the last such occurrence.
func lastTopLevelKeyword(text, keyword string) int {
	found := -1
	forEachTopLevelKeyword(text, keyword, func(offset int) {
		found = offset
	})
	return found
}

// forEachTopLevelKeyword visits every whole-word, case-insensitive occurrence
// of keyword that lies outside every parenthesis and quote.
//
// The comparison is done against the original text rather than a lower-cased
// copy on purpose: lower-casing can change a string's byte length (U+0130
// folds to two runes), which would slide the offsets out from under the
// top-level marks computed on the original.
func forEachTopLevelKeyword(text, keyword string, visit func(offset int)) {
	offsets := topLevelOffsets(text)
	for i := 0; i+len(keyword) <= len(text); i++ {
		if !offsets[i] {
			continue
		}
		if !strings.EqualFold(text[i:i+len(keyword)], keyword) {
			continue
		}
		if i > 0 && isIdentifierByte(text[i-1]) {
			continue
		}
		if i+len(keyword) < len(text) && isIdentifierByte(text[i+len(keyword)]) {
			continue
		}
		visit(i)
	}
}

// topLevelOffsets marks the byte offsets that lie outside every parenthesis,
// single-quoted string and quoted identifier.
func topLevelOffsets(text string) []bool {
	marks := make([]bool, len(text))
	scanSQLIndexed(text, func(index int, _ rune, topLevel bool) {
		marks[index] = topLevel
	})
	return marks
}

func isIdentifierByte(b byte) bool {
	return b == '_' || b == '$' ||
		('0' <= b && b <= '9') ||
		('a' <= b && b <= 'z') ||
		('A' <= b && b <= 'Z')
}

// scanSQL walks the text reporting, for each rune, whether it sits outside
// every parenthesis, single-quoted string and quoted identifier.
func scanSQL(text string, visit func(r rune, topLevel bool)) {
	scanSQLIndexed(text, func(_ int, r rune, topLevel bool) { visit(r, topLevel) })
}

func scanSQLIndexed(text string, visit func(index int, r rune, topLevel bool)) {
	depth := 0
	inString := false
	inQuotedIdentifier := false
	for index, r := range text {
		switch {
		case inString:
			if r == '\'' {
				inString = false
			}
		case inQuotedIdentifier:
			if r == '"' {
				inQuotedIdentifier = false
			}
		case r == '\'':
			inString = true
		case r == '"':
			inQuotedIdentifier = true
		case r == '(':
			depth++
		case r == ')':
			depth--
		}
		visit(index, r, depth == 0 && !inString && !inQuotedIdentifier)
	}
}

// cutLeadingKeyword removes a leading whole-word keyword, reporting whether it
// was there.
func cutLeadingKeyword(text, keyword string) (string, bool) {
	trimmed := strings.TrimSpace(text)
	if len(trimmed) < len(keyword) {
		return text, false
	}
	if !strings.EqualFold(trimmed[:len(keyword)], keyword) {
		return text, false
	}
	rest := trimmed[len(keyword):]
	if rest != "" && isIdentifierByte(rest[0]) {
		return text, false
	}
	return strings.TrimSpace(rest), true
}

// stripSQLComments removes line and block comments so a comment can neither
// hide a keyword nor invent one.
func stripSQLComments(text string) string {
	var out strings.Builder
	inString := false
	inQuotedIdentifier := false
	for i := 0; i < len(text); i++ {
		switch {
		case inString:
			if text[i] == '\'' {
				inString = false
			}
		case inQuotedIdentifier:
			if text[i] == '"' {
				inQuotedIdentifier = false
			}
		case text[i] == '\'':
			inString = true
		case text[i] == '"':
			inQuotedIdentifier = true
		case strings.HasPrefix(text[i:], "--"):
			end := strings.IndexByte(text[i:], '\n')
			if end < 0 {
				return out.String()
			}
			i += end
			out.WriteByte('\n')
			continue
		case strings.HasPrefix(text[i:], "/*"):
			end := strings.Index(text[i+2:], "*/")
			if end < 0 {
				return out.String()
			}
			i += 2 + end + 1
			out.WriteByte(' ')
			continue
		}
		out.WriteByte(text[i])
	}
	return out.String()
}
