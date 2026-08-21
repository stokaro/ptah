// Package viewprojection parses a view body into the two things a caller can
// reason about without a full SQL grammar: the top-level select list, and the
// text of the relations it reads.
//
// It exists because two features need the same answer for different reasons.
// The PostgreSQL planner asks whether CREATE OR REPLACE VIEW is legal, which
// turns on whether the output columns and their sources moved. Column-level
// lineage asks where a view column comes from, which is the same select list
// read for its references rather than for its shape (stokaro/ptah#1712).
//
// What it deliberately does not do is guess. A body whose shape it does not
// model answers false, and the caller is expected to say so rather than
// proceed on a parse it did not get.
package viewprojection

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// Item is one entry of a view's top-level select list, reduced to two
// properties: the output column name, and the text that determines where its
// value comes from.
//
// Both consumers read the same two fields for different questions. The planner
// compares them between two bodies to decide whether a replace keeps every
// column's type; lineage reads the source of one body to say which base column
// a view column flows from.
type Item struct {
	// Column is the output column name the server records for the item.
	Column string
	// Source is the item's value-determining text: sourceColumnPrefix plus the
	// referenced column name for a plain reference, or sourceExpressionPrefix
	// plus the normalized expression otherwise. The prefixes keep a bare
	// reference to "x" distinct from an expression that happens to read "x",
	// which is why this is one string rather than two fields -- the planner
	// compares items with ==, and a pair of fields would let a reference and an
	// expression compare equal by accident.
	Source string
}

const (
	// sourceColumnPrefix marks a Source that is a plain column reference.
	sourceColumnPrefix = "col:"
	// sourceExpressionPrefix marks a Source that is a computed expression.
	sourceExpressionPrefix = "expr:"
)

// ColumnReference returns the column name this item reads, and whether it reads
// one at all.
//
// An item that computes its value answers false. That is the honest answer for
// lineage rather than an empty name: "this column is derived and this parser
// does not open the expression" and "this column comes from nothing" are
// different facts, and only one of them is true.
func (i Item) ColumnReference() (string, bool) {
	rest, found := strings.CutPrefix(i.Source, sourceColumnPrefix)
	return rest, found
}

// Parse parses a view body into its top-level select list and the
// normalized text of the relations it reads. The last result is false whenever
// the body has a shape this parser does not model, in which case the caller
// knows nothing about the change and must not treat the view as replaceable.
func Parse(body string) ([]Item, string, bool) {
	projection, from, ok := viewProjectionText(body)
	if !ok {
		return nil, "", false
	}

	parts := splitTopLevel(projection, ',')
	items := make([]Item, 0, len(parts))
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
func parseViewSelectItem(text string) (Item, bool) {
	expression := strings.TrimSpace(text)
	if expression == "" {
		return Item{}, false
	}

	alias := ""
	if as := lastTopLevelKeyword(expression, "as"); as >= 0 {
		aliasText := strings.TrimSpace(expression[as+len("as"):])
		name, ok := plainIdentifier(aliasText)
		if !ok {
			return Item{}, false
		}
		alias = name
		expression = strings.TrimSpace(expression[:as])
	}

	if expression == "" {
		return Item{}, false
	}
	// A star projection does not spell its columns out, so neither their names
	// nor their count can be read here.
	if strings.HasSuffix(expression, "*") {
		return Item{}, false
	}

	if column, ok := plainColumnReference(expression); ok {
		name := alias
		if name == "" {
			name = column
		}
		return Item{Column: name, Source: sourceColumnPrefix + column}, true
	}

	// An expression with no alias takes an output name PostgreSQL derives from
	// the expression itself, which this parser does not reproduce.
	if alias == "" {
		return Item{}, false
	}
	return Item{Column: alias, Source: sourceExpressionPrefix + normalizeExpression(expression)}, true
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
