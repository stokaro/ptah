package sqlite

import (
	"slices"
	"strings"

	"ptah.run/core/platform/identifier"
)

// The checks in this file read the text SQLite stored for an index, a CHECK, a
// generated column, a view or a trigger, and answer one question: does it read
// a given column of a given table? A rebuild that drops the column would leave
// the object to fail when it runs, so the plan refuses it by name.
//
// They read names, not a parse tree. Comments and literals are skipped. A name
// is taken as the table's column only where the text leaves that open: a
// qualified name counts when its qualifier is the table, an alias the text
// gives it, or NEW and OLD in a trigger on it; an unqualified name counts in a
// statement that names the table. A name written into another table's column
// list, or set by an UPDATE of another table, belongs to that table. Where the
// text leaves the owner open, the check answers yes, so an uncertain reading
// refuses rather than breaks the object.

// sqlToken is one token of stored SQL text: an identifier without its quotes,
// a punctuation mark or an operator, or a literal.
type sqlToken struct {
	text    string
	word    bool
	quoted  bool
	literal bool
}

// is reports whether t is the bare keyword keyword.
func (t sqlToken) is(keyword string) bool {
	return t.word && !t.quoted && strings.EqualFold(t.text, keyword)
}

// tokenizeSQL splits text into tokens, without whitespace and comments.
func tokenizeSQL(text string) []sqlToken {
	var tokens []sqlToken
	for i := 0; i < len(text); {
		if next, skipped := skipSpaceOrComment(text, i); skipped {
			i = next
			continue
		}
		token, next := scanToken(text, i)
		tokens = append(tokens, token)
		i = next
	}
	return tokens
}

// skipSpaceOrComment returns the position after the whitespace character or
// the comment that starts at i, and whether one does. A comment that never
// closes runs to the end of text.
func skipSpaceOrComment(text string, i int) (int, bool) {
	switch {
	case strings.IndexByte(" \t\n\r\f", text[i]) >= 0:
		return i + 1, true
	case strings.HasPrefix(text[i:], "--"):
		if end := strings.IndexByte(text[i:], '\n'); end >= 0 {
			return i + end + 1, true
		}
		return len(text), true
	case strings.HasPrefix(text[i:], "/*"):
		if end := strings.Index(text[i+2:], "*/"); end >= 0 {
			return i + end + 4, true
		}
		return len(text), true
	default:
		return i, false
	}
}

// twoCharacterOperators are the operators a token takes two characters for.
var twoCharacterOperators = []string{"==", "!=", "<>", "<=", ">=", "||", "<<", ">>", "->"}

// scanToken returns the token that starts at i and the position after it.
func scanToken(text string, i int) (sqlToken, int) {
	ch := text[i]
	next := byte(0)
	if i+1 < len(text) {
		next = text[i+1]
	}
	switch {
	case ch == '\'':
		return sqlToken{text: "'", literal: true}, quotedEnd(text, i, '\'')
	case (ch == 'x' || ch == 'X') && next == '\'':
		return sqlToken{text: "'", literal: true}, quotedEnd(text, i+1, '\'')
	case ch == '"' || ch == '`' || ch == '[':
		return quotedIdentifier(text, i)
	case isIdentifierStart(ch):
		end := scanWhile(text, i, isIdentifierPart)
		return sqlToken{text: text[i:end], word: true}, end
	case isDigit(ch) || ch == '.' && isDigit(next):
		end := scanWhile(text, i, func(ch byte) bool { return isIdentifierPart(ch) || ch == '.' })
		return sqlToken{text: "0", literal: true}, end
	default:
		width := 1
		if i+1 < len(text) && slices.Contains(twoCharacterOperators, text[i:i+2]) {
			width = 2
		}
		return sqlToken{text: text[i : i+width]}, i + width
	}
}

// scanWhile returns the position of the first character from i on that part
// does not accept, or the end of text.
func scanWhile(text string, i int, part func(byte) bool) int {
	for i < len(text) && part(text[i]) {
		i++
	}
	return i
}

// quotedIdentifier returns the identifier quoted at i, in double quotes,
// backticks or brackets, without its quotes, and the position after it.
func quotedIdentifier(text string, i int) (sqlToken, int) {
	closing := map[byte]byte{'"': '"', '`': '`', '[': ']'}[text[i]]
	end := quotedEnd(text, i, closing)
	inner := text[i+1 : max(i+1, end-1)]
	if closing != ']' {
		inner = strings.ReplaceAll(inner, string([]byte{closing, closing}), string(closing))
	}
	return sqlToken{text: inner, word: true, quoted: true}, end
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

// quotedEnd returns the position after the quoted text that opens at start,
// where a doubled closing character stays inside it. Text that never closes
// ends at the end.
func quotedEnd(text string, start int, closing byte) int {
	for i := start + 1; i < len(text); i++ {
		if text[i] != closing {
			continue
		}
		if closing != ']' && i+1 < len(text) && text[i+1] == closing {
			i++
			continue
		}
		return i + 1
	}
	return len(text)
}

func isIdentifierStart(ch byte) bool {
	return ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= 0x80
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || isDigit(ch) || ch == '$'
}

// tokenAt returns the token at i, or the zero token past either end.
func tokenAt(tokens []sqlToken, i int) sqlToken {
	if i < 0 || i >= len(tokens) {
		return sqlToken{}
	}
	return tokens[i]
}

// columnReference reports whether the token at i names a column: an
// identifier that is not a qualifier, a function, or the collation after
// COLLATE.
func columnReference(tokens []sqlToken, i int) bool {
	token := tokens[i]
	if !token.word || tokenAt(tokens, i+1).text == "." || tokenAt(tokens, i-1).is("COLLATE") {
		return false
	}
	return token.quoted || tokenAt(tokens, i+1).text != "("
}

// expressionReads reports whether an expression over one table reads column,
// as a CHECK, a generated column or an index does: every column it names
// belongs to the table.
func expressionReads(expression, column string, semantics identifier.Semantics) bool {
	tokens := tokenizeSQL(expression)
	for i, token := range tokens {
		if columnReference(tokens, i) && semantics.ColumnIdentityKey(token.text) == semantics.ColumnIdentityKey(column) {
			return true
		}
	}
	return false
}

// indexReads reports whether the statement SQLite stored for an index reads
// column: in its key, a key expression or its WHERE clause.
func indexReads(definition, column string, semantics identifier.Semantics) bool {
	tokens := tokenizeSQL(definition)
	on := slices.IndexFunc(tokens, func(token sqlToken) bool { return token.is("ON") })
	if on < 0 {
		return expressionReads(definition, column, semantics)
	}
	start := on + 2
	if tokenAt(tokens, on+2).text == "." {
		start = on + 4
	}
	for i := start; i < len(tokens); i++ {
		if columnReference(tokens, i) && semantics.ColumnIdentityKey(tokens[i].text) == semantics.ColumnIdentityKey(column) {
			return true
		}
	}
	return false
}

// relationReference names a column a view or a trigger may read: column of
// table, where onTable says the trigger fires on table, so NEW and OLD name
// its rows.
type relationReference struct {
	table, column string
	onTable       bool
	semantics     identifier.Semantics
}

func (r relationReference) isTable(name string) bool {
	return r.semantics.QualifiedTableIdentityKey(name) == r.semantics.QualifiedTableIdentityKey(r.table)
}

func (r relationReference) isColumn(name string) bool {
	return r.semantics.ColumnIdentityKey(name) == r.semantics.ColumnIdentityKey(r.column)
}

// viewReads reports whether a view's query reads the column.
func (r relationReference) viewReads(query string) bool {
	return r.statementReads(tokenizeSQL(query))
}

// triggerReads reports whether a trigger reads the column: in the column list
// of UPDATE OF, in its WHEN clause or in a statement of its body. text is the
// statement SQLite stored, or the body alone.
func (r relationReference) triggerReads(text, when string) bool {
	tokens := tokenizeSQL(text)
	body := tokens
	if begin := slices.IndexFunc(tokens, func(token sqlToken) bool { return token.is("BEGIN") }); begin >= 0 {
		if r.headerReads(tokens[:begin]) {
			return true
		}
		body = tokens[begin+1:]
	}
	if last := len(body) - 1; last >= 0 && body[last].is("END") {
		body = body[:last]
	}
	if when != "" && r.statementReads(tokenizeSQL(when)) {
		return true
	}
	for statement := range splitStatements(body) {
		if r.statementReads(statement) {
			return true
		}
	}
	return false
}

// headerReads reports whether a trigger's header reads the column: the
// columns of UPDATE OF belong to the table the trigger fires on, and the WHEN
// clause is read as a statement.
func (r relationReference) headerReads(header []sqlToken) bool {
	for of := 1; r.onTable && of < len(header); of++ {
		if !header[of].is("OF") || !header[of-1].is("UPDATE") {
			continue
		}
		for i := of + 1; i < len(header) && !header[i].is("ON"); i++ {
			if header[i].word && r.isColumn(header[i].text) {
				return true
			}
		}
	}
	if when := slices.IndexFunc(header, func(token sqlToken) bool { return token.is("WHEN") }); when >= 0 {
		return r.statementReads(header[when+1:])
	}
	return false
}

// splitStatements yields the statements of a trigger body, split at each
// semicolon outside parentheses.
func splitStatements(tokens []sqlToken) func(func([]sqlToken) bool) {
	return func(yield func([]sqlToken) bool) {
		depth, start := 0, 0
		for i, token := range tokens {
			switch token.text {
			case "(":
				depth++
			case ")":
				depth--
			case ";":
				if depth == 0 && !token.literal {
					if !yield(tokens[start:i]) {
						return
					}
					start = i + 1
				}
			}
		}
		if start < len(tokens) {
			yield(tokens[start:])
		}
	}
}

// statementReads reports whether one statement reads the column.
func (r relationReference) statementReads(tokens []sqlToken) bool {
	names, aliases := r.tableReferences(tokens)
	others := otherTablesColumns(tokens, r.isTable)
	for i, token := range tokens {
		if others[i] || !columnReference(tokens, i) || !r.isColumn(token.text) {
			continue
		}
		if tokenAt(tokens, i-1).text != "." {
			if names {
				return true
			}
			continue
		}
		qualifier := tokenAt(tokens, i-2)
		if r.isTable(qualifier.text) || aliases[strings.ToLower(qualifier.text)] ||
			r.onTable && (qualifier.is("NEW") || qualifier.is("OLD")) {
			return true
		}
	}
	return false
}

// tableReferences reports whether a statement names the table, which lets
// an unqualified name in it be the table's column, and the aliases it gives
// the table, lower-cased.
func (r relationReference) tableReferences(tokens []sqlToken) (bool, map[string]bool) {
	names := false
	aliases := make(map[string]bool)
	for i, token := range tokens {
		if !token.word || tokenAt(tokens, i+1).text == "." || !r.isTable(token.text) {
			continue
		}
		names = true
		next := i + 1
		if tokenAt(tokens, next).is("AS") {
			next++
		}
		if alias := tokenAt(tokens, next); alias.word && !aliasKeywords[strings.ToUpper(alias.text)] {
			aliases[strings.ToLower(alias.text)] = true
		}
	}
	return names, aliases
}

// aliasKeywords are the keywords that may follow a table's name in a statement
// without being an alias of it.
var aliasKeywords = map[string]bool{
	"WHERE": true, "JOIN": true, "INNER": true, "LEFT": true, "RIGHT": true, "FULL": true, "CROSS": true,
	"NATURAL": true, "OUTER": true, "ON": true, "USING": true, "SET": true, "GROUP": true, "ORDER": true,
	"LIMIT": true, "UNION": true, "EXCEPT": true, "INTERSECT": true, "VALUES": true, "DEFAULT": true,
	"INDEXED": true, "NOT": true, "WINDOW": true, "HAVING": true, "RETURNING": true, "SELECT": true,
	"WITH": true, "AND": true, "OR": true, "END": true,
}

// otherTablesColumns marks the tokens of a statement that name another table's
// columns: the column list of an INSERT into another table, and the columns an
// UPDATE of another table sets. isTable recognizes the table being checked.
func otherTablesColumns(tokens []sqlToken, isTable func(string) bool) map[int]bool {
	marked := make(map[int]bool)
	for i, token := range tokens {
		switch {
		case token.is("INTO"):
			name := i + 1
			if tokenAt(tokens, name+1).text == "." {
				name += 2
			}
			if isTable(tokenAt(tokens, name).text) || tokenAt(tokens, name+1).text != "(" {
				continue
			}
			for j, depth := name+1, 0; j < len(tokens); j++ {
				depth += map[string]int{"(": 1, ")": -1}[tokens[j].text]
				marked[j] = true
				if depth == 0 {
					break
				}
			}
		case token.is("UPDATE") && i == 0:
			markSetTargets(tokens, isTable, marked)
		}
	}
	return marked
}

// markSetTargets marks the columns the SET clause of an UPDATE of another
// table assigns: each name directly before an equals sign outside
// parentheses, from SET to the WHERE, FROM or RETURNING that ends the clause.
func markSetTargets(tokens []sqlToken, isTable func(string) bool, marked map[int]bool) {
	target := 1
	if tokenAt(tokens, target).is("OR") {
		target += 2
	}
	if tokenAt(tokens, target+1).text == "." {
		target += 2
	}
	if isTable(tokenAt(tokens, target).text) {
		return
	}
	set := slices.IndexFunc(tokens, func(token sqlToken) bool { return token.is("SET") })
	if set < 0 {
		return
	}
	depth := 0
	for j := set + 1; j < len(tokens); j++ {
		token := tokens[j]
		depth += map[string]int{"(": 1, ")": -1}[token.text]
		if depth == 0 && (token.is("WHERE") || token.is("FROM") || token.is("RETURNING")) {
			return
		}
		if depth == 0 && token.word && tokenAt(tokens, j+1).text == "=" {
			marked[j] = true
		}
	}
}

// namesTable reports whether text names table anywhere outside its comments
// and literals: as a table, a qualifier or a word that happens to be spelled
// the same. A rename check only needs to know that it might.
func namesTable(text, table string, semantics identifier.Semantics) bool {
	key := semantics.QualifiedTableIdentityKey(table)
	return slices.ContainsFunc(tokenizeSQL(text), func(token sqlToken) bool {
		return token.word && semantics.QualifiedTableIdentityKey(token.text) == key
	})
}
