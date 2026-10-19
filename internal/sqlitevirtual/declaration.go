package sqlitevirtual

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlident"
)

// DeclarationRendered reports whether one table's module declaration appears in
// any of the rendered documents.
//
// The needle is the complete statement the renderer emits: CREATE VIRTUAL
// TABLE, the exact qualified name, USING, the module, the catalog-recorded
// arguments, and the terminator. Two looser rules were wrong on a table
// literally named `CREATE VIRTUAL TABLE`: searching for the bare keyword
// matched the HCL block `table "CREATE VIRTUAL TABLE"` and suppressed a real
// loss, and looking only for the name joined to USING let a comment such as
// `# "docs" USING fts5` pretend that lossy HCL carried the declaration.
// Checking only the statement prefix was also insufficient: the module
// arguments define the virtual table's columns and behavior.
//
// The module is spelled through the same helper the renderer writes it with, so
// a module whose name needs quoting -- `USING "fts-5"` -- is looked for the way
// it is emitted. Spelling it bare here reported a SQL document that carried the
// declaration as lossy, and strict compatibility refused it.
func DeclarationRendered(rendered []string, table Table) bool {
	qualifiedName := sqlident.Quote(platform.SQLite, table.Name)
	if table.Schema != "" {
		qualifiedName = sqlident.Quote(platform.SQLite, table.Schema) + "." + qualifiedName
	}
	prefix := "CREATE VIRTUAL TABLE " + qualifiedName +
		" USING " + sqlident.BareOrQuoted(platform.SQLite, table.Module)
	suffixes := []string{";"}
	if arguments := table.Arguments; arguments != "" {
		suffixes[0] = "(" + arguments + ");"
	} else {
		// SQLite creates the same object for a module with no argument list
		// and for an explicitly empty one. The catalog reader deliberately
		// represents both with empty Arguments.
		suffixes = append(suffixes, "();")
	}
	prefixKey := foldSQLiteASCII(prefix)
	for _, document := range rendered {
		for _, suffix := range suffixes {
			if documentContainsSQLiteDeclaration(document, prefix, prefixKey, suffix) {
				return true
			}
		}
	}
	return false
}

// documentContainsSQLiteDeclaration reports whether the exact canonical
// declaration starts at an executable line boundary. Arguments can contain
// newlines, so comparing one split line at a time would reject SQL that carries
// the declaration exactly. Text after the terminator must remain whitespace;
// this also prevents a longer or commented fragment from proving survival.
func documentContainsSQLiteDeclaration(document, prefix, prefixKey, suffix string) bool {
	statementLength := len(prefix) + len(suffix)
	lexr := lexer.NewLexerWithOptions(document, dialectlexer.Options(platform.SQLite))
	tokens := make([]lexer.Token, 0)
	for token := lexr.NextToken(); token.Type != lexer.TokenEOF; token = lexr.NextToken() {
		tokens = append(tokens, token)
	}
	for _, token := range tokens {
		if token.Type != lexer.TokenIdentifier || !strings.EqualFold(token.Value, "CREATE") {
			continue
		}
		lineStart := strings.LastIndexByte(document[:token.Start], '\n') + 1
		if !sqliteTriviaOnly(document, tokens, lineStart, token.Start) ||
			len(document)-token.Start < statementLength {
			continue
		}
		candidate := document[token.Start : token.Start+statementLength]
		if foldSQLiteASCII(candidate[:len(prefix)]) == prefixKey &&
			candidate[len(prefix):] == suffix &&
			sqliteLineRemainderIsTrivia(document, tokens, token.Start+statementLength) {
			return true
		}
	}
	return false
}

// sqliteLineRemainderIsTrivia reports whether an executable declaration owns
// the rest of its line. A trailing SQL comment remains trivia; ordinary text
// does not. Tokenizing the complete document is important because a block
// comment may start before the line under inspection and end inside it.
func sqliteLineRemainderIsTrivia(document string, tokens []lexer.Token, start int) bool {
	end := len(document)
	if offset := strings.IndexByte(document[start:], '\n'); offset >= 0 {
		end = start + offset
	}
	return sqliteTriviaOnly(document, tokens, start, end)
}

// sqliteTriviaOnly reports whether a document range contains only whitespace
// and bytes covered by SQLite comment tokens. The tokens come from the whole
// document so a range beginning in the middle of a multiline comment still
// sees that comment's lexical context. This helper is used only outside the
// declaration candidate: module argument bytes remain exact even when they
// look like SQL comments.
func sqliteTriviaOnly(document string, tokens []lexer.Token, start, end int) bool {
	cursor := start
	for _, token := range tokens {
		if token.Type != lexer.TokenComment || token.End <= start || token.Start >= end {
			continue
		}
		commentStart := max(start, token.Start)
		if commentStart > cursor && strings.TrimSpace(document[cursor:commentStart]) != "" {
			return false
		}
		cursor = max(cursor, min(end, token.End))
	}
	return strings.TrimSpace(document[cursor:end]) == ""
}

// foldSQLiteASCII applies SQLite's identifier comparison to a rendered SQL
// statement prefix. SQLite folds ASCII letters but leaves non-ASCII bytes
// distinct, so Unicode case conversion would let one quoted table declaration
// stand in for a different table whose name differs only by non-ASCII case.
// Module arguments are deliberately outside the prefix: they are module-owned
// data whose bytes remain significant.
func foldSQLiteASCII(value string) string {
	folded := []byte(value)
	for index, letter := range folded {
		if letter >= 'A' && letter <= 'Z' {
			folded[index] = letter + ('a' - 'A')
		}
	}
	return string(folded)
}
