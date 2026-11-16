// Package mysqldefiner finds the definer accounts MySQL and MariaDB statements
// name, and decides before the first of them runs whether the connected
// account may name them.
//
// A CREATE or ALTER that writes DEFINER= for another account fails with ERROR
// 1227 unless the account that runs it holds the privilege to name another
// definer. MySQL and MariaDB run DDL outside a transaction, so a statement
// that fails in the middle of a schema apply or a migration run leaves the
// statements before it applied and, for a migration, the revision dirty. The
// apply paths ask [Check] before they send anything, so an account without
// the privilege is refused while nothing has changed.
package mysqldefiner

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/definer"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Named is one statement that names a definer account.
type Named struct {
	// Object is what the statement writes, as its kind and name, such as
	// "view my_docs" or "procedure app.refresh".
	Object string
	// Definer is the account the statement names, user@host, unquoted. A
	// definer written without a host is the account with host %, as the
	// server reads it.
	Definer string
}

// Scan returns the definers the statements name, one entry for each CREATE or
// ALTER of a view, routine, trigger or event that writes a DEFINER clause, in
// statement order. A clause naming CURRENT_USER names the account that runs
// the statement and is left out, as is a statement the lexer cannot follow to
// an account.
func Scan(statements []string) []Named {
	var named []Named
	for _, statement := range statements {
		if found, ok := scanStatement(statement); ok {
			named = append(named, found)
		}
	}
	return named
}

func scanStatement(statement string) (Named, bool) {
	tokens := significantTokens(statement)
	if len(tokens) < 2 {
		return Named{}, false
	}
	head := strings.ToUpper(tokens[0].Value)
	if tokens[0].Type != lexer.TokenIdentifier || head != "CREATE" && head != "ALTER" {
		return Named{}, false
	}
	at := 1
	at = skipWords(tokens, at, "OR", "REPLACE")
	at = skipAssignment(tokens, at, "ALGORITHM")
	if !isWord(tokens, at, "DEFINER") {
		return Named{}, false
	}
	account, next, ok := readAccount(tokens, at+1)
	if !ok {
		return Named{}, false
	}
	at = skipWords(tokens, next, "SQL", "SECURITY")
	if at != next {
		at++
	}
	object, ok := readObject(tokens, at)
	if !ok {
		return Named{}, false
	}
	return Named{Object: object, Definer: account}, true
}

// significantTokens lexes statement as MySQL does and drops whitespace and
// comments.
func significantTokens(statement string) []lexer.Token {
	lex := lexer.NewLexerWithOptions(statement, dialectlexer.Options(platform.MySQL))
	var tokens []lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		default:
			tokens = append(tokens, token)
		}
	}
}

// readAccount reads `= user[@host]` from at, and reports an account of
// CURRENT_USER, or one it cannot read, as not found.
func readAccount(tokens []lexer.Token, at int) (string, int, bool) {
	if at < len(tokens) && tokens[at].MatchOperatorValue("=") {
		at++
	}
	if at >= len(tokens) || tokens[at].Type != lexer.TokenIdentifier && tokens[at].Type != lexer.TokenString {
		return "", at, false
	}
	user := unquote(tokens[at])
	if tokens[at].Type == lexer.TokenIdentifier && strings.EqualFold(user, "CURRENT_USER") {
		return "", at, false
	}
	at++
	host := "%"
	if at+1 < len(tokens) && tokens[at].MatchOperatorValue("@") {
		host = unquote(tokens[at+1])
		at += 2
	}
	if user == "" || host == "" {
		return "", at, false
	}
	return user + "@" + host, at, true
}

// readObject reads the kind and the name of the object a statement writes.
func readObject(tokens []lexer.Token, at int) (string, bool) {
	if isWord(tokens, at, "AGGREGATE") {
		at++
	}
	if at >= len(tokens) {
		return "", false
	}
	kind := strings.ToUpper(tokens[at].Value)
	switch kind {
	case "VIEW", "PROCEDURE", "FUNCTION", "TRIGGER", "EVENT":
	default:
		return "", false
	}
	at = skipWords(tokens, at+1, "IF", "NOT", "EXISTS")
	if at >= len(tokens) || tokens[at].Type != lexer.TokenIdentifier {
		return "", false
	}
	name := unquote(tokens[at])
	if at+2 < len(tokens) && tokens[at+1].MatchOperatorValue(".") && tokens[at+2].Type == lexer.TokenIdentifier {
		name += "." + unquote(tokens[at+2])
	}
	return strings.ToLower(kind) + " " + name, true
}

// skipWords skips the words at at when all of them are there in order.
func skipWords(tokens []lexer.Token, at int, words ...string) int {
	for offset, word := range words {
		if !isWord(tokens, at+offset, word) {
			return at
		}
	}
	return at + len(words)
}

// skipAssignment skips `word = value` at at.
func skipAssignment(tokens []lexer.Token, at int, word string) int {
	if isWord(tokens, at, word) && at+2 < len(tokens) && tokens[at+1].MatchOperatorValue("=") {
		return at + 3
	}
	return at
}

func isWord(tokens []lexer.Token, at int, word string) bool {
	return at < len(tokens) && tokens[at].Type == lexer.TokenIdentifier && strings.EqualFold(tokens[at].Value, word)
}

// unquote returns a token's text without the backticks, single or double
// quotes around it, with a doubled quote read as one.
func unquote(token lexer.Token) string {
	value := token.Value
	if len(value) < 2 {
		return value
	}
	quote := value[0]
	if quote != '`' && quote != '\'' && quote != '"' || value[len(value)-1] != quote {
		return value
	}
	return strings.ReplaceAll(value[1:len(value)-1], string([]byte{quote, quote}), string(quote))
}

// sameAccount reports whether a named definer is the connected account.
func sameAccount(named, account string) bool {
	return definer.Replace(named, account) == definer.Kept
}
