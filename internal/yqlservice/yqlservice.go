// Package yqlservice finds a statement of Ptah's own in a YDB query: a
// YQL-shaped statement that YQL does not have, which Ptah's YDB connection
// runs through one of YDB's services instead of sending it to the query
// service.
//
// Ptah writes such a statement where YDB keeps state that no YQL statement
// changes, so plans and migration files stay text and every command that runs
// a migration runs it the same way. A comment, which YDB keeps as a user
// attribute of a table, is one (internal/ydbcomment). The package holds what
// every such statement shares: where it may stand in a query, which path
// prefix is in effect for it, and how a path it names resolves.
package yqlservice

import (
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Located is one of Ptah's own statements as a query holds it.
type Located struct {
	// Tokens are the statement's significant tokens: no whitespace, no
	// comment, no terminator.
	Tokens []lexer.Token
	// PathPrefix is the path the query's `PRAGMA TablePathPrefix` sets ahead
	// of the statement, or empty where it sets none.
	PathPrefix string
}

// Locate reports whether text is a query running a statement opens
// recognizes, and returns that statement.
//
// Such a query is the statement, preceded by nothing but definitions: the
// translation setting at its head, PRAGMA, DECLARE, DEFINE and IMPORT
// statements and named expressions, which a migration carries into every query
// that follows them. Of those, only `PRAGMA TablePathPrefix` changes what the
// statement names. A query holding two such statements, holding one that is
// not last, or holding one after a statement that is not a definition is
// refused with an error that says so, because service runs the one statement
// and nothing else; the caller wraps it with its own sentinel. Text that holds
// no such statement -- a string or a comment that merely mentions one
// included -- is not recognized and reports no error.
func Locate(text string, opens func([]lexer.Token) bool, service string) (Located, bool, error) {
	statements := sqlutil.SplitSourceStatements(text, platform.YDB)
	tokens := make([][]lexer.Token, len(statements))
	found := -1
	for i, statement := range statements {
		tokens[i] = SignificantTokens(statement.Text)
		if len(tokens[i]) == 0 || !opens(tokens[i]) {
			continue
		}
		if found >= 0 {
			return Located{}, true, fmt.Errorf("a query runs one %s statement, and this one holds two; "+
				"put each in a query of its own", FirstWords(tokens[i], 3))
		}
		found = i
	}
	if found < 0 {
		return Located{}, false, nil
	}
	opening := FirstWords(tokens[found], 3)
	if found != len(statements)-1 {
		return Located{}, true, fmt.Errorf("%s is followed by another statement in its query, and Ptah runs "+
			"it through YDB's %s, which takes it alone", opening, service)
	}
	located := Located{Tokens: tokens[found]}
	for _, definition := range tokens[:found] {
		prefix, isPrefix, err := readDefinition(definition, opening, service)
		if err != nil {
			return Located{}, true, err
		}
		if isPrefix {
			located.PathPrefix = prefix
		}
	}
	return located, true, nil
}

// SignificantTokens are the tokens of a statement that carry meaning: not
// whitespace, not comments, not a terminator, and not the translation setting
// at the head of a query.
func SignificantTokens(statement string) []lexer.Token {
	lexr := lexer.NewLexerWithOptions(statement, dialectlexer.Options(platform.YDB))
	var tokens []lexer.Token
	for {
		token := lexr.NextToken()
		switch {
		case token.Type == lexer.TokenEOF:
			return tokens
		case token.Type == lexer.TokenWhitespace, token.Type == lexer.TokenComment,
			token.Type == lexer.TokenSemicolon:
			continue
		case token.Type == lexer.TokenUnknown && strings.HasPrefix(token.Value, "--!"):
			continue
		default:
			tokens = append(tokens, token)
		}
	}
}

// FirstWords names a statement by up to count of its opening words, upper
// case, for a message.
func FirstWords(tokens []lexer.Token, count int) string {
	words := make([]string, 0, count)
	for _, token := range tokens[:min(count, len(tokens))] {
		words = append(words, strings.ToUpper(token.Value))
	}
	return strings.Join(words, " ")
}

// definitionVerbs open the statements that define a name or a setting for
// the rest of their query.
var definitionVerbs = []string{"PRAGMA", "DECLARE", "DEFINE", "IMPORT"}

// readDefinition checks that tokens are a definition, and reads the path a
// TablePathPrefix pragma sets.
func readDefinition(tokens []lexer.Token, statement, service string) (prefix string, isPrefix bool, err error) {
	first := tokens[0]
	isNamed := first.Type == lexer.TokenIdentifier && strings.HasPrefix(first.Value, "$")
	if !isNamed && !slices.ContainsFunc(definitionVerbs, first.MatchIdentifierValue) {
		return "", false, fmt.Errorf("%s shares its query with %s, and Ptah runs it through YDB's %s, "+
			"which takes it alone", statement, FirstWords(tokens, 3), service)
	}
	if !first.MatchIdentifierValue("PRAGMA") || len(tokens) < 2 || !tokens[1].MatchIdentifierValue("TablePathPrefix") {
		return "", false, nil
	}
	// PRAGMA TablePathPrefix("path") or PRAGMA TablePathPrefix = "path".
	rest := tokens[2:]
	var value lexer.Token
	switch {
	case len(rest) == 3 && rest[0].MatchOperatorValue("(") && rest[2].MatchOperatorValue(")"):
		value = rest[1]
	case len(rest) == 2 && rest[0].MatchOperatorValue("="):
		value = rest[1]
	default:
		return "", false, fmt.Errorf("%s follows a TablePathPrefix pragma Ptah cannot read; "+
			"write it as PRAGMA TablePathPrefix(\"/database/path\")", statement)
	}
	text, ok := StringValue(value)
	if !ok {
		return "", false, fmt.Errorf("%s follows a TablePathPrefix pragma whose path is not a plain "+
			"string", statement)
	}
	return text, true, nil
}

// StringValue reads a YQL string token -- single- or double-quoted, with or
// without the Utf8 suffix -- into the text it denotes. It undoes the escapes
// YQL reads in both quote styles: a backslash before a backslash or either
// quote, `\n`, `\r`, `\t`, and `\x` with two hex digits. It reports false for
// a token that is not such a string, and for an escape outside that set,
// rather than guess what the server would read.
func StringValue(token lexer.Token) (string, bool) {
	if token.Type != lexer.TokenString {
		return "", false
	}
	text := strings.TrimSuffix(token.Value, "u")
	if len(text) < 2 || (text[0] != '\'' && text[0] != '"') || text[len(text)-1] != text[0] {
		return "", false
	}
	quote := text[0]
	body := text[1 : len(text)-1]
	var b strings.Builder
	for i := 0; i < len(body); i++ {
		c := body[i]
		switch {
		case c == quote:
			return "", false
		case c != '\\':
			b.WriteByte(c)
			continue
		case i+1 == len(body):
			return "", false
		}
		i++
		switch body[i] {
		case '\\', '\'', '"':
			b.WriteByte(body[i])
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case 'x':
			if i+2 >= len(body) {
				return "", false
			}
			value, err := strconv.ParseUint(body[i+1:i+3], 16, 8)
			if err != nil {
				return "", false
			}
			b.WriteByte(byte(value))
			i += 2
		default:
			return "", false
		}
	}
	return b.String(), true
}

// Absolute is the absolute path of an object a statement names as name, on
// the database whose absolute path is database, under the TablePathPrefix
// prefix (empty for none).
//
// A relative prefix is refused, as YDB refuses one: measured on 25.1.4.7 and
// 26.2.1.14, `PRAGMA TablePathPrefix("relp"); CREATE TABLE t (...)` answers
// `Table path not in database, path: relp/t, database: /local`.
func Absolute(name, prefix, database string) (string, error) {
	database = "/" + strings.Trim(database, "/")
	if strings.HasPrefix(name, "/") {
		return path.Clean(name), nil
	}
	switch {
	case prefix == "":
		return path.Join(database, name), nil
	case strings.HasPrefix(prefix, "/"):
		return path.Join(prefix, name), nil
	default:
		return "", fmt.Errorf("path %s is not in database %s: the TablePathPrefix %q is relative, and YDB "+
			"reads a prefix only as an absolute path", path.Join(prefix, name), database, prefix)
	}
}

// Within reports whether absolute is a path strictly below root, both
// absolute, and returns it relative to root.
func Within(absolute, root string) (string, bool) {
	root = "/" + strings.Trim(root, "/")
	relative, inside := strings.CutPrefix(path.Clean(absolute), strings.TrimSuffix(root, "/")+"/")
	if !inside || relative == "" {
		return "", false
	}
	return relative, true
}
