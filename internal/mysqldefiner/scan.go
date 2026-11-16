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
	"strconv"
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
	// definer written without a host is the account with host %, as MySQL
	// reads it.
	Definer string
	// Hostless reports that the statement wrote no host. MariaDB reads such a
	// definer as a role where one of that name exists, and the scan cannot
	// tell a role from an account.
	Hostless bool
}

// Scan returns the definers the statements name on server, one entry for each
// CREATE or ALTER of a view, routine, trigger, event or package that writes a
// DEFINER clause, in statement order.
//
// The text of an executable comment server runs is read as part of the
// statement: `/*!50013 DEFINER=... */` as mysqldump writes it, a comment gated
// on a version no later than the server's, and on MariaDB `/*M! ... */`. Where
// the server's version is unknown, only a comment every supported server runs
// is read, one with no version or a version before 5.7.
//
// A clause naming CURRENT_USER names the account that runs the statement and
// is left out. CURRENT_ROLE is an ordinary account name on MySQL and the
// current role on MariaDB, and is returned host-less, as MariaDB reads a role.
// A quoted name holding a backslash, whose meaning depends on the server's
// sql_mode, is left out, as is a statement the lexer cannot follow to an
// account.
func Scan(server Server, statements []string) []Named {
	var named []Named
	for _, statement := range statements {
		if found, ok := scanStatement(server, statement); ok {
			named = append(named, found)
		}
	}
	return named
}

func scanStatement(server Server, statement string) (Named, bool) {
	tokens := significantTokens(server, statement)
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
	return Named{Object: object, Definer: account.name, Hostless: account.hostless}, true
}

// significantTokens lexes statement as server does and drops whitespace and
// comments. An executable comment server runs is replaced by the tokens of its
// text.
func significantTokens(server Server, statement string) []lexer.Token {
	dialect := platform.MySQL
	if server.Dialect == platform.MariaDB {
		dialect = platform.MariaDB
	}
	lex := lexer.NewLexerWithOptions(statement, dialectlexer.Options(dialect))
	var tokens []lexer.Token
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		}
		if text, ok := executedCommentText(server, token); ok {
			tokens = append(tokens, significantTokens(server, text)...)
			continue
		}
		tokens = append(tokens, token)
	}
}

// executedCommentText returns the text of an executable comment server runs.
//
// The server runs `/*! ... */` with no version, and one gated on a version
// no later than its own, a five- or six-digit number spelled as
// [VersionID] spells a version; MariaDB also runs `/*M! ... */`, spelled with
// a capital M, on the same terms, and leaves a `/*!` gated on 5.7 to 9.9,
// which name MySQL releases, unrun. mysqldump writes its DEFINER clauses under 50013 and 50017, which
// every supported server runs. Where the server's version is unknown, a gated
// comment counts only before 5.7.
func executedCommentText(server Server, token lexer.Token) (string, bool) {
	if token.Type != lexer.TokenUnknown {
		return "", false
	}
	text, mysqlStyle := strings.CutPrefix(token.Value, "/*!")
	ok := mysqlStyle
	if !ok && server.Dialect == platform.MariaDB {
		text, ok = strings.CutPrefix(token.Value, "/*M!")
	}
	if !ok {
		return "", false
	}
	text, ok = strings.CutSuffix(text, "*/")
	if !ok {
		return "", false
	}
	digits := 0
	for digits < len(text) && text[digits] >= '0' && text[digits] <= '9' {
		digits++
	}
	if digits == 0 {
		return text, true
	}
	gate, err := strconv.Atoi(text[:digits])
	if err != nil || digits != 5 && digits != 6 {
		return "", false
	}
	return text[digits:], runsGatedComment(server, mysqlStyle, gate)
}

// runsGatedComment reports whether server runs a comment gated on version gate,
// written `/*!` when mysqlStyle and `/*M!` otherwise.
func runsGatedComment(server Server, mysqlStyle bool, gate int) bool {
	switch {
	case server.Dialect == platform.MariaDB && mysqlStyle && gate >= 50700 && gate < 100000:
		return false
	case server.VersionID == 0:
		return gate < 50700
	default:
		return server.VersionID >= gate
	}
}

// account is a definer as a statement writes it.
type account struct {
	// name is user@host, unquoted, with host % where none was written.
	name string
	// hostless reports that no host was written.
	hostless bool
}

// readAccount reads `= user[@host]` from at. An account of CURRENT_USER, or
// one it cannot read, is reported as not found.
func readAccount(tokens []lexer.Token, at int) (account, int, bool) {
	if at < len(tokens) && tokens[at].MatchOperatorValue("=") {
		at++
	}
	if at >= len(tokens) || tokens[at].Type != lexer.TokenIdentifier && tokens[at].Type != lexer.TokenString {
		return account{}, at, false
	}
	user, ok := accountPart(tokens[at])
	if !ok || tokens[at].Type == lexer.TokenIdentifier && strings.EqualFold(user, "CURRENT_USER") {
		return account{}, at, false
	}
	at++
	host, hostless := "%", true
	if at+1 < len(tokens) && tokens[at].MatchOperatorValue("@") {
		host, ok = accountPart(tokens[at+1])
		if !ok {
			return account{}, at, false
		}
		hostless = false
		at += 2
	}
	if user == "" || host == "" {
		return account{}, at, false
	}
	return account{name: user + "@" + host, hostless: hostless}, at, true
}

// accountPart returns the user or host a token spells, and false for a quoted
// string holding a backslash: under the default sql_mode the server reads it
// as an escape, and under NO_BACKSLASH_ESCAPES as itself.
func accountPart(token lexer.Token) (string, bool) {
	if token.Type == lexer.TokenString && strings.Contains(token.Value, `\`) {
		return "", false
	}
	return unquote(token), true
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
	case "PACKAGE":
		if isWord(tokens, at+1, "BODY") {
			kind, at = "PACKAGE BODY", at+1
		}
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
