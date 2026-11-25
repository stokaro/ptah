// Package mysqlstored reads the head of a MySQL or MariaDB statement that
// defines a stored object: a view, a stored function or procedure, a trigger,
// an event, or a MariaDB package or package body. It reads the statement as
// the server reads it, and three callers need the same answer from it. The
// dev-database replay guard (internal/devclean/mysqlreplay) refuses a
// statement that writes a stored body. The strict session a Ptah plan runs on
// (internal/connector/mysqlconnect) gives a statement whose object keeps the
// session's sql_mode the session's own mode back. The definer check
// (internal/mysqldefiner) finds the account a DEFINER clause names.
//
// A reader that misses a spelling the server accepts is a hole in the first
// two: the guard lets the body reach another database, and the body keeps the
// strict mode and fails the application's later statements. So the head is
// read as the server reads it, measured on MySQL 8.4 and MariaDB 11.8: a
// DEFINER clause after CREATE or ALTER, a host the server reads unquoted
// across several tokens, such as localhost.localdomain, and an executable
// comment the server runs, which [Tokens] reads as part of the statement.
package mysqlstored

import (
	"strings"

	"ptah.run/internal/lexer"
)

// The kinds of stored object a [Definition] names.
const (
	View        = "VIEW"
	Function    = "FUNCTION"
	Procedure   = "PROCEDURE"
	Trigger     = "TRIGGER"
	Event       = "EVENT"
	Package     = "PACKAGE"
	PackageBody = "PACKAGE BODY"
)

// Definition is what the head of a CREATE or ALTER statement says about the
// stored object the statement defines.
type Definition struct {
	// Verb is CREATE or ALTER.
	Verb string
	// Kind is the kind of object, one of [View], [Function], [Procedure],
	// [Trigger], [Event], [Package] and [PackageBody]. A loadable function
	// (CREATE FUNCTION ... SONAME) and an aggregate function are a
	// [Function].
	Kind string
	// Name is the object's name without its quotes, with its database and a
	// dot in front where the statement writes one, and "" where the reader
	// cannot follow the statement to a name.
	Name string
	// Definer is the account the statement's DEFINER clause names, and nil
	// where it writes none.
	Definer *Definer
	// writesBody reports an ALTER EVENT that writes DO, which replaces the
	// event's body.
	writesBody bool
}

// Definer is the account a DEFINER clause names, as the statement writes it.
type Definer struct {
	// User is the token that names the user: a name, a quoted name, a
	// string, or CURRENT_USER or CURRENT_ROLE, with or without the empty
	// parentheses the server accepts after those two.
	User lexer.Token
	// Host is the token that names the host, and nil where no @ follows the
	// user. A host the server reads unquoted across several tokens, such as
	// localhost.localdomain or 10.0.0.1, is one identifier token holding the
	// whole name. An @ with no name directly behind it, as in app@ TRIGGER,
	// which MySQL reads as the host '' and MariaDB as '%', is an empty one.
	Host *lexer.Token
}

// Body reports whether the object keeps a body the server runs later: every
// kind but a view. The replay guard refuses such a statement, a CREATE or an
// ALTER alike, since either can change what the body does or whose
// privileges it runs with.
func (d Definition) Body() bool {
	return d.Kind != "" && d.Kind != View
}

// KeepsSessionMode reports whether the server stores the sql_mode of the
// session that runs the statement with the object, and runs the body under
// it each time: a CREATE of an object with a body, and an ALTER EVENT that
// writes DO. Measured on MySQL 8.4 and MariaDB 11.8, an ALTER that leaves the
// body as it is keeps the mode the object has: ALTER PROCEDURE or ALTER
// FUNCTION, which change only characteristics such as COMMENT, and an ALTER
// EVENT without DO, such as RENAME TO or DISABLE. MariaDB has no ALTER
// PACKAGE.
func (d Definition) KeepsSessionMode() bool {
	return d.Body() && (d.Verb == "CREATE" || d.writesBody)
}

// Read reads the head of a statement from its significant tokens, as
// [Tokens] returns them, and reports false for any statement but a CREATE or
// ALTER of a stored object.
//
// It reads CREATE [OR REPLACE] [ALGORITHM = ...] [DEFINER = account]
// [SQL SECURITY ...] and the same after ALTER, then the kind -- AGGREGATE
// FUNCTION, PACKAGE BODY and the single words -- and IF NOT EXISTS and the
// name. The account is read as the server reads it. Its host is one quoted
// token directly after the @, or the run of unquoted tokens directly after
// it with nothing between them, which is how the server reads a host name;
// whitespace or a comment ends the run. A keyword written in quotes is a
// name to the server, never a keyword, and reads as one here too.
func Read(tokens []lexer.Token) (Definition, bool) {
	if len(tokens) == 0 {
		return Definition{}, false
	}
	verb := strings.ToUpper(tokens[0].Value)
	if !isWord(tokens, 0, "CREATE") && !isWord(tokens, 0, "ALTER") {
		return Definition{}, false
	}
	definition := Definition{Verb: verb}
	at := 1
	if verb == "CREATE" {
		at = skipWords(tokens, at, "OR", "REPLACE")
	}
	at = skipAssignment(tokens, at, "ALGORITHM")
	if isWord(tokens, at, "DEFINER") {
		definer, next, ok := readDefiner(tokens, at+1)
		if !ok {
			return Definition{}, false
		}
		definition.Definer, at = &definer, next
	}
	if isWord(tokens, at, "SQL") && isWord(tokens, at+1, "SECURITY") {
		at += 3
	}
	kind, next, ok := readKind(tokens, at)
	if !ok {
		return Definition{}, false
	}
	definition.Kind = kind
	at = next
	if verb == "CREATE" {
		at = skipWords(tokens, at, "IF", "NOT", "EXISTS")
	}
	definition.Name, at = readName(tokens, at)
	definition.writesBody = verb == "ALTER" && kind == Event && containsWord(tokens[at:], "DO")
	return definition, true
}

// readKind reads the kind of object at at.
func readKind(tokens []lexer.Token, at int) (string, int, bool) {
	if isWord(tokens, at, "AGGREGATE") && isWord(tokens, at+1, "FUNCTION") {
		return Function, at + 2, true
	}
	if isWord(tokens, at, "PACKAGE") && isWord(tokens, at+1, "BODY") {
		return PackageBody, at + 2, true
	}
	for _, kind := range []string{View, Function, Procedure, Trigger, Event, Package} {
		if isWord(tokens, at, kind) {
			return kind, at + 1, true
		}
	}
	return "", at, false
}

// readDefiner reads `= account` from at, and reports false where no account
// follows the =.
func readDefiner(tokens []lexer.Token, at int) (Definer, int, bool) {
	if at >= len(tokens) || !tokens[at].MatchOperatorValue("=") {
		return Definer{}, at, false
	}
	at++
	if at >= len(tokens) || tokens[at].Type != lexer.TokenIdentifier && tokens[at].Type != lexer.TokenString {
		return Definer{}, at, false
	}
	definer := Definer{User: tokens[at]}
	at++
	if isWord(tokens, at-1, "CURRENT_USER") || isWord(tokens, at-1, "CURRENT_ROLE") {
		if at+1 < len(tokens) && tokens[at].MatchOperatorValue("(") && tokens[at+1].MatchOperatorValue(")") {
			at += 2
		}
	}
	if at < len(tokens) && tokens[at].MatchOperatorValue("@") {
		host, next := readHost(tokens, at)
		definer.Host, at = &host, next
	}
	return definer, at, true
}

// readHost reads the host after the @ at at, as the server reads it: a
// quoted name directly after the @, or the run of unquoted name and number
// tokens and dots directly after it, each starting where the one before it
// ends. Measured on MySQL 8.4 and MariaDB 11.8, app@localhost.localdomain
// names the host localhost.localdomain, app@ followed by a space names an
// empty host, and app@ 'h' and app @ h are refused.
func readHost(tokens []lexer.Token, at int) (lexer.Token, int) {
	end := tokens[at].End
	host := lexer.Token{Type: lexer.TokenIdentifier, Start: end, End: end}
	next := at + 1
	if next < len(tokens) && tokens[next].Start == end && isQuoted(tokens[next]) {
		return tokens[next], next + 1
	}
	var name strings.Builder
	for next < len(tokens) && tokens[next].Start == end && hostPart(tokens[next]) {
		name.WriteString(tokens[next].Value)
		end = tokens[next].End
		next++
	}
	host.Value, host.End = name.String(), end
	return host, next
}

// isQuoted reports a string or a quoted name.
func isQuoted(token lexer.Token) bool {
	switch {
	case token.Type == lexer.TokenString:
		return true
	case token.Type == lexer.TokenIdentifier && token.Value != "":
		return token.Value[0] == '`' || token.Value[0] == '"'
	default:
		return false
	}
}

// hostPart reports a token an unquoted host name is made of: an unquoted
// name or number, or a dot.
func hostPart(token lexer.Token) bool {
	return token.MatchOperatorValue(".") || token.Type == lexer.TokenIdentifier && !isQuoted(token)
}

// readName reads the object's name at at, [database.]name, without quotes,
// and returns "" where no name is there.
func readName(tokens []lexer.Token, at int) (string, int) {
	if at >= len(tokens) || tokens[at].Type != lexer.TokenIdentifier {
		return "", at
	}
	name := Unquote(tokens[at])
	at++
	if at+1 < len(tokens) && tokens[at].MatchOperatorValue(".") && tokens[at+1].Type == lexer.TokenIdentifier {
		name += "." + Unquote(tokens[at+1])
		at += 2
	}
	return name, at
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

// containsWord reports whether word stands among tokens outside parentheses.
func containsWord(tokens []lexer.Token, word string) bool {
	depth := 0
	for index, token := range tokens {
		switch {
		case token.MatchOperatorValue("("):
			depth++
		case token.MatchOperatorValue(")"):
			depth = max(depth-1, 0)
		case depth == 0 && isWord(tokens, index, word):
			return true
		}
	}
	return false
}

// isWord reports an unquoted word at at, in any case.
func isWord(tokens []lexer.Token, at int, word string) bool {
	return at >= 0 && at < len(tokens) && tokens[at].Type == lexer.TokenIdentifier &&
		strings.EqualFold(tokens[at].Value, word)
}

// Unquote returns the text of a name or string token without the quotes
// around it, with each doubled quote inside read as one quote. Any other
// token's text is returned as written.
func Unquote(token lexer.Token) string {
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
