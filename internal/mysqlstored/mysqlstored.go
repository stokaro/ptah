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
// from the bytes after the @, such as localhost.localdomain or $, and an
// executable comment the server runs, which [Tokens] reads as part of the
// statement. A CREATE or ALTER whose head it cannot follow to a kind of
// object it knows is an error, never a statement that defines nothing, so a
// spelling it misses is refused rather than let through.
package mysqlstored

import (
	"errors"
	"fmt"
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
	// user. A host the server reads unquoted, such as localhost.localdomain,
	// 10.0.0.1 or $, is one identifier token holding the bytes the server
	// reads as the name. An @ with no name directly behind it, as in app@
	// TRIGGER, which MySQL reads as the host '' and MariaDB as '%', is an
	// empty one.
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

// ErrUnreadableDefinition is the error [Read] wraps for a CREATE or ALTER
// statement whose head it cannot follow to a kind of object it knows. Such a
// statement can define a stored object in a spelling the reader misses, so a
// caller that has to know refuses it rather than guess.
var ErrUnreadableDefinition = errors.New("a CREATE or ALTER statement whose kind of object is not known")

// Read reads the head of statement from its significant tokens, as [Tokens]
// returns them for statement: each token's position is a byte offset in
// statement. It reports true for a CREATE or ALTER of a stored object, and
// false with a nil error for any other statement and for a CREATE or ALTER of
// an object that keeps no body, such as a table, an index, a database, a
// user, a sequence or a tablespace.
//
// It reads CREATE [OR REPLACE] [ALGORITHM = ...] [DEFINER = account]
// [SQL SECURITY ...] and the same after ALTER, then the kind -- AGGREGATE
// FUNCTION, PACKAGE BODY and the single words -- and IF NOT EXISTS and the
// name. The account is read as the server reads it. Its host is one quoted
// token directly after the @, or else the bytes directly after it that the
// server reads as a host name: ASCII letters and digits, '.', '_', '$' and
// every byte outside ASCII. A keyword is matched in ASCII letters of either
// case alone, as the server matches it, and a keyword written in quotes is a
// name to the server, never a keyword, and reads as one here too. A SET
// STATEMENT ... FOR reads as the statement after FOR; see [Wrapped].
//
// It returns an error wrapping [ErrUnreadableDefinition] for a CREATE or
// ALTER it cannot read that way: a DEFINER clause with no account, a token
// that runs past the end of the host the server reads, a kind it does not
// know after an ALGORITHM, DEFINER or SQL SECURITY clause, which only a view
// and a stored object take, or a first word after CREATE [OR REPLACE] or
// ALTER that names no kind of object it knows.
func Read(statement string, tokens []lexer.Token) (Definition, bool, error) {
	if start, ok := Wrapped(tokens); ok {
		return Read(statement, tokens[start:])
	}
	if !isWord(tokens, 0, "CREATE") && !isWord(tokens, 0, "ALTER") {
		return Definition{}, false, nil
	}
	verb := asciiUpper(tokens[0].Value)
	definition := Definition{Verb: verb}
	at := 1
	if verb == "CREATE" {
		at = skipWords(tokens, at, "OR", "REPLACE")
	}
	head := at
	at = skipAssignment(tokens, at, "ALGORITHM")
	if isWord(tokens, at, "DEFINER") {
		definer, next, err := readDefiner(statement, tokens, at+1)
		if err != nil {
			return Definition{}, false, err
		}
		definition.Definer, at = &definer, next
	}
	if isWord(tokens, at, "SQL") && isWord(tokens, at+1, "SECURITY") {
		at += 3
	}
	kind, next, ok := readKind(tokens, at)
	switch {
	case !ok && at == head && keepsNoBody(verb, tokens, at):
		return Definition{}, false, nil
	case !ok:
		return Definition{}, false, unreadableDefinition(statement, tokens, at, "no kind of object it knows stands there")
	}
	definition.Kind = kind
	at = next
	if verb == "CREATE" {
		at = skipWords(tokens, at, "IF", "NOT", "EXISTS")
	}
	definition.Name, at = readName(tokens, at)
	definition.writesBody = verb == "ALTER" && kind == Event && containsWord(tokens[at:], "DO")
	return definition, true, nil
}

// keepsNoBody reports whether the words at at, directly after CREATE [OR
// REPLACE] or ALTER, name a kind of object that keeps no body, in a spelling
// MySQL 8.4 or MariaDB 11.8 accepts.
func keepsNoBody(verb string, tokens []lexer.Token, at int) bool {
	spellings := createdWithoutBody
	if verb == "ALTER" {
		spellings = alteredWithoutBody
	}
	for _, words := range spellings {
		if skipWords(tokens, at, words...) != at {
			return true
		}
	}
	return false
}

// createdWithoutBody are the spellings of a kind of object CREATE makes that
// keeps no body. A word that is not here, after CREATE, makes the statement
// one [Read] cannot read.
var createdWithoutBody = [][]string{
	{"TABLE"}, {"TEMPORARY", "TABLE"}, {"TEMPORARY", "SEQUENCE"}, {"SEQUENCE"},
	{"INDEX"}, {"UNIQUE", "INDEX"}, {"FULLTEXT", "INDEX"}, {"SPATIAL", "INDEX"}, {"VECTOR", "INDEX"},
	{"DATABASE"}, {"SCHEMA"}, {"USER"}, {"ROLE"}, {"SERVER"},
	{"TABLESPACE"}, {"UNDO", "TABLESPACE"}, {"LOGFILE", "GROUP"}, {"RESOURCE", "GROUP"},
	{"SPATIAL", "REFERENCE", "SYSTEM"},
}

// alteredWithoutBody are the spellings of a kind of object ALTER changes that
// keeps no body.
var alteredWithoutBody = [][]string{
	{"TABLE"}, {"ONLINE", "TABLE"}, {"IGNORE", "TABLE"}, {"ONLINE", "IGNORE", "TABLE"}, {"SEQUENCE"},
	{"DATABASE"}, {"SCHEMA"}, {"USER"}, {"SERVER"}, {"INSTANCE"},
	{"TABLESPACE"}, {"UNDO", "TABLESPACE"}, {"LOGFILE", "GROUP"}, {"RESOURCE", "GROUP"},
}

// unreadableDefinition is the error of a statement whose head [Read] cannot
// follow past the token at at, for reason.
func unreadableDefinition(statement string, tokens []lexer.Token, at int, reason string) error {
	near := "the end of the statement"
	if at < len(tokens) {
		near = fmt.Sprintf("%q", commentHead(statement[tokens[at].Start:]))
	}
	return fmt.Errorf("%w: at %s: %s", ErrUnreadableDefinition, near, reason)
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

// readDefiner reads `= account` from at, and returns an error where no
// account follows the = or the host cannot be read.
func readDefiner(statement string, tokens []lexer.Token, at int) (Definer, int, error) {
	if at >= len(tokens) || !tokens[at].MatchOperatorValue("=") {
		return Definer{}, at, unreadableDefinition(statement, tokens, at, "DEFINER is not followed by =")
	}
	at++
	if at >= len(tokens) || tokens[at].Type != lexer.TokenIdentifier && tokens[at].Type != lexer.TokenString {
		return Definer{}, at, unreadableDefinition(statement, tokens, at, "DEFINER = names no account")
	}
	definer := Definer{User: tokens[at]}
	at++
	if isWord(tokens, at-1, "CURRENT_USER") || isWord(tokens, at-1, "CURRENT_ROLE") {
		if at+1 < len(tokens) && tokens[at].MatchOperatorValue("(") && tokens[at+1].MatchOperatorValue(")") {
			at += 2
		}
	}
	if at < len(tokens) && tokens[at].MatchOperatorValue("@") {
		host, next, err := readHost(statement, tokens, at)
		if err != nil {
			return Definer{}, at, err
		}
		definer.Host, at = &host, next
	}
	return definer, at, nil
}

// readHost reads the host after the @ at at, as the server reads it: a
// quoted name directly after the @, or else the bytes directly after it that
// the server reads as an unquoted host name, which may be none. It returns
// the index of the first token at or after the host's end, and an error where
// a token begins inside the host and ends past it, since the server and the
// lexer then disagree about where the host ends.
//
// Measured on MySQL 8.4.13 and MariaDB 11.8.9, app@localhost.localdomain names
// the host localhost.localdomain, root@$, root@x.$, root@1$ and root@$$ name
// the hosts $, x.$, 1$ and $$, app@ followed by a space names an empty host,
// and app@ 'h' and app @ h are refused. MySQL reads a byte outside ASCII as
// part of the host, as in root@hé, and MariaDB refuses that statement.
func readHost(statement string, tokens []lexer.Token, at int) (lexer.Token, int, error) {
	start := tokens[at].End
	next := at + 1
	if next < len(tokens) && tokens[next].Start == start && isQuoted(tokens[next]) {
		return tokens[next], next + 1, nil
	}
	end := start
	for end < len(statement) && isHostByte(statement[end]) {
		end++
	}
	for next < len(tokens) && tokens[next].Start < end {
		if tokens[next].End > end {
			return lexer.Token{}, next, unreadableDefinition(statement, tokens, next,
				"a token runs past the end of the host name the server reads")
		}
		next++
	}
	host := lexer.Token{Type: lexer.TokenIdentifier, Value: statement[start:end], Start: start, End: end}
	return host, next, nil
}

// isHostByte reports a byte the server reads as part of an unquoted host
// name: an ASCII letter or digit, '.', '_', '$', or a byte outside ASCII.
func isHostByte(b byte) bool {
	return 'a' <= b && b <= 'z' || 'A' <= b && b <= 'Z' || '0' <= b && b <= '9' ||
		b == '.' || b == '_' || b == '$' || b >= 0x80
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

// isWord reports an unquoted word at at, its ASCII letters in either case.
// The server matches a keyword that way, so `ſET` with a long s is a name
// there, never SET, though Unicode case folding reads the two as one.
func isWord(tokens []lexer.Token, at int, word string) bool {
	return at >= 0 && at < len(tokens) && tokens[at].Type == lexer.TokenIdentifier &&
		asciiUpper(tokens[at].Value) == word
}

// asciiUpper is s with its ASCII letters in upper case and every other byte
// as written.
func asciiUpper(s string) string {
	upper := []byte(s)
	for index, b := range upper {
		if 'a' <= b && b <= 'z' {
			upper[index] = b - 'a' + 'A'
		}
	}
	return string(upper)
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
