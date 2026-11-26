package mysqlstored

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlmode"
)

// ServerFor is the server and session a statement is read for: dialect at
// versionID, as [VersionID] spells a version, under the modes of sqlMode
// that change where a string, a quoted name or a comment ends.
//
// A stored object keeps the sql_mode of the session that created it, and
// SHOW CREATE prints its definition in the spelling that mode reads: MariaDB
// prints the DEFINER of a trigger created under ANSI_QUOTES as "root"@"%",
// and a body stored under NO_BACKSLASH_ESCAPES holds a backslash as the
// character. So such a definition is read for the mode SHOW CREATE reports
// beside it, not for the session that asked.
func ServerFor(dialect string, versionID int, sqlMode string) Server {
	return Server{
		Dialect:            dialect,
		VersionID:          versionID,
		NoBackslashEscapes: sqlmode.Has(sqlMode, sqlmode.NoBackslashEscapes),
		AnsiQuotes:         sqlmode.Has(sqlMode, sqlmode.AnsiQuotes),
		BracketNames:       dialect == platform.MariaDB && sqlmode.Has(sqlMode, sqlmode.MSSQL),
	}
}

// ErrUnreadableBody is the error [Body] wraps for a definition whose body it
// cannot find, and [Respell] for a body it cannot write for another session.
var ErrUnreadableBody = errors.New("a stored body that cannot be read")

// Body returns the body of the trigger, stored function or stored procedure
// statement defines, where statement is the definition SHOW CREATE TRIGGER,
// SHOW CREATE FUNCTION or SHOW CREATE PROCEDURE prints and server is the
// server that printed it, under the sql_mode the object was stored with; see
// [ServerFor]. The body is the text after the head, from its first byte that
// is not whitespace, byte for byte as statement holds it.
//
// information_schema reports a body with its string literals rewritten:
// measured on MySQL 8.0.46, 8.4.13, 9.7.4 and 26.10.0, ACTION_STATEMENT,
// ROUTINE_DEFINITION and EVENT_DEFINITION hold each literal's value between
// its quotes, so 'a\'b' reads back as 'a'b', 'a\qb' as 'aqb', and a routine
// whose literal holds \0 is cut at it; on MariaDB 10.11.19, 11.8.9 and
// 12.3.3 they hold an escaped quote as two quotes and 'a\b' for 'a\\b',
// which is a backspace. SHOW CREATE keeps the body as the statement that created the
// object wrote it, on both servers.
//
// The head is read as both servers print it. A trigger's ends at FOR EACH ROW
// and the FOLLOWS or PRECEDES clause after it, where one is printed: MySQL
// prints a head of its own, and MariaDB the head as written, with the DEFINER
// clause put in after CREATE and the FOLLOWS clause left out. A function's
// ends after its parameter list, the RETURNS clause, which both print on the
// line it starts on, and the characteristics, which both print on lines of
// their own; MariaDB's ORACLE mode prints RETURN in place of RETURNS, and the
// AS or IS after the characteristics is the body's first word, as
// ROUTINE_DEFINITION holds it. A procedure's head is a function's without
// the RETURNS clause.
//
// A comment in the body is never read, so a body whose executable comment
// [Tokens] refuses is still returned. It returns an error wrapping
// [ErrUnreadableBody] for a statement that defines no trigger, function or
// procedure, a head it cannot follow, and a head that ends inside an
// executable comment, and one wrapping [ErrUnreadableComment] for a comment
// in the head it cannot read.
func Body(server Server, statement string) (string, error) {
	read := headRead{server: server}
	commentErr := read.collect(statement, 0, false)
	stored, ok, err := readHead(statement, read.tokens)
	definition, at := stored.definition, stored.next
	switch {
	case err != nil:
		return "", fmt.Errorf("%w: %w", ErrUnreadableBody, errors.Join(err, commentErr))
	case !ok || definition.Verb != "CREATE":
		return "", read.unreadable(statement, commentErr, "it is not a CREATE of a stored object")
	}
	var last int
	switch definition.Kind {
	case Trigger:
		last, ok = read.triggerHeadEnd(at)
	case Function, Procedure:
		last, ok = read.routineHeadEnd(statement, at, definition.Kind)
	default:
		return "", fmt.Errorf("%w: a %s keeps no body this reads", ErrUnreadableBody, strings.ToLower(definition.Kind))
	}
	switch {
	case !ok:
		return "", read.unreadable(statement, commentErr, "its head does not end where a "+strings.ToLower(definition.Kind)+"'s does")
	case read.commented[last]:
		return "", fmt.Errorf("%w: its head ends inside an executable comment", ErrUnreadableBody)
	}
	body := strings.TrimLeft(statement[read.tokens[last].End:], " \t\r\n\f\v")
	if body == "" {
		return "", fmt.Errorf("%w: nothing follows its head", ErrUnreadableBody)
	}
	return body, nil
}

// headRead is the significant tokens of a definition, as [Tokens] reads
// them, each beside whether it stands inside an executable comment.
type headRead struct {
	server    Server
	tokens    []lexer.Token
	commented []bool
}

// collect appends the significant tokens of text, which starts at offset in
// the statement, as [Tokens] reads them, and stops at the first comment it
// cannot read, keeping what it read before it. A head ends before its body
// begins, so a comment in the body that cannot be read leaves every token of
// the head read.
func (r *headRead) collect(text string, offset int, inComment bool) error {
	lex := lexer.NewLexerWithOptions(text, LexerOptions(r.server))
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return nil
		case lexer.TokenWhitespace, lexer.TokenSemicolon:
			continue
		case lexer.TokenComment:
			run, err := readComment(r.server, token.Value)
			if err != nil {
				return err
			}
			if run.runs {
				if err := r.collect(run.text, offset+token.Start+run.at, true); err != nil {
					return err
				}
			}
			continue
		}
		token.Start += offset
		token.End += offset
		r.tokens = append(r.tokens, token)
		r.commented = append(r.commented, inComment)
	}
}

// unreadable is the error of a head that cannot be followed for reason, with
// the comment that stopped the reading, if one did: the head may run on past
// it.
func (r *headRead) unreadable(statement string, commentErr error, reason string) error {
	err := fmt.Errorf("%w: %q: %s", ErrUnreadableBody, commentHead(statement), reason)
	return errors.Join(err, commentErr)
}

// triggerHeadEnd returns the index of the last token of a trigger's head,
// which starts at at: ROW in FOR EACH ROW, or the name a FOLLOWS or PRECEDES
// clause after it names. A table's name is quoted when it is a word, so the
// first unquoted FOR EACH ROW is the head's.
func (r *headRead) triggerHeadEnd(at int) (int, bool) {
	for index := at; index+2 < len(r.tokens); index++ {
		if !isWord(r.tokens, index, "FOR") || !isWord(r.tokens, index+1, "EACH") || !isWord(r.tokens, index+2, "ROW") {
			continue
		}
		last := index + 2
		if (isWord(r.tokens, last+1, "FOLLOWS") || isWord(r.tokens, last+1, "PRECEDES")) && last+2 < len(r.tokens) {
			last += 2
		}
		return last, true
	}
	return 0, false
}

// routineHeadEnd returns the index of the last token of the head of a
// routine of kind, Function or Procedure, which starts at at with the
// parameter list.
func (r *headRead) routineHeadEnd(statement string, at int, kind string) (int, bool) {
	last, ok := r.closingParenthesis(at)
	if ok && kind == Function {
		last, ok = r.returnsEnd(statement, last+1)
	}
	if !ok {
		return 0, false
	}
	return CharacteristicsEnd(r.tokens, last+1) - 1, true
}

// returnsEnd returns the index of the last token of a function's RETURNS
// clause, which starts at at. The clause starts on the line the parameter
// list ends on and is every token on that line, since its type takes words a
// body can start with, such as BINARY; a RETURN on a line of its own starts
// the body.
func (r *headRead) returnsEnd(statement string, at int) (int, bool) {
	returns := isWord(r.tokens, at, "RETURNS") || isWord(r.tokens, at, "RETURN")
	if !returns || r.newlineBefore(statement, at) {
		return 0, false
	}
	last := at
	for next := at + 1; next < len(r.tokens) && !r.newlineBefore(statement, next); next++ {
		last = next
	}
	return last, last != at
}

// newlineBefore reports whether a line ends between the token at index and
// the one before it.
func (r *headRead) newlineBefore(statement string, index int) bool {
	return strings.Contains(statement[r.tokens[index-1].End:r.tokens[index].Start], "\n")
}

// closingParenthesis returns the index of the parenthesis that closes the
// one at at.
func (r *headRead) closingParenthesis(at int) (int, bool) {
	if at >= len(r.tokens) || !r.tokens[at].MatchOperatorValue("(") {
		return 0, false
	}
	depth := 0
	for index := at; index < len(r.tokens); index++ {
		switch {
		case r.tokens[index].MatchOperatorValue("("):
			depth++
		case r.tokens[index].MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return index, true
			}
		}
	}
	return 0, false
}

// CharacteristicsEnd returns the index after the routine characteristics
// that begin at at among tokens, in any number and order: COMMENT 'text',
// LANGUAGE SQL, SQL SECURITY DEFINER or INVOKER, [NOT] DETERMINISTIC,
// CONTAINS SQL, NO SQL, READS SQL DATA and MODIFIES SQL DATA. It returns at
// itself where none begins there. No statement a body can start with begins
// with one of these words, so after the parameter list of a procedure the
// index is where its body begins.
func CharacteristicsEnd(tokens []lexer.Token, at int) int {
	for {
		next := characteristic(tokens, at)
		if next == at {
			return at
		}
		at = next
	}
}

// characteristic returns the index after the routine characteristic at at,
// and at itself where none stands there.
func characteristic(tokens []lexer.Token, at int) int {
	switch {
	case isWord(tokens, at, "COMMENT") && at+1 < len(tokens) && tokens[at+1].Type == lexer.TokenString:
		return at + 2
	case isWord(tokens, at, "LANGUAGE") && at+1 < len(tokens) && tokens[at+1].Type == lexer.TokenIdentifier:
		return at + 2
	case isWord(tokens, at, "SQL") && isWord(tokens, at+1, "SECURITY") && at+2 < len(tokens):
		return at + 3
	}
	for _, words := range characteristicWords {
		if next := skipWords(tokens, at, words...); next != at {
			return next
		}
	}
	return at
}

// characteristicWords are the routine characteristics written in words alone.
var characteristicWords = [][]string{
	{"NOT", "DETERMINISTIC"}, {"DETERMINISTIC"},
	{"CONTAINS", "SQL"}, {"NO", "SQL"}, {"READS", "SQL", "DATA"}, {"MODIFIES", "SQL", "DATA"},
}

// Respell writes body, a stored body as a session read as from reads it, so
// that a session read as to reads the same program. Where the two read a
// backslash and a double quote alike, body is returned as it is. Otherwise
// each string whose value to would read differently is written again between
// single quotes, as to reads one, and each name from reads between double
// quotes that to would read otherwise is written again between backquotes;
// every other byte stays. Square brackets are left as they are: a server whose
// mode reads them as quotes, MSSQL on MariaDB, is one whose bodies the reader
// refuses before it respells any.
//
// The server runs a body under the sql_mode it was stored with, so the same
// bytes created again under another mode can be another program: 'a\b' is
// a, a backslash and b under NO_BACKSLASH_ESCAPES and a and a backspace
// under the default mode, and "code" is a name under ANSI_QUOTES and a
// string without it.
//
// It returns an error wrapping [ErrUnreadableBody] for a string from cannot
// read, and for an executable comment holding a quote or a backslash, whose
// text it does not write again.
func Respell(body string, from, to Server) (string, error) {
	if from.NoBackslashEscapes == to.NoBackslashEscapes && from.AnsiQuotes == to.AnsiQuotes {
		return body, nil
	}
	fromOptions, toOptions := LexerOptions(from), LexerOptions(to)
	lex := lexer.NewLexerWithOptions(body, fromOptions)
	var b strings.Builder
	written := 0
	for token := lex.NextToken(); token.Type != lexer.TokenEOF; token = lex.NextToken() {
		literal := body[token.Start:token.End]
		var spelled string
		switch {
		case token.Type == lexer.TokenString:
			value, ok := lexer.StringValue(literal, fromOptions)
			if !ok {
				return "", fmt.Errorf("%w: the string %q cannot be read", ErrUnreadableBody, commentHead(literal))
			}
			if readsAs(literal, value, toOptions) {
				continue
			}
			spelled = quoteString(value, toOptions)
		case token.Type == lexer.TokenIdentifier && quotedName(literal, from) && !quotedName(literal, to):
			spelled = "`" + strings.ReplaceAll(Unquote(lexer.Token{Value: literal}), "`", "``") + "`"
		case token.Type == lexer.TokenComment && WrittenAsExecutable(literal) && strings.ContainsAny(literal, "'\"`\\"):
			return "", fmt.Errorf("%w: the executable comment %q holds a quote or a backslash", ErrUnreadableBody, commentHead(literal))
		default:
			continue
		}
		b.WriteString(body[written:token.Start])
		b.WriteString(spelled)
		written = token.End
	}
	b.WriteString(body[written:])
	return b.String(), nil
}

// readsAs reports whether options read literal as one string holding value.
func readsAs(literal, value string, options lexer.Options) bool {
	token := lexer.NewLexerWithOptions(literal, options).NextToken()
	if token.Type != lexer.TokenString || token.End != len(literal) {
		return false
	}
	read, ok := lexer.StringValue(literal, options)
	return ok && read == value
}

// quoteString writes value between single quotes as options read a string,
// each quote in it doubled and, where a backslash starts an escape, each
// backslash doubled.
func quoteString(value string, options lexer.Options) string {
	if options.BackslashEscapes {
		value = strings.ReplaceAll(value, `\`, `\\`)
	}
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// quotedName reports whether server reads literal, a name token, as a name
// between double quotes.
func quotedName(literal string, server Server) bool {
	return strings.HasPrefix(literal, `"`) && server.AnsiQuotes
}
