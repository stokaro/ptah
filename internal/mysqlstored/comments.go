package mysqlstored

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Server is the server, and the session on it, a statement is read for.
type Server struct {
	// Dialect is the server's dialect, MySQL or MariaDB.
	Dialect string
	// VersionID is the server's version as [VersionID] spells it, and 0 where
	// it is unknown.
	VersionID int
	// NoBackslashEscapes reports a session whose sql_mode names
	// NO_BACKSLASH_ESCAPES, which reads a backslash in a string as the
	// character rather than the start of an escape.
	NoBackslashEscapes bool
}

// ErrUnreadableComment is the error [Tokens] wraps for an executable comment
// whose effect on the server it cannot tell. Such a comment can hold the
// words that make the statement define a stored object, so a caller that has
// to know refuses the statement rather than guess.
var ErrUnreadableComment = errors.New("an executable comment whose effect on the server is not known")

// Tokens returns the significant tokens of statement as server reads it:
// whitespace, semicolons and comments are left out, and the text of each
// executable comment the server runs stands in its place, so a statement
// mysqldump writes as `/*!50003 CREATE*/ /*!50003 TRIGGER ... */` reads as
// CREATE TRIGGER. Each token keeps its position in statement.
//
// The comments read are these, measured on MySQL 8.4.13 and MariaDB 11.8.9:
//
//   - `/*! ... */` runs on both servers. Directly after the !, five or six
//     digits are a version, and the comment runs on a server at that version
//     or later; MariaDB leaves one gated on 50700 to 99999, which name MySQL
//     releases, unrun. One to four digits are no version: the server reads
//     them as part of the SQL.
//   - `/*M! ... */`, with a capital M, runs on MariaDB on the same terms and
//     is an ordinary comment on MySQL. `/*m! ... */` is an ordinary comment on
//     both.
//   - Where the server's version is unknown, a version gate before 50700
//     runs, as on every supported server.
//
// It wraps [ErrUnreadableComment] for a comment it cannot read that way: one
// that does not close; one holding another comment, which MySQL nests in a
// comment it skips and the lexer does not; a version of seven or more digits,
// which MySQL 8.4 reads as five digits and the rest as SQL, with a warning
// that this may change, and MariaDB as six; a gate on a later version than
// 50700 where the server's version is unknown; and a comment whose text,
// read as SQL, leaves a string, a quoted name or a line comment open at the
// comment's end, since the server reads such text past the */ the comment
// ends at.
func Tokens(server Server, statement string) ([]lexer.Token, error) {
	return appendTokens(server, nil, statement, 0)
}

// Statement is [Read] over [Tokens]: the head of statement, as server reads
// it.
func Statement(server Server, statement string) (Definition, bool, error) {
	tokens, err := Tokens(server, statement)
	if err != nil {
		return Definition{}, false, err
	}
	definition, ok := Read(tokens)
	return definition, ok, nil
}

// LexerOptions are the options [Tokens] reads a statement with for server:
// the dialect's lexer, every block comment read as a comment so the
// executable ones are decided here, and the session's rule for a backslash.
// MySQL and MariaDB read the same text the same way apart from those.
func LexerOptions(server Server) lexer.Options {
	dialect := platform.MySQL
	if server.Dialect == platform.MariaDB {
		dialect = platform.MariaDB
	}
	options := dialectlexer.Options(dialect)
	options.ExecutableComments = lexer.ExecutableCommentsNone
	options.BackslashEscapes = options.BackslashEscapes && !server.NoBackslashEscapes
	return options
}

// WrittenAsExecutable reports whether a block comment is written as an
// executable comment of either server, `/*!` or `/*M!` with the M in either
// case, whether or not a given server runs it. The replay guard, which does
// not know which server the replay reaches, refuses every such comment.
func WrittenAsExecutable(comment string) bool {
	return strings.HasPrefix(comment, "/*!") || len(comment) >= 4 && strings.EqualFold(comment[:4], "/*M!")
}

// appendTokens appends the significant tokens of text to tokens, each moved
// by offset, the position text starts at in the statement.
func appendTokens(server Server, tokens []lexer.Token, text string, offset int) ([]lexer.Token, error) {
	lex := lexer.NewLexerWithOptions(text, LexerOptions(server))
	for {
		token := lex.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens, nil
		case lexer.TokenWhitespace, lexer.TokenSemicolon:
			continue
		case lexer.TokenComment:
			run, err := readComment(server, token.Value)
			if err != nil {
				return nil, err
			}
			if run.runs {
				if tokens, err = appendTokens(server, tokens, run.text, offset+token.Start+run.at); err != nil {
					return nil, err
				}
			}
			continue
		}
		token.Start += offset
		token.End += offset
		tokens = append(tokens, token)
	}
}

// executed is what server does with a comment: whether it runs it, and the
// text it reads, which starts at at in the comment.
type executed struct {
	runs bool
	text string
	at   int
}

// readComment decides what server does with comment; see [Tokens].
func readComment(server Server, comment string) (executed, error) {
	marker := "/*!"
	if !strings.HasPrefix(comment, marker) {
		marker = "/*M!"
		if server.Dialect != platform.MariaDB || !strings.HasPrefix(comment, marker) {
			return executed{}, nil
		}
	}
	body, closed := strings.CutSuffix(comment[len(marker):], "*/")
	switch {
	case !closed:
		return executed{}, unreadable(comment, "it does not close")
	case strings.Contains(body, "/*"):
		return executed{}, unreadable(comment, "it holds another comment")
	}
	digits := len(body) - len(strings.TrimLeft(body, "0123456789"))
	run := executed{runs: true, text: body, at: len(marker)}
	switch {
	case digits >= 7:
		return executed{}, unreadable(comment, "its version has seven or more digits")
	case digits >= 5:
		runs, err := runsGate(server, marker == "/*!", number(body[:digits]))
		if err != nil {
			return executed{}, unreadable(comment, err.Error())
		}
		if !runs {
			return executed{}, nil
		}
		run.text, run.at = body[digits:], len(marker)+digits
	}
	if !closesAsSQL(server, run.text) {
		return executed{}, unreadable(comment, "a string, a quoted name or a comment in it reads past its end")
	}
	return run, nil
}

// runsGate reports whether server runs a comment gated on version gate,
// written `/*!` when bang and `/*M!` otherwise.
func runsGate(server Server, bang bool, gate int) (bool, error) {
	switch {
	case server.Dialect == platform.MariaDB && bang && gate >= 50700 && gate < 100000:
		return false, nil
	case server.VersionID != 0:
		return server.VersionID >= gate, nil
	case gate < 50700:
		return true, nil
	default:
		return false, errors.New("the server's version is not known")
	}
}

// closesAsSQL reports whether the server, reading text as SQL, reaches the
// */ the comment ends at: the lexer ends a comment at the first */, and the
// server ends the text of one it runs at the first */ it reads as SQL, so a
// string, a quoted name or a line comment that text leaves open would carry
// the server past it.
func closesAsSQL(server Server, text string) bool {
	lex := lexer.NewLexerWithOptions(text+"*/", LexerOptions(server))
	var last, beforeLast lexer.Token
	for {
		token := lex.NextToken()
		if token.Type == lexer.TokenEOF {
			break
		}
		beforeLast, last = last, token
	}
	return beforeLast.MatchOperatorValue("*") && beforeLast.Start == len(text) &&
		last.MatchOperatorValue("/") && last.Start == len(text)+1
}

// unreadable is the error of comment the server may read differently, for
// reason.
func unreadable(comment, reason string) error {
	return fmt.Errorf("%w: %q: %s", ErrUnreadableComment, commentHead(comment), reason)
}

// number is the value of digits, a run of at most six ASCII digits.
func number(digits string) int {
	value := 0
	for _, digit := range digits {
		value = value*10 + int(digit-'0')
	}
	return value
}

// commentHead is the start of comment, enough to find it in the statement.
func commentHead(comment string) string {
	const shown = 32
	for index := range comment {
		if index >= shown {
			return comment[:index] + "..."
		}
	}
	return comment
}
