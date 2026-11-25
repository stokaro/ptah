// Package mysqlbody recognizes a MySQL-family statement that defines an
// executable body: a stored function, a stored procedure, a trigger or an
// event. The server keeps such a body and runs it later, under the sql_mode
// the session that created it had, and that is why two readers need the same
// answer. The dev-database replay guard confines what a body may do, and the
// strict session a Ptah plan runs on gives the statement the session's own
// sql_mode back, so the body does not keep the strict one.
package mysqlbody

import (
	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// Kind names the kind of executable object a CREATE or ALTER defines --
// FUNCTION, PROCEDURE, TRIGGER or EVENT -- or returns "" for any other
// statement. tokens are the statement's significant tokens, as
// [replay.Tokens] returns them. CREATE OR REPLACE, a DEFINER clause and an
// AGGREGATE FUNCTION are read; a loadable function (CREATE FUNCTION ...
// SONAME) is a FUNCTION.
func Kind(tokens []lexer.Token) string {
	if !replay.IsDDLAction(tokens) {
		return ""
	}
	first := replay.NormalizedIdentifier(tokens[0])
	if first != "CREATE" && first != "ALTER" {
		return ""
	}
	index := 1
	if first == "CREATE" && replay.TokenSequenceAt(tokens, index, "OR", "REPLACE") {
		index += 2
	}
	if first == "CREATE" && replay.TokenSequenceAt(tokens, index, "DEFINER") {
		index = skipDefiner(tokens, index+1)
	}
	if replay.TokenSequenceAt(tokens, index, "AGGREGATE", "FUNCTION") {
		return "FUNCTION"
	}
	for _, kind := range []string{"EVENT", "FUNCTION", "PROCEDURE", "TRIGGER"} {
		if replay.TokenSequenceAt(tokens, index, kind) {
			return kind
		}
	}
	return ""
}

// StatementKind is [Kind] for a statement as text. It reads the statement with
// the MySQL lexer and skips comments, so a comment in front of CREATE does not
// hide the kind. An executable comment (/*! ... */) is a comment to it: a
// statement written that way names no kind.
func StatementKind(statement string) string {
	return Kind(replay.Tokens(statement, LexerOptions()))
}

// LexerOptions are the options [StatementKind] reads a statement with: the
// MySQL lexer, with an executable comment read as a comment. MariaDB writes
// the same statements the same way.
func LexerOptions() lexer.Options {
	options := dialectlexer.Options(platform.MySQL)
	options.ExecutableComments = lexer.ExecutableCommentsNone
	return options
}

func skipDefiner(tokens []lexer.Token, index int) int {
	if index < len(tokens) && tokens[index].MatchOperatorValue("=") {
		index++
	}
	if index >= len(tokens) {
		return index
	}
	index++
	if index < len(tokens) && tokens[index].MatchOperatorValue("(") {
		index = skipBalancedParentheses(tokens, index)
	}
	if index < len(tokens) && tokens[index].MatchOperatorValue("@") {
		index += min(2, len(tokens)-index)
	}
	return index
}

func skipBalancedParentheses(tokens []lexer.Token, index int) int {
	depth := 0
	for index < len(tokens) {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth--
			if depth == 0 {
				return index + 1
			}
		}
		index++
	}
	return index
}
