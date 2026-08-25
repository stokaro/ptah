package parser

import (
	"strconv"
	"strings"

	"go.5x5.cz/ptah/internal/lexer"
)

// isSQLServerGoBatchSeparatorAt reports whether a GO token is a SQL Server
// utility batch separator command on its own line. Identifiers such as
// "AS go" or variables such as "@go" are ordinary T-SQL tokens.
//
// The statement splitter in core/sqlutil keeps a parallel scan of the same
// shape because it also extracts the optional GO repeat count, which the
// parser skips as ordinary tokens.
func isSQLServerGoBatchSeparatorAt(input string, start, end int) bool {
	if !sqlServerGoLinePrefixIsEmpty(input, start) {
		return false
	}
	return sqlServerGoTrailerIsBatchSeparator(input, end)
}

func sqlServerGoLinePrefixIsEmpty(input string, start int) bool {
	for i := start - 1; i >= 0 && input[i] != '\n' && input[i] != '\r'; i-- {
		if input[i] != ' ' && input[i] != '\t' {
			return false
		}
	}
	return true
}

// sqlServerGoTrailerIsBatchSeparator reports whether everything between a GO
// token and the end of its line is batch separator trailer: an optional GO
// repeat count, comments, and horizontal space.
func sqlServerGoTrailerIsBatchSeparator(input string, pos int) bool {
	i := pos
	consumedCount := false
	for {
		i = skipSQLServerHorizontalSpace(input, i)
		if !consumedCount && i < len(input) && input[i] >= '0' && input[i] <= '9' {
			consumedCount = true
			countStart := i
			i = skipSQLServerDigits(input, i)
			if _, err := strconv.Atoi(input[countStart:i]); err != nil {
				return false
			}
			continue
		}
		switch {
		case i >= len(input) || input[i] == '\n' || input[i] == '\r':
			return true
		case strings.HasPrefix(input[i:], "--"):
			return true
		case strings.HasPrefix(input[i:], "/*"):
			next, ok := skipSQLServerBlockComment(input, i)
			if !ok {
				return false
			}
			i = next
		default:
			return false
		}
	}
}

func skipSQLServerHorizontalSpace(input string, pos int) int {
	for pos < len(input) && (input[pos] == ' ' || input[pos] == '\t') {
		pos++
	}
	return pos
}

func skipSQLServerDigits(input string, pos int) int {
	for pos < len(input) && input[pos] >= '0' && input[pos] <= '9' {
		pos++
	}
	return pos
}

func skipSQLServerBlockComment(input string, pos int) (int, bool) {
	commentEnd := strings.Index(input[pos+2:], "*/")
	if commentEnd == -1 {
		return pos, false
	}
	return pos + commentEnd + len("/**/"), true
}

// isScalarIFExpressionFragment reports whether fragment starts with a MySQL
// scalar IF(...) expression tail rather than a procedural IF ... THEN block.
func isScalarIFExpressionFragment(fragment string) bool {
	l := lexer.NewLexer(fragment)
	tok := nextNonTriviaToken(l)
	if !tok.MatchOperatorValue("(") {
		return false
	}

	depth := 1
	for depth > 0 {
		tok = l.NextToken()
		switch {
		case tok.Type == lexer.TokenEOF:
			return true
		case tok.MatchOperatorValue("("):
			depth++
		case tok.MatchOperatorValue(")"):
			depth--
		}
	}

	tok = nextNonTriviaToken(l)
	return !tok.MatchIdentifierValue("THEN")
}

func nextNonTriviaToken(l *lexer.Lexer) lexer.Token {
	for {
		tok := l.NextToken()
		if tok.Type != lexer.TokenWhitespace && tok.Type != lexer.TokenComment {
			return tok
		}
	}
}
