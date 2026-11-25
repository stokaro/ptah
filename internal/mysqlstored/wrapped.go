package mysqlstored

import "ptah.run/internal/lexer"

// Wrapped returns the index of the first token of the statement tokens runs
// inside itself, and false when it runs none. Measured on MariaDB 11.8.9,
// both of these run the statement they hold:
//
//   - SET STATEMENT var = value [, ...] FOR statement runs statement with the
//     variables set for its duration;
//   - ANALYZE [FORMAT = name] statement runs a SELECT, INSERT, UPDATE, DELETE
//     or REPLACE and reports how it ran. ANALYZE TABLE is maintenance and holds
//     no statement.
//
// MySQL has neither form. Its EXPLAIN ANALYZE runs a SELECT and, measured on
// MySQL 8.4.13, leaves the rows of an UPDATE as they were, so it holds no
// statement here. [Read] reads the definition a SET STATEMENT holds, and the
// replay guard reads either held statement as its own.
func Wrapped(tokens []lexer.Token) (int, bool) {
	switch {
	case isWord(tokens, 0, "SET") && isWord(tokens, 1, "STATEMENT"):
		return statementAfterFor(tokens)
	case isWord(tokens, 0, "ANALYZE"):
		return analyzedStatement(tokens)
	default:
		return 0, false
	}
}

// statementAfterFor is the statement after the first FOR outside parentheses
// in a SET STATEMENT: a value is an expression, and only a subquery in
// parentheses can hold the word.
func statementAfterFor(tokens []lexer.Token) (int, bool) {
	depth := 0
	for index := 2; index < len(tokens)-1; index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth = max(depth-1, 0)
		case depth == 0 && isWord(tokens, index, "FOR"):
			return index + 1, true
		}
	}
	return 0, false
}

// analyzedStatement is the statement an ANALYZE runs, after an optional
// FORMAT = name. ANALYZE TABLE, TABLES, NO_WRITE_TO_BINLOG or LOCAL is the
// table maintenance statement, which runs no other.
func analyzedStatement(tokens []lexer.Token) (int, bool) {
	at := 1
	switch {
	case at >= len(tokens), isWord(tokens, at, "TABLE"), isWord(tokens, at, "TABLES"),
		isWord(tokens, at, "NO_WRITE_TO_BINLOG"), isWord(tokens, at, "LOCAL"):
		return 0, false
	case isWord(tokens, at, "FORMAT"):
		at += 3
	}
	if at >= len(tokens) {
		return 0, false
	}
	return at, true
}
