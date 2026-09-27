// Package mysqlcheck holds MySQL's rule for a CHECK written on a column: it may
// name that column and no other. The SQL reader refuses a column CHECK that
// names another column, because MySQL does, both in CREATE TABLE and in the
// column an ALTER TABLE adds or modifies. One implementation serves every
// place the reader meets such a CHECK.
package mysqlcheck

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// ErrNamesOtherColumn is the class of a CHECK written on a column that names
// another column, which MySQL refuses. See [OtherColumn].
var ErrNamesOtherColumn = errors.New("a CHECK written on a column names another column")

// Refusal is the error for a CHECK written on column that names other, in
// MySQL's words and with what to write instead. It wraps
// [ErrNamesOtherColumn].
func Refusal(column, other string) error {
	return fmt.Errorf(
		"%w: the CHECK on column %s names column %s, and MySQL answers ERROR 3813 (HY000) "+
			"\"Column check constraint ... references other column\"; write it as a table-level CHECK",
		ErrNamesOtherColumn, column, other)
}

// OtherColumn answers the first column of columns, other than column itself,
// that expression names, and false when it names none. Names are compared
// without case, as MySQL compares column names.
//
// MySQL refuses a CHECK written on a column that names another column.
// Measured on MySQL 8.4.11 and 26.7.0, `CREATE TABLE c (a int, b int CHECK (b
// > a))` is `ERROR 3813 (HY000): Column check constraint 'c_chk_1' references
// other column.`, and so is the same CHECK on a column ALTER TABLE adds or
// modifies, a second CHECK on the column, and `c.a` or “ `A` “ for `a`.
//
// A name counts only where MySQL reads a column: not in a string, not as a
// function, not as the type after AS, not as the unit after INTERVAL, the
// character set after USING or the collation after COLLATE, and not as the
// type of a typed literal. Measured on 8.4.11, a column named `year` beside
// `DATE_SUB('2020-01-01', INTERVAL 1 YEAR)` is accepted.
func OtherColumn(column, expression string, columns []string) (string, bool) {
	tokens := significantTokens(expression)
	operands := keywordOperands(tokens)
	for position := range tokens {
		name, ok := identifierAt(tokens, position)
		if !ok || operands[position] || operatorAt(tokens, position+1, "(") ||
			operatorAt(tokens, position+1, ".") || stringAt(tokens, position+1) {
			continue
		}
		for _, declared := range columns {
			if strings.EqualFold(declared, name) && !strings.EqualFold(declared, column) {
				return declared, true
			}
		}
	}
	return "", false
}

// keywordOperands marks the positions that hold the operand of a keyword whose
// operand is not a column: the type after AS, the character set after USING,
// the collation after COLLATE, and the unit after an INTERVAL's value. The
// value itself is an expression and keeps its columns.
func keywordOperands(tokens []lexer.Token) map[int]bool {
	operands := make(map[int]bool)
	for position := range tokens {
		switch {
		case keywordAt(tokens, position, "as"), keywordAt(tokens, position, "using"),
			keywordAt(tokens, position, "collate"):
			operands[position+1] = true
		case keywordAt(tokens, position, "interval"):
			unit := position + 2
			if operatorAt(tokens, position+1, "(") {
				unit = skipBalanced(tokens, position+1)
			}
			operands[unit] = true
		}
	}
	return operands
}

// significantTokens reads expression the way MySQL's lexer does and keeps every
// token but whitespace and comments.
func significantTokens(expression string) []lexer.Token {
	scanner := lexer.NewLexerWithOptions(expression, dialectlexer.Options(platform.MySQL))
	var tokens []lexer.Token
	for {
		token := scanner.NextToken()
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

// identifierAt answers the name the token at position spells: a backticked
// name without its backticks, an unquoted one as written. A number is not a
// name.
func identifierAt(tokens []lexer.Token, position int) (string, bool) {
	if position >= len(tokens) || tokens[position].Type != lexer.TokenIdentifier {
		return "", false
	}
	value := tokens[position].Value
	if value == "" || (value[0] >= '0' && value[0] <= '9') {
		return "", false
	}
	if strings.HasPrefix(value, "`") {
		return strings.ReplaceAll(strings.Trim(value, "`"), "``", "`"), true
	}
	return value, true
}

func keywordAt(tokens []lexer.Token, position int, word string) bool {
	return position < len(tokens) && tokens[position].Type == lexer.TokenIdentifier &&
		strings.EqualFold(tokens[position].Value, word)
}

func operatorAt(tokens []lexer.Token, position int, value string) bool {
	return position >= 0 && position < len(tokens) && tokens[position].MatchOperatorValue(value)
}

func stringAt(tokens []lexer.Token, position int) bool {
	return position < len(tokens) && tokens[position].Type == lexer.TokenString
}

// skipBalanced returns the position after the parenthesis that closes the one
// at position.
func skipBalanced(tokens []lexer.Token, position int) int {
	depth := 0
	for ; position < len(tokens); position++ {
		switch {
		case operatorAt(tokens, position, "("):
			depth++
		case operatorAt(tokens, position, ")"):
			depth--
			if depth == 0 {
				return position + 1
			}
		}
	}
	return position
}
