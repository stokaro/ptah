package pgname

import (
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
)

// checkLabel is the label PostgreSQL ends the name of an unnamed CHECK with.
const checkLabel = "check"

// Check answers the name PostgreSQL gives an unnamed CHECK constraint on
// table, as AddRelationNewConstraints chooses it: `<table>_<column>_check`
// when expression references exactly one distinct column of the table, and
// `<table>_check` when it references none or more than one. The name is fitted
// into 63 bytes and numbered past the names taken answers true for, as
// [Constraint] does. Whether the CHECK was written on a column or on the table
// makes no difference.
//
// columns are the table's column names as the server stores them, and table is
// the bare relation name. expression is the condition as written; an unquoted
// name in it is folded to lower case, as the server folds it.
//
// Measured on PostgreSQL 18.6, `CREATE TABLE d (lo int CHECK (lo > 0), hi int,
// CHECK (lo < hi), CHECK (hi > 0))` names the three `d_lo_check`, `d_check`
// and `d_hi_check`; a column-level `CHECK (a IS NULL OR b IS NOT NULL)` on
// `e` is `e_check`; `CHECK (true)` on `f` is `f_check`.
//
// A reference is a name the table declares as a column. A string literal, a
// function name, a type name after `::` or in CAST, a collation, the field of
// EXTRACT and the words of AT TIME ZONE are not references, whatever they are
// spelled like. The table's own name where no column carries it is a
// whole-row reference, which the server counts as one more distinct
// reference.
func Check(table, expression string, columns []string, taken func(string) bool) string {
	var addition []string
	if column, ok := checkColumn(table, expression, columns); ok {
		addition = []string{column}
	}
	return Constraint(table, addition, checkLabel, taken)
}

// checkColumn answers the one column of table that expression references,
// and false when it references none, more than one, or the whole row.
func checkColumn(table, expression string, columns []string) (string, bool) {
	declared := make(map[string]bool, len(columns))
	for _, column := range columns {
		declared[column] = true
	}
	reader := checkReader{
		tokens:   significantTokens(expression),
		table:    table,
		declared: declared,
		found:    make(map[string]bool),
	}
	reader.read()
	if reader.wholeRow || len(reader.found) != 1 {
		return "", false
	}
	for column := range reader.found {
		return column, true
	}
	return "", false
}

// significantTokens reads expression the way PostgreSQL's lexer does and keeps
// every token but whitespace and comments.
func significantTokens(expression string) []lexer.Token {
	scanner := lexer.NewLexerWithOptions(expression, dialectlexer.Options(platform.Postgres))
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

// checkReader walks the tokens of one CHECK expression and records the
// columns it references.
type checkReader struct {
	tokens   []lexer.Token
	table    string
	declared map[string]bool
	found    map[string]bool
	wholeRow bool
}

func (r *checkReader) read() {
	for position := 0; position < len(r.tokens); {
		position = r.readAt(position)
	}
}

// readAt reads the construct that starts at position and returns the position
// after it.
func (r *checkReader) readAt(position int) int {
	if r.operatorAt(position, ":") && r.operatorAt(position+1, ":") {
		return r.skipTypeName(position + 2)
	}
	name, ok := r.nameAt(position)
	if !ok {
		return position + 1
	}
	if next, skipped := r.skipKeywordConstruct(position); skipped {
		return next
	}
	switch {
	case r.operatorAt(position+1, "("):
		// A function call.
	case r.operatorAt(position+1, "."):
		// A qualifier: the column, if any, is the part after the dot. The
		// table's own name before `.*` is the whole row.
		if name == r.table && r.operatorAt(position+2, "*") {
			r.wholeRow = true
		}
	case r.stringLiteralAt(position + 1):
		// A typed literal, such as `date '2000-01-01'`.
		return r.skipWords(position+2, intervalFields)
	case r.declared[name]:
		r.found[name] = true
	case name == r.table && !r.operatorAt(position-1, "."):
		r.wholeRow = true
	}
	return position + 1
}

// skipKeywordConstruct steps over the keyword syntaxes whose words could be
// spelled like a column: a type name after AS in CAST, a collation name, the
// words of AT TIME ZONE and the field of EXTRACT. Measured on PostgreSQL 18.6
// with columns named `year` and `zone`, `extract(year from ts) > 2000` and
// `ts AT TIME ZONE 'UTC' > '2000-01-01'` each reference `ts` alone.
func (r *checkReader) skipKeywordConstruct(position int) (int, bool) {
	switch {
	case r.keywordAt(position, "as"):
		return r.skipTypeName(position + 1), true
	case r.keywordAt(position, "collate"):
		return r.skipQualifiedName(position + 1), true
	case r.keywordAt(position, "at") && r.keywordAt(position+1, "time") && r.keywordAt(position+2, "zone"):
		return position + 3, true
	case r.keywordAt(position, "extract") && r.operatorAt(position+1, "("):
		return position + 3, true
	default:
		return position, false
	}
}

// typeNameWords continue a type name after its first word, as in
// `character varying`, `double precision` and `timestamp with time zone`, and
// intervalFields qualify an interval, as in `interval '1' day to second`.
var (
	typeNameWords  = []string{"varying", "precision", "with", "without", "time", "zone"}
	intervalFields = []string{"year", "month", "day", "hour", "minute", "second", "to"}
)

// skipTypeName returns the position after the type name that starts at
// position: a possibly qualified name, the words that continue it, a
// parenthesized modifier and array brackets, in any order after the name.
func (r *checkReader) skipTypeName(position int) int {
	position = r.skipQualifiedName(position)
	for {
		switch {
		case r.operatorAt(position, "("):
			position = r.skipBalanced(position, "(", ")")
		case r.operatorAt(position, "["):
			position = r.skipBalanced(position, "[", "]")
		case r.keywordIn(position, typeNameWords), r.keywordIn(position, intervalFields):
			position++
		default:
			return position
		}
	}
}

// skipQualifiedName returns the position after the dotted name that starts at
// position.
func (r *checkReader) skipQualifiedName(position int) int {
	if _, ok := r.nameAt(position); !ok {
		return position
	}
	position++
	for r.operatorAt(position, ".") {
		if _, ok := r.nameAt(position + 1); !ok {
			return position
		}
		position += 2
	}
	return position
}

// skipBalanced returns the position after the closer that balances the opener
// at position.
func (r *checkReader) skipBalanced(position int, opener, closer string) int {
	depth := 0
	for ; position < len(r.tokens); position++ {
		switch {
		case r.operatorAt(position, opener):
			depth++
		case r.operatorAt(position, closer):
			depth--
			if depth == 0 {
				return position + 1
			}
		}
	}
	return position
}

// skipWords returns the first position from position on that is not one of
// words.
func (r *checkReader) skipWords(position int, words []string) int {
	for r.keywordIn(position, words) {
		position++
	}
	return position
}

func (r *checkReader) operatorAt(position int, value string) bool {
	return position >= 0 && position < len(r.tokens) && r.tokens[position].MatchOperatorValue(value)
}

func (r *checkReader) stringLiteralAt(position int) bool {
	if position >= len(r.tokens) {
		return false
	}
	token := r.tokens[position]
	return token.Type == lexer.TokenString && !strings.HasPrefix(token.Value, `"`)
}

// keywordAt reports whether the token at position is word written unquoted.
func (r *checkReader) keywordAt(position int, word string) bool {
	return position < len(r.tokens) && r.tokens[position].Type == lexer.TokenIdentifier &&
		foldUnquoted(r.tokens[position].Value) == word
}

func (r *checkReader) keywordIn(position int, words []string) bool {
	for _, word := range words {
		if r.keywordAt(position, word) {
			return true
		}
	}
	return false
}

// nameAt answers the name the token at position spells, as the server stores
// it: a quoted identifier keeps its case, an unquoted one is folded. A number
// is not a name.
func (r *checkReader) nameAt(position int) (string, bool) {
	if position >= len(r.tokens) {
		return "", false
	}
	token := r.tokens[position]
	switch {
	case token.Type == lexer.TokenString && strings.HasPrefix(token.Value, `"`):
		return unquoteIdentifier(token.Value), true
	case token.Type == lexer.TokenIdentifier && token.Value != "" && !isDigit(token.Value[0]):
		return foldUnquoted(token.Value), true
	default:
		return "", false
	}
}

// foldUnquoted folds an unquoted identifier as PostgreSQL does: ASCII letters
// to lower case, every other character kept.
func foldUnquoted(value string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, value)
}

// unquoteIdentifier removes the double quotes around an identifier and undoes
// the doubling of a quote inside it.
func unquoteIdentifier(value string) string {
	value = strings.TrimPrefix(value, `"`)
	value = strings.TrimSuffix(value, `"`)
	return strings.ReplaceAll(value, `""`, `"`)
}

func isDigit(character byte) bool {
	return character >= '0' && character <= '9'
}
