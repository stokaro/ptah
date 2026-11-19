// Package precedence reads an SQL expression by an engine's operator
// precedence and prints the operations it groups, so that two spellings of one
// expression compare equal. MySQL stores the default (2 * 3 + 1) as
// ((2 * 3) + 1), and ClickHouse stores 2 * 3 + 1 as (2 * 3) + 1; read by the
// engine's precedence both are 2 * 3 + 1, while (1 + 2) * 3 keeps the
// parentheses that make it a different expression.
//
// The schema comparison in migration/schemadiff reads an expression default
// through it where a dialect's descriptor names a precedence
// (descriptor.Comparison.OperatorPrecedence). It reads the text with the lexer
// options its caller gives it.
package precedence

import (
	"strings"

	"ptah.run/internal/lexer"
)

// MySQLOperators names the operator table MySQL and MariaDB group an
// expression by, as section 15.4.1 of the MySQL manual and MariaDB's Operator
// Precedence page list it. A descriptor selects it by this name, so an engine
// that shares the table names it without an edit here.
const MySQLOperators = "mysql-operators"

// ClickHouseOperators names the operator table ClickHouse groups an
// expression by, from the priority its Operators reference page gives each
// operator, checked against what formatQuerySingleLine prints on ClickHouse
// 24.10 and 26.10, which agree on every form the table reads.
const ClickHouseOperators = "clickhouse-operators"

// Canonical returns expression printed as the operations it groups under
// scheme, and true. The text is a comparison key, not SQL to run: it keeps a
// parenthesis only where the grouping needs one, puts one space around each
// binary operator, quotes every name in backticks, writes a function name in
// lower case and a keyword operator or an INTERVAL unit in upper case, and
// writes each spelling of one operation one way: MOD as %, != as <>,
// CURRENT_TIMESTAMP and current_timestamp() as now(), DATE_ADD(d, INTERVAL 1
// DAY) as d + INTERVAL 1 DAY, and a string as its value in single quotes
// without the introducer the server adds for the session.
//
// On ClickHouse it writes x BETWEEN low AND high as x >= low AND x <= high,
// and x NOT BETWEEN low AND high as x < low OR x > high, as the server stores
// them.
//
// It returns "" and false when scheme names no precedence this package holds,
// or when expression holds anything outside the operators, literals, names and
// function calls its table reads, such as IN, CASE, a qualified name or
// another character set introducer, and on MySQL NOT, IS and BETWEEN. A minus before an
// integer above 9223372036854775807 is not read either. A caller then compares
// the expression as written, because reading a construct by the wrong grouping
// could make two different expressions equal.
func Canonical(expression, scheme string, lexing lexer.Options) (string, bool) {
	table, known := schemes[scheme]
	if !known {
		return "", false
	}
	tokens, ok := table.tokenize(expression, lexing)
	if !ok || len(tokens) == 0 {
		return "", false
	}
	r := reader{tokens: tokens, table: table}
	text, _, ok := r.expression(0)
	if !ok || r.pos != len(r.tokens) {
		return "", false
	}
	return text, true
}

// atomPrecedence is the precedence of a literal, a name, a function call and a
// parenthesized group, which no operator splits.
const atomPrecedence = 100

// table is one scheme's operators. A binary operator groups to the left, as
// every binary operator in MySQL's grammar does.
type table struct {
	// binary maps a binary operator, a keyword in upper case, to its
	// precedence. A higher number binds tighter.
	binary map[string]int
	// prefix maps a prefix operator to its precedence.
	prefix map[string]int
	// prefixWords maps a prefix operator written as a word, in upper case,
	// to its precedence. Its operand is every operation that binds tighter.
	prefixWords map[string]int
	// postfix maps the words of a postfix operator, in upper case and joined
	// by one space, to its precedence.
	postfix map[string]int
	// between is the precedence of [NOT] BETWEEN, or 0 where the table does
	// not read it. Its operand and both bounds are operations that bind
	// tighter.
	between int
	// calledPrefix holds the prefix words a release of the engine reads as a
	// function call when a parenthesis follows them: ClickHouse 24.10 reads
	// NOT (a) IS NULL as (NOT a) IS NULL, and 26.10 as NOT (a IS NULL). The
	// reader takes such a word before a parenthesis only where the group is
	// the whole operand, which every release reads alike.
	calledPrefix map[string]bool
	// spelling maps an operator synonym to the spelling the key prints.
	spelling map[string]string
	// symbols lists the operators written with symbols, longest first, so
	// that adjacent symbol characters read as the longest operator they
	// spell.
	symbols []string
	// keywords holds the words that are not a name or a function name here:
	// an operator or construct the table does not read, or a word that stands
	// for a value of its own. A word in it stops the reading.
	keywords map[string]bool
	// literals holds the words that are a value, printed in upper case.
	literals map[string]bool
	// niladic maps a word that calls a function without parentheses, in upper
	// case, to the function the key prints for it.
	niladic map[string]string
	// synonyms maps a function name, in lower case, to the name of the one
	// function the key prints for every synonym of it.
	synonyms map[string]string
	// units holds the words, in upper case, an INTERVAL ends with.
	units map[string]bool
	// shifts maps a function name, in lower case, that adds an INTERVAL to its
	// first argument to the operator it is: date_add(d, INTERVAL 1 DAY) is
	// d + INTERVAL 1 DAY.
	shifts map[string]string
	// introducers holds the character set introducers, in lower case, the
	// server writes before a string literal of a session that did not write
	// one. A string written right after one of them is that string.
	introducers map[string]bool
	// doubleQuotedNames reports that double quotes delimit a name, not a
	// string.
	doubleQuotedNames bool
	// functionCase reports that the engine tells function names apart by
	// case, so the key keeps a name as written instead of in lower case.
	functionCase bool
	// decimalNumbers reports that the engine stores a number as its decimal
	// value: ClickHouse writes 010 as 10, 1.50 as 1.5, 1.0 as 1. and -0 as 0,
	// though not -(0), which is negate over the literal. The key writes a
	// number that way.
	decimalNumbers bool
}

var schemes = map[string]*table{
	MySQLOperators: {
		binary: map[string]int{
			"OR":  1,
			"XOR": 2,
			"AND": 3,
			"=":   6, "<=>": 6, ">=": 6, ">": 6, "<=": 6, "<": 6, "<>": 6, "!=": 6,
			"|":  7,
			"&":  8,
			"<<": 9, ">>": 9,
			"+": 10, "-": 10,
			"*": 11, "/": 11, "DIV": 11, "%": 11, "MOD": 11,
			"^": 12,
		},
		prefix:   map[string]int{"-": 13, "~": 13, "!": 14},
		spelling: map[string]string{"MOD": "%", "!=": "<>"},
		symbols: []string{
			"<=>",
			"<=", ">=", "<>", "!=", "<<", ">>",
			"=", "<", ">", "|", "&", "+", "-", "*", "/", "%", "^", "~", "!",
		},
		keywords: wordSet(
			// Operators and constructs the table does not read. NOT and
			// BETWEEN bind between the comparisons and AND, and a prefix word
			// written before a parenthesis is no function call.
			"AND", "OR", "XOR", "DIV", "MOD", "NOT", "IS", "LIKE", "IN",
			"BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END", "INTERVAL",
			"BINARY", "COLLATE", "REGEXP", "RLIKE", "SOUNDS", "MEMBER", "OF",
			"ESCAPE", "EXISTS", "SELECT", "DISTINCT", "AS", "ALL", "ANY", "SOME",
			"ROW", "VALUES", "DEFAULT", "MATCH", "AGAINST", "USING", "FROM",
			"FOR", "ON", "WITH", "UNKNOWN", "CHARACTER", "CHARSET",
			// Words that stand for a value without parentheses and that the
			// table does not read. Read as a name they would match a quoted
			// column of that name.
			"CURRENT_USER", "CURRENT_ROLE", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP",
		),
		literals: wordSet("NULL", "TRUE", "FALSE"),
		// MySQL 26.10 stores CURRENT_TIMESTAMP and LOCALTIMESTAMP as now(),
		// MariaDB 12.3 stores NOW() as current_timestamp(), and both store
		// CURRENT_DATE as curdate(). Each name calls one function on both
		// servers.
		niladic: map[string]string{
			"CURRENT_TIMESTAMP": "now", "LOCALTIME": "now", "LOCALTIMESTAMP": "now",
			"CURRENT_DATE": "curdate", "CURRENT_TIME": "curtime",
		},
		synonyms: map[string]string{
			"current_timestamp": "now", "localtime": "now", "localtimestamp": "now",
			"current_date": "curdate", "current_time": "curtime",
		},
		units: wordSet(
			"MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH",
			"QUARTER", "YEAR", "SECOND_MICROSECOND", "MINUTE_MICROSECOND",
			"MINUTE_SECOND", "HOUR_MICROSECOND", "HOUR_SECOND", "HOUR_MINUTE",
			"DAY_MICROSECOND", "DAY_SECOND", "DAY_MINUTE", "DAY_HOUR", "YEAR_MONTH",
		),
		// Both servers store DATE_ADD(NOW(), INTERVAL 1 DAY) as
		// now() + interval 1 day.
		shifts: map[string]string{"date_add": "+", "date_sub": "-"},
		// MySQL 26.10 stores concat('a', 'b') as
		// concat(_utf8mb4'a',_utf8mb4'b') for a session in utf8mb4, the
		// character set Ptah's driver connects with.
		introducers: wordSet("_utf8mb4"),
	},
	// ClickHouse's Operators page lists the operators by priority, and
	// formatQuerySingleLine prints how a release groups an expression.
	// ClickHouse 24.10 and 26.10 print 1 + (2 * 3), (a - b) - c,
	// (1 + 2) > 2, (a = 1) IS NULL, NOT (a IS NULL), NOT (a = 1),
	// (NOT a) AND b and (a = 1) OR ((b = 2) AND (c = 3)), so NOT binds looser
	// than IS NULL, which binds looser than the comparisons. Both store a
	// default as that text. They write x BETWEEN 1 AND 3 back as
	// (x >= 1) AND (x <= 3), and print ((a = x) >= 1) AND ((a = x) <= 3) for
	// a = x BETWEEN 1 AND 3, (x >= 1) AND (x <= (3 = 1)) for
	// x BETWEEN 1 AND 3 = 1 and ((x >= 1) AND (x <= 3)) IS NULL for
	// x BETWEEN 1 AND 3 IS NULL, so BETWEEN binds between IS NULL and the
	// comparisons. || is written back as concat(a, b), and IN, LIKE, the
	// lambda arrow and the ternary operator are not read.
	ClickHouseOperators: {
		binary: map[string]int{
			"OR":  1,
			"AND": 2,
			"=":   6, "==": 6, "!=": 6, "<>": 6, "<": 6, ">": 6, "<=": 6, ">=": 6,
			"+": 7, "-": 7,
			"*": 8, "/": 8, "%": 8,
		},
		prefix:       map[string]int{"-": 9},
		prefixWords:  map[string]int{"NOT": 3},
		postfix:      map[string]int{"IS NULL": 4, "IS NOT NULL": 4},
		between:      5,
		calledPrefix: wordSet("NOT"),
		spelling:     map[string]string{"==": "=", "<>": "!="},
		symbols: []string{
			"==", "!=", "<>", "<=", ">=",
			"=", "<", ">", "+", "-", "*", "/", "%",
		},
		keywords: wordSet(
			"AND", "OR", "NOT", "IS", "LIKE", "ILIKE", "IN", "GLOBAL", "BETWEEN",
			"CASE", "WHEN", "THEN", "ELSE", "END", "INTERVAL", "DIV", "MOD",
			"AS", "EXISTS", "SELECT", "DISTINCT", "ALL", "ANY", "FROM", "WITH",
		),
		literals:          wordSet("NULL", "TRUE", "FALSE"),
		doubleQuotedNames: true,
		functionCase:      true,
		decimalNumbers:    true,
	},
}

func wordSet(words ...string) map[string]bool {
	set := make(map[string]bool, len(words))
	for _, word := range words {
		set[word] = true
	}
	return set
}

type kind uint8

const (
	kindLiteral kind = iota + 1
	kindName
	kindWord
	kindSymbol
	kindOpen
	kindClose
	kindComma
)

type token struct {
	kind kind
	text string
}

// tokenize reads expression into the tokens the reader parses. Whitespace and
// comments separate tokens and are dropped. Adjacent symbol characters are
// read as the longest operators of the table they spell.
func (t *table) tokenize(expression string, lexing lexer.Options) ([]token, bool) {
	scan := lexer.NewLexerWithOptions(expression, lexing)
	read := tokens{table: t, lexing: lexing, wordEnd: -1}
	var run strings.Builder
	runEnd := -1
	flush := func() bool {
		if run.Len() == 0 {
			return true
		}
		symbols, ok := t.splitSymbols(run.String())
		run.Reset()
		read.list = append(read.list, symbols...)
		return ok
	}
	for {
		tok := scan.NextToken()
		if tok.Type == lexer.TokenOperator && t.isSymbol(tok.Value) {
			if run.Len() > 0 && tok.Start != runEnd && !flush() {
				return nil, false
			}
			run.WriteString(tok.Value)
			runEnd = tok.End
			continue
		}
		if !flush() {
			return nil, false
		}
		if tok.Type == lexer.TokenEOF {
			return read.list, true
		}
		if !read.add(tok) {
			return nil, false
		}
	}
}

// tokens collects what tokenize reads apart from symbols.
type tokens struct {
	table  *table
	lexing lexer.Options
	list   []token
	// wordEnd is where the last identifier ended, so that an introducer is
	// read only right before its string.
	wordEnd int
}

// add appends the token tok stands for, and reports false for a token the
// reader cannot read.
func (r *tokens) add(tok lexer.Token) bool {
	switch tok.Type {
	case lexer.TokenWhitespace, lexer.TokenComment:
		return true
	case lexer.TokenString:
		if r.table.doubleQuotedNames && strings.HasPrefix(tok.Value, `"`) {
			name, ok := lexer.StringValue(tok.Value, r.lexing)
			r.list = append(r.list, token{kind: kindName, text: name})
			return ok
		}
		literal, ok := stringLiteral(tok, r.lexing)
		if !ok {
			return false
		}
		last := len(r.list) - 1
		if last >= 0 && r.list[last].kind == kindWord && r.table.introducers[strings.ToLower(r.list[last].text)] && r.wordEnd == tok.Start {
			r.list = r.list[:last]
		}
		r.list = append(r.list, literal)
		return true
	case lexer.TokenIdentifier:
		next, ok := identifierToken(tok.Value)
		if ok && next.kind == kindLiteral && r.table.decimalNumbers {
			next.text = decimalNumber(next.text)
		}
		r.list = append(r.list, next)
		r.wordEnd = tok.End
		return ok
	case lexer.TokenOperator:
		next, ok := punctuation(tok.Value)
		r.list = append(r.list, next)
		return ok
	default:
		return false
	}
}

// stringLiteral reads a string token as the literal the key prints: its value,
// in single quotes with a quote doubled, so that a quote escaped with a
// backslash and a doubled quote give one key where the lexer reads both.
func stringLiteral(tok lexer.Token, lexing lexer.Options) (token, bool) {
	value, ok := lexer.StringValue(tok.Value, lexing)
	if !ok {
		return token{}, false
	}
	return token{kind: kindLiteral, text: "'" + strings.ReplaceAll(value, "'", "''") + "'"}, true
}

// isSymbol reports whether value is one character an operator of the table is
// written with.
func (t *table) isSymbol(value string) bool {
	if len(value) != 1 {
		return false
	}
	for _, symbol := range t.symbols {
		if strings.Contains(symbol, value) {
			return true
		}
	}
	return false
}

// splitSymbols reads a run of adjacent symbol characters as the longest
// operators of the table it spells, and fails where no operator matches.
func (t *table) splitSymbols(run string) ([]token, bool) {
	var out []token
	for run != "" {
		matched := ""
		for _, symbol := range t.symbols {
			if strings.HasPrefix(run, symbol) {
				matched = symbol
				break
			}
		}
		if matched == "" {
			return nil, false
		}
		out = append(out, token{kind: kindSymbol, text: matched})
		run = run[len(matched):]
	}
	return out, true
}

// identifierToken classifies what the lexer reads as an identifier: a quoted
// name, a number or a word.
func identifierToken(value string) (token, bool) {
	switch {
	case strings.HasPrefix(value, "`"):
		if len(value) < 2 || !strings.HasSuffix(value, "`") {
			return token{}, false
		}
		return token{kind: kindName, text: strings.ReplaceAll(value[1:len(value)-1], "``", "`")}, true
	case value[0] >= '0' && value[0] <= '9':
		return token{kind: kindLiteral, text: value}, isNumber(value)
	default:
		return token{kind: kindWord, text: value}, true
	}
}

// decimalNumber writes the number value, which [isNumber] accepts, as its
// decimal value: no leading zero before another digit, and no trailing zero
// after the decimal point, which stays where value has one.
func decimalNumber(value string) string {
	whole, fraction, point := strings.Cut(value, ".")
	whole = strings.TrimLeft(whole, "0")
	if whole == "" {
		whole = "0"
	}
	if !point {
		return whole
	}
	return whole + "." + strings.TrimRight(fraction, "0")
}

// isNumber reports whether value is digits with at most one decimal point.
func isNumber(value string) bool {
	whole, fraction, _ := strings.Cut(value, ".")
	return whole != "" && allDigits(whole) && allDigits(fraction)
}

func allDigits(value string) bool {
	for i := range len(value) {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func punctuation(value string) (token, bool) {
	switch value {
	case "(":
		return token{kind: kindOpen}, true
	case ")":
		return token{kind: kindClose}, true
	case ",":
		return token{kind: kindComma}, true
	default:
		return token{}, false
	}
}

// reader parses tokens by precedence climbing and prints each operation as it
// closes, so a parenthesized group adds nothing of its own: the printer adds
// the parentheses an operand needs.
type reader struct {
	tokens []token
	pos    int
	table  *table
}

func (r *reader) peek() (token, bool) {
	if r.pos >= len(r.tokens) {
		return token{}, false
	}
	return r.tokens[r.pos], true
}

// expression reads operations whose operators bind at least as tightly as
// minimum, and returns their text and the precedence of the outermost one.
func (r *reader) expression(minimum int) (text string, precedence int, ok bool) {
	return r.operations(minimum, minimum)
}

// operations is [reader.expression] whose first operand is read as the
// operand of an operator that needs at least first: the operand of a prefix
// word may open with that word again, as NOT NOT a does.
func (r *reader) operations(minimum, first int) (text string, precedence int, ok bool) {
	text, precedence, ok = r.unary(first)
	if !ok {
		return "", 0, false
	}
	for {
		if operator, bound, words := r.postfix(); words > 0 && bound >= minimum {
			r.pos += words
			text = enclose(text, precedence, bound) + " " + operator
			precedence = bound
			continue
		}
		if comparisons, words := r.betweenAt(); words > 0 && r.table.between >= minimum {
			r.pos += words
			text, precedence, ok = r.betweenBounds(text, precedence, comparisons)
			if !ok {
				return "", 0, false
			}
			continue
		}
		operator, bound, found := r.binary()
		if !found || bound < minimum {
			return text, precedence, true
		}
		r.pos++
		right, rightBound, ok := r.expression(bound + 1)
		if !ok {
			return "", 0, false
		}
		if operator == "+" && isInterval(text, precedence) && !isInterval(right, rightBound) {
			// INTERVAL 1 DAY + d is d + INTERVAL 1 DAY, which is how both
			// servers store it.
			text, precedence, right, rightBound = right, rightBound, text, precedence
		}
		text = enclose(text, precedence, bound) + " " + r.spell(operator) + " " + enclose(right, rightBound, bound+1)
		precedence = bound
	}
}

// binary returns the binary operator at the reader's position and its
// precedence, without consuming it.
func (r *reader) binary() (operator string, precedence int, found bool) {
	tok, more := r.peek()
	if !more {
		return "", 0, false
	}
	switch tok.kind {
	case kindSymbol:
		operator = tok.text
	case kindWord:
		operator = strings.ToUpper(tok.text)
	default:
		return "", 0, false
	}
	precedence, found = r.table.binary[operator]
	return operator, precedence, found
}

// betweenComparisons are the comparisons an engine stores for a BETWEEN:
// operand below low, join, operand above high.
type betweenComparisons struct {
	below, above, join string
}

var (
	// withinBounds is what x BETWEEN low AND high is stored as.
	withinBounds = betweenComparisons{below: ">=", above: "<=", join: "AND"}
	// outsideBounds is what x NOT BETWEEN low AND high is stored as.
	outsideBounds = betweenComparisons{below: "<", above: ">", join: "OR"}
)

// betweenAt reports how many words of BETWEEN or NOT BETWEEN start at the
// reader's position, and the comparisons they stand for, without consuming
// them. It answers 0 words where the table does not read BETWEEN.
func (r *reader) betweenAt() (betweenComparisons, int) {
	if r.table.between == 0 {
		return betweenComparisons{}, 0
	}
	for index := r.pos; index < len(r.tokens) && index < r.pos+2; index++ {
		tok := r.tokens[index]
		switch {
		case tok.kind != kindWord:
			return betweenComparisons{}, 0
		case strings.EqualFold(tok.text, "BETWEEN") && index == r.pos:
			return withinBounds, 1
		case strings.EqualFold(tok.text, "BETWEEN"):
			return outsideBounds, 2
		case !strings.EqualFold(tok.text, "NOT"):
			return betweenComparisons{}, 0
		}
	}
	return betweenComparisons{}, 0
}

// betweenBounds reads the bounds of the BETWEEN whose words the reader has
// consumed, after the operand operand, whose precedence is bound, and writes
// the comparisons the engine stores for it: operand >= low AND operand <= high
// for [withinBounds], operand < low OR operand > high for [outsideBounds]. An
// operand that binds no tighter than BETWEEN, such as a IS NULL or another
// BETWEEN, is not read, since no measurement says how the engine groups it.
func (r *reader) betweenBounds(operand string, bound int, comparisons betweenComparisons) (text string, precedence int, ok bool) {
	between := r.table.between
	if bound <= between {
		return "", 0, false
	}
	low, lowBound, ok := r.expression(between + 1)
	if !ok {
		return "", 0, false
	}
	if and, more := r.peek(); !more || and.kind != kindWord || !strings.EqualFold(and.text, "AND") {
		return "", 0, false
	}
	r.pos++
	high, highBound, ok := r.expression(between + 1)
	if !ok {
		return "", 0, false
	}
	below, above, join := comparisons.below, comparisons.above, comparisons.join
	compared := r.table.binary[below]
	joined := r.table.binary[join]
	lower := enclose(operand, bound, compared) + " " + below + " " + enclose(low, lowBound, compared+1)
	upper := enclose(operand, bound, compared) + " " + above + " " + enclose(high, highBound, compared+1)
	return lower + " " + join + " " + upper, joined, true
}

// postfix returns the postfix operator whose words start at the reader's
// position, its precedence and how many words it spans, without consuming
// it, and 0 words where none starts there.
func (r *reader) postfix() (operator string, precedence, words int) {
	var spelled []string
	for index := r.pos; index < len(r.tokens) && len(spelled) < 3; index++ {
		tok := r.tokens[index]
		if tok.kind != kindWord && (tok.kind != kindLiteral || !r.table.literals[strings.ToUpper(tok.text)]) {
			break
		}
		spelled = append(spelled, strings.ToUpper(tok.text))
		if bound, found := r.table.postfix[strings.Join(spelled, " ")]; found {
			operator, precedence, words = strings.Join(spelled, " "), bound, len(spelled)
		}
	}
	return operator, precedence, words
}

// unary reads a prefix operator and its operand, or an atom, as the operand
// of an operator that needs at least minimum.
func (r *reader) unary(minimum int) (text string, precedence int, ok bool) {
	tok, more := r.peek()
	if !more {
		return "", 0, false
	}
	if tok.kind == kindWord {
		if bound, isPrefix := r.table.prefixWords[strings.ToUpper(tok.text)]; isPrefix {
			return r.prefixWord(strings.ToUpper(tok.text), bound, minimum)
		}
	}
	bound, isPrefix := r.table.prefix[tok.text]
	if tok.kind != kindSymbol || !isPrefix {
		return r.atom()
	}
	r.pos++
	// ClickHouse folds a minus written right before a number into the
	// literal, and reads one before anything else, a group included, as
	// negate: -0 is the UInt8 literal 0, while -(0) and - -0 are negate(0),
	// an Int16 that makes -(0) + 18446744073709551614 an Int64 -2.
	next, _ := r.peek()
	folded := next.kind == kindLiteral
	operand, operandBound, ok := r.unary(bound)
	if !ok {
		return "", 0, false
	}
	if tok.text == "-" && folded && operand == "0" && r.table.decimalNumbers {
		return operand, atomPrecedence, true
	}
	if tok.text == "-" && aboveInt64(operand) {
		return "", 0, false
	}
	return tok.text + enclose(operand, operandBound, bound), bound, true
}

// aboveInt64 reports whether text is an integer literal above
// 9223372036854775807, counting one written with more digits than that as
// above it. A minus written right before such a literal and one written before
// a group around it read differently: ClickHouse 24.10 and 26.10 fold
// -18446744073709551615 into one Float64 literal, -18446744073709552000, and
// read -(18446744073709551615) as negate over the UInt64, which wraps to 1.
// One key for both spellings would make the two defaults equal, so neither is
// read and the caller compares them as written.
func aboveInt64(text string) bool {
	if text == "" || !allDigits(text) {
		return false
	}
	const largest = "9223372036854775807"
	return len(text) > len(largest) || len(text) == len(largest) && text > largest
}

// prefixWord reads the operand of the prefix word operator, whose precedence
// is bound, and which the reader is at. An operator that needs more than
// bound cannot take it as an operand without a parenthesis, so the reading
// stops there rather than guess how the engine groups it. Before a
// parenthesis, a word of [table.calledPrefix] is read only where the group is
// its whole operand.
func (r *reader) prefixWord(word string, bound, minimum int) (text string, precedence int, ok bool) {
	if minimum > bound {
		return "", 0, false
	}
	r.pos++
	next, more := r.peek()
	if more && next.kind == kindOpen && r.table.calledPrefix[word] {
		group, groupBound, ok := r.atom()
		if !ok || r.bindsTighter(bound) {
			return "", 0, false
		}
		return word + " " + enclose(group, groupBound, bound+1), bound, true
	}
	operand, operandBound, ok := r.operations(bound+1, bound)
	if !ok {
		return "", 0, false
	}
	return word + " " + enclose(operand, operandBound, bound+1), bound, true
}

// bindsTighter reports whether the operator at the reader's position binds
// tighter than bound.
func (r *reader) bindsTighter(bound int) bool {
	if _, precedence, words := r.postfix(); words > 0 {
		return precedence > bound
	}
	_, precedence, found := r.binary()
	return found && precedence > bound
}

// atom reads a literal, a name, a function call or a parenthesized group.
func (r *reader) atom() (text string, precedence int, ok bool) {
	tok, more := r.peek()
	if !more {
		return "", 0, false
	}
	r.pos++
	switch tok.kind {
	case kindLiteral:
		return tok.text, atomPrecedence, true
	case kindName:
		if next, more := r.peek(); more && next.kind == kindOpen {
			return "", 0, false
		}
		return quoteName(tok.text), atomPrecedence, true
	case kindWord:
		return r.word(tok.text)
	case kindOpen:
		text, precedence, ok = r.expression(0)
		if !ok || !r.consume(kindClose) {
			return "", 0, false
		}
		return text, precedence, true
	default:
		return "", 0, false
	}
}

// word reads a bare word: a literal, an INTERVAL, a function call or a name.
func (r *reader) word(word string) (text string, precedence int, ok bool) {
	upper := strings.ToUpper(word)
	opens := r.consume(kindOpen)
	switch {
	case r.table.literals[upper] && !opens:
		return upper, atomPrecedence, true
	case upper == "INTERVAL" && r.table.units != nil:
		if opens {
			r.pos--
		}
		return r.interval()
	case r.table.niladic[upper] != "" && !opens:
		return r.table.niladic[upper] + "()", atomPrecedence, true
	case r.table.keywords[upper] || r.table.literals[upper]:
		return "", 0, false
	case !opens:
		return quoteName(word), atomPrecedence, true
	}
	if r.table.functionCase {
		return r.call(word)
	}
	return r.call(strings.ToLower(word))
}

// call reads the arguments of a call of the function name, whose opening
// parenthesis the reader has consumed.
func (r *reader) call(name string) (text string, precedence int, ok bool) {
	var arguments []string
	var bounds []int
	if !r.consume(kindClose) {
		for {
			argument, bound, ok := r.expression(0)
			if !ok {
				return "", 0, false
			}
			arguments = append(arguments, argument)
			bounds = append(bounds, bound)
			if r.consume(kindClose) {
				break
			}
			if !r.consume(kindComma) {
				return "", 0, false
			}
		}
	}
	if operator, shifts := r.table.shifts[name]; shifts && len(arguments) == 2 && isInterval(arguments[1], bounds[1]) {
		bound := r.table.binary[operator]
		return enclose(arguments[0], bounds[0], bound) + " " + operator + " " + arguments[1], bound, true
	}
	if synonym, found := r.table.synonyms[name]; found {
		name = synonym
	}
	return name + "(" + strings.Join(arguments, ", ") + ")", atomPrecedence, true
}

// interval reads the expression and the unit of an INTERVAL, whose keyword the
// reader has consumed.
func (r *reader) interval() (text string, precedence int, ok bool) {
	amount, bound, ok := r.expression(0)
	if !ok {
		return "", 0, false
	}
	unit, more := r.peek()
	if !more || unit.kind != kindWord || !r.table.units[strings.ToUpper(unit.text)] {
		return "", 0, false
	}
	r.pos++
	return "INTERVAL " + enclose(amount, bound, atomPrecedence) + " " + strings.ToUpper(unit.text), atomPrecedence, true
}

// isInterval reports whether an operand's text is an INTERVAL. No other atom
// the key prints begins with the word: a name is quoted, a function name is
// in lower case and a literal word is NULL, TRUE or FALSE.
func isInterval(text string, precedence int) bool {
	return precedence == atomPrecedence && strings.HasPrefix(text, "INTERVAL ")
}

func (r *reader) consume(want kind) bool {
	tok, more := r.peek()
	if !more || tok.kind != want {
		return false
	}
	r.pos++
	return true
}

func (r *reader) spell(operator string) string {
	if spelled, found := r.table.spelling[operator]; found {
		return spelled
	}
	return operator
}

// enclose wraps the text of an operand in parentheses where its precedence
// is below least, the precedence its place in the expression needs.
func enclose(text string, precedence, least int) string {
	if precedence < least {
		return "(" + text + ")"
	}
	return text
}

func quoteName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
