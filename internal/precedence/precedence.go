// Package precedence reads an SQL expression by an engine's operator
// precedence and prints the operations it groups, so that two spellings of one
// expression compare equal. MySQL stores the default (2 * 3 + 1) as
// ((2 * 3) + 1); read by MySQL's precedence both are 2 * 3 + 1, while
// (1 + 2) * 3 keeps the parentheses that make it a different expression.
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
// It returns "" and false when scheme names no precedence this package holds,
// or when expression holds anything outside the operators, literals, names and
// function calls it reads, such as NOT, IS, IN, CASE, a qualified name or
// another character set introducer. A caller then compares the expression as
// written, because reading a construct by the wrong grouping could make two
// different expressions equal.
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
	text, precedence, ok = r.unary()
	if !ok {
		return "", 0, false
	}
	for {
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

// unary reads a prefix operator and its operand, or an atom.
func (r *reader) unary() (text string, precedence int, ok bool) {
	tok, more := r.peek()
	if !more {
		return "", 0, false
	}
	bound, isPrefix := r.table.prefix[tok.text]
	if tok.kind != kindSymbol || !isPrefix {
		return r.atom()
	}
	r.pos++
	operand, operandBound, ok := r.unary()
	if !ok {
		return "", 0, false
	}
	return tok.text + enclose(operand, operandBound, bound), bound, true
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
