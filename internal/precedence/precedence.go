// Package precedence reads an SQL expression by an engine's operator
// precedence and prints the operations it groups, so that two spellings of one
// expression compare equal. MySQL stores the default (2 * 3 + 1) as
// ((2 * 3) + 1), and ClickHouse stores 2 * 3 + 1 as (2 * 3) + 1; read by the
// engine's precedence both are 2 * 3 + 1, while (1 + 2) * 3 keeps the
// parentheses that make it a different expression.
//
// The schema comparison in migration/schemadiff reads an expression default,
// a CHECK condition and a generated column's expression through it where a
// dialect's descriptor names a precedence
// (descriptor.Comparison.OperatorPrecedence). A CHECK and a generated
// expression are read this way only in addition to the text comparison: two
// expressions that group alike are the same, and two that do not are compared
// as before. It reads the text with the lexer options its caller gives it.
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
// On MySQL and MariaDB it writes NOT over an operation as the operation it
// negates, as both servers store it: NOT (a = 1) as a <> 1, NOT (a IS NULL)
// as a IS NOT NULL, NOT (c LIKE 'x') as c NOT LIKE 'x', NOT (a = 1 AND b = 2)
// as a <> 1 OR b <> 2, and ! as NOT. It writes a IN (1) as a = 1,
// x REGEXP y as regexp_like(x, y), a AND (b AND c) as a AND b AND c, a
// character set introducer other than the session's in front of its string,
// and X'41', 0x41 and b'01000001' each as 0x41.
//
// On ClickHouse it writes x BETWEEN low AND high as x >= low AND x <= high,
// and x NOT BETWEEN low AND high as x < low OR x > high, as the server stores
// them. It reads [NOT] IN and [NOT] LIKE with the comparisons, and writes the
// operand of IN in parentheses, so that a IN 1 + 2 is a IN (1 + 2).
//
// It returns "" and false when scheme names no precedence this package holds,
// or when expression holds anything outside the operators, literals, names and
// function calls its table reads, such as CASE, a qualified name, LIKE with
// ESCAPE, or on ClickHouse a character set introducer. A minus before an
// integer above 9223372036854775807 is not read either. A caller then
// compares the expression as written, because reading a construct by the
// wrong grouping could make two different expressions equal.
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
	op, ok := r.expression(0)
	if !ok || r.pos != len(r.tokens) {
		return "", false
	}
	return op.text, true
}

// Keyword reports whether scheme reads word, in any case, as a keyword, an
// operator word or a literal rather than as a name or a function name. An
// unknown scheme reads every word as a name.
func Keyword(scheme, word string) bool {
	table, known := schemes[scheme]
	if !known {
		return false
	}
	upper := strings.ToUpper(word)
	_, binary := table.binary[upper]
	_, prefix := table.prefixWords[upper]
	return table.keywords[upper] || table.literals[upper] || binary || prefix
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
	// membership is the precedence of [NOT] IN and [NOT] LIKE, or 0 where the
	// table does not read them. Each groups to the left with the binary
	// operators of its precedence, and its right operand is every operation
	// that binds tighter, or for IN a parenthesized list of two or more.
	membership int
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
	// keepsIntroducers reports that a word of the form _charset before a
	// string, with or without whitespace between, is a character set
	// introducer, and that the key keeps one outside introducers in front of
	// the string in lower case: _binary 'a' is _binary'a'.
	keepsIntroducers bool
	// nationalIntroducer is the introducer N'...' stands for, or empty where
	// the table does not read N'...'.
	nationalIntroducer string
	// binaryLiterals reports that X'...', 0x..., B'...' and 0b... are
	// literals of bytes, which the key writes as 0x and two lower-case
	// hexadecimal digits a byte, and X'' where there are none.
	binaryLiterals bool
	// pushesNot reports that the server stores NOT over an operation as the
	// operation it negates: a comparison as its complement, IS x as IS NOT x,
	// [NOT] LIKE, [NOT] IN and [NOT] BETWEEN as the other form, AND and OR by
	// De Morgan's laws, and NOT over NOT over a truth value as that value. The
	// key writes NOT that way too, and ! as NOT.
	pushesNot bool
	// associative holds the binary operators the key writes without
	// parentheses around a right operand of the same operator: a AND (b AND
	// c) is a AND b AND c.
	associative map[string]bool
	// keepsBetween reports that the server stores [NOT] BETWEEN as written,
	// rather than as the comparisons [betweenComparisons] holds.
	keepsBetween bool
	// singleMemberIn reports that IN takes a parenthesized list only, and
	// that a list of one is the comparison with its member: a IN (1) is
	// a = 1, and a NOT IN (1) is a <> 1.
	singleMemberIn bool
	// regexpCalls reports that x REGEXP y and x RLIKE y read at the
	// precedence of IN and are the call regexp_like(x, y).
	regexpCalls bool
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
		prefix:      map[string]int{"-": 13, "~": 13, "!": 14},
		prefixWords: map[string]int{"NOT": 4},
		postfix: map[string]int{
			"IS NULL": 6, "IS NOT NULL": 6,
			"IS TRUE": 6, "IS NOT TRUE": 6,
			"IS FALSE": 6, "IS NOT FALSE": 6,
		},
		between:        5,
		membership:     6,
		keepsBetween:   true,
		singleMemberIn: true,
		regexpCalls:    true,
		// MySQL 26.10 and MariaDB 12.3 store NOT (a = 1) as a <> 1,
		// NOT (a = 1 AND b = 2) as a <> 1 OR b <> 2, NOT (a IS NULL) as
		// a IS NULL, NOT (c NOT LIKE 'x') as c LIKE 'x', !(a > 0) as a <= 0
		// and NOT NOT (a = 1) as a = 1. MySQL stores NOT (c LIKE 'x') as
		// not((c like 'x')) where MariaDB stores c NOT LIKE 'x', and both
		// store NOT (a <=> 1) with the NOT.
		pushesNot: true,
		// Both store a AND b AND c, with AND, OR and XOR each joining all its
		// operands at one level.
		associative: wordSet("AND", "OR", "XOR"),
		spelling:    map[string]string{"MOD": "%", "!=": "<>"},
		symbols: []string{
			"<=>",
			"<=", ">=", "<>", "!=", "<<", ">>",
			"=", "<", ">", "|", "&", "+", "-", "*", "/", "%", "^", "~", "!",
		},
		keywords: wordSet(
			// Operators, and constructs the table does not read. A prefix
			// word written before a parenthesis is no function call.
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
		// character set Ptah's driver connects with. It stores
		// _binary 'a' as _binary'a', N'a' as _utf8mb3'a', X'4a' and 0x4A each
		// as 0x4a, 0xABC as 0x0abc, 0b101 as 0x05 and b'100000000' as 0x0100.
		introducers:        wordSet("_utf8mb4"),
		keepsIntroducers:   true,
		nationalIntroducer: "_utf8mb3",
		binaryLiterals:     true,
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
	// comparisons. They print (a + 1) IN (2, 3), (s LIKE 'a') = 1,
	// NOT (a IN (1)) and ((a IN (1, 2)) = b) IN (3) for
	// a IN (1, 2) = b IN (3), so [NOT] IN and [NOT] LIKE bind with the
	// comparisons, and a IN (1 + 2) for both a IN 1 + 2 and a IN (1 + 2).
	// || is written back as concat(a, b), and ILIKE, GLOBAL IN, the lambda
	// arrow and the ternary operator are not read.
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
		membership:   6,
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
		literal, ok := r.prefixedString(tok)
		r.list = append(r.list, literal)
		return ok
	case lexer.TokenIdentifier:
		if r.followsZero(tok) {
			// The lexer reads 0x41 as 0 and x41.
			literal, ok := radixLiteral(tok.Value)
			r.list[len(r.list)-1] = literal
			r.wordEnd = tok.End
			return ok
		}
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

// prefixedString reads the string token tok, together with the word before it
// that makes it another literal, as the literal the key prints, and removes
// such a word from the list.
//
// A string is its value, in single quotes with a quote doubled, so that a
// quote escaped with a backslash and a doubled quote give one key where the
// lexer reads both. Written right after X or B on a table that reads
// [table.binaryLiterals], it is bytes; right after N, a string with the
// table's [table.nationalIntroducer]; and after one of [table.introducers],
// the string itself. On a table that [table.keepsIntroducers], another
// introducer stays in front of it, and whitespace may separate any
// introducer from its string, as MySQL 26.10 reads _binary 'a'.
func (r *tokens) prefixedString(tok lexer.Token) (token, bool) {
	value, ok := lexer.StringValue(tok.Value, r.lexing)
	if !ok {
		return token{}, false
	}
	literal := token{kind: kindLiteral, text: "'" + strings.ReplaceAll(value, "'", "''") + "'"}
	last := len(r.list) - 1
	if last < 0 || r.list[last].kind != kindWord {
		return literal, true
	}
	word := strings.ToLower(r.list[last].text)
	adjacent := r.wordEnd == tok.Start
	introduced := adjacent || r.table.keepsIntroducers
	switch {
	case r.table.binaryLiterals && adjacent && word == "x":
		r.list = r.list[:last]
		return hexLiteral(value)
	case r.table.binaryLiterals && adjacent && word == "b":
		r.list = r.list[:last]
		return bitLiteral(value)
	case r.table.nationalIntroducer != "" && adjacent && word == "n":
		r.list = r.list[:last]
		literal.text = r.table.nationalIntroducer + literal.text
	case r.table.introducers[word] && introduced:
		r.list = r.list[:last]
	case r.table.keepsIntroducers && isIntroducer(word):
		r.list = r.list[:last]
		literal.text = word + literal.text
	}
	return literal, true
}

// isIntroducer reports whether word, in lower case, is spelled as a character
// set introducer: an underscore and the name of a character set.
func isIntroducer(word string) bool {
	name, found := strings.CutPrefix(word, "_")
	if !found || name == "" {
		return false
	}
	for i := range len(name) {
		ch := name[i]
		if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '_' {
			return false
		}
	}
	return true
}

// followsZero reports whether the identifier tok, written right after the
// literal 0, makes it a literal of bytes on a table that reads them: 0x41 or
// 0b0101. MySQL 26.10 reads 0X41 as a name, so only a lower-case x or b does.
func (r *tokens) followsZero(tok lexer.Token) bool {
	last := len(r.list) - 1
	if !r.table.binaryLiterals || last < 0 || r.wordEnd != tok.Start {
		return false
	}
	previous := r.list[last]
	return previous.kind == kindLiteral && previous.text == "0" &&
		(strings.HasPrefix(tok.Value, "x") || strings.HasPrefix(tok.Value, "b"))
}

// radixLiteral reads what follows the 0 of 0x41 or 0b0101 as the literal of
// bytes the key prints.
func radixLiteral(value string) (token, bool) {
	if digits, hex := strings.CutPrefix(value, "x"); hex && digits != "" {
		if len(digits)%2 == 1 {
			digits = "0" + digits
		}
		return hexLiteral(digits)
	}
	digits, _ := strings.CutPrefix(value, "b")
	if digits == "" {
		return token{}, false
	}
	return bitLiteral(digits)
}

// hexLiteral reads the digits of X'...' as the literal of bytes the key
// prints. MySQL refuses an odd number of digits there, and so does the key.
func hexLiteral(digits string) (token, bool) {
	if len(digits)%2 == 1 {
		return token{}, false
	}
	for i := range len(digits) {
		if !isHexDigit(digits[i]) {
			return token{}, false
		}
	}
	if digits == "" {
		return token{kind: kindLiteral, text: "X''"}, true
	}
	return token{kind: kindLiteral, text: "0x" + strings.ToLower(digits)}, true
}

func isHexDigit(ch byte) bool {
	return ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f' || ch >= 'A' && ch <= 'F'
}

// bitLiteral reads the digits of B'...' as the literal of bytes the key
// prints: the bits padded with zeros on the left to whole bytes. MySQL 26.10
// stores b'0101' as 0x05 and b'100000000' as 0x0100. No bits are not read.
func bitLiteral(bits string) (token, bool) {
	if bits == "" || strings.Trim(bits, "01") != "" {
		return token{}, false
	}
	if extra := len(bits) % 8; extra != 0 {
		bits = strings.Repeat("0", 8-extra) + bits
	}
	const hexDigits = "0123456789abcdef"
	var b strings.Builder
	b.WriteString("0x")
	for i := 0; i < len(bits); i += 4 {
		nibble := 0
		for _, bit := range bits[i : i+4] {
			nibble = nibble<<1 | int(bit-'0')
		}
		b.WriteByte(hexDigits[nibble])
	}
	return token{kind: kindLiteral, text: b.String()}, true
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

// operation is what the reader read: the text the key prints for it, the
// precedence of its outermost operator, and how it reads under NOT.
type operation struct {
	text       string
	precedence int
	// truth reports that the operation answers true, false or NULL, as a
	// comparison does, so that NOT over NOT over it is the operation itself.
	truth bool
	// negation returns the operation NOT over this one is stored as on a table
	// that pushes NOT into the operation ([table.pushesNot]). Nil means NOT is
	// written before it.
	negation func() operation
}

// enclosed returns the operation's text, in parentheses where its precedence
// is below least, the precedence its place in the expression needs.
func (op operation) enclosed(least int) string {
	return enclose(op.text, op.precedence, least)
}

// atomOperation is a literal, a name, a function call or an INTERVAL, which
// no operator splits.
func atomOperation(text string) operation {
	return operation{text: text, precedence: atomPrecedence}
}

// complements maps a comparison to the one NOT over it is stored as, after
// [reader.spell].
var complements = map[string]string{
	"=": "<>", "<>": "=", "<": ">=", ">=": "<", ">": "<=", "<=": ">",
}

// deMorgan maps a logical operator to the one NOT over it joins the negated
// operands with.
var deMorgan = map[string]string{"AND": "OR", "OR": "AND"}

// truthOperators holds the binary operators that answer true, false or NULL.
var truthOperators = wordSet("=", "<>", "!=", "<", ">", "<=", ">=", "<=>", "AND", "OR", "XOR")

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
// minimum.
func (r *reader) expression(minimum int) (operation, bool) {
	return r.operations(minimum, minimum)
}

// operations is [reader.expression] whose first operand is read as the
// operand of an operator that needs at least first: the operand of a prefix
// word may open with that word again, as NOT NOT a does.
func (r *reader) operations(minimum, first int) (operation, bool) {
	left, ok := r.unary(first)
	if !ok {
		return operation{}, false
	}
	for {
		if operator, bound, words := r.postfix(); words > 0 && bound >= minimum {
			r.pos += words
			left = r.postfixOperation(operator, bound, left)
			continue
		}
		if comparisons, words := r.betweenAt(); words > 0 && r.table.between >= minimum {
			r.pos += words
			if left, ok = r.betweenBounds(left, comparisons); !ok {
				return operation{}, false
			}
			continue
		}
		if operator, words := r.membershipAt(); words > 0 && r.table.membership >= minimum {
			r.pos += words
			if left, ok = r.membership(left, operator); !ok {
				return operation{}, false
			}
			continue
		}
		operator, bound, found := r.binary()
		if !found || bound < minimum {
			return left, true
		}
		r.pos++
		right, ok := r.expression(bound + 1)
		if !ok {
			return operation{}, false
		}
		if operator == "+" && isInterval(left) && !isInterval(right) {
			// INTERVAL 1 DAY + d is d + INTERVAL 1 DAY, which is how both
			// servers store it.
			left, right = right, left
		}
		left = r.binaryOperation(r.spell(operator), bound, left, right)
	}
}

// binaryOperation writes left operator right, where operator, as spelled,
// binds at bound.
func (r *reader) binaryOperation(operator string, bound int, left, right operation) operation {
	if r.table.pushesNot && (operator == "=" || operator == "<>") && left.text == "0" {
		// MySQL 26.10 stores NOT a as 0 = a, where MariaDB 12.3 stores
		// a = 0, so 0 compared for equality is written on the right.
		left, right = right, left
	}
	rightLeast := bound + 1
	if r.table.associative[operator] {
		rightLeast = bound
	}
	op := operation{
		text:       left.enclosed(bound) + " " + operator + " " + right.enclosed(rightLeast),
		precedence: bound,
		truth:      truthOperators[operator],
	}
	if complement, found := complements[operator]; found {
		op.negation = func() operation { return r.binaryOperation(complement, bound, left, right) }
	}
	if joined, found := deMorgan[operator]; found {
		op.negation = func() operation {
			return r.binaryOperation(joined, r.table.binary[joined], r.not(left), r.not(right))
		}
	}
	return op
}

// not returns NOT over op as the table stores it. Where the table pushes NOT
// into the operation, that is op's negation where op has one, op = 0 where op
// is no truth value, and NOT written before op otherwise. Elsewhere it is NOT
// written before op.
func (r *reader) not(op operation) operation {
	switch {
	case r.table.pushesNot && op.negation != nil:
		return op.negation()
	case r.table.pushesNot && !op.truth:
		// MySQL 26.10 stores NOT a, NOT abs(a) and NOT (a + 1) as 0 = a,
		// 0 = abs(a) and 0 = (a + 1), and NOT NOT a as 0 <> a. MariaDB 12.3
		// stores NOT a as a = 0, and NOT abs(a) as !abs(a).
		return r.binaryOperation("=", r.table.binary["="], op, atomOperation("0"))
	}
	bound := r.table.prefixWords["NOT"]
	negated := operation{text: "NOT " + op.enclosed(bound+1), precedence: bound, truth: true}
	if r.table.pushesNot {
		negated.negation = func() operation { return op }
	}
	return negated
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

// betweenComparisons are how an engine stores a BETWEEN: the words it keeps
// on a table that keeps them ([table.keepsBetween]), and otherwise the
// comparisons operand below low, join, operand above high.
type betweenComparisons struct {
	written            string
	below, above, join string
}

var (
	// withinBounds is what x BETWEEN low AND high is stored as.
	withinBounds = betweenComparisons{written: "BETWEEN", below: ">=", above: "<=", join: "AND"}
	// outsideBounds is what x NOT BETWEEN low AND high is stored as.
	outsideBounds = betweenComparisons{written: "NOT BETWEEN", below: "<", above: ">", join: "OR"}
)

// negation returns the comparisons of the BETWEEN NOT over this one is.
func (c betweenComparisons) negation() betweenComparisons {
	if c == withinBounds {
		return outsideBounds
	}
	return withinBounds
}

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
// consumed, after operand, and writes the operation. An operand that binds no
// tighter than BETWEEN, such as a IS NULL or another BETWEEN, is not read,
// since no measurement says how the engine groups it.
func (r *reader) betweenBounds(operand operation, comparisons betweenComparisons) (operation, bool) {
	between := r.table.between
	if operand.precedence <= between {
		return operation{}, false
	}
	low, ok := r.expression(between + 1)
	if !ok {
		return operation{}, false
	}
	if and, more := r.peek(); !more || and.kind != kindWord || !strings.EqualFold(and.text, "AND") {
		return operation{}, false
	}
	r.pos++
	high, ok := r.expression(between + 1)
	if !ok {
		return operation{}, false
	}
	return r.betweenOperation(operand, low, high, comparisons), true
}

// betweenOperation writes the BETWEEN of operand within or outside low and
// high as the engine stores it: as written on a table that keeps it, and
// otherwise as operand >= low AND operand <= high for [withinBounds] and
// operand < low OR operand > high for [outsideBounds].
func (r *reader) betweenOperation(operand, low, high operation, comparisons betweenComparisons) operation {
	if !r.table.keepsBetween {
		below := r.binaryOperation(comparisons.below, r.table.binary[comparisons.below], operand, low)
		above := r.binaryOperation(comparisons.above, r.table.binary[comparisons.above], operand, high)
		return r.binaryOperation(comparisons.join, r.table.binary[comparisons.join], below, above)
	}
	least := r.table.between + 1
	return operation{
		text: operand.enclosed(least) + " " + comparisons.written + " " +
			low.enclosed(least) + " AND " + high.enclosed(least),
		precedence: r.table.between,
		truth:      true,
		negation: func() operation {
			return r.betweenOperation(operand, low, high, comparisons.negation())
		},
	}
}

// membershipAt returns the operator whose words start at the reader's
// position, IN, LIKE, and REGEXP or RLIKE where the table reads them as calls
// ([table.regexpCalls]), each with or without NOT before it, and how many
// words it spans, without consuming it. It answers 0 words where none starts
// there or the table does not read them.
func (r *reader) membershipAt() (operator string, words int) {
	if r.table.membership == 0 {
		return "", 0
	}
	var spelled []string
	for index := r.pos; index < len(r.tokens) && index < r.pos+2; index++ {
		tok := r.tokens[index]
		if tok.kind != kindWord {
			return "", 0
		}
		word := strings.ToUpper(tok.text)
		spelled = append(spelled, word)
		switch {
		case word == "IN" || word == "LIKE" || r.table.regexpCalls && (word == "REGEXP" || word == "RLIKE"):
			return strings.Join(spelled, " "), len(spelled)
		case word != "NOT":
			return "", 0
		}
	}
	return "", 0
}

// membership reads the right operand of operator, which [reader.membershipAt]
// returned and whose words the reader has consumed, after the left operand
// left, and writes the operation.
func (r *reader) membership(left operation, operator string) (operation, bool) {
	level := r.table.membership
	name, negated := strings.CutPrefix(operator, "NOT ")
	switch name {
	case "IN":
		list, single, ok := r.members()
		switch {
		case !ok:
			return operation{}, false
		case single != nil:
			// MySQL 26.10 and MariaDB 12.3 store a IN (1) as a = 1, and
			// a NOT IN (1) as a <> 1.
			comparison := "="
			if negated {
				comparison = "<>"
			}
			return r.binaryOperation(comparison, r.table.binary[comparison], left, *single), true
		}
		return r.memberOperation(operator, left, list), true
	case "REGEXP", "RLIKE":
		pattern, ok := r.expression(level + 1)
		if !ok {
			return operation{}, false
		}
		call := atomOperation("regexp_like(" + left.text + ", " + pattern.text + ")")
		if negated {
			return r.not(call), true
		}
		return call, true
	}
	right, ok := r.expression(level + 1)
	if !ok {
		return operation{}, false
	}
	return r.memberOperation(operator, left, right.enclosed(level+1)), true
}

// memberOperation writes left operator right, where operator is IN or LIKE,
// with or without NOT before it.
func (r *reader) memberOperation(operator string, left operation, right string) operation {
	return operation{
		text:       left.enclosed(r.table.membership) + " " + operator + " " + right,
		precedence: r.table.membership,
		truth:      true,
		negation:   func() operation { return r.memberOperation(negatedOperator(operator), left, right) },
	}
}

// negatedOperator returns NOT operator, and operator without its NOT where it
// has one.
func negatedOperator(operator string) string {
	if positive, negated := strings.CutPrefix(operator, "NOT "); negated {
		return positive
	}
	return "NOT " + operator
}

// members reads the operand of IN and writes it in parentheses: a list of two
// or more where a parenthesized list follows IN. On a table that reads a list
// of one as its member ([table.singleMemberIn]) it returns that member, and
// IN takes nothing else. Elsewhere it is every operation that binds tighter
// than IN, so that a IN 1 + 2, a IN (1 + 2) and a IN (1) + 2 give one key, as
// ClickHouse 24.10 and 26.10 read them. A list before an operator that binds
// tighter is not read, since no measurement says how the engine groups it.
func (r *reader) members() (list string, single *operation, ok bool) {
	level := r.table.membership
	if list, ok := r.list(); ok {
		return list, nil, !r.bindsTighter(level)
	}
	if r.table.singleMemberIn {
		if !r.consume(kindOpen) {
			return "", nil, false
		}
		member, ok := r.expression(0)
		if !ok || !r.consume(kindClose) || r.bindsTighter(level) {
			return "", nil, false
		}
		return "", &member, true
	}
	operand, ok := r.expression(level + 1)
	return "(" + operand.text + ")", nil, ok
}

// list reads the parenthesized list of two or more expressions at the
// reader's position and writes it, and leaves the reader where it is where
// no such list starts there.
func (r *reader) list() (string, bool) {
	start := r.pos
	if !r.consume(kindOpen) {
		return "", false
	}
	var items []string
	for {
		item, ok := r.expression(0)
		if !ok {
			break
		}
		items = append(items, item.text)
		if r.consume(kindClose) {
			if len(items) > 1 {
				return "(" + strings.Join(items, ", ") + ")", true
			}
			break
		}
		if !r.consume(kindComma) {
			break
		}
	}
	r.pos = start
	return "", false
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

// postfixOperation writes operand operator, where operator binds at bound.
// NOT over IS x is IS NOT x, and NOT over IS NOT x is IS x, where the table
// reads both.
func (r *reader) postfixOperation(operator string, bound int, operand operation) operation {
	op := operation{text: operand.enclosed(bound) + " " + operator, precedence: bound, truth: true}
	complement, negates := strings.CutPrefix(operator, "IS NOT ")
	if negates {
		complement = "IS " + complement
	} else if rest, is := strings.CutPrefix(operator, "IS "); is {
		complement, negates = "IS NOT "+rest, true
	}
	if negates && r.table.postfix[complement] == bound {
		op.negation = func() operation { return r.postfixOperation(complement, bound, operand) }
	}
	return op
}

// unary reads a prefix operator and its operand, or an atom, as the operand
// of an operator that needs at least minimum.
func (r *reader) unary(minimum int) (operation, bool) {
	tok, more := r.peek()
	if !more {
		return operation{}, false
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
	operand, ok := r.unary(bound)
	if !ok {
		return operation{}, false
	}
	switch {
	case tok.text == "-" && folded && operand.text == "0" && r.table.decimalNumbers:
		return atomOperation(operand.text), true
	case tok.text == "-" && aboveInt64(operand.text):
		return operation{}, false
	case tok.text == "!" && r.table.pushesNot:
		// ! is NOT that binds tighter: MySQL 26.10 and MariaDB 12.3 store
		// !(a > 0) as a <= 0.
		return r.not(operand), true
	}
	return operation{text: tok.text + operand.enclosed(bound), precedence: bound}, true
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
// its whole operand. NOT is the one prefix word a table holds.
func (r *reader) prefixWord(word string, bound, minimum int) (operation, bool) {
	if minimum > bound {
		return operation{}, false
	}
	r.pos++
	next, more := r.peek()
	if more && next.kind == kindOpen && r.table.calledPrefix[word] {
		group, ok := r.atom()
		if !ok || r.bindsTighter(bound) {
			return operation{}, false
		}
		return r.not(group), true
	}
	operand, ok := r.operations(bound+1, bound)
	if !ok {
		return operation{}, false
	}
	return r.not(operand), true
}

// bindsTighter reports whether the operator at the reader's position binds
// tighter than bound.
func (r *reader) bindsTighter(bound int) bool {
	if _, precedence, words := r.postfix(); words > 0 {
		return precedence > bound
	}
	if _, words := r.membershipAt(); words > 0 {
		return r.table.membership > bound
	}
	_, precedence, found := r.binary()
	return found && precedence > bound
}

// atom reads a literal, a name, a function call or a parenthesized group.
func (r *reader) atom() (operation, bool) {
	tok, more := r.peek()
	if !more {
		return operation{}, false
	}
	r.pos++
	switch tok.kind {
	case kindLiteral:
		return atomOperation(tok.text), true
	case kindName:
		if next, more := r.peek(); more && next.kind == kindOpen {
			return operation{}, false
		}
		return atomOperation(quoteName(tok.text)), true
	case kindWord:
		return r.word(tok.text)
	case kindOpen:
		group, ok := r.expression(0)
		if !ok || !r.consume(kindClose) {
			return operation{}, false
		}
		return group, true
	default:
		return operation{}, false
	}
}

// word reads a bare word: a literal, an INTERVAL, a function call or a name.
func (r *reader) word(word string) (operation, bool) {
	upper := strings.ToUpper(word)
	opens := r.consume(kindOpen)
	switch {
	case r.table.literals[upper] && !opens:
		return atomOperation(upper), true
	case upper == "INTERVAL" && r.table.units != nil:
		if opens {
			r.pos--
		}
		return r.interval()
	case r.table.niladic[upper] != "" && !opens:
		return atomOperation(r.table.niladic[upper] + "()"), true
	case r.table.keywords[upper] || r.table.literals[upper]:
		return operation{}, false
	case !opens:
		return atomOperation(quoteName(word)), true
	}
	if r.table.functionCase {
		return r.call(word)
	}
	return r.call(strings.ToLower(word))
}

// call reads the arguments of a call of the function name, whose opening
// parenthesis the reader has consumed.
func (r *reader) call(name string) (operation, bool) {
	var arguments []operation
	if !r.consume(kindClose) {
		for {
			argument, ok := r.expression(0)
			if !ok {
				return operation{}, false
			}
			arguments = append(arguments, argument)
			if r.consume(kindClose) {
				break
			}
			if !r.consume(kindComma) {
				return operation{}, false
			}
		}
	}
	if operator, shifts := r.table.shifts[name]; shifts && len(arguments) == 2 && isInterval(arguments[1]) {
		return r.binaryOperation(operator, r.table.binary[operator], arguments[0], arguments[1]), true
	}
	if synonym, found := r.table.synonyms[name]; found {
		name = synonym
	}
	texts := make([]string, len(arguments))
	for i, argument := range arguments {
		texts[i] = argument.text
	}
	return atomOperation(name + "(" + strings.Join(texts, ", ") + ")"), true
}

// interval reads the expression and the unit of an INTERVAL, whose keyword the
// reader has consumed.
func (r *reader) interval() (operation, bool) {
	amount, ok := r.expression(0)
	if !ok {
		return operation{}, false
	}
	unit, more := r.peek()
	if !more || unit.kind != kindWord || !r.table.units[strings.ToUpper(unit.text)] {
		return operation{}, false
	}
	r.pos++
	return atomOperation("INTERVAL " + amount.enclosed(atomPrecedence) + " " + strings.ToUpper(unit.text)), true
}

// isInterval reports whether an operand is an INTERVAL. No other atom the key
// prints begins with the word: a name is quoted, a function name is in lower
// case and a literal word is NULL, TRUE or FALSE.
func isInterval(op operation) bool {
	return op.precedence == atomPrecedence && strings.HasPrefix(op.text, "INTERVAL ")
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
