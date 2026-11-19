package precedence_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
	"ptah.run/internal/precedence"
)

// mysqlLexing reads text the way the MySQL dialect's lexer does: standard
// strings with backslash escapes, and -- as a comment only before whitespace.
var mysqlLexing = lexer.Options{StandardStrings: true, BackslashEscapes: true, RequireWhitespaceAfterDashDash: true}

// TestCanonical_HappyPath pins the key for a declared expression and for the
// spelling MySQL 26.10 or MariaDB 12.3 stores for it. Each stored spelling is
// what information_schema.COLUMNS.COLUMN_DEFAULT reported for the declared one.
func TestCanonical_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
		want     string
	}{
		{name: "multiplication before addition", declared: "2 * 3 + 1", stored: "((2 * 3) + 1)", want: "2 * 3 + 1"},
		{name: "addition after multiplication", declared: "1 + 2 * 3", stored: "(1 + (2 * 3))", want: "1 + 2 * 3"},
		{name: "a group precedence needs", declared: "(1 + 2) * 3", stored: "((1 + 2) * 3)", want: "(1 + 2) * 3"},
		{name: "subtraction groups to the left", declared: "1 - 2 - 3", stored: "((1 - 2) - 3)", want: "1 - 2 - 3"},
		{name: "a right-hand group of one precedence", declared: "1 - (2 - 3)", stored: "(1 - (2 - 3))", want: "1 - (2 - 3)"},
		{name: "addition groups to the left", declared: "1 + 2 + 3", stored: "((1 + 2) + 3)", want: "1 + 2 + 3"},
		{name: "division and multiplication", declared: "10 / 2 * 3", stored: "((10 / 2) * 3)", want: "10 / 2 * 3"},
		{name: "a negative literal", declared: "-1 + 2", stored: "(-(1) + 2)", want: "-1 + 2"},
		{name: "a negated group", declared: "-(1 + 2)", stored: "-((1 + 2))", want: "-(1 + 2)"},
		{name: "MOD is %", declared: "7 % 4 * 2", stored: "(7 MOD 4 * 2)", want: "7 % 4 * 2"},
		{name: "!= is <>", declared: "1 != 2", stored: "(1 <> 2)", want: "1 <> 2"},
		{name: "column references", declared: "a + b * 2", stored: "(`a` + (`b` * 2))", want: "`a` + `b` * 2"},
		{name: "a function call", declared: "CONCAT('a', 1 + 2)", stored: "concat('a',(1 + 2))", want: "concat('a', 1 + 2)"},
		{name: "a function without arguments", declared: "now()", stored: "NOW( )", want: "now()"},
		{name: "AND before OR", declared: "1 = 1 OR 2 = 2 AND 3 = 4", stored: "((1 = 1) or ((2 = 2) and (3 = 4)))", want: "1 = 1 OR 2 = 2 AND 3 = 4"},
		{name: "a group before AND", declared: "(1 = 1 OR 2 = 2) AND 3 = 4", stored: "(((1 = 1) or (2 = 2)) and (3 = 4))", want: "(1 = 1 OR 2 = 2) AND 3 = 4"},
		{name: "& before |", declared: "1 | 2 & 3", stored: "(1 | (2 & 3))", want: "1 | 2 & 3"},
		{name: "+ before <<", declared: "1 << 2 + 1", stored: "(1 << (2 + 1))", want: "1 << 2 + 1"},
		{name: "^ before unary minus", declared: "-2 ^ 2", stored: "(-(2) ^ 2)", want: "-2 ^ 2"},
		{name: "a negated power", declared: "-(2 ^ 2)", stored: "-((2 ^ 2))", want: "-(2 ^ 2)"},
		{name: "symbols written together", declared: "1<=>2", stored: "(1 <=> 2)", want: "1 <=> 2"},
		{name: "a comment between operands", declared: "1 /* one */ + 2", stored: "(1 + 2)", want: "1 + 2"},
		{name: "a quote escaped two ways", declared: `'a''b'`, stored: `('a\'b')`, want: `'a''b'`},
		{name: "a double-quoted string", declared: `upper("x")`, stored: `upper('x')`, want: `upper('x')`},
		{name: "the session's introducer", declared: "concat('a', 'b')", stored: "concat(_utf8mb4'a',_utf8mb4'b')", want: "concat('a', 'b')"},
		{name: "a literal word", declared: "null", stored: "(NULL)", want: "NULL"},
		{name: "spaces MariaDB adds", declared: "1+2", stored: "(1 + 2)", want: "1 + 2"},
		{name: "a negative literal alone", declared: "-1", stored: "-(1)", want: "-1"},
		{
			name:     "CURRENT_TIMESTAMP and an INTERVAL on MySQL",
			declared: "CURRENT_TIMESTAMP + INTERVAL 1 DAY",
			stored:   "(now() + interval 1 day)",
			want:     "now() + INTERVAL 1 DAY",
		},
		{
			name:     "CURRENT_TIMESTAMP and an INTERVAL on MariaDB",
			declared: "CURRENT_TIMESTAMP + INTERVAL 1 DAY",
			stored:   "(current_timestamp() + interval 1 day)",
			want:     "now() + INTERVAL 1 DAY",
		},
		{name: "LOCALTIMESTAMP", declared: "LOCALTIMESTAMP + INTERVAL 1 DAY", stored: "(localtimestamp() + interval 1 day)", want: "now() + INTERVAL 1 DAY"},
		{name: "CURRENT_DATE", declared: "CURRENT_DATE + INTERVAL 1 DAY", stored: "(curdate() + interval 1 day)", want: "curdate() + INTERVAL 1 DAY"},
		{name: "a precision", declared: "CURRENT_TIMESTAMP(3) - INTERVAL 2 MINUTE", stored: "(now(3) - interval 2 minute)", want: "now(3) - INTERVAL 2 MINUTE"},
		{name: "an INTERVAL amount MySQL groups", declared: "NOW() + INTERVAL 1 + 1 HOUR", stored: "(now() + interval (1 + 1) hour)", want: "now() + INTERVAL (1 + 1) HOUR"},
		{name: "an INTERVAL amount MariaDB keeps", declared: "NOW() + INTERVAL 1 + 1 HOUR", stored: "(current_timestamp() + interval 1 + 1 hour)", want: "now() + INTERVAL (1 + 1) HOUR"},
		{name: "an INTERVAL written first", declared: "INTERVAL 1 DAY + NOW()", stored: "(now() + interval 1 day)", want: "now() + INTERVAL 1 DAY"},
		{name: "DATE_ADD", declared: "DATE_ADD(NOW(), INTERVAL 1 DAY)", stored: "(now() + interval 1 day)", want: "now() + INTERVAL 1 DAY"},
		{name: "DATE_SUB", declared: "DATE_SUB(CURDATE(), INTERVAL 1 + 1 DAY)", stored: "(curdate() - interval (1 + 1) day)", want: "curdate() - INTERVAL (1 + 1) DAY"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declared, declaredRead := precedence.Canonical(test.declared, precedence.MySQLOperators, mysqlLexing)
			stored, storedRead := precedence.Canonical(test.stored, precedence.MySQLOperators, mysqlLexing)

			c.Assert(declaredRead, qt.IsTrue)
			c.Assert(storedRead, qt.IsTrue)
			c.Assert(declared, qt.Equals, test.want)
			c.Assert(stored, qt.Equals, test.want)
		})
	}
}

// TestCanonical_Conditions_HappyPath pins the key for a declared condition
// and for the spelling MySQL 26.10 or MariaDB 12.3 stores for it. Each stored
// spelling is what information_schema.CHECK_CONSTRAINTS.CHECK_CLAUSE reported
// for the declared one in a CHECK, with MySQL's extra escaping and encoding
// taken off as the MySQL reader takes them off.
func TestCanonical_Conditions_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
		want     string
	}{
		{
			name:     "BETWEEN inside AND on MySQL",
			declared: "id between 1 and 10 and c is not null",
			stored:   "((`id` between 1 and 10) and (`c` is not null))",
			want:     "`id` BETWEEN 1 AND 10 AND `c` IS NOT NULL",
		},
		{
			name:     "BETWEEN inside AND on MariaDB",
			declared: "id between 1 and 10 and c is not null",
			stored:   "`id` between 1 and 10 and `c` is not null",
			want:     "`id` BETWEEN 1 AND 10 AND `c` IS NOT NULL",
		},
		{name: "NOT LIKE on MySQL", declared: "c NOT LIKE 'x%'", stored: "(not((`c` like _utf8mb4'x%')))", want: "`c` NOT LIKE 'x%'"},
		{name: "NOT LIKE on MariaDB", declared: "c NOT LIKE 'x%'", stored: "`c`  not like 'x%'", want: "`c` NOT LIKE 'x%'"},
		{name: "NOT over NOT LIKE", declared: "NOT (c NOT LIKE 'x')", stored: "(`c` like _utf8mb4'x')", want: "`c` LIKE 'x'"},
		{name: "X'41' on MySQL", declared: "b <> X'41'", stored: "(`b` <> 0x41)", want: "`b` <> 0x41"},
		{name: "X'41' on MariaDB", declared: "b <> X'41'", stored: "`b` <> X'41'", want: "`b` <> 0x41"},
		{
			name:     "literals of bytes",
			declared: "d <> b'100000000' AND d <> 0b101 AND d <> X'' AND d <> 0xABC AND d <> x'4a'",
			stored:   "((`d` <> 0x0100) and (`d` <> 0x05) and (`d` <> X'') and (`d` <> 0x0abc) and (`d` <> 0x4a))",
			want:     "`d` <> 0x0100 AND `d` <> 0x05 AND `d` <> X'' AND `d` <> 0x0abc AND `d` <> 0x4a",
		},
		{name: "an introducer apart from its string", declared: "b <> _binary 'ab'", stored: "(`b` <> _binary'ab')", want: "`b` <> _binary'ab'"},
		{
			name:     "introducers",
			declared: "c <> _latin1 'ab' AND c <> _UTF8MB4'cd' AND c <> N'ef' AND c <> n'gh'",
			stored:   "((`c` <> _latin1'ab') and (`c` <> _utf8mb4'cd') and (`c` <> _utf8mb3'ef') and (`c` <> _utf8mb3'gh'))",
			want:     "`c` <> _latin1'ab' AND `c` <> 'cd' AND `c` <> _utf8mb3'ef' AND `c` <> _utf8mb3'gh'",
		},
		{name: "a string beyond ASCII", declared: "c <> 'héllo ☃'", stored: "(`c` <> _utf8mb4'héllo ☃')", want: "`c` <> 'héllo ☃'"},
		{name: "NOT IN", declared: "c NOT IN ('x', 'y')", stored: "(`c` not in (_utf8mb4'x',_utf8mb4'y'))", want: "`c` NOT IN ('x', 'y')"},
		{name: "NOT over IN", declared: "NOT (a IN (1, 2))", stored: "(`a` not in (1,2))", want: "`a` NOT IN (1, 2)"},
		{name: "IN over one member", declared: "a IN (1)", stored: "(`a` = 1)", want: "`a` = 1"},
		{name: "NOT IN over one member", declared: "a NOT IN (1)", stored: "`a` <> 1", want: "`a` <> 1"},
		{name: "NOT BETWEEN", declared: "a NOT BETWEEN 1 AND 5", stored: "(`a` not between 1 and 5)", want: "`a` NOT BETWEEN 1 AND 5"},
		{name: "NOT over BETWEEN", declared: "NOT (a BETWEEN 1 AND 2)", stored: "(`a` not between 1 and 2)", want: "`a` NOT BETWEEN 1 AND 2"},
		{name: "NOT REGEXP on MySQL", declared: "c NOT REGEXP '^x'", stored: "(not(regexp_like(`c`,_utf8mb4'^x')))", want: "regexp_like(`c`, '^x') = 0"},
		{name: "NOT REGEXP on MariaDB", declared: "c NOT REGEXP '^x'", stored: "`c` not regexp '^x'", want: "regexp_like(`c`, '^x') = 0"},
		{name: "RLIKE", declared: "c RLIKE '^x'", stored: "regexp_like(`c`,_utf8mb4'^x')", want: "regexp_like(`c`, '^x')"},
		{name: "NOT over =", declared: "NOT (a = 1)", stored: "(`a` <> 1)", want: "`a` <> 1"},
		{name: "NOT binds looser than =", declared: "NOT a = 1", stored: "(`a` <> 1)", want: "`a` <> 1"},
		{name: "NOT over <", declared: "NOT (a < 1)", stored: "(`a` >= 1)", want: "`a` >= 1"},
		{name: "NOT over <>", declared: "NOT (a <> 1)", stored: "(`a` = 1)", want: "`a` = 1"},
		{name: "! over >", declared: "!(a > 0)", stored: "(`a` <= 0)", want: "`a` <= 0"},
		{name: "NOT over NOT over =", declared: "NOT NOT (a = 1)", stored: "(`a` = 1)", want: "`a` = 1"},
		{name: "NOT over AND on MySQL", declared: "NOT (a = 1 AND b = 2)", stored: "((`a` <> 1) or (`b` <> 2))", want: "`a` <> 1 OR `b` <> 2"},
		{name: "NOT over AND on MariaDB", declared: "NOT (a = 1 AND b = 2)", stored: "`a` <> 1 or `b` <> 2", want: "`a` <> 1 OR `b` <> 2"},
		{
			name:     "NOT over OR inside AND",
			declared: "a NOT BETWEEN 1 AND 2 AND NOT (a > 5 OR b > 5)",
			stored:   "((`a` not between 1 and 2) and ((`a` <= 5) and (`b` <= 5)))",
			want:     "`a` NOT BETWEEN 1 AND 2 AND `a` <= 5 AND `b` <= 5",
		},
		{name: "NOT over IS NULL", declared: "a IS NOT NULL AND NOT (a IS NULL)", stored: "((`a` is not null) and (`a` is not null))", want: "`a` IS NOT NULL AND `a` IS NOT NULL"},
		{name: "NOT over IS NOT NULL", declared: "NOT (a IS NOT NULL)", stored: "(`a` is null)", want: "`a` IS NULL"},
		{name: "IS TRUE on MySQL", declared: "(a = 1) IS TRUE", stored: "((`a` = 1) is true)", want: "`a` = 1 IS TRUE"},
		{name: "IS TRUE on MariaDB", declared: "(a = 1) IS TRUE", stored: "`a` = 1 is true", want: "`a` = 1 IS TRUE"},
		{name: "NOT over IS TRUE", declared: "NOT ((a = 1) IS TRUE)", stored: "((`a` = 1) is not true)", want: "`a` = 1 IS NOT TRUE"},
		{name: "IS NOT FALSE", declared: "a > 0 IS NOT FALSE", stored: "((`a` > 0) is not false)", want: "`a` > 0 IS NOT FALSE"},
		{name: "NOT binds looser than IS", declared: "NOT (a > 0) IS NULL", stored: "((`a` > 0) is not null)", want: "`a` > 0 IS NOT NULL"},
		{name: "NOT over <=> on MySQL", declared: "NOT (a <=> 1)", stored: "(not((`a` <=> 1)))", want: "NOT `a` <=> 1"},
		{name: "NOT over <=> on MariaDB", declared: "NOT (a <=> 1)", stored: "!(`a` <=> 1)", want: "NOT `a` <=> 1"},
		{name: "NOT over a column on MySQL", declared: "NOT a OR b > 0", stored: "((0 = `a`) or (`b` > 0))", want: "`a` = 0 OR `b` > 0"},
		{name: "NOT over a column on MariaDB", declared: "NOT a OR b > 0", stored: "`a` = 0 or `b` > 0", want: "`a` = 0 OR `b` > 0"},
		{name: "NOT over NOT over a column on MySQL", declared: "NOT NOT a", stored: "(0 <> `a`)", want: "`a` <> 0"},
		{name: "NOT over NOT over a column on MariaDB", declared: "NOT NOT a", stored: "`a` <> 0", want: "`a` <> 0"},
		{name: "NOT over a call on MySQL", declared: "NOT abs(a)", stored: "(0 = abs(`a`))", want: "abs(`a`) = 0"},
		{name: "NOT over a call on MariaDB", declared: "NOT abs(a)", stored: "!abs(`a`)", want: "abs(`a`) = 0"},
		{name: "NOT over a sum on MySQL", declared: "NOT (a + 1)", stored: "(0 = (`a` + 1))", want: "`a` + 1 = 0"},
		{name: "NOT over a sum on MariaDB", declared: "NOT (a + 1)", stored: "!(`a` + 1)", want: "`a` + 1 = 0"},
		{name: "0 = a", declared: "0 = a", stored: "(0 = `a`)", want: "`a` = 0"},
		{name: "AND before OR", declared: "a = 1 OR b = 2 AND c IS NULL", stored: "((`a` = 1) or ((`b` = 2) and (`c` is null)))", want: "`a` = 1 OR `b` = 2 AND `c` IS NULL"},
		{name: "a group around OR", declared: "(a = 1 OR b = 2) AND c IS NULL", stored: "(((`a` = 1) or (`b` = 2)) and (`c` is null))", want: "(`a` = 1 OR `b` = 2) AND `c` IS NULL"},
		{name: "AND inside AND", declared: "a > 0 AND (b > 0 AND c > 0)", stored: "((`a` > 0) and (`b` > 0) and (`c` > 0))", want: "`a` > 0 AND `b` > 0 AND `c` > 0"},
		{name: "XOR", declared: "a > 0 XOR b > 0", stored: "((`a` > 0) xor (`b` > 0))", want: "`a` > 0 XOR `b` > 0"},
		{name: "arithmetic", declared: "a + b * 2 > 0", stored: "((`a` + (`b` * 2)) > 0)", want: "`a` + `b` * 2 > 0"},
		{name: "unary minus on MySQL", declared: "a - -1 > 0 AND -a < 5", stored: "(((`a` - -(1)) > 0) and (-(`a`) < 5))", want: "`a` - -1 > 0 AND -`a` < 5"},
		{name: "unary minus on MariaDB", declared: "a - -1 > 0 AND -a < 5", stored: "`a` - -1 > 0 and -`a` < 5", want: "`a` - -1 > 0 AND -`a` < 5"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declared, declaredRead := precedence.Canonical(test.declared, precedence.MySQLOperators, mysqlLexing)
			stored, storedRead := precedence.Canonical(test.stored, precedence.MySQLOperators, mysqlLexing)

			c.Assert(declaredRead, qt.IsTrue)
			c.Assert(storedRead, qt.IsTrue)
			c.Assert(declared, qt.Equals, test.want)
			c.Assert(stored, qt.Equals, test.want)
		})
	}
}

// TestCanonical_DifferentGroupingsDiffer pins that parentheses precedence needs
// stay part of the key, so two expressions that group differently never
// compare equal.
func TestCanonical_DifferentGroupingsDiffer(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
	}{
		{name: "a group around the addition", left: "(1 + 2) * 3", right: "1 + 2 * 3"},
		{name: "a right-hand subtraction", left: "1 - (2 - 3)", right: "1 - 2 - 3"},
		{name: "a right-hand addition", left: "1 + (2 + 3)", right: "1 + 2 + 3"},
		{name: "a group around OR", left: "(1 = 1 OR 2 = 2) AND 3 = 4", right: "1 = 1 OR 2 = 2 AND 3 = 4"},
		{name: "a negated sum", left: "-(1 + 2)", right: "-1 + 2"},
		{name: "a name and a string", left: "`a`", right: "'a'"},
		{name: "DIV and /", left: "7 DIV 2", right: "7 / 2"},
		{name: "another unit", left: "now() + INTERVAL 1 DAY", right: "now() + INTERVAL 1 HOUR"},
		{name: "an INTERVAL subtracted", left: "now() - INTERVAL 1 DAY", right: "now() + INTERVAL 1 DAY"},
		{name: "CURDATE and NOW", left: "curdate()", right: "now()"},
		{name: "CURRENT_DATE and CURRENT_TIME", left: "CURRENT_DATE", right: "CURRENT_TIME"},
		{name: "% binds as tightly as *", left: "7 % 4 * 2", right: "7 % (4 * 2)"},
		{name: "* binds as tightly as %", left: "2 * 7 % 4", right: "2 * (7 % 4)"},
		{name: "strings in another case", left: "'a'", right: "'A'"},
		{name: "NOT over NOT over a number", left: "NOT NOT a", right: "a"},
		{name: "NOT over <=>, which has no complement", left: "NOT (a <=> 1)", right: "a <> 1"},
		{name: "NOT over XOR", left: "NOT (a XOR b)", right: "NOT a XOR b"},
		{name: "a group around OR under AND", left: "a AND (b OR c)", right: "a AND b OR c"},
		{name: "IN over two members", left: "a IN (1, 2)", right: "a = 1"},
		{name: "LIKE and NOT LIKE", left: "c LIKE 'x'", right: "c NOT LIKE 'x'"},
		{name: "IS NULL and IS NOT NULL", left: "a IS NULL", right: "a IS NOT NULL"},
		{name: "BETWEEN and NOT BETWEEN", left: "a BETWEEN 1 AND 2", right: "a NOT BETWEEN 1 AND 2"},
		{name: "0 before <", left: "0 < a", right: "a < 0"},
		{name: "an introducer that is not the session's", left: "c <> _binary'a'", right: "c <> 'a'"},
		{name: "a national string", left: "c <> N'a'", right: "c <> 'a'"},
		{name: "bytes and their characters", left: "c <> 0x41", right: "c <> 'A'"},
		{name: "no bytes and the empty string", left: "c <> X''", right: "c <> ''"},
		{name: "two bytes and one", left: "c <> b'100000000'", right: "c <> b'00000001'"},
		{name: "REGEXP and LIKE", left: "c REGEXP 'x'", right: "c LIKE 'x'"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			left, leftRead := precedence.Canonical(test.left, precedence.MySQLOperators, mysqlLexing)
			right, rightRead := precedence.Canonical(test.right, precedence.MySQLOperators, mysqlLexing)

			c.Assert(leftRead, qt.IsTrue)
			c.Assert(rightRead, qt.IsTrue)
			c.Assert(left, qt.Not(qt.Equals), right)
		})
	}
}

// TestCanonical_FailurePath pins the expressions the MySQL table does not
// read. Each one holds a construct whose grouping the table cannot state, so
// the comparison falls back to the text as written.
func TestCanonical_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		scheme     string
	}{
		{name: "an unknown scheme", expression: "1 + 2", scheme: "postgres"},
		{name: "no scheme", expression: "1 + 2", scheme: ""},
		{name: "an INTERVAL without a unit", expression: "now() + INTERVAL 1", scheme: precedence.MySQLOperators},
		{name: "an INTERVAL with an unknown unit", expression: "now() + INTERVAL 1 FORTNIGHT", scheme: precedence.MySQLOperators},
		{name: "the INTERVAL function", expression: "INTERVAL(1, 2, 3)", scheme: precedence.MySQLOperators},
		{name: "a niladic keyword the table does not read", expression: "CURRENT_USER", scheme: precedence.MySQLOperators},
		{name: "a character set introducer", expression: `concat(_utf8mb4\'a\',_utf8mb4\'b\')`, scheme: precedence.MySQLOperators},
		{name: "a qualified name", expression: "t.a + 1", scheme: precedence.MySQLOperators},
		{name: "|| reads by SQL mode", expression: "1 || 2", scheme: precedence.MySQLOperators},
		{name: "&&", expression: "1 && 2", scheme: precedence.MySQLOperators},
		{name: "adjacent strings", expression: "'a' 'b'", scheme: precedence.MySQLOperators},
		{name: "an exponent", expression: "1e3", scheme: precedence.MySQLOperators},
		{name: "a quoted function name", expression: "`f`(1)", scheme: precedence.MySQLOperators},
		{name: "an unclosed string", expression: "'a", scheme: precedence.MySQLOperators},
		{name: "0X before digits, which MySQL reads as a name", expression: "a <> 0X41", scheme: precedence.MySQLOperators},
		{name: "an odd number of hexadecimal digits", expression: "a <> X'abc'", scheme: precedence.MySQLOperators},
		{name: "a hexadecimal digit out of range", expression: "a <> X'4g'", scheme: precedence.MySQLOperators},
		{name: "0x without digits", expression: "a <> 0x", scheme: precedence.MySQLOperators},
		{name: "no bits", expression: "a <> b''", scheme: precedence.MySQLOperators},
		{name: "a digit other than a bit", expression: "a <> b'012'", scheme: precedence.MySQLOperators},
		{name: "IS UNKNOWN", expression: "a IS UNKNOWN", scheme: precedence.MySQLOperators},
		{name: "LIKE with ESCAPE", expression: "c LIKE 'x!%' ESCAPE '!'", scheme: precedence.MySQLOperators},
		{name: "CASE", expression: "CASE WHEN a > 0 THEN 1 ELSE 0 END = 1", scheme: precedence.MySQLOperators},
		{name: "IN without a parenthesis", expression: "a IN 1", scheme: precedence.MySQLOperators},
		{name: "a list of one before an operator that binds tighter", expression: "a IN (1) + 2", scheme: precedence.MySQLOperators},
		{name: "a BETWEEN operand that binds no tighter", expression: "a BETWEEN 1 AND 2 BETWEEN 0 AND 1", scheme: precedence.MySQLOperators},
		{name: "BETWEEN without AND", expression: "a BETWEEN 1 OR 2", scheme: precedence.MySQLOperators},
		{name: "NOT where an operand of = stands", expression: "a = NOT b", scheme: precedence.MySQLOperators},
		{name: "a function named MOD", expression: "MOD(7, 4)", scheme: precedence.MySQLOperators},
		{name: "an unclosed group", expression: "(1 + 2", scheme: precedence.MySQLOperators},
		{name: "a missing operand", expression: "1 +", scheme: precedence.MySQLOperators},
		{name: "a row", expression: "(1, 2)", scheme: precedence.MySQLOperators},
		{name: "an executable comment", expression: "1 /*!80000 + 2 */", scheme: precedence.MySQLOperators},
		{name: "empty", expression: " ", scheme: precedence.MySQLOperators},
		{name: "a minus before an integer above the Int64 range", expression: "-(18446744073709551615)", scheme: precedence.MySQLOperators},
		{name: "a minus before a wide integer with a leading zero", expression: "1 - -018446744073709551615", scheme: precedence.MySQLOperators},
		{name: "a minus before a number padded past 19 digits", expression: "-00000000000000000001", scheme: precedence.MySQLOperators},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, read := precedence.Canonical(test.expression, test.scheme, lexer.Options{
				StandardStrings:                true,
				BackslashEscapes:               true,
				RequireWhitespaceAfterDashDash: true,
				ExecutableComments:             lexer.ExecutableCommentsMySQL,
			})

			c.Assert(read, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}
