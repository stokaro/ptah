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
		{name: "NOT", expression: "NOT 1 + 1", scheme: precedence.MySQLOperators},
		{name: "NOT before a parenthesis", expression: "NOT (1) + 1", scheme: precedence.MySQLOperators},
		{name: "IS NULL", expression: "a IS NULL", scheme: precedence.MySQLOperators},
		{name: "IN", expression: "a IN (1, 2)", scheme: precedence.MySQLOperators},
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
		{name: "another introducer", expression: "concat(_latin1'a')", scheme: precedence.MySQLOperators},
		{name: "an introducer apart from its string", expression: "concat(_utf8mb4 'a')", scheme: precedence.MySQLOperators},
		{name: "an unclosed string", expression: "'a", scheme: precedence.MySQLOperators},
		{name: "a function named MOD", expression: "MOD(7, 4)", scheme: precedence.MySQLOperators},
		{name: "an unclosed group", expression: "(1 + 2", scheme: precedence.MySQLOperators},
		{name: "a missing operand", expression: "1 +", scheme: precedence.MySQLOperators},
		{name: "a row", expression: "(1, 2)", scheme: precedence.MySQLOperators},
		{name: "an executable comment", expression: "1 /*!80000 + 2 */", scheme: precedence.MySQLOperators},
		{name: "empty", expression: " ", scheme: precedence.MySQLOperators},
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
