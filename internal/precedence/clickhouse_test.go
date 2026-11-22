package precedence_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lexer"
	"ptah.run/internal/precedence"
)

// clickHouseLexing reads text the way the ClickHouse dialect's lexer does:
// standard strings with backslash escapes.
var clickHouseLexing = lexer.Options{StandardStrings: true, BackslashEscapes: true}

// TestCanonical_ClickHouse_HappyPath pins the key for a declared expression
// and for the spelling ClickHouse 26.10 stores for it. Each stored spelling is
// what formatQuerySingleLine prints for the declared one on ClickHouse 26.10
// and 24.10, the text system.columns.default_expression reports for a default
// declared that way.
func TestCanonical_ClickHouse_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		stored   string
		want     string
	}{
		{name: "multiplication before addition", declared: "2 * 3 + 1", stored: "(2 * 3) + 1", want: "2 * 3 + 1"},
		{name: "addition after multiplication", declared: "1 + 2 * 3", stored: "1 + (2 * 3)", want: "1 + 2 * 3"},
		{name: "a group precedence needs", declared: "(1 + 2) * 3", stored: "(1 + 2) * 3", want: "(1 + 2) * 3"},
		{name: "a comparison after addition", declared: "1 + 2 > 2", stored: "(1 + 2) > 2", want: "1 + 2 > 2"},
		{name: "subtraction groups to the left", declared: "a - b - c", stored: "(a - b) - c", want: "`a` - `b` - `c`"},
		{name: "a right-hand group of one precedence", declared: "a - (b - c)", stored: "a - (b - c)", want: "`a` - (`b` - `c`)"},
		{name: "IS NOT NULL after addition", declared: "1 + 2 IS NOT NULL", stored: "(1 + 2) IS NOT NULL", want: "1 + 2 IS NOT NULL"},
		{name: "IS NULL after a comparison", declared: "a = 1 IS NULL", stored: "(a = 1) IS NULL", want: "`a` = 1 IS NULL"},
		{name: "NOT after AND", declared: "1 + 2 AND NOT 3", stored: "(1 + 2) AND (NOT 3)", want: "1 + 2 AND NOT 3"},
		{name: "NOT before a comparison", declared: "NOT a = 1", stored: "NOT (a = 1)", want: "NOT `a` = 1"},
		{name: "NOT before IS NULL", declared: "NOT a IS NULL", stored: "NOT (a IS NULL)", want: "NOT `a` IS NULL"},
		{name: "NOT before addition", declared: "NOT 1 + 2", stored: "NOT (1 + 2)", want: "NOT 1 + 2"},
		{name: "NOT before AND", declared: "NOT a AND b", stored: "(NOT a) AND b", want: "NOT `a` AND `b`"},
		{name: "NOT twice", declared: "NOT NOT a", stored: "NOT (NOT a)", want: "NOT (NOT `a`)"},
		{name: "NOT before a negation", declared: "NOT -a", stored: "NOT (-a)", want: "NOT -`a`"},
		{name: "AND before OR", declared: "a = 1 OR b = 2 AND c = 3", stored: "(a = 1) OR ((b = 2) AND (c = 3))", want: "`a` = 1 OR `b` = 2 AND `c` = 3"},
		{name: "a group before AND", declared: "(a = 1 OR b = 2) AND c = 3", stored: "((a = 1) OR (b = 2)) AND (c = 3)", want: "(`a` = 1 OR `b` = 2) AND `c` = 3"},
		{name: "OR before NOT and AND", declared: "a OR NOT b AND c", stored: "a OR ((NOT b) AND c)", want: "`a` OR NOT `b` AND `c`"},
		{name: "two IS NULL predicates", declared: "a IS NULL AND b IS NOT NULL", stored: "(a IS NULL) AND (b IS NOT NULL)", want: "`a` IS NULL AND `b` IS NOT NULL"},
		{name: "comparisons group to the left", declared: "a < b = c > 1", stored: "((a < b) = c) > 1", want: "`a` < `b` = `c` > 1"},
		{name: "a negation before multiplication", declared: "-a * 2", stored: "(-a) * 2", want: "-`a` * 2"},
		{name: "a negated group", declared: "-(1 + 2)", stored: "-(1 + 2)", want: "-(1 + 2)"},
		{name: "a negative operand", declared: "1 - -2", stored: "1 - -2", want: "1 - -2"},
		{name: "<> is !=", declared: "a <> b", stored: "a != b", want: "`a` != `b`"},
		{name: "== is =", declared: "a == b", stored: "a = b", want: "`a` = `b`"},
		{name: "a function keeps its case", declared: "plus(1, 2 * 3)", stored: "plus(1, 2 * 3)", want: "plus(1, 2 * 3)"},
		{name: "a double-quoted name", declared: `"a" + 1`, stored: "a + 1", want: "`a` + 1"},
		{name: "a backquoted name", declared: "`a` % 3 * 2", stored: "(a % 3) * 2", want: "`a` % 3 * 2"},
		{name: "a string", declared: "concat('x', 'y')", stored: "concat('x', 'y')", want: "concat('x', 'y')"},
		{name: "a decimal with a trailing zero", declared: "1.0 + 2", stored: "1. + 2", want: "1. + 2"},
		{name: "a decimal with trailing zeros", declared: "a * 1.50", stored: "a * 1.5", want: "`a` * 1.5"},
		{name: "a leading zero", declared: "010 * 2", stored: "10 * 2", want: "10 * 2"},
		{name: "a negative zero", declared: "-0 + a", stored: "0 + a", want: "0 + `a`"},
		{name: "a negated zero in parentheses", declared: "a - -(0)", stored: "a - -(0)", want: "`a` - -0"},
		{name: "a minus before a negative zero", declared: "- -0", stored: "-(0)", want: "-0"},
		{name: "a negative zero after a space", declared: "a - - 0", stored: "a - 0", want: "`a` - 0"},
		{name: "IS NOT NULL after a comparison", declared: "a = 1 IS NOT NULL", stored: "(a = 1) IS NOT NULL", want: "`a` = 1 IS NOT NULL"},
		{name: "IS NOT NULL before a comparison", declared: "a IS NOT NULL = b", stored: "(a IS NOT NULL) = b", want: "(`a` IS NOT NULL) = `b`"},
		{name: "NOT before IS NOT NULL", declared: "NOT a IS NOT NULL", stored: "NOT (a IS NOT NULL)", want: "NOT `a` IS NOT NULL"},
		{name: "IS NOT NULL after NOT and a comparison", declared: "NOT a = 1 IS NOT NULL", stored: "NOT ((a = 1) IS NOT NULL)", want: "NOT `a` = 1 IS NOT NULL"},
		{name: "a negated integer at the top of the Int64 range", declared: "-9223372036854775807", stored: "-9223372036854775807", want: "-9223372036854775807"},
		{name: "an integer above the Int64 range", declared: "18446744073709551615 + 1", stored: "18446744073709551615 + 1", want: "18446744073709551615 + 1"},
		{name: "BETWEEN", declared: "x BETWEEN 1 AND 3", stored: "(x >= 1) AND (x <= 3)", want: "`x` >= 1 AND `x` <= 3"},
		{name: "NOT BETWEEN", declared: "x NOT BETWEEN 1 AND 3", stored: "(x < 1) OR (x > 3)", want: "`x` < 1 OR `x` > 3"},
		{name: "BETWEEN before AND", declared: "x BETWEEN 1 AND 3 AND y", stored: "((x >= 1) AND (x <= 3)) AND y", want: "`x` >= 1 AND `x` <= 3 AND `y`"},
		{name: "BETWEEN before OR", declared: "x BETWEEN 1 AND 3 OR y", stored: "((x >= 1) AND (x <= 3)) OR y", want: "`x` >= 1 AND `x` <= 3 OR `y`"},
		{name: "NOT BETWEEN before AND", declared: "x NOT BETWEEN 1 AND 3 AND y", stored: "((x < 1) OR (x > 3)) AND y", want: "(`x` < 1 OR `x` > 3) AND `y`"},
		{name: "BETWEEN bounds that add and multiply", declared: "x BETWEEN 1 + 1 AND 3 * 2", stored: "(x >= (1 + 1)) AND (x <= (3 * 2))", want: "`x` >= 1 + 1 AND `x` <= 3 * 2"},
		{name: "BETWEEN after addition", declared: "x + 1 BETWEEN 1 AND 3", stored: "((x + 1) >= 1) AND ((x + 1) <= 3)", want: "`x` + 1 >= 1 AND `x` + 1 <= 3"},
		{name: "NOT before BETWEEN", declared: "NOT x BETWEEN 1 AND 3", stored: "NOT ((x >= 1) AND (x <= 3))", want: "NOT (`x` >= 1 AND `x` <= 3)"},
		{name: "IS NULL after BETWEEN", declared: "x BETWEEN 1 AND 3 IS NULL", stored: "((x >= 1) AND (x <= 3)) IS NULL", want: "(`x` >= 1 AND `x` <= 3) IS NULL"},
		{name: "a comparison in the upper bound", declared: "x BETWEEN 1 AND 3 = 1", stored: "(x >= 1) AND (x <= (3 = 1))", want: "`x` >= 1 AND `x` <= (3 = 1)"},
		{name: "a comparison in the lower bound", declared: "x BETWEEN a = 1 AND 3", stored: "(x >= (a = 1)) AND (x <= 3)", want: "`x` >= (`a` = 1) AND `x` <= 3"},
		{name: "a comparison before BETWEEN", declared: "a = x BETWEEN 1 AND 3", stored: "((a = x) >= 1) AND ((a = x) <= 3)", want: "`a` = `x` >= 1 AND `a` = `x` <= 3"},
		{name: "a negative bound", declared: "x BETWEEN -1 AND 3", stored: "(x >= -1) AND (x <= 3)", want: "`x` >= -1 AND `x` <= 3"},
		{name: "grouped bounds", declared: "x BETWEEN (1) AND (3)", stored: "(x >= (1)) AND (x <= (3))", want: "`x` >= 1 AND `x` <= 3"},
		{name: "IN and NOT IN after OR", declared: "a IN (1, 2) OR b NOT IN (3)", stored: "(a IN (1, 2)) OR (b NOT IN (3))", want: "`a` IN (1, 2) OR `b` NOT IN (3)"},
		{name: "LIKE and NOT LIKE after AND", declared: "s LIKE 'a%' AND s NOT LIKE 'b%'", stored: "(s LIKE 'a%') AND (s NOT LIKE 'b%')", want: "`s` LIKE 'a%' AND `s` NOT LIKE 'b%'"},
		{name: "IN of one value", declared: "a IN 1", stored: "a IN (1)", want: "`a` IN (1)"},
		{name: "IN of a sum", declared: "a IN 1 + 2", stored: "a IN (1 + 2)", want: "`a` IN (1 + 2)"},
		// ClickHouse 24.10 prints a IN (1 + 2) for this one.
		{name: "IN of a group and a sum", declared: "a IN (1) + 2", stored: "a IN ((1) + 2)", want: "`a` IN (1 + 2)"},
		{name: "IN after an addition", declared: "a + 1 NOT IN (2, 3)", stored: "(a + 1) NOT IN (2, 3)", want: "`a` + 1 NOT IN (2, 3)"},
		{name: "IN after a minus", declared: "-a IN (1)", stored: "(-a) IN (1)", want: "-`a` IN (1)"},
		{name: "IN beside a comparison", declared: "a IN (1, 2) = b IN (3)", stored: "((a IN (1, 2)) = b) IN (3)", want: "`a` IN (1, 2) = `b` IN (3)"},
		{name: "IN twice", declared: "a IN (1, 2) IN (1)", stored: "(a IN (1, 2)) IN (1)", want: "`a` IN (1, 2) IN (1)"},
		{name: "a comparison after NOT IN", declared: "a NOT IN (1) = 0", stored: "(a NOT IN (1)) = 0", want: "`a` NOT IN (1) = 0"},
		{name: "a comparison after LIKE", declared: "s LIKE 'a' = 1", stored: "(s LIKE 'a') = 1", want: "`s` LIKE 'a' = 1"},
		{name: "NOT before IN", declared: "NOT a IN (1)", stored: "NOT (a IN (1))", want: "NOT `a` IN (1)"},
		{name: "IN after IS NULL", declared: "a IS NULL IN (1)", stored: "(a IS NULL) IN (1)", want: "(`a` IS NULL) IN (1)"},
		{name: "LIKE of a call", declared: "s LIKE concat('a', '%')", stored: "s LIKE concat('a', '%')", want: "`s` LIKE concat('a', '%')"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declared, declaredRead := precedence.Canonical(test.declared, precedence.ClickHouseOperators, clickHouseLexing)
			stored, storedRead := precedence.Canonical(test.stored, precedence.ClickHouseOperators, clickHouseLexing)

			c.Assert(declaredRead, qt.IsTrue)
			c.Assert(storedRead, qt.IsTrue)
			c.Assert(declared, qt.Equals, test.want)
			c.Assert(stored, qt.Equals, test.want)
		})
	}
}

// TestCanonical_ClickHouse_DistinctExpressions pins pairs of expressions that
// ClickHouse evaluates differently, which must keep different keys.
func TestCanonical_ClickHouse_DistinctExpressions(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
	}{
		{name: "a group around the addition", left: "(1 + 2) * 3", right: "1 + 2 * 3"},
		{name: "a right-hand subtraction", left: "a - (b - c)", right: "a - b - c"},
		{name: "NOT around AND", left: "NOT (a AND b)", right: "NOT a AND b"},
		{name: "IS NULL around NOT", left: "(NOT a) IS NULL", right: "NOT a IS NULL"},
		{name: "IS NULL and IS NOT NULL", left: "a IS NULL", right: "a IS NOT NULL"},
		{name: "a group around OR", left: "(a OR b) AND c", right: "a OR b AND c"},
		{name: "function names in two cases", left: "myFunc(1)", right: "myfunc(1)"},
		{name: "names in two cases", left: "a + 1", right: "A + 1"},
		{name: "a name and a string", left: `"a"`, right: "'a'"},
		{name: "a negated sum", left: "-(1 + 2)", right: "-1 + 2"},
		{name: "a decimal and its tenth", left: "1.5", right: "1.05"},
		{name: "a negative float zero", left: "-0.0 + a", right: "0 + a"},
		// -(0) is negate(0), an Int16, so adding it to the largest UInt64
		// but one gives the Int64 -2, where 0 is a UInt8 and the sum stays
		// 18446744073709551614.
		{name: "a negated zero in parentheses", left: "-(0) + 18446744073709551614", right: "0 + 18446744073709551614"},
		{name: "a minus before a negative zero", left: "- -0 + a", right: "-0 + a"},
		{name: "BETWEEN and NOT BETWEEN", left: "x BETWEEN 1 AND 3", right: "x NOT BETWEEN 1 AND 3"},
		{name: "an AND inside the upper bound", left: "x BETWEEN 1 AND (3 AND y)", right: "x BETWEEN 1 AND 3 AND y"},
		{name: "IN and NOT IN", left: "a IN (1, 2)", right: "a NOT IN (1, 2)"},
		{name: "LIKE and NOT LIKE", left: "s LIKE 'a'", right: "s NOT LIKE 'a'"},
		{name: "a list and a sum", left: "a IN (1, 2)", right: "a IN (1 + 2)"},
		{name: "a comparison inside IN", left: "a IN (1 = 1)", right: "a IN 1 = 1"},
		{name: "a group around IN after a comparison", left: "a = (b IN (1))", right: "a = b IN (1)"},
		{name: "IN around NOT", left: "(NOT a) IN (1)", right: "NOT a IN (1)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			left, leftRead := precedence.Canonical(test.left, precedence.ClickHouseOperators, clickHouseLexing)
			right, rightRead := precedence.Canonical(test.right, precedence.ClickHouseOperators, clickHouseLexing)

			c.Assert(leftRead, qt.IsTrue)
			c.Assert(rightRead, qt.IsTrue)
			c.Assert(left, qt.Not(qt.Equals), right)
		})
	}
}

// TestCanonical_ClickHouse_FailurePath pins the expressions the ClickHouse
// table does not read. NOT before a group followed by IS NULL, IN, LIKE or an
// arithmetic operator is read as a call by ClickHouse 24.10 and as the start
// of NOT's operand by 26.10, and a prefix NOT after an operator that binds
// tighter has no grouping the table can state. BETWEEN after an operand that
// binds no tighter, or with a bound that does not, is not read, nor is a list
// after IN before an operator that binds tighter, and ClickHouse writes || back
// as another expression and IN () as IN tuple(). A minus before an integer above the Int64
// range means one value written right before the integer and another written
// before a group around it.
func TestCanonical_ClickHouse_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		expression string
	}{
		{name: "NOT before a group and IS NULL", expression: "NOT (a) IS NULL"},
		{name: "NOT before a group and an addition", expression: "NOT (1) + 2"},
		{name: "NOT after a comparison", expression: "a = NOT b"},
		{name: "NOT after a negation", expression: "-NOT a"},
		{name: "BETWEEN after IS NULL", expression: "a IS NULL BETWEEN 1 AND 2"},
		{name: "BETWEEN twice", expression: "x BETWEEN 1 AND 3 BETWEEN 0 AND 1"},
		{name: "BETWEEN without AND", expression: "x BETWEEN 1 OR 3"},
		{name: "BETWEEN with NOT in a bound", expression: "x BETWEEN NOT 1 AND 3"},
		{name: "BETWEEN with IS NULL in a bound", expression: "x BETWEEN a IS NULL AND 3"},
		{name: "BETWEEN without an upper bound", expression: "x BETWEEN 1 AND"},
		{name: "NOT before a group and IN", expression: "NOT (a) IN (1)"},
		{name: "NOT before a group and LIKE", expression: "NOT (s) LIKE 'a'"},
		{name: "a list before an addition", expression: "a IN (1, 2) + 3"},
		{name: "IN of nothing", expression: "a IN ()"},
		{name: "IN of a list in a group", expression: "a IN ((1, 2))"},
		{name: "IN without an operand", expression: "a IN"},
		{name: "NOT twice before IN", expression: "a NOT NOT IN (1)"},
		{name: "ILIKE", expression: "s ILIKE 'x'"},
		{name: "GLOBAL IN", expression: "a GLOBAL IN (1)"},
		{name: "concatenation", expression: "a || b"},
		{name: "a lambda", expression: "arrayMap(x -> x + 1, a)"},
		{name: "a ternary", expression: "a ? 1 : 2"},
		{name: "a cast", expression: "a::Int32"},
		{name: "a qualified name", expression: "t.a + 1"},
		{name: "IS without NULL", expression: "a IS 1"},
		{name: "an array", expression: "[1, 2]"},
		// ClickHouse folds a minus written right before an integer into the
		// literal and reads one before a group as negate, and the two differ
		// above the Int64 range: -18446744073709551615 is
		// -18446744073709552000 and -(18446744073709551615) is 1.
		{name: "a minus before the largest UInt64", expression: "-18446744073709551615"},
		{name: "a minus before a group around the largest UInt64", expression: "-(18446744073709551615)"},
		{name: "a minus and a space before the largest UInt64", expression: "- 18446744073709551615 + 1"},
		{name: "a minus before 2 to the 63", expression: "-9223372036854775808"},
		{name: "a minus before a group around 2 to the 63", expression: "-(9223372036854775808)"},
		{name: "a minus before 2 to the 63 with a leading zero", expression: "a * -09223372036854775808"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, read := precedence.Canonical(test.expression, precedence.ClickHouseOperators, clickHouseLexing)

			c.Assert(read, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestKeyword_ClickHouse reads the words ClickHouse's operator table holds as
// keywords, operators and literals, in any case, and every other word as a
// name: a column, a function or a type.
func TestKeyword_ClickHouse(t *testing.T) {
	tests := []struct {
		word string
		want bool
	}{
		{word: "AND", want: true},
		{word: "and", want: true},
		{word: "Or", want: true},
		{word: "NOT", want: true},
		{word: "is", want: true},
		{word: "ILIKE", want: true},
		{word: "in", want: true},
		{word: "between", want: true},
		{word: "NULL", want: true},
		{word: "true", want: true},
		{word: "v", want: false},
		{word: "V", want: false},
		{word: "length", want: false},
		{word: "Int64", want: false},
		{word: "toInt64", want: false},
	}
	for _, test := range tests {
		t.Run(test.word, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(precedence.Keyword(precedence.ClickHouseOperators, test.word), qt.Equals, test.want)
		})
	}
}

// TestKeyword_AnUnknownSchemeReadsEveryWordAsAName holds a scheme no table
// declares to no keyword at all.
func TestKeyword_AnUnknownSchemeReadsEveryWordAsAName(t *testing.T) {
	c := qt.New(t)

	c.Assert(precedence.Keyword("no-such-scheme", "AND"), qt.IsFalse)
}
