package compare

// White-box testing required: the canonical form is an internal detail of how
// two check expressions are compared, and the comparison entry points take
// whole schemas. Reaching this through them would need a live catalog per case
// and would assert on a diff rather than on the normalization under test.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestNormalizeCheckExpression_SetMembershipHasOneSpelling pins the fix for
// the churn stokaro/ptah#1716 is about.
//
// SQL Server does not keep `col IN ('a','b')`. It stores the disjunction it
// compiles the list into, in an order of its own, so a text comparison against
// the declaration never matched and every `schema apply` planned the same DROP
// and ADD of the same constraint -- applied, never converging, never failing.
func TestNormalizeCheckExpression_SetMembershipHasOneSpelling(t *testing.T) {
	tests := []struct {
		name      string
		declared  string
		stored    string
		wantEqual bool
	}{
		{
			name:      "the order SQL Server chose",
			declared:  "[status] IN ('new', 'paid', 'shipped')",
			stored:    "([status]='shipped' OR [status]='paid' OR [status]='new')",
			wantEqual: true,
		},
		{
			name:      "quoting the declaration did not write",
			declared:  "status IN ('new', 'paid')",
			stored:    "([status]='paid' OR [status]='new')",
			wantEqual: true,
		},
		{
			name:      "a value the declaration dropped",
			declared:  "[status] IN ('new')",
			stored:    "([status]='paid' OR [status]='new')",
			wantEqual: false,
		},
		{
			name:      "a value the declaration added",
			declared:  "[status] IN ('new', 'paid', 'shipped')",
			stored:    "([status]='paid' OR [status]='new')",
			wantEqual: false,
		},
		{
			name:      "a different column",
			declared:  "[status] IN ('new', 'paid')",
			stored:    "([state]='paid' OR [state]='new')",
			wantEqual: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			equal := normalizeCheckExpression(test.declared) == normalizeCheckExpression(test.stored)

			c.Assert(equal, qt.Equals, test.wantEqual)
		})
	}
}

// TestCanonicalizeCheckSetMembership_LeavesEverythingElseAlone is the control
// on the transform's reach.
//
// A canonicalization that folded expressions it does not understand would make
// unrelated checks compare equal, which is the same silent no-op this fix
// exists to avoid -- just somewhere else.
func TestCanonicalizeCheckSetMembership_LeavesEverythingElseAlone(t *testing.T) {
	tests := []struct {
		name string
		expr string
	}{
		{name: "a range test", expr: "age>0andage<130"},
		{name: "a disjunction of ranges", expr: "age<0orage>130"},
		{name: "equality against a column", expr: "a=borb=a"},
		{name: "in with a subquery", expr: "statusin(selectxfromt)"},
		{name: "a single equality", expr: "status='new'"},
		// Two different columns are not a set. Folding them would make
		// `a='x' or b='y'` compare equal to `a in ('x','y')`, which describes
		// something else entirely.
		{name: "a disjunction over two columns", expr: "a='x'orb='y'"},
		// A branch comparing against another column is not membership either.
		{name: "equality against a column, same left side", expr: "status=xorstatus=y"},
		// The branches are equalities against the same left side, and the OR
		// does follow a value's end -- but the right sides are calls, not
		// literals. A set of expressions is not a set of values.
		{name: "branches comparing against calls", expr: "status=lower('x')orstatus=lower('y')"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(canonicalizeCheckSetMembership(test.expr), qt.Equals, test.expr)
		})
	}
}

// TestCanonicalizeCheckSetMembership_ReadsMembershipOnAnExpression pins that
// the left side need not be a bare column.
//
// `lower(status) IN ('new','paid')` is a membership test like any other, and
// the engine may store it as a disjunction just the same.
func TestCanonicalizeCheckSetMembership_ReadsMembershipOnAnExpression(t *testing.T) {
	c := qt.New(t)

	declared := canonicalizeCheckSetMembership("lower(status)in('new','paid')")
	stored := canonicalizeCheckSetMembership("lower(status)='paid'orlower(status)='new'")

	c.Assert(declared, qt.Equals, stored)
}

// TestCanonicalizeCheckSetMembership_ReadsAnOperatorSpellingInsideALiteral
// pins that a value containing the operator's own text is data.
//
// `'in('` inside a value would end the scan at the wrong place and split one
// value into two, silently changing which set the check describes.
func TestCanonicalizeCheckSetMembership_ReadsAnOperatorSpellingInsideALiteral(t *testing.T) {
	c := qt.New(t)

	declared := normalizeCheckExpression("[label] IN ('in(x', 'y')")
	stored := normalizeCheckExpression("([label]='y' OR [label]='in(x')")

	c.Assert(declared, qt.Equals, stored)
	c.Assert(declared, qt.Contains, "'in(x'")
}

// TestCanonicalizeCheckSetMembership_FindsTheOperatorPastALiteralHoldingIt
// pins that the search for the operator skips string literals.
//
// `f('in(') IN ('a','b')` carries the operator's own spelling inside a value
// that precedes the real operator. Read as syntax, it splits the expression at
// the wrong place and the parse that follows is about a different column and a
// different set.
func TestCanonicalizeCheckSetMembership_FindsTheOperatorPastALiteralHoldingIt(t *testing.T) {
	c := qt.New(t)

	declared := normalizeCheckExpression("f('in(') IN ('a', 'b')")
	stored := normalizeCheckExpression("(f('in(')='b' OR f('in(')='a')")

	c.Assert(declared, qt.Equals, stored)
	c.Assert(declared, qt.Contains, "f('in(')")
}

// TestCanonicalizeCheckSetMembership_DoesNotReadInsideALiteral pins that the
// scan respects quoting.
//
// A value spelled `or` or containing a comma is data, not syntax, and reading
// it as an operator would split one value into two -- silently changing which
// set the check describes.
func TestCanonicalizeCheckSetMembership_DoesNotReadInsideALiteral(t *testing.T) {
	c := qt.New(t)

	declared := normalizeCheckExpression("[label] IN ('a or b', 'c, d')")
	stored := normalizeCheckExpression("([label]='c, d' OR [label]='a or b')")

	c.Assert(declared, qt.Equals, stored)
	c.Assert(declared, qt.Contains, "'a or b'")
	c.Assert(declared, qt.Contains, "'c, d'")
}

// TestCanonicalizeCheckSetMembership_KeepsAnIdentifierCarryingASeparator is
// the separator's own control: the `or` inside `color`, and the `in` at the end
// of `origin`, are letters in a name rather than operators.
//
// The two spellings fail differently, which is why both are here: `origin`
// ends with the word, so only the character AFTER it distinguishes them, and
// `color` carries it in the middle, where only the character BEFORE does.
func TestCanonicalizeCheckSetMembership_KeepsAnIdentifierCarryingASeparator(t *testing.T) {
	tests := []struct {
		name string
		expr string
		want string
	}{
		{name: "in at the end of a name", expr: "origin='eu'", want: "origin='eu'"},
		{name: "or in the middle of a name", expr: "color='red'", want: "color='red'"},
		{
			name: "or in the middle, and a real one after a value",
			expr: "color='red'orcolor='blue'",
			want: "color in ('blue','red')",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(canonicalizeCheckSetMembership(test.expr), qt.Equals, test.want)
		})
	}
}
