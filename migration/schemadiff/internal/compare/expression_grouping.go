package compare

import (
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/normalize"
	"ptah.run/internal/precedence"
)

// checkExpressionsAlike reports whether a declared CHECK condition and the one
// a catalog reports for it are one condition on dialect: equal once
// [normalize.CheckExpression] folds both, or grouped alike by the dialect's
// operator precedence ([expressionsGroupAlike]).
//
// Every comparison that decides whether two CHECKs are the same asks this one
// predicate. The pairing of a server-named CHECK with an unnamed declaration
// and the comparison of the paired constraint must agree: a pairing that
// accepted a spelling the comparison then rejects would plan the constraint
// again on every run.
func checkExpressionsAlike(declared, reported, dialect string) bool {
	quoting := normalize.QuotingOf(dialect)
	return normalize.CheckExpression(declared, quoting) == normalize.CheckExpression(reported, quoting) ||
		expressionsGroupAlike(declared, reported, dialect)
}

// expressionsGroupAlike reports whether two expressions are the same
// operations under the operator precedence dialect's descriptor names
// ([precedence.Canonical]). MySQL 26.10 stores the CHECK condition
// `id between 1 and 10 and c is not null` as
// `((id between 1 and 10) and (c is not null))` and `c NOT LIKE 'x%'` as
// `(not((c like _utf8mb4'x%')))`, and a generated column's
// `concat(c, 'x')` as `concat(c,_utf8mb4'x')`; read by that
// precedence, each pair is one expression.
//
// It answers false where the dialect names no precedence or the precedence
// cannot read either side, so a caller keeps whatever its textual comparison
// decided: the reading only ever recognizes two spellings as one.
func expressionsGroupAlike(left, right, dialect string) bool {
	scheme := comparisonOf(dialect).OperatorPrecedence
	if scheme == "" {
		return false
	}
	lexing := dialectlexer.Options(dialect)
	groupedLeft, leftRead := precedence.Canonical(left, scheme, lexing)
	groupedRight, rightRead := precedence.Canonical(right, scheme, lexing)
	return leftRead && rightRead && groupedLeft == groupedRight
}
