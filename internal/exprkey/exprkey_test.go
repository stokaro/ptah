package exprkey_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/exprkey"
)

func semanticsFor(dialect string) identifier.Semantics {
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = "public"
	return semantics
}

// TestCheck_ComponentsCannotForgeABoundary pins the property the identity key
// is a struct for.
//
// A key that joined its components with a separator would spell one string for
// two different constraints as soon as a name contained the separator, and a
// catalog reports names bare -- a table really named `orders.2024` arrives as
// those exact bytes with nothing marking where the name begins.
func TestCheck_ComponentsCannotForgeABoundary(t *testing.T) {
	c := qt.New(t)
	semantics := semanticsFor(platform.Postgres)

	dottedTable := exprkey.CheckParts(semantics, "", "orders.2024", "c")
	dottedConstraint := exprkey.CheckParts(semantics, "", "orders", "2024.c")

	c.Assert(dottedTable, qt.Not(qt.Equals), dottedConstraint,
		qt.Commentf("two distinct constraints must not share a key"))
}

// TestCheck_TheTwoSidesOfOneConstraintAgree pins that the spelling a
// declaration uses and the components a catalog reports reach the same key.
//
// They are built by different functions because the two sources genuinely
// differ: a declaration writes one possibly qualified string and a catalog
// reports the schema separately. A key that did not survive that difference
// would leave every resolved expression unreadable.
func TestCheck_TheTwoSidesOfOneConstraintAgree(t *testing.T) {
	c := qt.New(t)
	semantics := semanticsFor(platform.Postgres)

	c.Assert(exprkey.Check(semantics, "public.t", "ck"), qt.Equals,
		exprkey.CheckParts(semantics, "public", "t", "ck"),
		qt.Commentf("a qualified declaration and its catalog row are one constraint"))

	c.Assert(exprkey.Check(semantics, "t", "ck"), qt.Equals,
		exprkey.CheckParts(semantics, "public", "t", "ck"),
		qt.Commentf("an unqualified declaration falls on the default schema"))
}

// TestCheck_TheFoldIsTheTargets pins that the rule comes from the dialect and
// is not hard-coded.
//
// PostgreSQL reports `t` and `T` as two tables; Oracle folds an unquoted name
// to one. A key that lower-cased everywhere merges the PostgreSQL pair, and the
// expression a server resolved for one table then answers for the other.
func TestCheck_TheFoldIsTheTargets(t *testing.T) {
	c := qt.New(t)

	postgres := semanticsFor(platform.Postgres)
	c.Assert(exprkey.Check(postgres, "t", "ck"), qt.Not(qt.Equals),
		exprkey.Check(postgres, "T", "ck"),
		qt.Commentf("PostgreSQL holds t and T as two tables"))

	oracle := identifier.ForDialect(platform.Oracle)
	c.Assert(exprkey.Check(oracle, "t", "ck"), qt.Equals,
		exprkey.Check(oracle, "T", "ck"),
		qt.Commentf("Oracle folds an unquoted name, so these are one table"))
}
