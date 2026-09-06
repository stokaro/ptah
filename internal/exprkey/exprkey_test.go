package exprkey_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/exprkey"
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

// TestGenerated_TheTwoSidesAgreeWithoutSharingAConnection pins the property the
// dialect parameter exists for.
//
// The map is filled by asking a dev database to spell each declaration and read
// while comparing against the target. Nothing guarantees the two connections
// resolved the same semantics, so the key is derived from the dialect both
// sides do share.
func TestGenerated_TheTwoSidesAgreeWithoutSharingAConnection(t *testing.T) {
	c := qt.New(t)

	producer := exprkey.Generated(platform.Oracle, "app", "orders", "total")
	consumer := exprkey.Generated(platform.Oracle, "app", "orders", "total")
	c.Assert(producer, qt.Equals, consumer)

	c.Assert(exprkey.Generated(platform.Oracle, "app", "orders", "total"), qt.Not(qt.Equals),
		exprkey.Generated(platform.Oracle, "app", "orders", "TOTAL_2"),
		qt.Commentf("two columns are two keys"))
}

// TestGenerated_ComponentsCannotForgeABoundary is
// [TestCheck_ComponentsCannotForgeABoundary] for the column family: a catalog
// reports a table name bare, dots included.
func TestGenerated_ComponentsCannotForgeABoundary(t *testing.T) {
	c := qt.New(t)

	c.Assert(exprkey.Generated(platform.Postgres, "", "orders.2024", "total"), qt.Not(qt.Equals),
		exprkey.Generated(platform.Postgres, "", "orders", "2024.total"))
}

// TestIndex_TheNamespaceIsTheTargets pins that whether an index name is unique
// within its table or across the schema comes from the dialect.
//
// A key folded from the name alone can express neither: it merges two indexes
// that PostgreSQL keeps apart by case, and it merges two MySQL indexes of the
// same name on different tables, which are two objects there.
func TestIndex_TheNamespaceIsTheTargets(t *testing.T) {
	c := qt.New(t)

	postgres := semanticsFor(platform.Postgres)
	c.Assert(exprkey.Index(postgres, "public.a", "Idx"), qt.Not(qt.Equals),
		exprkey.Index(postgres, "public.a", "idx"),
		qt.Commentf("PostgreSQL preserves an index name's case"))
	c.Assert(exprkey.Index(postgres, "public.a", "idx"), qt.Equals,
		exprkey.Index(postgres, "public.b", "idx"),
		qt.Commentf("an index name is unique across the schema on PostgreSQL"))

	mysql := identifier.ForDialect(platform.MySQL)
	c.Assert(exprkey.Index(mysql, "a", "idx"), qt.Not(qt.Equals),
		exprkey.Index(mysql, "b", "idx"),
		qt.Commentf("an index name is unique within its table on MySQL"))
}

// TestIndex_TheTwoSidesOfOneIndexAgree holds the declaration's spelling and the
// components a catalog reports to each other.
func TestIndex_TheTwoSidesOfOneIndexAgree(t *testing.T) {
	c := qt.New(t)
	semantics := semanticsFor(platform.Postgres)

	c.Assert(exprkey.Index(semantics, "public.t", "idx"), qt.Equals,
		exprkey.IndexParts(semantics, "public", "t", "idx"))
	c.Assert(exprkey.Index(semantics, "t", "idx"), qt.Equals,
		exprkey.IndexParts(semantics, "public", "t", "idx"),
		qt.Commentf("an unqualified declaration falls on the default schema"))
}
