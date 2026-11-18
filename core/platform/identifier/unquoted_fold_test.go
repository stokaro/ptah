package identifier_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
)

// TestForDialect_UnquotedFold reads the fold and the bare characters each
// dialect's descriptor states. Oracle folds unquoted names to upper case and
// accepts $ and # in them; a dialect stating no fold keys every name as it
// always has.
func TestForDialect_UnquotedFold(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		fold      identifier.Fold
		bareChars string
	}{
		{name: "oracle folds to upper case", dialect: "oracle", fold: identifier.FoldUpper, bareChars: "$#"},
		{name: "an alias reads the same descriptor", dialect: "oracledb", fold: identifier.FoldUpper, bareChars: "$#"},
		{name: "postgres states no fold", dialect: "postgres"},
		{name: "a dialect nothing describes", dialect: "duckdb"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := identifier.ForDialect(test.dialect)

			c.Assert(got.UnquotedFold, qt.Equals, test.fold)
			c.Assert(got.BareChars, qt.Equals, test.bareChars)
		})
	}
}

// TestUnquotedFold_KeysAQuotedNameByItsSpelling compares names under Oracle's
// rules. `"orders"` is the table stored as orders, which neither `orders` nor
// ORDERS reaches, since both are stored as ORDERS. A quoted name already in
// upper case, or one that could not be written unquoted, is what its unquoted
// spelling reaches, so it keys as that. $ and # may be written unquoted on
// Oracle, so `"a$b"` is told apart from A$B as `"orders"` is from ORDERS.
func TestUnquotedFold_KeysAQuotedNameByItsSpelling(t *testing.T) {
	semantics := identifier.ForDialect("oracle")
	tests := []struct {
		name string
		a, b string
		same bool
	}{
		{name: "a quoted lower-case name is not the bare one", a: `"orders"`, b: "orders", same: false},
		{name: "a quoted lower-case name is not the folded one", a: `"orders"`, b: "ORDERS", same: false},
		{name: "two quoted names differing in case", a: `"orders"`, b: `"Orders"`, same: false},
		{name: "a quoted name in the fold's case", a: `"ORDERS"`, b: "orders", same: true},
		{name: "a quoted name that is not plain", a: `"order-lines"`, b: "order-lines", same: true},
		{name: "a quoted name with a dollar sign", a: `"a$b"`, b: "A$B", same: false},
		{name: "a quoted name with a number sign", a: `"a#b"`, b: "a#b", same: false},
		{name: "a quoted upper-case name with a dollar sign", a: `"A$B"`, b: "a$b", same: true},
		{name: "two bare spellings", a: "orders", b: "ORDERS", same: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(semantics.TableIdentityKey(test.a) == semantics.TableIdentityKey(test.b), qt.Equals, test.same)
			c.Assert(semantics.TableConflictKey(test.a) == semantics.TableConflictKey(test.b), qt.Equals, test.same)
			c.Assert(semantics.ColumnIdentityKey(test.a) == semantics.ColumnIdentityKey(test.b), qt.Equals, test.same)
			c.Assert(semantics.IndexIdentityKey(test.a) == semantics.IndexIdentityKey(test.b), qt.Equals, test.same)
			c.Assert(semantics.QualifiedTableIdentityKey("APP."+test.a) == semantics.QualifiedTableIdentityKey("app."+test.b),
				qt.Equals, test.same)
		})
	}
}

// TestUnquotedFold_WithoutAFoldAQuotedNameFolds pins a target that states no
// fold: a quoted name keys as the name without its quotes.
func TestUnquotedFold_WithoutAFoldAQuotedNameFolds(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("oracle")
	semantics.UnquotedFold = ""

	c.Assert(semantics.TableIdentityKey(`"Orders"`), qt.Equals, semantics.TableIdentityKey(`"orders"`))
	c.Assert(semantics.QualifiedTableIdentityKey(`APP."orders"`), qt.Equals, semantics.QualifiedTableIdentityKey("app.orders"))
}

// TestUnquotedFold_RoundTripsThroughJSON keeps the fold and the bare
// characters through a snapshot and Normalize, which is how a plan carries the
// rules it was compared under.
func TestUnquotedFold_RoundTripsThroughJSON(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("oracle")
	semantics.DefaultSchema = "APP"

	encoded, err := json.Marshal(semantics)
	c.Assert(err, qt.IsNil)
	var decoded identifier.Semantics
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)

	c.Assert(string(encoded), qt.Contains, `"unquoted_fold":"upper"`)
	c.Assert(string(encoded), qt.Contains, `"bare_chars":"$#"`)
	c.Assert(decoded.Normalize("oracle").Equal(semantics), qt.IsTrue)
	c.Assert(decoded.IsZero(), qt.IsFalse)
}

// TestUnquotedFold_NormalizeRefusesAnUndeclaredFold falls back to the
// dialect's rules for a fold no target states.
func TestUnquotedFold_NormalizeRefusesAnUndeclaredFold(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("oracle")
	semantics.UnquotedFold = "title"

	got := semantics.Normalize("oracle")

	c.Assert(got.Equal(identifier.ForDialect("oracle")), qt.IsTrue)
	c.Assert(semantics.Equal(identifier.ForDialect("oracle")), qt.IsFalse)
}

// TestUnquotedFold_EqualReadsTheBareCharacters tells apart two rule sets that
// differ only in what an unquoted name may hold.
func TestUnquotedFold_EqualReadsTheBareCharacters(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("oracle")
	without := semantics
	without.BareChars = ""

	c.Assert(semantics.Equal(without), qt.IsFalse)
	c.Assert(without.TableIdentityKey(`"a$b"`), qt.Equals, without.TableIdentityKey("A$B"))
}
