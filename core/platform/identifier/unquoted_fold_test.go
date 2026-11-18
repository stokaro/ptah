package identifier_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
)

// TestForCatalog_HappyPath reads the rules a live read compares under. Oracle
// states that it folds unquoted names to upper case, and only the catalog
// rules carry it: a declaration and a schema predicted from one spell a name as
// it was written, so the offline rules leave a quoted spelling alone.
func TestForCatalog_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    identifier.Fold
	}{
		{name: "oracle folds to upper case", dialect: "oracle", want: identifier.FoldUpper},
		{name: "an alias reads the same descriptor", dialect: "oracledb", want: identifier.FoldUpper},
		{name: "postgres states no fold", dialect: "postgres", want: ""},
		{name: "a dialect nothing describes", dialect: "duckdb", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := identifier.ForCatalog(test.dialect)

			c.Assert(got.UnquotedFold, qt.Equals, test.want)
			want := identifier.ForDialect(test.dialect)
			want.UnquotedFold = test.want
			c.Assert(got.Equal(want), qt.IsTrue)
			c.Assert(identifier.ForDialect(test.dialect).UnquotedFold, qt.Equals, identifier.Fold(""))
		})
	}
}

// TestUnquotedFold_KeysAQuotedNameByItsSpelling compares names under Oracle's
// catalog rules. `"orders"` is the table stored as orders, which neither
// `orders` nor ORDERS reaches, since both are stored as ORDERS. A quoted name
// already in upper case, or one that is not a plain identifier, is what its
// unquoted spelling reaches, so it keys as that.
func TestUnquotedFold_KeysAQuotedNameByItsSpelling(t *testing.T) {
	semantics := identifier.ForCatalog("oracle")
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

// TestUnquotedFold_OfflineRulesKeepTheirKeys pins the rules a declaration is
// compared under: without the fold a quoted name keys as it always did.
func TestUnquotedFold_OfflineRulesKeepTheirKeys(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("oracle")

	c.Assert(semantics.TableIdentityKey(`"Orders"`), qt.Equals, semantics.TableIdentityKey(`"orders"`))
	c.Assert(semantics.QualifiedTableIdentityKey(`APP."orders"`), qt.Equals, semantics.QualifiedTableIdentityKey("app.orders"))
}

// TestUnquotedFold_RoundTripsThroughJSON keeps the fold through a snapshot and
// Normalize, which is how a plan carries the rules it was compared under.
func TestUnquotedFold_RoundTripsThroughJSON(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForCatalog("oracle")
	semantics.DefaultSchema = "APP"

	encoded, err := json.Marshal(semantics)
	c.Assert(err, qt.IsNil)
	var decoded identifier.Semantics
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)

	c.Assert(string(encoded), qt.Contains, `"unquoted_fold":"upper"`)
	c.Assert(decoded.Normalize("oracle").Equal(semantics), qt.IsTrue)
	c.Assert(decoded.IsZero(), qt.IsFalse)
}

// TestUnquotedFold_NormalizeRefusesAnUndeclaredFold falls back to the offline
// rules for a fold no target states.
func TestUnquotedFold_NormalizeRefusesAnUndeclaredFold(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForCatalog("oracle")
	semantics.UnquotedFold = "title"

	got := semantics.Normalize("oracle")

	c.Assert(got.Equal(identifier.ForDialect("oracle")), qt.IsTrue)
	c.Assert(semantics.Equal(identifier.ForCatalog("oracle")), qt.IsFalse)
}
