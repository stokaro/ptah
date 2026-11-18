package sqlident_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlident"
)

// TestCatalogName names an object by the spelling its catalog stores. On
// Oracle an unquoted name folds to upper case, so a stored name that is plain,
// not a keyword and not in upper case was created quoted and is named quoted;
// every other stored name is written back as Ident writes it already. A
// dialect whose descriptor states no fold names every object as it is stored.
func TestCatalogName(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		stored  string
		want    string
	}{
		{name: "oracle lower case", dialect: "oracle", stored: "orders", want: `"orders"`},
		{name: "oracle mixed case", dialect: "oracle", stored: "Orders", want: `"Orders"`},
		{name: "oracle upper case", dialect: "oracle", stored: "ORDERS", want: "ORDERS"},
		{name: "oracle upper case with digits and underscores", dialect: "oracle", stored: "ORDER_LINES_2", want: "ORDER_LINES_2"},
		{name: "oracle keyword, which Ident quotes anyway", dialect: "oracle", stored: "size", want: "size"},
		{name: "oracle name that is not plain", dialect: "oracle", stored: "order-lines", want: "order-lines"},
		{name: "oracle quote inside", dialect: "oracle", stored: `a"b`, want: `a"b`},
		{name: "oracle lower case with a dollar sign", dialect: "oracle", stored: "a$b", want: `"a$b"`},
		{name: "oracle upper case with a number sign", dialect: "oracle", stored: "A#B", want: "A#B"},
		{name: "oracle dollar sign first, which no bare name starts with", dialect: "oracle", stored: "$ab", want: "$ab"},
		{name: "postgres states no fold", dialect: "postgres", stored: "Orders", want: "Orders"},
		{name: "a dialect nothing describes", dialect: "duckdb", stored: "orders", want: "orders"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlident.CatalogName(test.dialect, test.stored), qt.Equals, test.want)
		})
	}
}

// TestIdent_OracleBareCharacters writes a name holding $ or # bare on Oracle,
// as Oracle accepts it unquoted, and quotes the same name on a dialect whose
// descriptor lists no such character.
func TestIdent_OracleBareCharacters(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		ident   string
		want    string
	}{
		{name: "oracle dollar sign", dialect: "oracle", ident: "a$b", want: "a$b"},
		{name: "oracle number sign", dialect: "oracle", ident: "a#b", want: "a#b"},
		{name: "oracle leading dollar sign", dialect: "oracle", ident: "$ab", want: `"$ab"`},
		{name: "sqlite lists no such character", dialect: "sqlite", ident: "a$b", want: `"a$b"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlident.Ident(test.dialect, test.ident), qt.Equals, test.want)
		})
	}
}
