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
// descriptor lists no such character. An Oracle name written bare starts with
// a letter, since `_A` answers ORA-00911, so a name starting with an
// underscore, a $, a # or a digit is quoted there.
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
		{name: "oracle leading underscore", dialect: "oracle", ident: "_A", want: `"_A"`},
		{name: "oracle leading number sign", dialect: "oracle", ident: "#a", want: `"#a"`},
		{name: "oracle leading digit", dialect: "oracle", ident: "1a", want: `"1a"`},
		{name: "oracle underscore after a letter", dialect: "oracle", ident: "a_b", want: "a_b"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlident.Ident(test.dialect, test.ident), qt.Equals, test.want)
		})
	}
}

// TestBareOrQuoted_LeadingUnderscore reads the leading-letter rule from the
// descriptor rather than applying it everywhere: Oracle quotes `_a`, and
// SQLite, whose descriptor does not state the rule, keeps it bare where it
// writes a plain name bare.
func TestBareOrQuoted_LeadingUnderscore(t *testing.T) {
	c := qt.New(t)

	c.Assert(sqlident.BareOrQuoted("oracle", "_a"), qt.Equals, `"_a"`)
	c.Assert(sqlident.BareOrQuoted("sqlite", "_a"), qt.Equals, "_a")
}

// TestNamesakeHint explains a bare reference that reached nothing when the
// schema declares a quoted namesake of it, on a dialect with an unquoted fold.
// The hint is empty where the reference is quoted, where nothing differs from
// it only in case, and on a dialect that states no fold, since a quoted and a
// bare name are the same object there.
func TestNamesakeHint(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		reference string
		declared  []string
		want      string
	}{
		{
			name: "a bare table reference with a quoted namesake", dialect: "oracle", reference: "customers",
			declared: []string{"ORDERS", `"customers"`},
			want:     `; the schema declares the table "customers" quoted, which oracle keeps apart from the bare customers: write "customers" to name it`,
		},
		{
			name: "a qualified bare reference", dialect: "oracle", reference: "APP.Customers",
			declared: []string{`"customers"`},
			want:     `; the schema declares the table "customers" quoted, which oracle keeps apart from the bare Customers: write "customers" to name it`,
		},
		{name: "a quoted reference", dialect: "oracle", reference: `"Customers"`, declared: []string{`"customers"`}},
		{name: "no namesake", dialect: "oracle", reference: "customers", declared: []string{`"orders"`, "CUSTOMER"}},
		{name: "a dialect without a fold", dialect: "postgres", reference: "customers", declared: []string{`"customers"`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlident.NamesakeHint(test.dialect, "table", test.reference, test.declared), qt.Equals, test.want)
		})
	}
}
