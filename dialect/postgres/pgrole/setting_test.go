package pgrole_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgrole"
)

func TestParseName_HappyPath(t *testing.T) {
	for _, test := range []struct{ written, want string }{
		{"statement_timeout", "statement_timeout"},
		{"Statement_Timeout", "statement_timeout"},
		{"TimeZone", "timezone"},
		{"app.tenant", "app.tenant"},
		{" app . tenant ", "app.tenant"},
		{`"App"."Tenant"`, "App.Tenant"},
		{`app."Tenant"`, "app.Tenant"},
		{`"a""b".c`, `a"b.c`},
		{"app.tenant_2", "app.tenant_2"},
	} {
		t.Run(test.written, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgrole.ParseName(test.written)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestParseName_FailurePath(t *testing.T) {
	for _, written := range []string{"", "   ", "app.", ".app", `"app`, `""`, "app tenant", "1app", "app-tenant", "app;drop", `app.""`} {
		t.Run(written, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgrole.ParseName(written)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestQuoteName_RoundTripsThroughParseName(t *testing.T) {
	for _, name := range []string{"statement_timeout", "app.tenant", "App.Tenant", `a"b.c`} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgrole.ParseName(pgrole.QuoteName(name))
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, name)
		})
	}
}

func TestQuoteName_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(pgrole.QuoteName("app.tenant"), qt.Equals, `"app"."tenant"`)
	c.Assert(pgrole.QuoteName(`a"b`), qt.Equals, `"a""b"`)
}

func TestParseValue_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, written, want string
		form                pgrole.ValueForm
	}{
		{name: "string", written: "'30s'", want: "'30s'"},
		{name: "doubled quote", written: "'it''s'", want: "'it''s'"},
		{name: "integer", written: "30000", want: "30000"},
		{name: "negative", written: "-1", want: "-1"},
		{name: "signed with space", written: "- 1", want: "-1"},
		{name: "fraction", written: "0.5", want: "0.5"},
		{name: "exponent", written: "1e3", want: "1e3"},
		{name: "word", written: "on", want: "on"},
		{name: "literal list", written: "'$user', public", want: "'$user', public"},
		{name: "identifier list", written: `"$user",public`, want: `"$user", public`},
		{name: "spaced list", written: "  a ,  b  ", want: "a, b"},
		{name: "default", written: "DEFAULT", form: pgrole.ValueDefault},
		{name: "default lower", written: "default", form: pgrole.ValueDefault},
		{name: "quoted default is a value", written: "'DEFAULT'", want: "'DEFAULT'"},
		{name: "semicolon in a literal", written: "'a;b'", want: "'a;b'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, form, err := pgrole.ParseValue(test.written)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(form, qt.Equals, test.form)
		})
	}
}

func TestParseValue_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, written, want string }{
		{name: "empty", written: " ", want: `.*needs a value`},
		{name: "unit without quotes", written: "30s", want: `.*30s is a number followed by a word; a value with a unit is a string literal: '30s'`},
		{name: "escape string", written: "E'x'", want: `.*an escape string.*`},
		{name: "dollar quote", written: "$$x$$", want: `.*unexpected "\$"`},
		{name: "unclosed quote", written: "'open", want: `.*an unclosed quote`},
		{name: "unclosed identifier", written: `"open`, want: `.*an unclosed quote`},
		{name: "operator", written: "1 + 1", want: `.*expected a comma after 1`},
		{name: "two words", written: "a b", want: `.*expected a comma after a`},
		{name: "trailing comma", written: "'a',", want: `.*ends in a comma`},
		{name: "statement after", written: "'a'; DROP ROLE x", want: `.*expected a comma after 'a'`},
		{name: "default in a list", written: "DEFAULT, public", want: `.*DEFAULT stands alone, never in a list`},
		{name: "lone sign", written: "-", want: `.*a sign or a point without a number`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, form, err := pgrole.ParseValue(test.written)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.Equals, "")
			c.Assert(form, qt.Equals, pgrole.ValueLiteral)
		})
	}
}

func TestStoredValue_HappyPath(t *testing.T) {
	for _, test := range []struct{ name, value, want string }{
		{"statement_timeout", "'30s'", "30s"},
		{"statement_timeout", "30000", "30000"},
		{"search_path", "'$user', public", `"$user", public`},
		{"search_path", `"$user", public`, `"$user", public`},
		{"app.tenant", "'it''s'", "it's"},
	} {
		t.Run(test.name+"="+test.value, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgrole.StoredValue(test.name, test.value), qt.Equals, test.want)
		})
	}
}

func TestMatches(t *testing.T) {
	for _, test := range []struct {
		name, declared, stored string
		want                   bool
	}{
		{"statement_timeout", "'30s'", "30s", true},
		{"statement_timeout", "'30s'", "30000", false},
		{"statement_timeout", "30000", "30000", true},
		{"search_path", "'$user', public", `"$user", public`, true},
		{"search_path", `"$user", public`, `"$user", public`, true},
		{"search_path", "public, '$user'", `"$user", public`, false},
		{"app.tenant", "'acme'", "acme", true},
		{"app.tenant", "'Acme'", "acme", false},
		{"app.tenant", "'it''s'", "it's", true},
	} {
		t.Run(test.name+"="+test.declared+"~"+test.stored, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgrole.Matches(test.name, test.declared, test.stored), qt.Equals, test.want)
		})
	}
}

// A stored value written back in SQL must be read by ParseValue as a value
// that matches what the server stores, or a reversal restores another value.
func TestSQLValue_RoundTripsThroughParseValue(t *testing.T) {
	for _, test := range []struct{ name, stored string }{
		{"statement_timeout", "30s"},
		{"search_path", `"$user", public`},
		{"app.tenant", "it's"},
		{"app.tenant", "a, b"},
	} {
		t.Run(test.name+"="+test.stored, func(t *testing.T) {
			c := qt.New(t)
			value, form, err := pgrole.ParseValue(pgrole.SQLValue(test.name, test.stored))
			c.Assert(err, qt.IsNil)
			c.Assert(form, qt.Equals, pgrole.ValueLiteral)
			c.Assert(pgrole.Matches(test.name, value, test.stored), qt.IsTrue)
			c.Assert(pgrole.StoredValue(test.name, value), qt.Equals, test.stored)
		})
	}
}
