package sqlident_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/lexerext"
	"ptah.run/internal/providerscan"
	"ptah.run/internal/providerscan/scantest"
	"ptah.run/internal/sqlident"
)

// pathQuoting is how an engine that names a table by one path in backticks
// writes a name, as a provider declares it.
var pathQuoting = descriptor.Quoting{Style: descriptor.QuoteBacktick, Bare: descriptor.BareNever, Paths: true}

// pathDialect is the descriptor the host builds from that declaration.
var pathDialect = descriptor.Dialect{Quoting: pathQuoting}

// escapingDialect is a provider-shaped descriptor of an engine whose quoted
// names take backslash escapes, as ClickHouse's do: backticks, never bare, and
// Lexical.IdentifierBackslashEscapes.
var escapingDialect = descriptor.Dialect{
	Quoting: descriptor.Quoting{Style: descriptor.QuoteBacktick, Bare: descriptor.BareNever},
	Lexical: descriptor.Lexical{IdentifierBackslashEscapes: true},
}

// A provider engine's declared quoting writes its names under each of its
// names, and with the provider's scanner registered its names read back.
func TestRegisterQuoting_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(sqlident.RegisterQuoting(pathDialect, "declared-paths", "declared-paths-alias"), qt.IsNil)
	c.Assert(sqlident.RegisterQuoting(pathDialect, "declared-paths"), qt.IsNil)
	release, err := lexerext.Provide(providerscan.New(scantest.Scan), "declared-paths")
	c.Assert(err, qt.IsNil)
	t.Cleanup(release)

	c.Assert(sqlident.NamesArePaths("declared-paths"), qt.IsTrue)
	c.Assert(sqlident.NamesArePaths(" Declared-Paths-Alias "), qt.IsTrue)
	c.Assert(sqlident.Quote("declared-paths", "a`b"), qt.Equals, "`a``b`")
	c.Assert(sqlident.Qualified("declared-paths", "shop", "items"), qt.Equals, "`shop/items`")
	name, ok := sqlident.ReadName("declared-paths", "`shop/it``ems`")
	c.Assert(ok, qt.IsTrue)
	c.Assert(name, qt.Equals, "shop/it`ems")
}

// A provider-shaped descriptor that states backslash escapes in names writes
// each backslash doubled, registered or held by the caller, as the ClickHouse
// descriptor does: one writer serves a registered and a provider dialect. A
// single backslash before the closing quote would leave the name open there.
func TestRegisterQuoting_BackslashEscapes_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(sqlident.RegisterQuoting(escapingDialect, "declared-escapes"), qt.IsNil)
	c.Assert(sqlident.Quote("declared-escapes", `ends\`), qt.Equals, "`ends\\\\`")
	c.Assert(sqlident.Ident("declared-escapes", "a`b\\c"), qt.Equals, "`a``b\\\\c`")
	c.Assert(sqlident.IdentWith(escapingDialect, `ends\`), qt.Equals, "`ends\\\\`")
	c.Assert(sqlident.IdentWith(escapingDialect, `ends\`), qt.Equals, sqlident.Ident("clickhouse", `ends\`))
	c.Assert(sqlident.IdentWith(pathDialect, `ends\`), qt.Equals, "`ends\\`")
	name, ok := sqlident.ReadName("declared-escapes", "`a\\\\b`")
	c.Assert(ok, qt.IsTrue)
	c.Assert(name, qt.Equals, `a\b`)
}

func TestRegisterQuoting_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(sqlident.RegisterQuoting(pathDialect, "declared-taken"), qt.IsNil)
	for _, test := range []struct {
		name      string
		described descriptor.Dialect
		names     []string
		want      string
	}{
		{"other quoting for a registered name", descriptor.Dialect{Quoting: descriptor.Quoting{Style: descriptor.QuoteDouble}}, []string{"declared-taken"},
			`sqlident: declared-taken was registered with another spelling`},
		{"other lexical rules for a registered name", descriptor.Dialect{Quoting: pathQuoting, Lexical: escapingDialect.Lexical}, []string{"declared-taken"},
			`sqlident: declared-taken was registered with another spelling`},
		{"a dialect Ptah describes", pathDialect, []string{"MySQL"}, `sqlident: mysql is a dialect Ptah describes, .*`},
		{"an empty name", pathDialect, []string{""}, `sqlident: declared quoting needs an engine name`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlident.RegisterQuoting(test.described, test.names...), qt.ErrorMatches, test.want)
		})
	}
	c.Assert(sqlident.Quote("declared-taken", "t"), qt.Equals, "`t`")
	c.Assert(sqlident.Quote("mysql", "t"), qt.Equals, "`t`")
	c.Assert(sqlident.NamesArePaths("mysql"), qt.IsFalse)
}
