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

// A provider engine's declared quoting writes its names under each of its
// names, and with the provider's scanner registered its names read back.
func TestRegisterQuoting_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(sqlident.RegisterQuoting(pathQuoting, "declared-paths", "declared-paths-alias"), qt.IsNil)
	c.Assert(sqlident.RegisterQuoting(pathQuoting, "declared-paths"), qt.IsNil)
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

func TestRegisterQuoting_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(sqlident.RegisterQuoting(pathQuoting, "declared-taken"), qt.IsNil)
	for _, test := range []struct {
		name    string
		quoting descriptor.Quoting
		names   []string
		want    string
	}{
		{"other quoting for a registered name", descriptor.Quoting{Style: descriptor.QuoteDouble}, []string{"declared-taken"},
			`sqlident: declared-taken was registered with other quoting`},
		{"a dialect Ptah describes", pathQuoting, []string{"MySQL"}, `sqlident: mysql is a dialect Ptah describes, .*`},
		{"an empty name", pathQuoting, []string{""}, `sqlident: declared quoting needs an engine name`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlident.RegisterQuoting(test.quoting, test.names...), qt.ErrorMatches, test.want)
		})
	}
	c.Assert(sqlident.Quote("declared-taken", "t"), qt.Equals, "`t`")
	c.Assert(sqlident.Quote("mysql", "t"), qt.Equals, "`t`")
	c.Assert(sqlident.NamesArePaths("mysql"), qt.IsFalse)
}
