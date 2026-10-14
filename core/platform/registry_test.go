package platform_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
)

func TestLookup_HappyPath(t *testing.T) {
	c := qt.New(t)

	described, found := platform.Lookup(" CRDB ")

	c.Assert(found, qt.IsTrue)
	c.Assert(described.Name, qt.Equals, platform.CockroachDB)
	c.Assert(described.Aliases, qt.DeepEquals, []string{"cockroach", "crdb"})
	c.Assert(described.Family, qt.Equals, platform.Postgres)
	c.Assert(described.DefaultSchema, qt.Equals, "public")
	c.Assert(described.Placeholder, qt.Equals, descriptor.PlaceholderDollar)
}

func TestLookup_FailurePath(t *testing.T) {
	c := qt.New(t)

	described, found := platform.Lookup("grpc")

	c.Assert(found, qt.IsFalse)
	c.Assert(described, qt.DeepEquals, descriptor.Dialect{})
}

// TestDialects_ServeTheSpellings pins that the package-level functions read
// one registry: every spelling DialectSpellings lists normalizes, through
// NormalizeDialect, to the descriptor Lookup returns for it.
func TestDialects_ServeTheSpellings(t *testing.T) {
	c := qt.New(t)
	spellings := platform.DialectSpellings()

	c.Assert(spellings, qt.DeepEquals, platform.Dialects().Spellings())
	c.Assert(len(spellings) >= 34, qt.IsTrue, qt.Commentf("spellings %q", spellings))
	for _, spelling := range spellings {
		described, found := platform.Lookup(spelling)
		c.Assert(found, qt.IsTrue, qt.Commentf("spelling %q", spelling))
		c.Assert(platform.NormalizeDialect(spelling), qt.Equals, described.Name, qt.Commentf("spelling %q", spelling))
	}
}
