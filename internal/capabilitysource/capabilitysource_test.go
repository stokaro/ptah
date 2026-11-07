package capabilitysource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/capabilitysource"
)

// A declared line answers under each name, without regard to case, until its
// release; the same line declared twice keeps the first registration until
// the first release ends it.
func TestDeclare_HappyPath(t *testing.T) {
	c := qt.New(t)
	first, err := capabilitysource.Declare("read from the switch file", "source-engine", "source-alias")
	c.Assert(err, qt.IsNil)
	second, err := capabilitysource.Declare("read from the switch file", "source-engine")
	c.Assert(err, qt.IsNil)

	source, found := capabilitysource.Lookup("Source-Alias")
	c.Assert(found, qt.IsTrue)
	c.Assert(source, qt.Equals, "read from the switch file")
	second()
	_, found = capabilitysource.Lookup("source-engine")
	c.Assert(found, qt.IsTrue)
	first()
	first()
	_, found = capabilitysource.Lookup("source-engine")
	c.Assert(found, qt.IsFalse)
}

func TestDeclare_FailurePath(t *testing.T) {
	held, err := capabilitysource.Declare("read from the switch file", "held-source-engine")
	qt.New(t).Assert(err, qt.IsNil)
	t.Cleanup(held)
	for _, test := range []struct {
		name, source string
		names        []string
		want         string
	}{
		{name: "an empty line", source: " ", names: []string{"empty-source-engine"}, want: `capabilitysource: an engine states no source line`},
		{name: "an empty name", source: "read", names: []string{" "}, want: `capabilitysource: a source line needs an engine name`},
		{name: "a dialect Ptah describes", source: "read", names: []string{platform.Postgres},
			want: `capabilitysource: postgres is a dialect Ptah describes, whose capability source the host words`},
		{name: "another line for a held name", source: "read elsewhere", names: []string{"fresh-source-engine", "held-source-engine"},
			want: `capabilitysource: held-source-engine is registered with another source line`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			release, err := capabilitysource.Declare(test.source, test.names...)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(release, qt.IsNil)
		})
	}
	t.Run("a refusal keeps no name", func(t *testing.T) {
		c := qt.New(t)
		_, found := capabilitysource.Lookup("fresh-source-engine")
		c.Assert(found, qt.IsFalse)
	})
}
