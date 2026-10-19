package atlasschema

// White-box testing required: Diff reads a dev URL's database root for two
// schema documents only after the SQL loader has read the same URL as its
// source context and refused it in its own words, so the diff's refusal and
// the paths that keep a root are reachable through Diff only with document
// formats this package's tests do not build for YDB.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dburlext/ydb"
)

// TestSetDiffRoot_HappyPath gives a comparison of two documents the database
// root the dev URL names, without its dev realm, and leaves a side that
// carries a root, an engine with none and an absent dev URL as they are.
func TestSetDiffRoot_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		devURL  string
		current string
		want    string
	}{
		{name: "a Docker dev URL", dialect: "ydb", devURL: "docker://ydb/26.2.1.14/local", want: "/local"},
		{name: "a dev realm", dialect: "ydb", devURL: "ydb://localhost:2136/local?dev_realm=abc", want: "/local"},
		{name: "a side that carries a root", dialect: "ydb", devURL: "ydb://localhost:2136/local", current: "/Root/db", want: "/Root/db"},
		{name: "no dev URL", dialect: "ydb", devURL: "", want: ""},
		{name: "an engine with no root", dialect: "postgres", devURL: "postgres://localhost/db", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := &catalog.Database{DatabasePath: test.current}

			err := setDiffRoot(test.dialect, test.devURL, current)

			c.Assert(err, qt.IsNil)
			c.Assert(current.DatabasePath, qt.Equals, test.want)
		})
	}
}

// TestSetDiffRoot_FailurePath refuses, in YDB's words, a dev URL no root can
// be read from.
func TestSetDiffRoot_FailurePath(t *testing.T) {
	for _, devURL := range []string{"ydb:///local", "docker://ydb?database=/other", "docker://postgres/17/dev"} {
		t.Run(devURL, func(t *testing.T) {
			c := qt.New(t)
			current := &catalog.Database{}

			err := setDiffRoot("ydb", devURL, current)

			c.Assert(err, qt.ErrorIs, ydb.ErrDiffRoot)
			c.Assert(err, qt.ErrorMatches, `YDB diff requires a valid database or Docker dev URL`)
			c.Assert(current.DatabasePath, qt.Equals, "")
		})
	}
}
