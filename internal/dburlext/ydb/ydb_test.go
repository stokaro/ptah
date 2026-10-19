package ydb_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/internal/dburlext"
	"ptah.run/internal/dburlext/ydb"
)

func TestSelect_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   dburlext.Selection
	}{
		{
			name:   "the database in the path, on the plaintext port",
			rawURL: "ydb://h/local",
			want:   dburlext.Selection{Endpoints: []dburlext.Endpoint{{Host: "h", Port: "2136"}}, Database: "/local"},
		},
		{
			name:   "the database in a parameter, on the TLS port",
			rawURL: "ydbs://h/?database=/ru-central1/b1g/etn",
			want:   dburlext.Selection{Endpoints: []dburlext.Endpoint{{Host: "h", Port: "2135"}}, Database: "/ru-central1/b1g/etn"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			described, found := platform.Lookup(platform.YDB)
			c.Assert(found, qt.IsTrue)

			got, err := ydb.Services().Endpoints.Select(must.Must(url.Parse(test.rawURL)), described)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestSelect_FailurePath(t *testing.T) {
	c := qt.New(t)
	described, found := platform.Lookup(platform.YDB)
	c.Assert(found, qt.IsTrue)

	got, err := ydb.Services().Endpoints.Select(must.Must(url.Parse("ydb://h/a?database=/b")), described)

	c.Assert(err, qt.ErrorMatches, `invalid YDB database URL: .*`)
	c.Assert(got, qt.DeepEquals, dburlext.Selection{})
}

func TestRename_Refuses(t *testing.T) {
	c := qt.New(t)

	got, err := ydb.Services().Rename.Rename(must.Must(url.Parse("ydb://h/local")), "scratch")

	c.Assert(err, qt.ErrorIs, ydb.ErrDatabaseName)
	c.Assert(got, qt.Equals, "")
}
