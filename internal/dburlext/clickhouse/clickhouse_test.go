package clickhouse_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/internal/dburlext"
	"ptah.run/internal/dburlext/clickhouse"
)

func TestSelect_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   dburlext.Selection
	}{
		{
			name:   "one address",
			rawURL: "clickhouse://u@h:9440/app",
			want:   dburlext.Selection{Endpoints: []dburlext.Endpoint{{Host: "h", Port: "9440"}}, Database: "app"},
		},
		{
			name:   "every address the URL lists",
			rawURL: "ch://u@h1:9000,h2:9001/app",
			want: dburlext.Selection{
				Endpoints: []dburlext.Endpoint{{Host: "h1", Port: "9000"}, {Host: "h2", Port: "9001"}}, Database: "app",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			described, found := platform.Lookup(platform.ClickHouse)
			c.Assert(found, qt.IsTrue)

			got, err := clickhouse.Services().Endpoints.Select(must.Must(url.Parse(test.rawURL)), described)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestSelect_FailurePath(t *testing.T) {
	c := qt.New(t)
	described, found := platform.Lookup(platform.ClickHouse)
	c.Assert(found, qt.IsTrue)

	got, err := clickhouse.Services().Endpoints.Select(must.Must(url.Parse("clickhouse://u:secret@h/app?dial_timeout=soon")), described)

	c.Assert(err, qt.ErrorMatches, `invalid ClickHouse database URL`)
	c.Assert(got, qt.DeepEquals, dburlext.Selection{})
}
