package ydburl_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydburl"
)

func TestParse_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		raw          string
		wantSecure   bool
		wantEndpoint string
		wantDatabase string
		wantQuery    url.Values
	}{
		{
			name:         "plaintext defaults to 2136",
			raw:          "ydb://localhost/local",
			wantEndpoint: "localhost:2136",
			wantDatabase: "/local",
			wantQuery:    url.Values{},
		},
		{
			name:         "TLS defaults to 2135",
			raw:          "ydbs://ydb.example/ru-central1/b1g/etn",
			wantSecure:   true,
			wantEndpoint: "ydb.example:2135",
			wantDatabase: "/ru-central1/b1g/etn",
			wantQuery:    url.Values{},
		},
		{
			name:         "an explicit port and an IPv6 host",
			raw:          "ydb://[::1]:45136/local/",
			wantEndpoint: "[::1]:45136",
			wantDatabase: "/local",
			wantQuery:    url.Values{},
		},
		{
			name:         "the database parameter is taken out of the query",
			raw:          "ydb://h:2136/?database=local&go_balancer=disable",
			wantEndpoint: "h:2136",
			wantDatabase: "/local",
			wantQuery:    url.Values{"go_balancer": {"disable"}},
		},
		{
			name:         "the path and the parameter may name the same database",
			raw:          "YDB://h/local?database=/local/",
			wantEndpoint: "h:2136",
			wantDatabase: "/local",
			wantQuery:    url.Values{},
		},
		{
			name:         "no database at all",
			raw:          "ydb://h:2136",
			wantEndpoint: "h:2136",
			wantDatabase: "",
			wantQuery:    url.Values{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydburl.Parse(test.raw)

			c.Assert(err, qt.IsNil)
			c.Assert(got.Secure, qt.Equals, test.wantSecure)
			c.Assert(got.Endpoint(), qt.Equals, test.wantEndpoint)
			c.Assert(got.Database, qt.Equals, test.wantDatabase)
			c.Assert(got.Query, qt.DeepEquals, test.wantQuery)
		})
	}
}

func TestParse_KeepsTheUser(t *testing.T) {
	c := qt.New(t)

	got, err := ydburl.Parse("ydb://alice:s3cret@h:2136/local")

	c.Assert(err, qt.IsNil)
	c.Assert(got.User.Username(), qt.Equals, "alice")
	password, set := got.User.Password()
	c.Assert(set, qt.IsTrue)
	c.Assert(password, qt.Equals, "s3cret")
}

func TestParse_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{
			name:    "grpc is not a YDB scheme",
			raw:     "grpc://h:2136/local",
			wantErr: `not a ydb:// or ydbs:// URL: "grpc"`,
		},
		{
			name:    "no host",
			raw:     "ydb:///local",
			wantErr: `a YDB URL needs a host: write ydb://host:2136/database`,
		},
		{
			name:    "an opaque URL has no host",
			raw:     "ydb:local",
			wantErr: `a YDB URL needs a host: write ydb://host:2136/database`,
		},
		{
			name:    "the path and the parameter disagree",
			raw:     "ydb://h/local?database=/dev",
			wantErr: `the URL names database /local in its path and /dev in the database parameter; name it once`,
		},
		{
			name:    "the parameter is given twice",
			raw:     "ydb://h/?database=/a&database=/a",
			wantErr: `the database parameter is given more than once`,
		},
		{
			name:    "the parameter is empty",
			raw:     "ydb://h/?database=/",
			wantErr: `the database parameter is empty`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydburl.Parse(test.raw)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.DeepEquals, ydburl.URL{})
		})
	}
}
