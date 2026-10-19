package ydb_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/internal/dburlext"
	"ptah.run/internal/dburlext/ydb"
	"ptah.run/internal/ydburl"
)

// TestRoot_HappyPath reads the database root a YDB URL names, from its path or
// its database parameter, apart from the dev realm it names in that database.
func TestRoot_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   dburlext.DatabaseRoot
	}{
		{name: "the database in the path", rawURL: "ydb://h:2136/local", want: dburlext.DatabaseRoot{Path: "/local"}},
		{
			name:   "the database in a parameter",
			rawURL: "ydbs://h/?database=/ru-central1/b1g/etn",
			want:   dburlext.DatabaseRoot{Path: "/ru-central1/b1g/etn"},
		},
		{
			name:   "a dev realm in the database",
			rawURL: "ydb://h:2136/local?" + ydburl.RealmParameter + "=k3j9",
			want:   dburlext.DatabaseRoot{Path: "/local", Realm: "k3j9"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydb.Services().Roots.Root(must.Must(url.Parse(test.rawURL)))

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestRoot_FailurePath refuses a URL that is not YDB's, and one that names a
// realm without a database to hold it.
func TestRoot_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantErr string
	}{
		{name: "another engine's scheme", rawURL: "postgres://h/db", wantErr: `not a ydb:// or ydbs:// URL: "postgres"`},
		{name: "no host", rawURL: "ydb:///local", wantErr: `a YDB URL needs a host: write ydb://host:2136/database`},
		{
			name:    "a realm and no database",
			rawURL:  "ydb://h:2136/?" + ydburl.RealmParameter + "=k3j9",
			wantErr: `the URL names a dev realm and no database to hold it`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ydb.Services().Roots.Root(must.Must(url.Parse(test.rawURL)))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, dburlext.DatabaseRoot{})
		})
	}
}

// TestDatabaseRoot spells the root of the database a Docker dev URL names with
// one leading slash, whatever slashes the name carries.
func TestDatabaseRoot(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "local", want: "/local"},
		{name: "/local/", want: "/local"},
		{name: "a/b", want: "/a/b"},
		{name: "", want: "/"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(ydb.Services().Roots.DatabaseRoot(test.name), qt.Equals, test.want)
		})
	}
}

// TestRefusals are YDB's own sentences for a URL no root could be read from.
func TestRefusals(t *testing.T) {
	c := qt.New(t)
	roots := ydb.Services().Roots

	c.Assert(roots.SourceRefusal(), qt.ErrorIs, ydb.ErrSourceRoot)
	c.Assert(roots.SourceRefusal(), qt.ErrorMatches, `YDB source context requires a valid YDB database or Docker dev URL`)
	c.Assert(roots.DiffRefusal(), qt.ErrorIs, ydb.ErrDiffRoot)
	c.Assert(roots.DiffRefusal(), qt.ErrorMatches, `YDB diff requires a valid database or Docker dev URL`)
}
