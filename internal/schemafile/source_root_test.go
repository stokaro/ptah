package schemafile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dburlext/ydb"
	"ptah.run/internal/schemafile"
)

// TestSourceRoot_HappyPath reads the database root a connection or Docker dev
// URL names, without the dev realm a connection URL may name under it.
func TestSourceRoot_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{name: "a connection URL", rawURL: "ydb://example:2136/Root/db", want: "/Root/db"},
		{name: "a dev realm stays out of the root", rawURL: "ydb://example:2136/local?dev_realm=isolated", want: "/local"},
		{name: "a Docker dev URL", rawURL: "docker://ydb/26.2.1.14/local", want: "/local"},
		{name: "a Docker dev URL with the default database", rawURL: "docker://ydb", want: "/local"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			root, err := schemafile.SourceRoot(platform.YDB, test.rawURL)

			c.Assert(err, qt.IsNil)
			c.Assert(root, qt.Equals, test.want)
		})
	}
}

// TestSourceRoot_FailurePath refuses a URL whose engine names no database root,
// whether the connection's dialect or a Docker dev URL's says so, and a URL
// its engine cannot read.
func TestSourceRoot_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		rawURL  string
		wantErr string
	}{
		{
			name:    "a dialect with no root",
			dialect: platform.Postgres,
			rawURL:  "postgres://example/db",
			wantErr: `dialect "postgres" names no database root a schema document can be read against`,
		},
		{
			name:    "a Docker dev URL of an engine with no root",
			dialect: platform.YDB,
			rawURL:  "docker://postgres/17/dev",
			wantErr: `dialect "postgres" names no database root a schema document can be read against`,
		},
		{
			name:    "a URL the engine cannot read",
			dialect: platform.YDB,
			rawURL:  "ydb:///local",
			wantErr: `a YDB URL needs a host: write ydb://host:2136/database`,
		},
		{
			// url.Parse repeats the URL in its error; the refusal must not
			// carry the password.
			name:    "a URL that does not parse",
			dialect: platform.YDB,
			rawURL:  "ydb://user:s3cret@exa mple:2136/local",
			wantErr: `the URL does not parse`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			root, err := schemafile.SourceRoot(test.dialect, test.rawURL)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(root, qt.Equals, "")
		})
	}
}

// TestLoadPath_RefusesASourceContextWithNoRoot refuses, in YDB's own words, a
// YDB document read against a URL no root can be read from.
func TestLoadPath_RefusesASourceContextWithNoRoot(t *testing.T) {
	for _, rawURL := range []string{"ydb://user:CONTEXT_SENTINEL@example:bad/local", "docker://postgres/17/dev"} {
		t.Run(rawURL, func(t *testing.T) {
			c := qt.New(t)

			database, err := readYQLPermissionFile(c, rawURL, "/local/shop/orders")

			c.Assert(err, qt.ErrorIs, ydb.ErrSourceRoot)
			c.Assert(err, qt.ErrorMatches, `YDB source context requires a valid YDB database or Docker dev URL`)
			c.Assert(database, qt.IsNil)
		})
	}
}
