package postgres_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/internal/dburlext"
	"ptah.run/internal/dburlext/postgres"
)

func TestSelect_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		rawURL  string
		want    dburlext.Selection
	}{
		{
			name: "one host and the driver's default port", dialect: platform.Postgres,
			rawURL: "postgres://u@db.example/app?sslmode=disable",
			want:   dburlext.Selection{Endpoints: []dburlext.Endpoint{{Host: "db.example", Port: "5432"}}, Database: "app"},
		},
		{
			// sslmode=prefer tries TLS and then plaintext, which pgconn reads
			// as a fallback to the same server.
			name: "the fallback a TLS preference adds", dialect: platform.Postgres,
			rawURL: "postgres://u@db.example/app",
			want: dburlext.Selection{
				Endpoints: []dburlext.Endpoint{{Host: "db.example", Port: "5432"}, {Host: "db.example", Port: "5432"}}, Database: "app",
			},
		},
		{
			name: "every host the driver falls back to", dialect: platform.Postgres,
			rawURL: "postgresql://u@/app?host=h1,h2&port=5433,5432&sslmode=disable",
			want: dburlext.Selection{
				Endpoints: []dburlext.Endpoint{{Host: "h1", Port: "5433"}, {Host: "h2", Port: "5432"}}, Database: "app",
			},
		},
		{
			name: "a pgx URL, read under the dialect's name", dialect: platform.Postgres,
			rawURL: "pgx://u@h:6000/app?sslmode=disable",
			want:   dburlext.Selection{Endpoints: []dburlext.Endpoint{{Host: "h", Port: "6000"}}, Database: "app"},
		},
		{
			name: "a CockroachDB URL, read under its wire scheme", dialect: platform.CockroachDB,
			rawURL: "cockroachdb://root@h:26257/defaultdb?sslmode=disable",
			want:   dburlext.Selection{Endpoints: []dburlext.Endpoint{{Host: "h", Port: "26257"}}, Database: "defaultdb"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			described, found := platform.Lookup(test.dialect)
			c.Assert(found, qt.IsTrue)

			got, err := postgres.Services().Endpoints.Select(must.Must(url.Parse(test.rawURL)), described)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// The refusal does not repeat the URL, which carries a password.
func TestSelect_FailurePath(t *testing.T) {
	c := qt.New(t)
	described, found := platform.Lookup(platform.Postgres)
	c.Assert(found, qt.IsTrue)

	got, err := postgres.Services().Endpoints.Select(must.Must(url.Parse("postgres://u:secret@h/app?sslmode=nonesuch")), described)

	c.Assert(err, qt.ErrorMatches, `invalid PostgreSQL database URL`)
	c.Assert(got, qt.DeepEquals, dburlext.Selection{})
}
