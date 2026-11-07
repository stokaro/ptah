package pgconnect_test

import (
	"database/sql/driver"
	"errors"
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/connector"
	"ptah.run/internal/connector/pgconnect"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/providerdiagnostic"
)

// errNotScripted answers every query after the banner, so a Describe that goes
// on past identification fails on the first catalog read.
var errNotScripted = errors.New("not scripted")

// banner answers SELECT version() with version and refuses every other query.
func banner(version string) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		if query != "SELECT version()" {
			return dbtest.QueryResult{}, errNotScripted
		}
		return dbtest.QueryResult{Columns: []string{"version"}, Rows: [][]driver.Value{{version}}}, nil
	}
}

func describe(c *qt.C, version string) (*dbtest.DB, error) {
	c.Helper()
	db := dbtest.Open(c, banner(version))
	parsed, err := url.Parse("postgres://app@127.0.0.1:5432/app?sslmode=disable")
	c.Assert(err, qt.IsNil)
	described, found := platform.Lookup(platform.Postgres)
	c.Assert(found, qt.IsTrue)
	_, err = pgconnect.Service{}.Describe(c.Context(), &connector.Pool{DB: db.SQL},
		connector.Request{Dialect: described, URL: parsed.String(), Parsed: parsed},
		catalog.ServerInfo{Dialect: platform.Postgres})
	return db, err
}

// A postgres:// URL that reaches Spanner's PostgreSQL interface is refused as
// soon as the banner names Spanner: the refusal is the catalog's structured
// diagnostic, and nothing past the banner is read, so the server is never
// inspected as PostgreSQL.
func TestDescribe_RefusesAServerTheBannerNamesAsSpanner(t *testing.T) {
	c := qt.New(t)

	db, err := describe(c, "PostgreSQL 14.1 on Cloud Spanner")

	var diagnostic *providerdiagnostic.Diagnostic
	c.Assert(err, qt.ErrorAs, &diagnostic)
	c.Assert(diagnostic.Code, qt.Equals, "provider_engine_identified")
	c.Assert(diagnostic.EngineID, qt.Equals, "spanner")
	c.Assert(diagnostic.ProviderID, qt.Equals, "github.com/stokaro/ptah-spanner-plugin")
	c.Assert(diagnostic.Error(), qt.Contains, "Connect with a spanner:// URL")
	c.Assert(db.QueryCount(), qt.Equals, 1)
}

// The control: a PostgreSQL banner is not identified, so Describe goes on to
// read the session's schema, which the script refuses.
func TestDescribe_ReadsOnPastAPostgreSQLBanner(t *testing.T) {
	c := qt.New(t)

	db, err := describe(c, "PostgreSQL 14.1")

	c.Assert(err, qt.ErrorIs, errNotScripted)
	c.Assert(db.QueryCount(), qt.Equals, 2)
}
