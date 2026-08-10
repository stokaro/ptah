//go:build integration

package dbschema_test

import (
	"net/url"
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
)

// TestMySQLLiveConnection_DefaultSchemaIsTheConnectedDatabase pins that a
// MySQL-family connection reports the connected database as the schema that
// owns unqualified objects (stokaro/ptah#1244).
//
// A schema on these engines is a database, so the offline dialect rules have no
// static default to offer and deliberately carry an empty one -- unlike
// PostgreSQL's "public" or SQLite's "main". The connection is the only place
// that knows the answer, and until it filled the field in, schema comparison
// keyed a table the catalog reports with no schema differently from the same
// table a desired state names explicitly, and planned to create and drop every
// one of them. SQL Server resolves the same field from the same place.
//
// Live-only: the value comes from the URL path or from SELECT DATABASE(), and
// asserting it against anything but a real connection would only restate the
// test's own fixture.
func TestMySQLLiveConnection_DefaultSchemaIsTheConnectedDatabase(t *testing.T) {
	tests := []struct {
		name           string
		environmentKey string
	}{
		{name: "mysql", environmentKey: "MYSQL_TEST_URL"},
		{name: "mariadb", environmentKey: "MARIADB_TEST_URL"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			databaseURL := requireLiveMySQLURL(c, test.environmentKey)
			wantSchema := databaseNameFromURL(c, databaseURL)

			conn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				dbschema.CloseAndWarn(conn)
			})

			info := conn.Info()

			c.Assert(info.Schema, qt.Equals, wantSchema)
			c.Assert(info.IdentifierSemantics.DefaultSchema, qt.Equals, wantSchema)
			// The pairing is the invariant, not either value alone: comparison
			// resolves an absent schema through DefaultSchema, and a connection
			// whose two fields disagree would key its own tables against a
			// database it is not connected to.
			c.Assert(info.IdentifierSemantics.DefaultSchema, qt.Equals, info.Schema)
		})
	}
}

func requireLiveMySQLURL(c *qt.C, environmentKey string) string {
	c.Helper()
	databaseURL := os.Getenv(environmentKey)
	if databaseURL == "" {
		c.Skip(environmentKey + " is not set")
	}
	return databaseURL
}

// databaseNameFromURL reads the database the URL selects. A MySQL URL without a
// path leaves the connection to resolve it with SELECT DATABASE(), which this
// test cannot predict, so it is skipped rather than guessed at.
func databaseNameFromURL(c *qt.C, rawURL string) string {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	name := strings.TrimPrefix(parsed.Path, "/")
	if name == "" {
		c.Skip("the configured URL names no database")
	}
	return name
}
