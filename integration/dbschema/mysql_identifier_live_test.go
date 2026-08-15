//go:build integration

package dbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/schemaselection"
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
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQL},
		{name: "mariadb", engine: dbtarget.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			databaseURL := dbtarget.URL(c, test.engine)
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

// databaseNameFromURL reads the database through the same semantic URL API used
// by the product. In particular, it supports the driver's tcp(host:port) form,
// which net/url rejects before it reaches the database path.
func databaseNameFromURL(c *qt.C, rawURL string) string {
	c.Helper()
	name, limited := schemaselection.URLScope(rawURL)
	c.Assert(limited, qt.IsTrue, qt.Commentf("the configured URL must name a database"))
	return name
}
