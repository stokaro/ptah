//go:build integration

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/sqlident"
)

// TestCreateDatabaseCreatesAMissingDatabaseOnceLive runs the statement the
// provisioner sends to an image the URL names, twice, on each engine family:
// the first call creates the database, and the second finds it and succeeds,
// which is the call an image that honored the variable naming its database
// receives (stokaro/ptah#4040).
func TestCreateDatabaseCreatesAMissingDatabaseOnceLive(t *testing.T) {
	tests := []struct {
		name    string
		engine  dbtarget.Engine
		dialect string
		// exists counts the databases named $1 in the server's catalog.
		exists string
	}{
		{
			name:    "postgresql",
			engine:  dbtarget.PostgreSQL,
			dialect: "postgres",
			exists:  "SELECT count(*) FROM pg_database WHERE datname = $1",
		},
		{
			name:    "mysql",
			engine:  dbtarget.MySQLAdmin,
			dialect: "mysql",
			exists:  "SELECT count(*) FROM information_schema.schemata WHERE schema_name = ?",
		},
		{
			name:    "mariadb",
			engine:  dbtarget.MariaDBAdmin,
			dialect: "mariadb",
			exists:  "SELECT count(*) FROM information_schema.schemata WHERE schema_name = ?",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			serverURL := dbtarget.URL(c, test.engine)
			suffix := make([]byte, 6)
			_, err := rand.Read(suffix)
			c.Assert(err, qt.IsNil)
			database := "ptah_created_" + hex.EncodeToString(suffix)
			conn, err := dbschema.ConnectToServer(t.Context(), serverURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = conn.Close() })
			c.Cleanup(func() {
				// #nosec G202 -- the name is generated above and quoted through sqlident.
				_, _ = conn.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+sqlident.Quote(test.dialect, database))
			})

			c.Assert(devdocker.CreateDatabase(t.Context(), serverURL, test.dialect, database), qt.IsNil)
			c.Assert(devdocker.CreateDatabase(t.Context(), serverURL, test.dialect, database), qt.IsNil)

			var count int
			c.Assert(conn.QueryRowContext(t.Context(), test.exists, database).Scan(&count), qt.IsNil)
			c.Assert(count, qt.Equals, 1)
		})
	}
}
