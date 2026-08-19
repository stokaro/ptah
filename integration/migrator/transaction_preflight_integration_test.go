//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestTransactionPreflight_RefusesBeforeReachingPostgres is the test the
// preflight cannot be trusted without.
//
// Both shapes used to reach the database and fail there with the server's own
// SQLSTATE after the earlier statements had run. The discriminating
// observable is not that the migration fails -- it failed before -- but that
// the failure is Ptah's sentence rather than PostgreSQL's code: an error
// carrying 25001 or 55P04 proves the statement was sent (stokaro/ptah#996).
func TestTransactionPreflight_RefusesBeforeReachingPostgres(t *testing.T) {
	tests := []struct {
		name        string
		setup       string
		sql         string
		wantMessage string
		wantRemedy  string
	}{
		{
			name: "concurrent index mixed with transactional DDL",
			sql: "CREATE TABLE pf_a (id integer PRIMARY KEY, v integer);\n" +
				"CREATE INDEX CONCURRENTLY pf_a_v_idx ON pf_a (v);\n",
			wantMessage: "CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block",
			wantRemedy:  "no_transaction",
		},
		{
			name:  "a value added to a pre-existing enum and then used",
			setup: "CREATE TYPE pf_mood AS ENUM ('ok')",
			sql: "CREATE TABLE pf_b (id integer PRIMARY KEY);\n" +
				"ALTER TYPE pf_mood ADD VALUE 'great';\n" +
				"CREATE TABLE pf_c (m pf_mood DEFAULT 'great');\n",
			wantMessage: "not usable until the transaction that added it commits",
			wantRemedy:  "no_transaction",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := preflightScratchConnection(t, test.setup)

			mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
				"0000000001_a.up.sql":   &fstest.MapFile{Data: []byte(test.sql)},
				"0000000001_a.down.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(mig.Initialize(t.Context()), qt.IsNil)

			err = mig.MigrateUp(t.Context())

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "cannot run inside a transaction")
			c.Assert(err.Error(), qt.Contains, test.wantMessage)
			c.Assert(err.Error(), qt.Contains, test.wantRemedy)
			// The server was never asked. Its refusals carry a SQLSTATE, and
			// this one does not.
			c.Assert(err.Error(), qt.Not(qt.Contains), "SQLSTATE")
			c.Assert(preflightRelationExists(c, conn, "pf_a"), qt.IsFalse)
			c.Assert(preflightRelationExists(c, conn, "pf_b"), qt.IsFalse)
		})
	}
}

// TestTransactionPreflight_RefusesWhatPostgresRefuses pins that the preflight
// is the engine's rule rather than one this repository invented.
//
// The same SQL is handed to PostgreSQL inside an explicit transaction, and the
// server has to refuse it too.
func TestTransactionPreflight_RefusesWhatPostgresRefuses(t *testing.T) {
	tests := []struct {
		name      string
		setup     string
		sql       string
		wantError string
	}{
		{
			name:      "concurrent index",
			sql:       "CREATE TABLE pr_a (id integer, v integer); CREATE INDEX CONCURRENTLY pr_a_v ON pr_a (v);",
			wantError: "cannot run inside a transaction block",
		},
		{
			name:      "a value added to a pre-existing enum and then used",
			setup:     "CREATE TYPE pr_mood AS ENUM ('ok')",
			sql:       "ALTER TYPE pr_mood ADD VALUE 'great'; CREATE TABLE pr_b (m pr_mood DEFAULT 'great');",
			wantError: "unsafe use of new value",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := preflightScratchConnection(t, test.setup)

			// One string with an explicit transaction, which is the condition
			// the preflight is about: the migrator wraps a file the same way.
			_, execErr := conn.ExecContext(t.Context(), "BEGIN; "+test.sql+" COMMIT;")

			c.Assert(execErr, qt.IsNotNil)
			c.Assert(execErr.Error(), qt.Contains, test.wantError)
		})
	}
}

// TestTransactionPreflight_KeepsTheValidEnumWorkflow is the control that
// stops the preflight from becoming a keyword check.
//
// A file that creates its own enum, adds a value and uses it runs inside one
// transaction on PostgreSQL -- measured -- so refusing it would break a
// workflow the issue explicitly protects. The no_transaction spelling of the
// same file must keep working too, since that is the remedy the diagnostic
// offers.
func TestTransactionPreflight_KeepsTheValidEnumWorkflow(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "transactional, enum created in the same file",
			sql: "CREATE TYPE ok_mood AS ENUM ('a');\n" +
				"ALTER TYPE ok_mood ADD VALUE 'b';\n" +
				"CREATE TABLE ok_t (c ok_mood DEFAULT 'b');\n",
		},
		{
			name: "no_transaction, the remedy the diagnostic names",
			sql: "-- +ptah no_transaction\n" +
				"CREATE TABLE ok_u (id integer PRIMARY KEY, v integer);\n" +
				"CREATE INDEX CONCURRENTLY ok_u_v_idx ON ok_u (v);\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := preflightScratchConnection(t, "")

			mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
				"0000000001_a.up.sql":   &fstest.MapFile{Data: []byte(test.sql)},
				"0000000001_a.down.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
			})
			c.Assert(err, qt.IsNil)
			c.Assert(mig.Initialize(t.Context()), qt.IsNil)

			c.Assert(mig.MigrateUp(t.Context()), qt.IsNil)
		})
	}
}

// preflightScratchConnection opens a connection to a database of this test's
// own.
//
// A schema plus SET search_path is not enough: the migrator opens its own
// sessions from the pool, and the setting does not travel with them, so the
// revision table created during Initialize is invisible by MigrateUp. A
// separate database is what the rest of this suite uses for the same reason.
func preflightScratchConnection(t *testing.T, setup string) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)

	adminConn, err := dbschema.ConnectToDatabase(t.Context(), postgresTestURL(t))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(adminConn)

	database := fmt.Sprintf("ptah_pf_%d", time.Now().UnixNano())
	_, err = adminConn.ExecContext(t.Context(), `CREATE DATABASE "`+database+`"`)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		cleanupConn, cleanupErr := dbschema.ConnectToDatabase(context.Background(), postgresTestURL(t))
		if cleanupErr != nil {
			return
		}
		defer dbschema.CloseAndWarn(cleanupConn)
		_, _ = cleanupConn.ExecContext(context.Background(), `DROP DATABASE IF EXISTS "`+database+`" WITH (FORCE)`)
	})

	conn, err := dbschema.ConnectToDatabase(t.Context(), preflightDatabaseURL(c, postgresTestURL(t), database))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	if strings.TrimSpace(setup) != "" {
		_, err = conn.ExecContext(t.Context(), setup)
		c.Assert(err, qt.IsNil)
	}
	return conn
}

// preflightDatabaseURL points an address at another database on the same
// server.
func preflightDatabaseURL(c *qt.C, address, database string) string {
	c.Helper()
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	return parsed.String()
}

func preflightRelationExists(c *qt.C, conn *dbschema.DatabaseConnection, name string) bool {
	c.Helper()
	var count int
	err := conn.QueryRowContext(context.Background(),
		`SELECT count(*) FROM pg_class c
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE c.relname = $1 AND n.nspname = current_schema()`, name).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count > 0
}
