//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/sijms/go-ora/v3" // registers the Oracle driver for database/sql

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestOracleSecondConnectionE2E pins the one thing a driver change can take
// away without failing a build, a vet or 243 packages of unit tests: the
// ability to hold two Oracle connections at once.
//
// Nothing in Ptah is exotic about wanting two. The capability probe waits for a
// server on one connection and measures it on the next; a comparison against a
// dev database holds the target and the dev server together; every command that
// resolves a desired state from one URL and applies it to another does the
// same. A driver that serves the first and refuses the second leaves all of
// that reporting a connection error against a server that is up.
//
// go-ora v3 caches the server's negotiation in a package-level store and
// replays it on the next connection to skip a round trip. Oracle 23.26.2.0.0
// closes the socket on the replay, and the driver clears the cached cookie only
// for driver.ErrBadConn while this one is io.EOF -- so one refused connection
// leaves every later one in the process refused too. Ptah turns that
// optimization off; see oracleDataSourceName (stokaro/ptah#1875).
//
// The third handle is opened after the second is used rather than before, so a
// build that recovered only by luck of ordering still fails here.
func TestOracleSecondConnectionE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	first, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(first)

	// Opened while the first is still open, which is the case that breaks. A
	// sequential pair would pass on a driver that cannot do this, because the
	// server-side session of the closed one has usually gone by then.
	second, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(second)

	// Connecting is not the assertion. A handle that pings and then cannot
	// answer a query is the same outage arriving one call later, so both are
	// asked for something the server has to compute.
	for _, conn := range []*dbschema.DatabaseConnection{first, second} {
		var answer int
		c.Assert(conn.QueryRowContext(ctx, "SELECT 1 FROM dual").Scan(&answer), qt.IsNil)
		c.Assert(answer, qt.Equals, 1)
	}

	third, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(third)

	// And the first still works after the others have come and gone, so the set
	// is live sessions rather than one handed back and forth.
	var answer int
	c.Assert(first.QueryRowContext(ctx, "SELECT 2 FROM dual").Scan(&answer), qt.IsNil)
	c.Assert(answer, qt.Equals, 2)
}
