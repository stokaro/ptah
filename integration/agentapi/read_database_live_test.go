//go:build integration

package agentapi_test

// Live PostgreSQL coverage for the agent surface's database authorization: that
// a denied inspection opens no connection, and that a permitted one actually
// reads the catalogs. Gated on POSTGRES_TEST_DSN / TEST_DATABASE_URL like the
// rest of the live suite.
//
// The pair matters more than either half. A refusal measured alone would pass
// for an implementation that refused everything, and a success measured alone
// would pass for one that authorized nothing.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/agentapi"
	"ptah.run/internal/agentpolicy"
	"ptah.run/internal/agenttarget"
	"ptah.run/internal/dbtarget"
)

// liveSession builds a session holding one configured PostgreSQL target.
func liveSession(c *qt.C, dbURL string, class agentpolicy.DatabaseClass) *agentapi.Session {
	c.Helper()
	policy, err := agentpolicy.Assemble()
	c.Assert(err, qt.IsNil)
	target, err := agenttarget.New(agenttarget.Config{Name: "live", URL: dbURL, Class: class})
	c.Assert(err, qt.IsNil)
	set, err := agenttarget.NewSet(target)
	c.Assert(err, qt.IsNil)
	session, err := agentapi.NewSession(agentapi.SessionConfig{
		Broker:  agentpolicy.NewBroker(policy),
		Targets: set,
	})
	c.Assert(err, qt.IsNil)
	return session
}

func TestReadDatabase_PermittedInspectionReadsTheCatalogsLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)

	// Ephemeral is the one class the builtin table allows outright, and it is
	// what a throwaway test database is.
	session := liveSession(c, dbURL, agentpolicy.ClassEphemeral)

	response, err := session.ReadDatabase(context.Background(), agentapi.ReadDatabaseRequest{})

	c.Assert(err, qt.IsNil)
	c.Assert(response.Dialect, qt.Equals, "postgres")
	c.Assert(response.Version, qt.Not(qt.Equals), "",
		qt.Commentf("the server answered, so authorization did not stop at the door"))
}

func TestReadDatabase_DeniedInspectionNeverConnectsLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)

	// The same reachable database, classified so the builtin table denies it.
	session := liveSession(c, dbURL, agentpolicy.ClassProduction)

	_, err := session.ReadDatabase(context.Background(), agentapi.ReadDatabaseRequest{})

	c.Assert(err, qt.ErrorAs, new(*agentpolicy.DeniedError))
}

func TestReadDatabase_TheErrorCarriesNoCredentialLive(t *testing.T) {
	// A refusal is shown to the model. The URL it is about must not be in it,
	// and the URL here is a real one with whatever the operator's DSN carries.
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	session := liveSession(c, dbURL, agentpolicy.ClassProduction)

	_, err := session.ReadDatabase(context.Background(), agentapi.ReadDatabaseRequest{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), dbURL)
}
