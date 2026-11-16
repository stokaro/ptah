package dbschema_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/dbschema"
)

// A provider session runs no probe, whatever engine name it serves: it has no
// database/sql transaction the host could roll back, so the method reports
// ran false with a nil error and never calls the body, on the root connection
// and on a session it opens. A name Ptah builds in, postgres here, changes
// nothing, because the session decides and not the engine's name.
func TestNativeConnectionRunsNoProbe_HappyPath(t *testing.T) {
	for _, served := range []string{"example", "postgres"} {
		t.Run(served, func(t *testing.T) {
			c := qt.New(t)
			fixture := &nativeFixture{metadata: &catalog.ServerInfo{Dialect: served, Schema: "main", Provider: nativeIdentity(),
				IdentifierSemantics: identifier.ForDialect("sqlite")}}
			ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{served: {Target: served, Connector: fixture}})
			c.Assert(err, qt.IsNil)
			conn, err := dbschema.ConnectToDatabase(ctx, served+"://database")
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			calls := 0
			body := func(context.Context, *sql.Tx) error { calls++; return nil }

			ran, err := conn.WithRolledBackTransaction(c.Context(), "probe", body)
			var scopedRan bool
			var scopedErr error
			c.Assert(conn.WithSession(c.Context(), func(scoped *dbschema.DatabaseConnection) error {
				scopedRan, scopedErr = scoped.WithRolledBackTransaction(c.Context(), "probe", body)
				return nil
			}), qt.IsNil)

			c.Assert(err, qt.IsNil)
			c.Assert(ran, qt.IsFalse)
			c.Assert(scopedErr, qt.IsNil)
			c.Assert(scopedRan, qt.IsFalse)
			c.Assert(calls, qt.Equals, 0)
		})
	}
}

// A connection that is neither a provider session nor open on a database is
// still refused: the provider's answer is not a way around a missing handle.
func TestWithRolledBackTransaction_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		conn *dbschema.DatabaseConnection
	}{
		{name: "nil connection"},
		{name: "connection without a database", conn: new(dbschema.DatabaseConnection)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0

			ran, err := test.conn.WithRolledBackTransaction(c.Context(), "probe", func(context.Context, *sql.Tx) error { calls++; return nil })

			c.Assert(err, qt.ErrorMatches, `probe: database connection is nil`)
			c.Assert(ran, qt.IsFalse)
			c.Assert(calls, qt.Equals, 0)
		})
	}
}
