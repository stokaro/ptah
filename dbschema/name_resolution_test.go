package dbschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
)

// TestResolveNamesAs_FailurePath covers the connections whose name resolution
// cannot be moved: a pool, whose sessions other users share, and a session of
// an engine with no search path. The PostgreSQL session that can be moved is
// driven by the schema apply engine tests under integration/.
func TestResolveNamesAs_FailurePath(t *testing.T) {
	like := catalog.ServerInfo{Dialect: "postgres", Schema: "rebase"}

	t.Run("a pool", func(t *testing.T) {
		c := qt.New(t)
		conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite:///:memory:")
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { _ = conn.Close() })

		moved, err := conn.ResolveNamesAs(context.Background(), like)

		c.Assert(err, qt.ErrorMatches, "moving name resolution requires a pinned database session")
		c.Assert(moved, qt.IsNil)
	})

	t.Run("a session of an engine with no search path", func(t *testing.T) {
		c := qt.New(t)
		conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite:///:memory:")
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { _ = conn.Close() })
		var moved *dbschema.DatabaseConnection

		err = conn.WithSession(context.Background(), func(session *dbschema.DatabaseConnection) error {
			var resolveErr error
			moved, resolveErr = session.ResolveNamesAs(context.Background(), like)
			return resolveErr
		})

		c.Assert(err, qt.ErrorMatches, `dialect "sqlite" does not resolve names along a search path`)
		c.Assert(moved, qt.IsNil)
	})
}
