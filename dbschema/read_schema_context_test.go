package dbschema_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// openSQLiteWithATable returns a connection to a fresh on-disk SQLite database
// holding one table, so a schema read has something to find and a read that
// returns nothing is distinguishable from a read that was stopped.
func openSQLiteWithATable(c *qt.C) *dbschema.DatabaseConnection {
	c.Helper()

	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		"sqlite://"+filepath.Join(c.TempDir(), "read-schema-context.db"),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	_, err = conn.ExecContext(
		c.Context(),
		"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)",
	)
	c.Assert(err, qt.IsNil)
	return conn
}

// TestReadSchemaContext_RefusesACanceledContext is the guarantee the Context
// half of the SchemaReader pair carries: the context governs the read.
//
// The cancellation is done BEFORE the call rather than during it. A SQLite
// database in a temporary directory answers a whole schema read in single-digit
// milliseconds, so a goroutine racing a cancel against the read would decide
// the assertion by scheduling luck. Canceling first asks the same question --
// does the context reach the driver at all -- with an answer that does not
// depend on timing.
//
// The live-context read below it is the control: it is the same call on the
// same connection, and it succeeds, so the refusal above is attributable to the
// context and not to a database the read could not open.
func TestReadSchemaContext_RefusesACanceledContext(t *testing.T) {
	c := qt.New(t)
	conn := openSQLiteWithATable(c)

	canceled, cancel := context.WithCancel(c.Context())
	cancel()

	_, err := conn.Reader().ReadSchemaContext(canceled)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err, qt.ErrorIs, context.Canceled)

	schema, err := conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 1)
}

// TestReadSchemaContext_RefusesAnExpiredDeadline is the deadline half of the
// same contract, because a caller bounding a read against an unresponsive
// server sets a deadline rather than canceling.
func TestReadSchemaContext_RefusesAnExpiredDeadline(t *testing.T) {
	c := qt.New(t)
	conn := openSQLiteWithATable(c)

	expired, cancel := context.WithDeadline(c.Context(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := conn.Reader().ReadSchemaContext(expired)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
}

// TestReadSchema_ReadsTheSameSchemaAsTheContextForm covers the other half of
// the pair: the context-free spelling is a real read and not a stub.
//
// It is the spelling an external caller compiled against a released version
// has, so nothing in this repository exercises it -- every in-tree caller holds
// a context and calls ReadSchemaContext. Left unmeasured, a delegation that
// dropped the read, or that reached a different reader, would ship green.
//
// The assertion compares against the Context form on the same connection rather
// than against a table count alone, so a delegation that read a different
// database, or a differently scoped one, fails here too.
func TestReadSchema_ReadsTheSameSchemaAsTheContextForm(t *testing.T) {
	c := qt.New(t)
	conn := openSQLiteWithATable(c)

	withoutContext, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(withoutContext.Tables, qt.HasLen, 1)

	withContext, err := conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(withoutContext, qt.DeepEquals, withContext)
}

// TestReadSchemaWithSchemasContext_RefusesACanceledContext holds the
// cancellation guarantee at the scoped entry point, which builds its own reader
// and could have dropped the context on the way there.
func TestReadSchemaWithSchemasContext_RefusesACanceledContext(t *testing.T) {
	c := qt.New(t)
	conn := openSQLiteWithATable(c)

	canceled, cancel := context.WithCancel(c.Context())
	cancel()

	_, err := dbschema.ReadSchemaWithSchemasContext(canceled, conn, []string{"main"})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err, qt.ErrorIs, context.Canceled)

	schema, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"main"})
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Tables, qt.HasLen, 1)
}

// TestReadSchemaWithSchemas_ReadsTheSameSchemaAsTheContextForm is the scoped
// entry point's half of the pair, for the same reason the reader's is: the
// allow-list has to survive the delegation, and no in-tree caller spells it
// this way.
func TestReadSchemaWithSchemas_ReadsTheSameSchemaAsTheContextForm(t *testing.T) {
	c := qt.New(t)
	conn := openSQLiteWithATable(c)

	withoutContext, err := dbschema.ReadSchemaWithSchemas(conn, []string{"main"})
	c.Assert(err, qt.IsNil)
	c.Assert(withoutContext.Tables, qt.HasLen, 1)

	withContext, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{"main"})
	c.Assert(err, qt.IsNil)
	c.Assert(withoutContext, qt.DeepEquals, withContext)
}
