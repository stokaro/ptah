package migrator_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// redactedRefusal rewrites the text of the error it wraps, the way the
// PostgreSQL writer takes a role password out of the server's message. The
// error it wraps still quotes the password.
type redactedRefusal struct {
	err  error
	text string
}

func (e redactedRefusal) Error() string { return e.text }
func (e redactedRefusal) Unwrap() error { return e.err }

// redactingSession is a provider session whose engine refuses every statement
// with a message that quotes a password, and whose writer takes the password
// out of the error it returns.
type redactingSession struct {
	*metadataSession
}

func (s *redactingSession) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return s, nil
}
func (s *redactingSession) OpenSession(context.Context) (dbschema.NativeSession, error) {
	return s, nil
}
func (s *redactingSession) Writer() catalog.SchemaWriter { return s }
func (s *redactingSession) ExecuteSQL(_ context.Context, statement string, _ ...any) error {
	server := errors.New(`ERROR: syntax error at or near "E'hunter2zz'" (SQLSTATE 42601)`)
	return fmt.Errorf("SQL execution failed: %w\nSQL: %s", redactedRefusal{
		err:  server,
		text: `ERROR: syntax error at or near "E'[secret]'" (SQLSTATE 42601)`,
	}, statement)
}

// The Atlas-shaped `error` column records the database's message without the
// context Ptah adds, and a writer's redaction is part of that message rather
// than context: stepping past it records the password the redaction took out.
func TestMigrateUp_AtlasRevisionErrorKeepsARedaction(t *testing.T) {
	c := qt.New(t)
	owner := &redactingSession{metadataSession: &metadataSession{}}
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{"redacting": {Target: "sqlite", Connector: owner}})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "redacting://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	migration := migrator.CreateMigrationFromSQL(1, "role", "ALTER ROLE app SET PASSWORD $PTAH_SECRET_PASSWORD_APP;", "")
	runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("main", "_revisions").
		WithTransactionMode(migrator.MigrationTxModeNone).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithoutMigrationLock()

	c.Assert(runner.MigrateUp(c.Context()), qt.ErrorMatches, `(?s).*E'\[secret\]'.*`)

	c.Assert(owner.history, qt.HasLen, 1)
	c.Assert(owner.history[0].Error, qt.Equals, `ERROR: syntax error at or near "E'[secret]'" (SQLSTATE 42601)`)
}
