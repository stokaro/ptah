package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// A migration's statements reach the server with the comments inside them,
// as the pinned community binary sends them, and without the comments before
// their first token, which are the file's notes to its reader. MySQL and SQL
// Server keep a trigger's or a routine's body as it arrives: measured on
// MySQL 8.4, a body written as `SET NEW.a = 1 -- note` was stored as
// `SET NEW.a = 1` when every comment was taken out, and the next
// `migrations generate` planned the trigger again because the declaration
// still had the comment.
func TestMigrateUpStatementComments_HappyPath(t *testing.T) {
	c := qt.New(t)
	recorder := newSessionRecorder()
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{
		"recorder": {Target: "postgres", Connector: recorder},
	})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "recorder://database")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	body := `-- the file's note
CREATE TABLE t (id int); -- a note after the semicolon
CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$ -- the body's note
;
-- a statement that is only a comment;
/* a block note */ CREATE INDEX i ON t /* an inner note */ (id);
`
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(
		migrator.CreateMigrationFromSQL(1, "comments", body, "DROP TABLE t;"),
	)).WithMigrationsTable("public", "_revisions").WithTransactionMode(migrator.MigrationTxModeNone).
		WithoutMigrationLock()

	err = m.MigrateUp(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(recorder.trail.statementsOn(recorder.trail.lastSession()), qt.DeepEquals, []string{
		"CREATE TABLE t (id int)",
		"CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$ -- the body's note",
		"CREATE INDEX i ON t /* an inner note */ (id)",
	})
}
