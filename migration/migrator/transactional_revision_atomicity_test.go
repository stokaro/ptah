package migrator_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestTransactionalMigration_SQLiteRevisionCompletionFailureRollsBackBody(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(
		ctx,
		"sqlite://"+filepath.Join(t.TempDir(), "transactional-revision.db"),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"000001_create_users.up.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);"),
		},
		"000001_create_users.down.sql": {
			Data: []byte("DROP TABLE users;"),
		},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(mig.Initialize(ctx), qt.IsNil)
	_, err = conn.ExecContext(ctx, `
		CREATE TRIGGER reject_applied_revision
		BEFORE UPDATE OF state ON schema_migrations
		WHEN NEW.state = 'applied'
		BEGIN
			SELECT RAISE(ABORT, 'reject applied revision');
		END;
	`)
	c.Assert(err, qt.IsNil)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorMatches, `(?s)failed to record migration 1: .*reject applied revision.*`)
	var tableCount int64
	c.Assert(conn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'users'").Scan(&tableCount), qt.IsNil)
	c.Assert(tableCount, qt.Equals, int64(0))
	var revisionState string
	c.Assert(conn.QueryRow("SELECT state FROM schema_migrations WHERE version = 1").Scan(&revisionState), qt.IsNil)
	c.Assert(revisionState, qt.Equals, "failed")
}
