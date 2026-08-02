package migrator_test

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestNoTransactionCancellation_RecordsCommittedProgress(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	conn, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+filepath.Join(t.TempDir(), "no-transaction-cancellation.db"),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	observer := migrator.StatementObserverFunc(func(context.Context, migrator.StatementEvent) error {
		cancel()
		return context.Canceled
	})
	provider, err := migrator.NewFSMigrationProvider(
		fstest.MapFS{
			"000001_create_users.up.sql": {
				Data: []byte("-- +ptah no_transaction\nCREATE TABLE users (id INTEGER PRIMARY KEY);\nINSERT INTO users (id) VALUES (1);"),
			},
			"000001_create_users.down.sql": {
				Data: []byte("DROP TABLE users;"),
			},
		},
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	mig := migrator.NewMigrator(conn, provider)

	err = mig.MigrateUp(ctx)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	var state string
	var applied, total int
	c.Assert(
		conn.QueryRow("SELECT state, applied, total FROM schema_migrations WHERE version = 1").Scan(&state, &applied, &total),
		qt.IsNil,
	)
	c.Assert(state, qt.Equals, "failed")
	c.Assert(applied, qt.Equals, 1)
	c.Assert(total, qt.Equals, 2)
}

func TestNoTransactionCancellation_AfterFinalUpStatementCompletesRevision(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	conn, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+filepath.Join(t.TempDir(), "no-transaction-final-up.db"),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	observer := migrator.StatementObserverFunc(func(context.Context, migrator.StatementEvent) error {
		cancel()
		return nil
	})
	provider, err := migrator.NewFSMigrationProvider(
		fstest.MapFS{
			"000001_create_users.up.sql": {
				Data: []byte("-- +ptah no_transaction\nCREATE TABLE users (id INTEGER PRIMARY KEY);"),
			},
			"000001_create_users.down.sql": {
				Data: []byte("DROP TABLE users;"),
			},
		},
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	mig := migrator.NewMigrator(conn, provider)

	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsTrue)
	c.Assert(noTransactionRevisionState(c, conn), qt.Equals, "applied")
}

func TestNoTransactionCancellation_AfterFinalDownStatementDeletesRevision(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(
		t.Context(),
		"sqlite://"+filepath.Join(t.TempDir(), "no-transaction-final-down.db"),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	fsys := fstest.MapFS{
		"000001_create_users.up.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);"),
		},
		"000001_create_users.down.sql": {
			Data: []byte("-- +ptah no_transaction\nDROP TABLE users;"),
		},
	}
	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(migrator.NewMigrator(conn, provider).MigrateUp(t.Context()), qt.IsNil)

	ctx, cancel := context.WithCancel(t.Context())
	observer := migrator.StatementObserverFunc(func(context.Context, migrator.StatementEvent) error {
		cancel()
		return nil
	})
	provider, err = migrator.NewFSMigrationProvider(fsys, migrator.WithStatementObserver(observer))
	c.Assert(err, qt.IsNil)
	mig := migrator.NewMigrator(conn, provider)

	c.Assert(mig.MigrateDownTo(ctx, 0), qt.IsNil)
	c.Assert(noTransactionTableExists(c, conn, "users"), qt.IsFalse)
	c.Assert(noTransactionRevisionCount(c, conn), qt.Equals, int64(0))
}
