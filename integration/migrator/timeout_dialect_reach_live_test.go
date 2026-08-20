//go:build integration

package migrator_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestMigrateUp_CockroachDBHonorsPerMigrationTimeouts is the reach the dialect
// switch used to deny.
//
// `timeoutStatements` handled three dialect names, so CockroachDB answered
// `migration timeouts are not supported for dialect "cockroachdb"` -- while the
// server takes `SET LOCAL statement_timeout` and `SET LOCAL lock_timeout`
// exactly as PostgreSQL does, measured on v25.4.0. It speaks the PostgreSQL
// wire protocol, so what refused it was Ptah's switch rather than the engine,
// on one of the two deployments where a long lock hurts most
// (stokaro/ptah#1713).
//
// The assertion is that the migration APPLIES. A timeout the server rejects
// fails the statement, so a run that reaches the end is a run whose timeout
// statements the server accepted.
func TestMigrateUp_CockroachDBHonorsPerMigrationTimeouts(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.CockroachDB)
	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS schema_migrations")
	_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS ptah_crdb_timeout_items")
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS schema_migrations")
		_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS ptah_crdb_timeout_items")
	}()

	fsys := fstest.MapFS{
		"0000000001_create_items.up.sql": &fstest.MapFile{
			Data: []byte("-- +ptah lock_timeout=5s\n" +
				"-- +ptah statement_timeout=30s\n" +
				"CREATE TABLE ptah_crdb_timeout_items (id INTEGER PRIMARY KEY);"),
		},
		"0000000001_create_items.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE ptah_crdb_timeout_items;"),
		},
	}

	mig, err := migrator.NewFSMigrator(conn, fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(mig.MigrateUp(ctx), qt.IsNil)

	var tables int
	err = conn.QueryRowContext(ctx,
		"SELECT count(*) FROM information_schema.tables WHERE table_name = 'ptah_crdb_timeout_items'").Scan(&tables)
	c.Assert(err, qt.IsNil)
	c.Assert(tables, qt.Equals, 1)
}
