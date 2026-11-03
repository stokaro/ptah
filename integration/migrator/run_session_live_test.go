//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// An up run executes every migration body on one session, on PostgreSQL as
// on any engine, so the search_path one migration sets decides where the next
// one's unqualified table lands (stokaro/ptah#5034). The revision table is
// qualified with the connection's schema while the run's setting is in
// effect, so the second migration records its own revision in a transaction
// that runs after `SET search_path`, and the third records its revision on
// the same session.

// searchPathRun is the three migrations, with the schema and the tables they
// create suffixed so runs on the shared database do not collide.
type searchPathRun struct {
	conn   *dbschema.DatabaseConnection
	suffix string
	table  string
	fsys   fstest.MapFS
}

func newSearchPathRun(c *qt.C) *searchPathRun {
	c.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	run := &searchPathRun{conn: conn, suffix: suffix, table: "schema_migrations_runsession_" + suffix}
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "DROP SCHEMA IF EXISTS rs_"+suffix+" CASCADE")
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS rs_t3_"+suffix)
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "DROP TABLE IF EXISTS "+run.table)
		dbschema.CloseAndWarn(conn)
	})
	run.fsys = fstest.MapFS{
		"0000000001_init.up.sql":   {Data: fmt.Appendf(nil, "CREATE SCHEMA rs_%s;\n", suffix)},
		"0000000001_init.down.sql": {Data: fmt.Appendf(nil, "DROP SCHEMA rs_%s CASCADE;\n", suffix)},
		"0000000002_set.up.sql": {Data: fmt.Appendf(nil,
			"SET search_path = rs_%[1]s;\nCREATE TABLE rs_t2_%[1]s (id int);\n", suffix)},
		"0000000002_set.down.sql":  {Data: []byte("SELECT 1;\n")},
		"0000000003_next.up.sql":   {Data: fmt.Appendf(nil, "CREATE TABLE rs_t3_%s (id int);\n", suffix)},
		"0000000003_next.down.sql": {Data: []byte("SELECT 1;\n")},
	}
	return run
}

// schemaOf returns the schema of the run's table with prefix, or the empty
// string when there is none.
func (r *searchPathRun) schemaOf(c *qt.C, prefix string) string {
	c.Helper()
	var schema string
	c.Assert(r.conn.QueryRowContext(context.Background(),
		"SELECT COALESCE(max(schemaname), '') FROM pg_tables WHERE tablename = $1", prefix+r.suffix,
	).Scan(&schema), qt.IsNil)
	return schema
}

// applied returns the versions the run's revision table records as applied.
func (r *searchPathRun) applied(c *qt.C) []int64 {
	c.Helper()
	rows, err := r.conn.QueryContext(context.Background(),
		"SELECT version FROM public."+r.table+" WHERE state = 'applied' ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var versions []int64
	for rows.Next() {
		var version int64
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

func TestMigrateUpRunSession_HappyPathLive(t *testing.T) {
	for _, txMode := range []migrator.MigrationTxMode{migrator.MigrationTxModeFile, migrator.MigrationTxModeNone} {
		t.Run(string(txMode), func(t *testing.T) {
			c := qt.New(t)
			run := newSearchPathRun(c)
			m, err := migrator.NewFSMigrator(run.conn, run.fsys)
			c.Assert(err, qt.IsNil)

			err = m.WithMigrationsTable("", run.table).WithTransactionMode(txMode).MigrateUp(context.Background())

			c.Assert(err, qt.IsNil)
			c.Assert(run.schemaOf(c, "rs_t2_"), qt.Equals, "rs_"+run.suffix)
			c.Assert(run.schemaOf(c, "rs_t3_"), qt.Equals, "rs_"+run.suffix)
			c.Assert(run.applied(c), qt.DeepEquals, []int64{1, 2, 3})
		})
	}
}
