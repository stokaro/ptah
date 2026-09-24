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
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// A migration that runs outside a transaction runs each statement in an
// implicit transaction of its own, where `SET LOCAL` is a warning that changes
// nothing. So the timeouts are asked for where the statements run: each body
// below records the settings its own session reports, and the expected value
// is the server's rendering of what the run asked for. A timeout that never
// reached the session reads back as 0 (stokaro/ptah#3501).
//
// Both directions, because the rollback of a no_transaction migration runs on
// a session of its own.
func TestNoTransactionMigrationRunsUnderItsTimeouts(t *testing.T) {
	tests := []struct {
		name          string
		engine        dbtarget.Engine
		wantLock      string
		wantStatement string
	}{
		{name: "postgresql", engine: dbtarget.PostgreSQL, wantLock: "1500ms", wantStatement: "45s"},
		// CockroachDB reports both settings in milliseconds without a unit.
		{name: "cockroachdb", engine: dbtarget.CockroachDB, wantLock: "1500", wantStatement: "45000"},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB, wantLock: "1500ms", wantStatement: "45s"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, test.engine))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			suffix := time.Now().UnixNano()
			migrationsTable := fmt.Sprintf("schema_migrations_nt_timeouts_%d", suffix)
			readings := fmt.Sprintf("ptah_nt_timeout_readings_%d", suffix)
			probe := fmt.Sprintf("ptah_nt_timeout_probe_%d", suffix)
			_, err = conn.ExecContext(ctx, "CREATE TABLE "+readings+
				" (direction TEXT NOT NULL, setting TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (direction, setting))")
			c.Assert(err, qt.IsNil)
			defer func() {
				for _, table := range []string{probe, readings, migrationsTable} {
					_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table)
				}
			}()

			mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
				"0000000001_record_timeouts.up.sql": {Data: fmt.Appendf(nil,
					"-- +ptah no_transaction\n"+
						"CREATE TABLE %[1]s (id INTEGER PRIMARY KEY);\n"+
						"INSERT INTO %[2]s VALUES ('up', 'lock_timeout', current_setting('lock_timeout'));\n"+
						"INSERT INTO %[2]s VALUES ('up', 'statement_timeout', current_setting('statement_timeout'));\n",
					probe, readings,
				)},
				"0000000001_record_timeouts.down.sql": {Data: fmt.Appendf(nil,
					"-- +ptah no_transaction\n"+
						"INSERT INTO %[2]s VALUES ('down', 'lock_timeout', current_setting('lock_timeout'));\n"+
						"INSERT INTO %[2]s VALUES ('down', 'statement_timeout', current_setting('statement_timeout'));\n"+
						"DROP TABLE %[1]s;\n",
					probe, readings,
				)},
			})
			c.Assert(err, qt.IsNil)
			mig = mig.WithMigrationsTable("", migrationsTable).WithDefaultTimeouts(migrationfile.Timeouts{
				LockTimeout:         1500 * time.Millisecond,
				StatementTimeout:    45 * time.Second,
				HasLockTimeout:      true,
				HasStatementTimeout: true,
			})

			c.Assert(mig.MigrateUp(ctx), qt.IsNil)
			c.Assert(mig.MigrateDownTo(ctx, 0), qt.IsNil)

			got := make(map[string]string)
			rows, err := conn.QueryContext(ctx, "SELECT direction || ' ' || setting, value FROM "+readings)
			c.Assert(err, qt.IsNil)
			defer rows.Close()
			for rows.Next() {
				var key, value string
				c.Assert(rows.Scan(&key, &value), qt.IsNil)
				got[key] = value
			}
			c.Assert(rows.Err(), qt.IsNil)
			c.Assert(got, qt.DeepEquals, map[string]string{
				"up lock_timeout":        test.wantLock,
				"up statement_timeout":   test.wantStatement,
				"down lock_timeout":      test.wantLock,
				"down statement_timeout": test.wantStatement,
			})
		})
	}
}
