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

// The run document names the statement a migration failed at and the code the
// server answered with, and nothing the server said in words. The codes come
// from the drivers' own error types, which only a live server produces: a
// fabricated driver error would measure the reading and not the chain a real
// failure builds on the way up.
func TestRunResultCarriesTheServerCodeLive(t *testing.T) {
	mysqlURL := func(t *testing.T) string {
		return mySQLFamilyTestURL(t, "mysql", dbtarget.MySQL)
	}
	// Each transaction mode reaches the statement through its own executor:
	// one transaction per file, a statement in flight marked outside any
	// transaction, and the batch that records the failure after its rollback.
	tests := []struct {
		name        string
		url         func(t *testing.T) string
		header      string
		txMode      migrator.MigrationTxMode
		sqlState    string
		errorNumber int
	}{
		{
			name:     "postgresql",
			url:      postgresTestURL,
			txMode:   migrator.MigrationTxModeFile,
			sqlState: "42P01",
		},
		{
			name:     "postgresql without a transaction",
			url:      postgresTestURL,
			header:   "-- +ptah no_transaction\n",
			txMode:   migrator.MigrationTxModeFile,
			sqlState: "42P01",
		},
		{
			name:     "postgresql in one transaction for the batch",
			url:      postgresTestURL,
			txMode:   migrator.MigrationTxModeAll,
			sqlState: "42P01",
		},
		{
			name:        "mysql",
			url:         mysqlURL,
			txMode:      migrator.MigrationTxModeFile,
			sqlState:    "42S02",
			errorNumber: 1146,
		},
		{
			name:        "mysql without a transaction",
			url:         mysqlURL,
			header:      "-- +ptah no_transaction\n",
			txMode:      migrator.MigrationTxModeFile,
			sqlState:    "42S02",
			errorNumber: 1146,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			conn, err := dbschema.ConnectToDatabase(ctx, test.url(t))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			suffix := time.Now().UnixNano()
			migrationsTable := fmt.Sprintf("schema_migrations_run_failure_%d", suffix)
			createdTable := fmt.Sprintf("ptah_run_failure_%d", suffix)
			defer func() {
				_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+createdTable)
				_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+migrationsTable)
			}()

			// The literal is what the run document must not repeat: the
			// server's message names the missing table, and the error field
			// quotes it.
			provider, err := migrator.NewFSMigrationProvider(fstest.MapFS{
				"000001_create.up.sql": {Data: fmt.Appendf(nil,
					"%sCREATE TABLE %s (id INT PRIMARY KEY);\nINSERT INTO %s_absent (id) VALUES (42);\n",
					test.header, createdTable, createdTable,
				)},
				"000001_create.down.sql": {Data: fmt.Appendf(nil, "DROP TABLE %s;\n", createdTable)},
			})
			c.Assert(err, qt.IsNil)
			m := migrator.NewMigrator(conn, provider).
				WithMigrationsTable("", migrationsTable).
				WithTransactionMode(test.txMode)
			c.Assert(m.Initialize(ctx), qt.IsNil)

			runErr := m.MigrateUp(ctx)
			c.Assert(runErr, qt.IsNotNil)
			after, err := m.GetMigrationStatus(ctx)
			c.Assert(err, qt.IsNil)

			result := migrator.NewRunResult(migrator.RunEvidence{
				Plan:  &migrator.MigrationPlan{Versions: []int64{1}},
				After: after,
				Err:   runErr,
			})

			c.Assert(result.Failure, qt.DeepEquals, &migrator.RunFailure{
				Version:     1,
				VersionKey:  "000001",
				Statement:   2,
				Statements:  2,
				SQLState:    test.sqlState,
				ErrorNumber: test.errorNumber,
			}, qt.Commentf("error: %s", result.Error))
			// The control: the message still quotes the server, so the object
			// carries the code alone by construction and not because the
			// server said nothing.
			c.Assert(result.Error, qt.Contains, createdTable+"_absent")
		})
	}
}
