//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/dbtarget"
)

// A foreign server somebody created still stops a realm cleanup, on every
// engine of the family that has foreign servers (stokaro/ptah#3693). The
// cleanup leaves out the objects the server makes at initdb, which is what lets
// it clean a YugabyteDB 2026.1 database holding the built-in
// yb_global_views_server; this is the control that the rule did not also leave
// out the objects it exists to refuse.
func TestWriterDropDatabaseRealm_LiveRefusesAForeignServerSomebodyCreated(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	tests := []postgresWriterFamilyLiveCase{
		{name: "postgres", engine: dbtarget.PostgreSQL},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, test.engine))
			defer liveDatabase.cleanup()
			db := liveDatabase.db
			_, err := db.ExecContext(ctx, `
				CREATE EXTENSION IF NOT EXISTS postgres_fdw;
				CREATE SERVER ptah_elsewhere FOREIGN DATA WRAPPER postgres_fdw;
			`)
			c.Assert(err, qt.IsNil)

			err = postgres.NewPostgreSQLWriter(db, "public").DropDatabaseRealm(ctx)

			c.Assert(err, qt.ErrorMatches,
				`refusing to clean PostgreSQL database realm with unsupported database-scoped foreign server "ptah_elsewhere"`)
		})
	}
}
