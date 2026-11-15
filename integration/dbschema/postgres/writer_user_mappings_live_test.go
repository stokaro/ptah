//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/dbtarget"
)

// An ordinary role lists the user mappings of the database it is connected to,
// its own, PUBLIC's and another role's alike. The catalog pg_user_mapping is
// readable by a superuser only, so a listing that reads it fails with SQLSTATE
// 42501 for any other role, and with it the claim of a dev database such a
// role owns. The rows are compared without their order, which follows the
// database's collation.
func TestWriterDatabaseScopedArtifacts_LiveListsUserMappingsToAnOrdinaryRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	tests := []postgresWriterFamilyLiveCase{
		{name: "postgres", engine: dbtarget.PostgreSQL},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			adminURL := requirePostgresWriterFamilyLiveURL(c, test.engine)
			liveDatabase := newPostgresWriterLiveDatabase(c, ctx, adminURL)
			defer liveDatabase.cleanup()
			admin := liveDatabase.db

			suffix := time.Now().UnixNano()
			reader := fmt.Sprintf("ptah_mapping_reader_%d", suffix)
			other := fmt.Sprintf("ptah_mapping_other_%d", suffix)
			readerIdent := pgx.Identifier{reader}.Sanitize()
			otherIdent := pgx.Identifier{other}.Sanitize()
			const password = "mapping_reader_fixture"
			defer func() {
				_, dropErr := admin.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf(`
					DROP SERVER IF EXISTS ptah_mapped CASCADE;
					DROP ROLE IF EXISTS %s;
					DROP ROLE IF EXISTS %s;
				`, readerIdent, otherIdent))
				c.Check(dropErr, qt.IsNil)
			}()
			_, err := admin.ExecContext(ctx, fmt.Sprintf(`
				CREATE ROLE %[1]s LOGIN PASSWORD '%[3]s' NOSUPERUSER NOCREATEDB NOCREATEROLE;
				CREATE ROLE %[2]s NOLOGIN;
				CREATE EXTENSION IF NOT EXISTS postgres_fdw;
				CREATE SERVER ptah_mapped FOREIGN DATA WRAPPER postgres_fdw;
				CREATE USER MAPPING FOR PUBLIC SERVER ptah_mapped;
				CREATE USER MAPPING FOR %[1]s SERVER ptah_mapped
					OPTIONS (user 'remote', password 'remote_fixture');
				CREATE USER MAPPING FOR %[2]s SERVER ptah_mapped;
			`, readerIdent, otherIdent, password))
			c.Assert(err, qt.IsNil)
			readerDB := openPostgresWriterLiveDatabaseAs(c, ctx, adminURL, liveDatabase.name, reader, password)
			defer func() { c.Check(readerDB.Close(), qt.IsNil) }()

			got, err := postgres.NewPostgreSQLWriter(readerDB, "public").DatabaseScopedArtifacts(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.ContentEquals, []dbreset.Object{
				{Kind: "foreign server", Name: "ptah_mapped"},
				{Kind: "user mapping", Name: "PUBLIC@ptah_mapped"},
				{Kind: "user mapping", Name: reader + "@ptah_mapped"},
				{Kind: "user mapping", Name: other + "@ptah_mapped"},
			})
		})
	}
}

// openPostgresWriterLiveDatabaseAs connects to database on the server rawURL
// names, as role with password.
func openPostgresWriterLiveDatabaseAs(
	c *qt.C,
	ctx context.Context,
	rawURL, database, role, password string,
) *sql.DB {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	parsed.RawPath = ""
	parsed.User = url.UserPassword(role, password)
	db, err := sql.Open("pgx", parsed.String())
	c.Assert(err, qt.IsNil)
	c.Assert(db.PingContext(ctx), qt.IsNil)
	return db
}
