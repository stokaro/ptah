//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/dbtarget"
)

// unownedPublicLiveDatabase is a database an ordinary role owns while the
// admin owns its schema public, as every new database is on PostgreSQL 13 and
// 14, and a connection to it as that role.
type unownedPublicLiveDatabase struct {
	admin     *sql.DB
	role      *sql.DB
	roleName  string
	adminName string
}

// newUnownedPublicLiveDatabase builds the PostgreSQL 13 and 14 shape on any
// server: the database belongs to an ordinary role, public to the admin, and
// PUBLIC holds CREATE on public. PostgreSQL 15 gives public to
// pg_database_owner, which the database owner holds. Everything it creates is
// removed when the test ends.
func newUnownedPublicLiveDatabase(c *qt.C, ctx context.Context, engine dbtarget.Engine) unownedPublicLiveDatabase {
	c.Helper()
	adminURL := requirePostgresWriterFamilyLiveURL(c, engine)
	liveDatabase := newPostgresWriterLiveDatabase(c, ctx, adminURL)
	c.Cleanup(liveDatabase.cleanup)
	admin := liveDatabase.db

	roleName := fmt.Sprintf("ptah_unowned_public_%d", time.Now().UnixNano())
	roleIdent := pgx.Identifier{roleName}.Sanitize()
	databaseIdent := pgx.Identifier{liveDatabase.name}.Sanitize()
	// rolePhrase is the role's password.
	const rolePhrase = "unowned_public_fixture"
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf(`
			ALTER DATABASE %[1]s OWNER TO CURRENT_USER;
			DROP OWNED BY %[2]s;
			DROP ROLE IF EXISTS %[2]s;
		`, databaseIdent, roleIdent))
		c.Check(dropErr, qt.IsNil)
	})
	_, err := admin.ExecContext(ctx, fmt.Sprintf(`
		CREATE ROLE %[1]s LOGIN PASSWORD '%[3]s' NOSUPERUSER NOCREATEDB NOCREATEROLE;
		ALTER DATABASE %[2]s OWNER TO %[1]s;
		ALTER SCHEMA public OWNER TO CURRENT_USER;
		GRANT CREATE ON SCHEMA public TO PUBLIC;
		COMMENT ON SCHEMA public IS 'claimed comment';
	`, roleIdent, databaseIdent, rolePhrase))
	c.Assert(err, qt.IsNil)
	var adminName string
	c.Assert(admin.QueryRowContext(ctx, `SELECT current_user`).Scan(&adminName), qt.IsNil)
	role := openPostgresWriterLiveDatabaseAs(c, ctx, adminURL, liveDatabase.name, roleName, rolePhrase)
	c.Cleanup(func() { c.Check(role.Close(), qt.IsNil) })
	return unownedPublicLiveDatabase{admin: admin, role: role, roleName: roleName, adminName: adminName}
}

// postgresRunObjects are what a run creates in public: a table, a view on it,
// a type and a routine, each owned by the role that ran it.
const postgresRunObjects = `
	CREATE TABLE public.notes (id bigint PRIMARY KEY);
	CREATE VIEW public.note_ids AS SELECT id FROM public.notes;
	CREATE TYPE public.mood AS ENUM ('calm');
	CREATE FUNCTION public.answer() RETURNS integer LANGUAGE sql AS 'SELECT 42';
`

// A realm cleanup through a role that owns the dev database but not its schema
// public empties public where it stands, and public keeps the owner,
// privileges and comment the claim recorded. Dropped and created again, every
// cleanup failed with `must be owner of schema public` (SQLSTATE 42501).
func TestWriterDropDatabaseRealmKeeping_LiveEmptiesAPublicTheRoleDoesNotOwn(t *testing.T) {
	tests := []postgresWriterFamilyLiveCase{
		{name: "postgres", engine: dbtarget.PostgreSQL},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			database := newUnownedPublicLiveDatabase(c, ctx, test.engine)
			claimed := readPostgresPublicSchemaState(c, ctx, database.admin)
			writer := postgres.NewPostgreSQLWriter(database.role, "public")
			states, err := writer.SchemaStates(ctx)
			c.Assert(err, qt.IsNil)
			_, err = database.role.ExecContext(ctx, postgresRunObjects)
			c.Assert(err, qt.IsNil)

			err = writer.DropDatabaseRealmKeeping(ctx, dbreset.Kept{SchemaStates: states})

			c.Assert(err, qt.IsNil)
			c.Assert(postgresWriterLiveRelationCount(c, ctx, database.admin, "public"), qt.Equals, 0)
			c.Assert(readPostgresPublicSchemaState(c, ctx, database.admin), qt.Equals, claimed)
			c.Assert(claimed.owner, qt.Equals, database.adminName)
		})
	}
}

// A role that holds a grant option on a public it does not own could change
// the schema's privileges in a run, and the cleanup, which may not drop the
// schema, could not take the change back. So the cleanup refuses before it
// drops anything, which is before the run when it is the run's first cleanup;
// and a cleanup that finds public changed by another role refuses rather than
// report it returned. Each refusal leaves what the run created in place, since
// the cleanup rolls back.
func TestWriterDropDatabaseRealmKeeping_LiveRefusesAPublicTheRoleCannotReturn(t *testing.T) {
	t.Run("the role holds a grant option on public", func(t *testing.T) {
		c := qt.New(t)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		database := newUnownedPublicLiveDatabase(c, ctx, dbtarget.PostgreSQL)
		_, err := database.admin.ExecContext(ctx, fmt.Sprintf(
			"GRANT USAGE ON SCHEMA public TO %s WITH GRANT OPTION", pgx.Identifier{database.roleName}.Sanitize(),
		))
		c.Assert(err, qt.IsNil)
		writer := postgres.NewPostgreSQLWriter(database.role, "public")
		states, err := writer.SchemaStates(ctx)
		c.Assert(err, qt.IsNil)
		_, err = database.role.ExecContext(ctx, postgresRunObjects)
		c.Assert(err, qt.IsNil)

		err = writer.DropDatabaseRealmKeeping(ctx, dbreset.Kept{SchemaStates: states})

		c.Assert(err, qt.ErrorMatches, fmt.Sprintf(
			`refusing to clean PostgreSQL database realm: schema "public" is owned by %q, not by the connecting role, `+
				`and the role holds a grant option on it; the cleanup cannot drop the schema or take back what a run `+
				`grants on it, so connect as %q or revoke the grant option`,
			database.adminName, database.adminName,
		))
		c.Assert(postgresWriterLiveRelationCount(c, ctx, database.admin, "public"), qt.Equals, 2)
	})

	t.Run("another role changed public after the claim", func(t *testing.T) {
		c := qt.New(t)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		database := newUnownedPublicLiveDatabase(c, ctx, dbtarget.PostgreSQL)
		writer := postgres.NewPostgreSQLWriter(database.role, "public")
		states, err := writer.SchemaStates(ctx)
		c.Assert(err, qt.IsNil)
		_, err = database.role.ExecContext(ctx, postgresRunObjects)
		c.Assert(err, qt.IsNil)
		_, err = database.admin.ExecContext(ctx, `COMMENT ON SCHEMA public IS 'changed during the run'`)
		c.Assert(err, qt.IsNil)

		err = writer.DropDatabaseRealmKeeping(ctx, dbreset.Kept{SchemaStates: states})

		c.Assert(err, qt.ErrorMatches, fmt.Sprintf(
			`PostgreSQL database realm cleanup cannot return schema "public" to its claimed state: %q owns it, not the connecting role`,
			database.adminName,
		))
		c.Assert(postgresWriterLiveRelationCount(c, ctx, database.admin, "public"), qt.Equals, 2)
	})
}
