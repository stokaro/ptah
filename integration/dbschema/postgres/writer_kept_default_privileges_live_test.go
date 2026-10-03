//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/dbtarget"
)

// A dev database's default privileges are its environment, as its extensions
// are: an image such as Supabase's grants its API roles on every table
// created in public through them (stokaro/ptah#4034). A cleanup handed the
// default privileges a claim recorded returns them to what they were: what
// the run added to a kept row, a row it created in the kept schema and a
// global row it created are taken back, and the kept rows stay.

// keptDefaultPrivilegeScope is what a dev database pinned to public keeps.
var keptDefaultPrivilegeScope = dbreset.DefaultPrivilegeScope{Schemas: []string{"public"}, Global: true}

// keptDefaultsSQL sets the defaults the dev database holds before a run, for
// the connecting role: SELECT on tables created in public, and USAGE on every
// sequence, for {role}.
const keptDefaultsSQL = `
	ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {role};
	ALTER DEFAULT PRIVILEGES GRANT USAGE ON SEQUENCES TO {role};`

// runDefaultsSQL is what a run does to them: it adds to the kept row in
// public, sets a row of its own there, takes the global grant back and sets a
// global one of its own, and creates a table.
const runDefaultsSQL = `
	CREATE TABLE public.replayed (id bigserial PRIMARY KEY);
	ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT ON TABLES TO {role};
	ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON TYPES TO {role};
	ALTER DEFAULT PRIVILEGES REVOKE USAGE ON SEQUENCES FROM {role};
	ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO {role};`

// liveDefaultPrivilegeRole creates a role for the defaults to name, dropped
// after the database the test's deferred cleanup drops.
func liveDefaultPrivilegeRole(c *qt.C, ctx context.Context) string {
	c.Helper()
	admin, err := sql.Open("pgx", requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	role := fmt.Sprintf("ptah_kept_dp_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+pgx.Identifier{role}.Sanitize())
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	return role
}

// liveDefaultACL lists every pg_default_acl row of db as schema, class and
// list, `<global>` for a global row.
func liveDefaultACL(c *qt.C, ctx context.Context, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT coalesce(n.nspname, '<global>') || ' ' || d.defaclobjtype::text || ' ' || array_to_string(d.defaclacl, ' ')
		FROM pg_default_acl d
		LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
		ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var acl []string
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		acl = append(acl, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return acl
}

// keptDefaultPrivilegeCleanups are the cleanups a dev database's resets run:
// the one a rehearsal runs on the pinned schema, and the one a replay runs on
// the whole realm.
var keptDefaultPrivilegeCleanups = []struct {
	name    string
	cleanup func(*postgres.PostgreSQLWriter, context.Context, dbreset.Kept) error
}{
	{name: "schema cleanup", cleanup: (*postgres.PostgreSQLWriter).DropAllTablesKeeping},
	{name: "realm cleanup", cleanup: (*postgres.PostgreSQLWriter).DropDatabaseRealmKeeping},
}

// TestWriterReturnsKeptDefaultPrivileges_LivePostgres records a baseline of a
// default set in public and a global one, has a run change both and add its
// own, and expects each cleanup to leave pg_default_acl as the baseline found
// it while it empties the schema.
func TestWriterReturnsKeptDefaultPrivileges_LivePostgres(t *testing.T) {
	for _, test := range keptDefaultPrivilegeCleanups {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			role := pgx.Identifier{liveDefaultPrivilegeRole(c, ctx)}.Sanitize()
			liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
			defer liveDatabase.cleanup()
			db := liveDatabase.db
			_, err := db.ExecContext(ctx, strings.ReplaceAll(keptDefaultsSQL, "{role}", role))
			c.Assert(err, qt.IsNil)
			writer := postgres.NewPostgreSQLWriter(db, "public")
			baseline, err := writer.DefaultPrivilegeBaseline(ctx, keptDefaultPrivilegeScope)
			c.Assert(err, qt.IsNil)
			before := liveDefaultACL(c, ctx, db)
			_, err = db.ExecContext(ctx, strings.ReplaceAll(runDefaultsSQL, "{role}", role))
			c.Assert(err, qt.IsNil)

			err = test.cleanup(writer, ctx, dbreset.Kept{DefaultPrivileges: baseline})

			c.Assert(err, qt.IsNil)
			c.Assert(liveDefaultACL(c, ctx, db), qt.DeepEquals, before)
			c.Assert(before, qt.HasLen, 2)
			c.Assert(postgresWriterLiveRelationCount(c, ctx, db, "public"), qt.Equals, 0)
		})
	}
}

// TestWriterRevokesDefaultPrivilegesWithoutABaseline_LivePostgres is the
// control: the same realm cleanup handed no baseline revokes the default set in
// public and returns the global one to the built-in default.
func TestWriterRevokesDefaultPrivilegesWithoutABaseline_LivePostgres(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	role := pgx.Identifier{liveDefaultPrivilegeRole(c, ctx)}.Sanitize()
	liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
	defer liveDatabase.cleanup()
	db := liveDatabase.db
	_, err := db.ExecContext(ctx, strings.ReplaceAll(keptDefaultsSQL, "{role}", role))
	c.Assert(err, qt.IsNil)

	err = postgres.NewPostgreSQLWriter(db, "public").DropDatabaseRealmKeeping(ctx, dbreset.Kept{})

	c.Assert(err, qt.IsNil)
	c.Assert(liveDefaultACL(c, ctx, db), qt.HasLen, 0)
}

// TestWriterResetObjectsLeavesOutKeptDefaultPrivileges_LivePostgres lists what
// a reset drops for the clean check. The defaults a claim keeps are not listed,
// since the reset returns them to what they are; the same defaults outside the
// kept scope are, by grantor, class and grantee.
func TestWriterResetObjectsLeavesOutKeptDefaultPrivileges_LivePostgres(t *testing.T) {
	tests := []struct {
		name string
		kept dbreset.DefaultPrivilegeScope
		// want lists the reset's objects, with "{user}" for the connecting
		// role and "{role}" for the grantee.
		want []dbreset.Object
	}{
		{name: "kept", kept: keptDefaultPrivilegeScope},
		{name: "global kept", kept: dbreset.DefaultPrivilegeScope{Global: true}, want: []dbreset.Object{
			{Kind: "default privilege", Schema: "public", Name: "{user}/r/{role}"},
		}},
		{name: "not kept", want: []dbreset.Object{
			{Kind: "default privilege", Name: "{user}/S/{role}"},
			{Kind: "default privilege", Schema: "public", Name: "{user}/r/{role}"},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			role := liveDefaultPrivilegeRole(c, ctx)
			liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
			defer liveDatabase.cleanup()
			db := liveDatabase.db
			_, err := db.ExecContext(ctx, strings.ReplaceAll(keptDefaultsSQL, "{role}", pgx.Identifier{role}.Sanitize()))
			c.Assert(err, qt.IsNil)
			var user string
			c.Assert(db.QueryRowContext(ctx, "SELECT current_user").Scan(&user), qt.IsNil)

			objects, err := postgres.NewPostgreSQLWriter(db, "public").ResetObjects(ctx, dbreset.Scope{
				Schemas: []string{"public"}, KeptDefaultPrivileges: test.kept,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(withRolesNamed(objects, user, role), qt.DeepEquals, test.want)
		})
	}
}

// withRolesNamed spells user as "{user}" and role as "{role}" in each object's
// name, so a table row can name an object whose roles depend on the server
// and on the test.
func withRolesNamed(objects []dbreset.Object, user, role string) []dbreset.Object {
	var named []dbreset.Object
	for _, object := range objects {
		object.Name = strings.NewReplacer(user+"/", "{user}/", "/"+role, "/{role}").Replace(object.Name)
		named = append(named, object)
	}
	return named
}
