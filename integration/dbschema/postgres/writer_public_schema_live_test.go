//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/dbtarget"
)

// postgresPublicSchemaState is what a role that connects after a cleanup sees
// of "public": its owner, its grants and its comment.
type postgresPublicSchemaState struct {
	owner   string
	grants  string
	comment string
}

// readPostgresPublicSchemaState reads "public" as the catalog holds it. The
// grants are one line per grantee, privilege and grant option, sorted, so two
// ACLs that grant the same privileges compare equal however they were built.
func readPostgresPublicSchemaState(c *qt.C, ctx context.Context, db *sql.DB) postgresPublicSchemaState {
	c.Helper()
	var state postgresPublicSchemaState
	c.Assert(db.QueryRowContext(ctx, `
		SELECT pg_get_userbyid(n.nspowner),
		       coalesce((
		           SELECT string_agg(g.line, ', ' ORDER BY g.line)
		           FROM (
		               SELECT CASE a.grantee WHEN 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END
		                   || ' ' || a.privilege_type || ' ' || a.is_grantable::text AS line
		               FROM aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) a
		           ) g
		       ), ''),
		       coalesce(obj_description(n.oid, 'pg_namespace'), '')
		FROM pg_namespace n
		WHERE n.nspname = 'public'`).Scan(&state.owner, &state.grants, &state.comment), qt.IsNil)
	return state
}

// A cleanup whose root is another schema drops "public" with the rest of the
// realm and brings it back as it was. Brought back bare, "public" was owned by
// the connecting role and PUBLIC lost USAGE on it, so every role created
// afterwards found no usable schema in its default search path
// (stokaro/ptah#3657). The first row is the "public" PostgreSQL creates; the
// second has been changed from that, and the change survives too.
func TestWriterDropDatabaseRealm_LivePostgresRestoresPublicAsItWas(t *testing.T) {
	tests := []struct {
		name  string
		setup string
	}{
		{
			name:  "the public schema PostgreSQL creates",
			setup: `CREATE SCHEMA app; CREATE TABLE public.stale (id integer)`,
		},
		{
			name: "a public schema with its own comment and grant",
			setup: `CREATE SCHEMA app; CREATE TABLE public.stale (id integer);
				COMMENT ON SCHEMA public IS 'shared scratch';
				GRANT CREATE ON SCHEMA public TO PUBLIC`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			liveDatabase := newPostgresWriterLiveDatabase(c, ctx, requirePostgresWriterFamilyLiveURL(c, dbtarget.PostgreSQL))
			defer liveDatabase.cleanup()
			db := liveDatabase.db
			_, err := db.ExecContext(ctx, test.setup)
			c.Assert(err, qt.IsNil)
			before := readPostgresPublicSchemaState(c, ctx, db)

			err = postgres.NewPostgreSQLWriter(db, "app").DropDatabaseRealm(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(postgresWriterLiveRelationCount(c, ctx, db, "public"), qt.Equals, 0)
			c.Assert(readPostgresPublicSchemaState(c, ctx, db), qt.Equals, before)
		})
	}
}
