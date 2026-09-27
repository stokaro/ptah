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
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/dbtarget"
)

// TestDefaultPrivilegeACL_LivePostgresFamilyReadsAndRevokes sets default
// privileges with the engine's own statements, reads them back through the
// reader, and cleans the schema through the writer.
//
// Every engine here stores the ACL in its own form, and the reader and the
// cleanup read it the same way on all of them. CockroachDB v25.4.16 stores
// defaclacl as text[], where aclexplode answers no rows: a read built on it
// describes no default privilege, leaves out a role granted one only by a
// default, and a cleanup built on it revokes nothing (stokaro/ptah#3802). CI runs one CockroachDB line; the others were measured
// by hand, v25.4.16, v26.2.7 and v26.3.1 among them.
//
// The expected rows are the grants the test made, and the cleanup is judged by
// what pg_default_acl still holds for the schema, which does not depend on how
// Ptah explodes the ACL.
func TestDefaultPrivilegeACL_LivePostgresFamilyReadsAndRevokes(t *testing.T) {
	tests := []postgresWriterFamilyLiveCase{
		{name: "postgres", engine: dbtarget.PostgreSQL},
		{name: "cockroachdb", engine: dbtarget.CockroachDB},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
			defer cancel()
			fixture := newDefaultACLFixture(c, ctx, test.engine)

			conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, test.engine))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{fixture.schema})

			c.Assert(err, qt.IsNil)
			c.Assert(live.DefaultPrivileges, qt.DeepEquals, []catalog.DefaultPrivilege{
				{
					Grantor: fixture.owner, Schema: fixture.schema, ObjectType: "SEQUENCES",
					Grantee: fixture.reader, Privilege: "USAGE", WithOption: true,
				},
				{
					Grantor: fixture.owner, Schema: fixture.schema, ObjectType: "TABLES",
					Grantee: "PUBLIC", Privilege: "SELECT",
				},
				{
					Grantor: fixture.owner, Schema: fixture.schema, ObjectType: "TABLES",
					Grantee: fixture.reader, Privilege: "INSERT",
				},
				{
					Grantor: fixture.owner, Schema: fixture.schema, ObjectType: "TABLES",
					Grantee: fixture.reader, Privilege: "SELECT",
				},
			})
			c.Assert(defaultACLRoleNames(live.Roles), qt.DeepEquals, []string{fixture.owner, fixture.reader})

			err = postgres.NewPostgreSQLWriter(fixture.db, fixture.schema).DropAllTables(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(fixture.defaultACLRows(c, ctx), qt.Equals, 0)
		})
	}
}

// defaultACLFixture is a schema holding default privileges one role set for
// another and for PUBLIC, on a connection to the engine under test.
type defaultACLFixture struct {
	db     *sql.DB
	schema string
	owner  string
	reader string
}

// newDefaultACLFixture creates the roles and the schema, grants the defaults
// the test reads back, and registers their removal.
//
// Every name is unique to the run, because the engines are shared servers and
// roles are cluster-wide. The names need no quoting: CockroachDB v26.2.7
// cannot read pg_default_acl at all while it holds a name that does
// (stokaro/ptah#3816).
func newDefaultACLFixture(c *qt.C, ctx context.Context, engine dbtarget.Engine) defaultACLFixture {
	c.Helper()
	db, err := sql.Open("pgx", requirePostgresWriterFamilyLiveURL(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	c.Assert(db.PingContext(ctx), qt.IsNil)

	stamp := time.Now().UnixNano()
	fixture := defaultACLFixture{
		db:     db,
		schema: fmt.Sprintf("ptah_defacl_%d", stamp),
		owner:  fmt.Sprintf("ptah_defacl_owner_%d", stamp),
		reader: fmt.Sprintf("ptah_defacl_reader_%d", stamp),
	}
	schema := pgx.Identifier{fixture.schema}.Sanitize()
	owner := pgx.Identifier{fixture.owner}.Sanitize()
	reader := pgx.Identifier{fixture.reader}.Sanitize()
	prefix := "ALTER DEFAULT PRIVILEGES FOR ROLE " + owner + " IN SCHEMA " + schema
	c.Cleanup(func() {
		// The schema goes first: its default privileges go with it, and a role
		// a default privilege still names cannot be dropped.
		for _, statement := range []string{
			"DROP SCHEMA IF EXISTS " + schema + " CASCADE",
			"DROP ROLE IF EXISTS " + reader,
			"DROP ROLE IF EXISTS " + owner,
		} {
			_, cleanupErr := db.ExecContext(context.Background(), statement)
			c.Check(cleanupErr, qt.IsNil, qt.Commentf("statement: %s", statement))
		}
	})
	for _, statement := range []string{
		"CREATE ROLE " + owner,
		"CREATE ROLE " + reader,
		"CREATE SCHEMA " + schema,
		prefix + " GRANT SELECT, INSERT ON TABLES TO " + reader,
		prefix + " GRANT USAGE ON SEQUENCES TO " + reader + " WITH GRANT OPTION",
		prefix + " GRANT SELECT ON TABLES TO PUBLIC",
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return fixture
}

// defaultACLRows counts the pg_default_acl rows the fixture schema still has.
func (f defaultACLFixture) defaultACLRows(c *qt.C, ctx context.Context) int {
	c.Helper()
	var rows int
	err := f.db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_default_acl d
		JOIN pg_namespace n ON n.oid = d.defaclnamespace
		WHERE n.nspname = $1`,
		f.schema,
	).Scan(&rows)
	c.Assert(err, qt.IsNil)
	return rows
}

// defaultACLRoleNames lists the names of roles, in the order the read gives
// them.
func defaultACLRoleNames(roles []catalog.Role) []string {
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		names = append(names, role.Name)
	}
	return names
}
