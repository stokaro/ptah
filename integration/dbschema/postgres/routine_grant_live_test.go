//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestPostgresRoutineGrant_SchemaFileRevokesWhatNobodyGranted drives the shape
// mosamlife/wpmgr's schema file carries through a server: a SECURITY DEFINER
// function whose EXECUTE is taken from PUBLIC and given to one role, and a
// table revoke that takes back what ALTER DEFAULT PRIVILEGES handed the role.
//
// It needs a server because both privileges are held without a GRANT the file
// wrote. PostgreSQL gives PUBLIC EXECUTE when CREATE FUNCTION runs and gives
// the role its table privileges when CREATE TABLE runs, so the plan is right
// only if it revokes them after creating the objects, and the second
// comparison is empty only if the catalog read reports the privileges the way
// the comparison keys them. The expected values are read back from the server
// with has_function_privilege and has_table_privilege.
func TestPostgresRoutineGrant_SchemaFileRevokesWhatNobodyGranted(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
	defer cancel()
	fixture := prepareRoutineGrantFixture(c, ctx)

	desired, _, err := sqlschema.Read([]byte(fixture.schemaFile()), platform.Postgres)
	c.Assert(err, qt.IsNil)

	created := fixture.apply(c, ctx, &desired)
	c.Assert(created.GrantsRemoved, qt.HasLen, 4, qt.Commentf("revokes planned: %#v", created.GrantsRemoved))
	c.Assert(fixture.privileges(c, ctx), qt.DeepEquals, routineGrantPrivileges{
		PublicExecutes: false, RoleExecutes: true, RoleSelects: true, RoleInserts: false, RoleDeletes: false,
	})

	settled := fixture.compare(c, ctx, &desired)
	c.Assert(settled.GrantsAdded, qt.HasLen, 0, qt.Commentf("grants added: %#v", settled.GrantsAdded))
	c.Assert(settled.GrantsRemoved, qt.HasLen, 0, qt.Commentf("grants removed: %#v", settled.GrantsRemoved))

	// Drift: someone gives both privileges back by hand. The same file plans
	// the two revokes and nothing else.
	fixture.exec(c, ctx,
		"GRANT EXECUTE ON FUNCTION "+fixture.schema+".purge(uuid) TO PUBLIC",
		"GRANT DELETE ON "+fixture.schema+".signatures TO "+fixture.role,
	)
	drifted := fixture.apply(c, ctx, &desired)
	c.Assert(drifted.GrantsRemoved, qt.HasLen, 2, qt.Commentf("revokes planned: %#v", drifted.GrantsRemoved))
	c.Assert(drifted.GrantsAdded, qt.HasLen, 0)
	c.Assert(fixture.privileges(c, ctx), qt.DeepEquals, routineGrantPrivileges{
		PublicExecutes: false, RoleExecutes: true, RoleSelects: true, RoleInserts: false, RoleDeletes: false,
	})
}

// TestPostgresRoutineGrant_AnExistingFunctionLosesTheImplicitPrivilege is the
// case the plan cannot reach by creating the function: the function is already
// there, created without a GRANT, so its ACL is the default one nobody wrote.
// The revoke is planned only if the catalog read reports PUBLIC's EXECUTE on
// it, which pg_proc.proacl leaves NULL to mean.
func TestPostgresRoutineGrant_AnExistingFunctionLosesTheImplicitPrivilege(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
	defer cancel()
	fixture := prepareRoutineGrantFixture(c, ctx)
	fixture.exec(c, ctx,
		"CREATE TABLE "+fixture.schema+".signatures (id bigint PRIMARY KEY)",
		"CREATE FUNCTION "+fixture.schema+".purge(p_id uuid) RETURNS integer LANGUAGE sql SECURITY DEFINER AS $$ SELECT 1 $$",
	)
	c.Assert(fixture.privileges(c, ctx).PublicExecutes, qt.IsTrue)

	desired, _, err := sqlschema.Read([]byte(fixture.schemaFile()), platform.Postgres)
	c.Assert(err, qt.IsNil)
	applied := fixture.apply(c, ctx, &desired)

	c.Assert(applied.GrantsRemoved, qt.HasLen, 4, qt.Commentf("revokes planned: %#v", applied.GrantsRemoved))
	c.Assert(fixture.privileges(c, ctx), qt.DeepEquals, routineGrantPrivileges{
		PublicExecutes: false, RoleExecutes: true, RoleSelects: true, RoleInserts: false, RoleDeletes: false,
	})
}

// routineGrantPrivileges is what the server answers about the fixture's
// privileges.
type routineGrantPrivileges struct {
	PublicExecutes bool
	RoleExecutes   bool
	RoleSelects    bool
	RoleInserts    bool
	RoleDeletes    bool
}

// routineGrantFixture is one schema and one role the connecting user granted
// default table privileges to in that schema.
type routineGrantFixture struct {
	conn   *dbschema.DatabaseConnection
	schema string
	role   string
}

// schemaFile is the schema file under test, in wpmgr's shape.
func (f routineGrantFixture) schemaFile() string {
	return strings.Join([]string{
		"CREATE TABLE " + f.schema + ".signatures (id bigint PRIMARY KEY);",
		"CREATE FUNCTION " + f.schema + ".purge(p_id uuid) RETURNS integer LANGUAGE sql SECURITY DEFINER AS $$ SELECT 1 $$;",
		"REVOKE ALL ON FUNCTION " + f.schema + ".purge(uuid) FROM PUBLIC;",
		"GRANT EXECUTE ON FUNCTION " + f.schema + ".purge(uuid) TO " + f.role + ";",
		"REVOKE INSERT, UPDATE, DELETE ON " + f.schema + ".signatures FROM " + f.role + ";",
	}, "\n")
}

// compare reads the fixture schema and compares desired with it.
func (f routineGrantFixture) compare(c *qt.C, ctx context.Context, desired *schemamodel.Database) *difftypes.SchemaDiff {
	c.Helper()
	read, err := dbschema.ReadSchemaWithSchemasContext(ctx, f.conn, []string{f.schema})
	c.Assert(err, qt.IsNil)
	opts := config.DefaultCompareOptions()
	opts.Dialect = f.conn.Info().Dialect
	return schemadiff.CompareWithOptions(desired, read, opts)
}

// apply compares, plans and executes the plan, and returns the diff it ran.
func (f routineGrantFixture) apply(c *qt.C, ctx context.Context, desired *schemamodel.Database) *difftypes.SchemaDiff {
	c.Helper()
	diff := f.compare(c, ctx, desired)
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, platform.Postgres)
	c.Assert(err, qt.IsNil)
	f.exec(c, ctx, statements...)
	return diff
}

func (f routineGrantFixture) exec(c *qt.C, ctx context.Context, statements ...string) {
	c.Helper()
	for _, statement := range statements {
		_, err := f.conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute: %s", statement))
	}
}

func (f routineGrantFixture) privileges(c *qt.C, ctx context.Context) routineGrantPrivileges {
	c.Helper()
	function := f.schema + ".purge(uuid)"
	table := f.schema + ".signatures"
	var got routineGrantPrivileges
	err := f.conn.QueryRowContext(ctx, `SELECT
			has_function_privilege('public', $1, 'EXECUTE'),
			has_function_privilege($2, $1, 'EXECUTE'),
			has_table_privilege($2, $3, 'SELECT'),
			has_table_privilege($2, $3, 'INSERT'),
			has_table_privilege($2, $3, 'DELETE')`,
		function, f.role, table,
	).Scan(&got.PublicExecutes, &got.RoleExecutes, &got.RoleSelects, &got.RoleInserts, &got.RoleDeletes)
	c.Assert(err, qt.IsNil)
	return got
}

// prepareRoutineGrantFixture creates the schema and the role, and gives the
// role default privileges on the tables the connecting user creates there. A
// role belongs to the cluster, so it is dropped after the schema, and DROP
// OWNED BY clears what is left of its privileges first.
func prepareRoutineGrantFixture(c *qt.C, ctx context.Context) routineGrantFixture {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})

	suffix := time.Now().UnixNano()
	fixture := routineGrantFixture{
		conn:   conn,
		schema: fmt.Sprintf("ptah_routine_grant_%d", suffix),
		role:   fmt.Sprintf("ptah_routine_grant_role_%d", suffix),
	}
	schemaIdent := pgx.Identifier{fixture.schema}.Sanitize()
	roleIdent := pgx.Identifier{fixture.role}.Sanitize()
	c.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for _, statement := range []string{
			"DROP SCHEMA IF EXISTS " + schemaIdent + " CASCADE",
			"DROP OWNED BY " + roleIdent,
			"DROP ROLE IF EXISTS " + roleIdent,
		} {
			_, dropErr := conn.ExecContext(cleanupCtx, statement)
			c.Check(dropErr, qt.IsNil, qt.Commentf("cleanup: %s", statement))
		}
	})

	fixture.exec(c, ctx,
		"CREATE ROLE "+roleIdent+" NOLOGIN",
		"CREATE SCHEMA "+schemaIdent,
		"GRANT USAGE ON SCHEMA "+schemaIdent+" TO "+roleIdent,
		"ALTER DEFAULT PRIVILEGES IN SCHEMA "+schemaIdent+" GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "+roleIdent,
	)
	return fixture
}
