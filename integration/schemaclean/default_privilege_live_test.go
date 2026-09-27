//go:build integration

package schemaclean_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/dbschema"
	"ptah.run/internal/schemaclean"
)

// TestDefaultPrivilegePlanRevokesWhatTheCatalogHolds_PostgresLive plans a
// cleanup of a schema holding default privileges, runs exactly that plan, and
// reads pg_default_acl back.
//
// The plan's revokes come from the read the writer's cleanup uses, so the plan
// names every grantee pg_default_acl holds, spells each revoke as the full
// cleanup runs it, and a narrowed plan that runs those statements leaves no
// default behind. A second reader of the relation spelled the revoke its own
// way and exploded the ACL with aclexplode (stokaro/ptah#3832). The grantee
// with a dash and capitals is the name the two spellings quote differently.
// The default in keepme is the scope control: the cleanup is scoped to the
// connection's schema, so that default is neither planned nor revoked.
func TestDefaultPrivilegePlanRevokesWhatTheCatalogHolds_PostgresLive(t *testing.T) {
	c := qt.New(t)
	ctx := c.Context()
	owner, reader := newDefaultPrivilegeRoles(c, ctx)
	conn := newPostgresCleanupLiveConnection(c, ctx)
	ownerIdent := pgx.Identifier{owner}.Sanitize()
	readerIdent := pgx.Identifier{reader}.Sanitize()
	for _, statement := range []string{
		"ALTER DEFAULT PRIVILEGES FOR ROLE " + ownerIdent + " IN SCHEMA public GRANT SELECT ON TABLES TO " + readerIdent,
		"ALTER DEFAULT PRIVILEGES FOR ROLE " + ownerIdent + " IN SCHEMA public GRANT USAGE ON SEQUENCES TO PUBLIC",
		"CREATE SCHEMA keepme",
		"ALTER DEFAULT PRIVILEGES FOR ROLE " + ownerIdent + " IN SCHEMA keepme GRANT SELECT ON TABLES TO " + readerIdent,
	} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	held := defaultPrivilegeGrantees(c, ctx, conn, "public")
	kept := defaultPrivilegeGrantees(c, ctx, conn, "keepme")

	plan, err := schemaclean.Inspect(ctx, conn)
	c.Assert(err, qt.IsNil)

	c.Assert(plannedDefaultPrivileges(plan), qt.DeepEquals, held)
	c.Assert(plannedDefaultPrivilegeSelectors(plan), qt.DeepEquals, []string{
		owner + ":S:PUBLIC",
		owner + ":r:" + reader,
	})
	c.Assert(plannedDefaultPrivilegeCommands(plan), qt.DeepEquals, []string{
		`ALTER DEFAULT PRIVILEGES FOR ROLE "` + owner + `" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON SEQUENCES FROM PUBLIC`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "` + owner + `" IN SCHEMA "public" REVOKE ALL PRIVILEGES ON TABLES FROM "` + reader + `"`,
	})

	c.Assert(schemaclean.ApplyPlan(ctx, conn, plan), qt.IsNil)
	c.Assert(defaultPrivilegeGrantees(c, ctx, conn, "public"), qt.HasLen, 0)
	c.Assert(defaultPrivilegeGrantees(c, ctx, conn, "keepme"), qt.DeepEquals, kept)
	c.Assert(kept, qt.HasLen, 1)
}

// newDefaultPrivilegeRoles creates the grantor and a grantee whose name needs
// quoting, both unique to the run because roles are cluster-wide.
//
// Their removal is registered before the throwaway database is created, so it
// runs after that database is dropped: a role a default privilege still names
// cannot be dropped while the database holding the default exists.
func newDefaultPrivilegeRoles(c *qt.C, ctx context.Context) (owner, reader string) {
	c.Helper()
	admin, err := sql.Open("pgx", requirePostgresCleanupLiveURL(c))
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(ctx), qt.IsNil)

	stamp := time.Now().UnixNano()
	owner = fmt.Sprintf("ptah_defacl_owner_%d", stamp)
	reader = fmt.Sprintf("Ptah-DefACL-Reader-%d", stamp)
	c.Cleanup(func() {
		for _, role := range []string{reader, owner} {
			_, dropErr := admin.ExecContext(context.WithoutCancel(ctx), "DROP ROLE IF EXISTS "+pgx.Identifier{role}.Sanitize())
			c.Check(dropErr, qt.IsNil, qt.Commentf("role: %s", role))
		}
		c.Check(admin.Close(), qt.IsNil)
	})
	for _, role := range []string{owner, reader} {
		_, err := admin.ExecContext(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize())
		c.Assert(err, qt.IsNil, qt.Commentf("role: %s", role))
	}
	return owner, reader
}

// defaultPrivilegeGrantees lists what pg_default_acl holds for schema, one
// entry per grantor, class and grantee, spelled as a plan names a default
// privilege. aclexplode is the oracle here because this is PostgreSQL, where
// it answers.
func defaultPrivilegeGrantees(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schema string,
) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, `
		SELECT DISTINCT
			pg_get_userbyid(d.defaclrole) || '/' || d.defaclobjtype::text || '/' ||
			CASE acl.grantee WHEN 0 THEN 'PUBLIC' ELSE pg_get_userbyid(acl.grantee) END
		FROM pg_default_acl d
		JOIN pg_namespace n ON n.oid = d.defaclnamespace
		CROSS JOIN LATERAL aclexplode(d.defaclacl) acl
		WHERE n.nspname = $1`, schema)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()

	var held []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		held = append(held, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	slices.Sort(held)
	return held
}

// plannedDefaultPrivileges lists the default privileges a plan names, sorted.
func plannedDefaultPrivileges(plan schemaclean.Plan) []string {
	var names []string
	for _, object := range plan.Objects {
		if object.Type == schemaclean.ObjectTypeDefaultPrivilege {
			names = append(names, object.Name)
		}
	}
	slices.Sort(names)
	return names
}

// plannedDefaultPrivilegeSelectors lists the names --include and --exclude
// match a plan's default privileges by, sorted.
func plannedDefaultPrivilegeSelectors(plan schemaclean.Plan) []string {
	var selectors []string
	for _, object := range plan.Objects {
		if object.Type == schemaclean.ObjectTypeDefaultPrivilege {
			selectors = append(selectors, object.SelectorName)
		}
	}
	slices.Sort(selectors)
	return selectors
}

// plannedDefaultPrivilegeCommands lists the statements a plan runs for its
// default privileges, sorted.
func plannedDefaultPrivilegeCommands(plan schemaclean.Plan) []string {
	var commands []string
	for _, change := range plan.Changes {
		if change.Type == schemaclean.ObjectTypeDefaultPrivilege {
			commands = append(commands, change.Cmd)
		}
	}
	slices.Sort(commands)
	return commands
}
