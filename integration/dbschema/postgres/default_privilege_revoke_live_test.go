//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// TestPostgresDefaultPrivilegeRevoke_ASchemaFileTakesBackADefault applies an
// ALTER DEFAULT PRIVILEGES ... REVOKE from a schema file to a schema whose
// role already holds SELECT, INSERT, UPDATE and DELETE on new tables there,
// and reads pg_default_acl back. The statement was read and dropped, so the
// defaults the file revoked stayed (stokaro/ptah#3580). A second comparison
// has to plan nothing, which only the server can confirm: the read reports
// the default ACL one row per privilege.
func TestPostgresDefaultPrivilegeRevoke_ASchemaFileTakesBackADefault(t *testing.T) {
	tests := []struct {
		name       string
		statements func(owner, schema, role string) string
	}{
		{
			name: "a revoke with no grant in the file",
			statements: func(owner, schema, role string) string {
				return "ALTER DEFAULT PRIVILEGES FOR ROLE " + owner + " IN SCHEMA " + schema +
					" REVOKE INSERT, UPDATE, DELETE ON TABLES FROM " + role + ";"
			},
		},
		{
			name: "a grant and a later revoke in the file",
			statements: func(owner, schema, role string) string {
				prefix := "ALTER DEFAULT PRIVILEGES FOR ROLE " + owner + " IN SCHEMA " + schema
				return prefix + " GRANT SELECT, TRUNCATE ON TABLES TO " + role + ";\n" +
					prefix + " REVOKE TRUNCATE ON TABLES FROM " + role + ";"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
			defer cancel()
			fixture := prepareRoutineGrantFixture(c, ctx)
			var owner string
			c.Assert(fixture.conn.QueryRowContext(ctx, "SELECT current_user").Scan(&owner), qt.IsNil)
			desired, _, err := sqlschema.Read([]byte(test.statements(owner, fixture.schema, fixture.role)), platform.Postgres)
			c.Assert(err, qt.IsNil)

			fixture.apply(c, ctx, &desired)
			settled := fixture.compare(c, ctx, &desired)

			c.Assert(fixture.defaultTablePrivileges(c, ctx), qt.Equals, "SELECT")
			c.Assert(settled.DefaultPrivilegesAdded, qt.HasLen, 0, qt.Commentf("added: %v", settled.DefaultPrivilegesAdded))
			c.Assert(settled.DefaultPrivilegesRemoved, qt.HasLen, 0, qt.Commentf("removed: %v", settled.DefaultPrivilegesRemoved))
		})
	}
}

// defaultTablePrivileges is what the fixture role receives by default on
// tables in the fixture schema, as pg_default_acl records it.
func (f routineGrantFixture) defaultTablePrivileges(c *qt.C, ctx context.Context) string {
	c.Helper()
	var privileges string
	err := f.conn.QueryRowContext(ctx, `
		SELECT COALESCE(string_agg(acl.privilege_type, ',' ORDER BY acl.privilege_type), '')
		FROM pg_default_acl d
		JOIN pg_namespace n ON n.oid = d.defaclnamespace
		CROSS JOIN LATERAL aclexplode(d.defaclacl) acl
		JOIN pg_roles grantee ON grantee.oid = acl.grantee
		WHERE n.nspname = $1 AND d.defaclobjtype = 'r' AND grantee.rolname = $2`,
		f.schema, f.role,
	).Scan(&privileges)
	c.Assert(err, qt.IsNil)
	return privileges
}
